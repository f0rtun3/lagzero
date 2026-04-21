from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from dataclasses import replace
from statistics import median

from lagzero.config.settings import Settings
from lagzero.events.emitter import EventEmitter
from lagzero.events.schema import IncidentEvent
from lagzero.kafka.offsets import OffsetFetcher, PartitionOffsets
from lagzero.monitoring.anomaly import detect_anomaly
from lagzero.monitoring.lag_calculator import compute_lag
from lagzero.monitoring.lag_velocity import compute_lag_velocity
from lagzero.monitoring.rate_calculator import compute_rate, rate_variance_high, smooth_rate
from lagzero.monitoring.time_lag import compute_time_lag
from lagzero.state.store import InMemoryStateStore, PartitionState

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CorrelationSignal:
    event_type: str
    created_at: float
    attributes: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class MonitorEngine:
    settings: Settings
    offset_fetcher: OffsetFetcher
    event_emitter: EventEmitter
    state_store: InMemoryStateStore = field(default_factory=InMemoryStateStore)
    correlation_signals: deque[CorrelationSignal] = field(default_factory=deque)

    def run_forever(self) -> None:
        logger.info(
            "Starting LagZero monitor for group=%s topics=%s interval=%.1fs",
            self.settings.consumer_group,
            ",".join(self.settings.topics),
            self.settings.poll_interval_sec,
        )

        while True:
            self.run_once()
            time.sleep(self.settings.poll_interval_sec)

    def run_once(self) -> list[IncidentEvent]:
        snapshots = self.offset_fetcher.fetch(self.settings.topics)
        partition_events = [self._build_partition_event(snapshot) for snapshot in snapshots]
        partition_events = self._apply_partition_skew(partition_events)
        events = []

        if partition_events:
            events.append(self._build_group_event(partition_events))
        events.extend(partition_events)

        for event in events:
            self.event_emitter.emit(event)

        return events

    def register_correlation_signal(self, kind: str, created_at: float | None = None) -> None:
        self.add_external_event(event_type=kind, timestamp=created_at)

    def add_external_event(
        self,
        event_type: str,
        timestamp: float | None = None,
        attributes: dict[str, object] | None = None,
    ) -> None:
        self.correlation_signals.append(
            CorrelationSignal(
                event_type=event_type,
                created_at=timestamp or time.time(),
                attributes=attributes or {},
            )
        )
        self._prune_correlation_signals(timestamp or time.time())

    def _build_partition_event(self, snapshot: PartitionOffsets) -> IncidentEvent:
        key = (snapshot.topic, snapshot.partition)
        previous_state = self.state_store.get(key)

        offset_lag = compute_lag(snapshot.latest_offset, snapshot.committed_offset)
        rate_sample = compute_rate(
            previous_committed_offset=(
                previous_state.committed_offset if previous_state is not None else None
            ),
            current_committed_offset=snapshot.committed_offset,
            previous_timestamp=previous_state.observed_at if previous_state is not None else None,
            current_timestamp=snapshot.observed_at,
        )
        recent_rates = [] if previous_state is None else list(previous_state.recent_rates)
        if rate_sample.messages_per_second is not None:
            recent_rates.append(rate_sample.messages_per_second)
        if rate_sample.state_reset:
            recent_rates = []

        processing_rate = smooth_rate(recent_rates, self.settings.rate_window_size)
        producer_rate_sample = compute_rate(
            previous_committed_offset=previous_state.latest_offset if previous_state is not None else None,
            current_committed_offset=snapshot.latest_offset,
            previous_timestamp=previous_state.observed_at if previous_state is not None else None,
            current_timestamp=snapshot.observed_at,
        )
        recent_producer_rates = (
            [] if previous_state is None else list(previous_state.recent_producer_rates)
        )
        if producer_rate_sample.messages_per_second is not None:
            recent_producer_rates.append(producer_rate_sample.messages_per_second)
        if producer_rate_sample.state_reset:
            recent_producer_rates = []

        producer_rate = smooth_rate(recent_producer_rates, self.settings.rate_window_size)
        backlog_growth_rate = None
        if producer_rate is not None and processing_rate is not None:
            backlog_growth_rate = producer_rate - processing_rate
        time_lag_estimate = compute_time_lag(
            offset_lag=offset_lag,
            processing_rate=processing_rate,
            observed_at=snapshot.observed_at,
            backlog_head_timestamp=snapshot.backlog_head_timestamp,
            timestamp_sampling_state=snapshot.timestamp_sampling_state,
            timestamp_type=snapshot.timestamp_type,
            lag_divergence_threshold_sec=self.settings.lag_divergence_threshold_sec,
        )
        time_lag_sec = time_lag_estimate.seconds
        lag_velocity = compute_lag_velocity(
            previous_lag=previous_state.offset_lag if previous_state is not None else None,
            current_lag=offset_lag,
            elapsed_seconds=rate_sample.elapsed_seconds,
        )
        no_offset_movement = (
            previous_state is not None
            and snapshot.committed_offset == previous_state.committed_offset
            and snapshot.latest_offset == previous_state.latest_offset
        )
        lag_decreasing = previous_state is not None and offset_lag < previous_state.offset_lag
        cold_start = snapshot.committed_offset == 0 or (
            previous_state is not None and previous_state.committed_offset == 0
        )
        catching_up = (
            cold_start
            and processing_rate is not None
            and processing_rate > 0
            and lag_decreasing
        )

        consecutive_zero_rate_intervals = 0
        if processing_rate == 0 and offset_lag > 0:
            consecutive_zero_rate_intervals = (
                previous_state.consecutive_zero_rate_intervals + 1 if previous_state else 1
            )

        consecutive_no_movement_intervals = 0
        if no_offset_movement and offset_lag > 0:
            consecutive_no_movement_intervals = (
                previous_state.consecutive_no_movement_intervals + 1 if previous_state else 1
            )

        anomaly = detect_anomaly(
            current_lag=offset_lag,
            previous_lag=previous_state.offset_lag if previous_state else None,
            processing_rate=processing_rate,
            stalled_intervals=self.settings.stalled_intervals,
            idle_intervals=self.settings.idle_intervals,
            consecutive_zero_rate_intervals=consecutive_zero_rate_intervals,
            consecutive_no_movement_intervals=consecutive_no_movement_intervals,
            lag_spike_multiplier=self.settings.lag_spike_multiplier,
            rate_variance_high=rate_variance_high(recent_rates, self.settings.rate_window_size),
            lag_velocity=lag_velocity,
            no_offset_movement=no_offset_movement,
            state_reset=rate_sample.state_reset,
            lag_divergence_sec=time_lag_estimate.lag_divergence_sec,
            lag_divergence_threshold_sec=self.settings.lag_divergence_threshold_sec,
            time_lag_source=time_lag_estimate.source,
            timestamp_type=time_lag_estimate.timestamp_type,
            catching_up=catching_up,
            producer_rate=producer_rate,
            backlog_growth_rate=backlog_growth_rate,
        )

        correlations = self._collect_correlations(snapshot.observed_at)

        self.state_store.set(
            key,
            PartitionState(
                committed_offset=snapshot.committed_offset,
                latest_offset=snapshot.latest_offset,
                observed_at=snapshot.observed_at,
                offset_lag=offset_lag,
                consecutive_zero_rate_intervals=consecutive_zero_rate_intervals,
                consecutive_no_movement_intervals=consecutive_no_movement_intervals,
                recent_rates=recent_rates[-self.settings.rate_window_size :],
                recent_producer_rates=recent_producer_rates[-self.settings.rate_window_size :],
                lag_velocity=lag_velocity,
                last_time_lag_sec=time_lag_sec,
            ),
        )

        return IncidentEvent(
            timestamp=snapshot.observed_at,
            consumer_group=self.settings.consumer_group,
            scope="partition",
            topic=snapshot.topic,
            partition=snapshot.partition,
            offset_lag=offset_lag,
            processing_rate=processing_rate,
            producer_rate=producer_rate,
            backlog_growth_rate=backlog_growth_rate,
            time_lag_sec=time_lag_sec,
            time_lag_source=time_lag_estimate.source,
            timestamp_type=time_lag_estimate.timestamp_type,
            backlog_head_timestamp=snapshot.backlog_head_timestamp,
            latest_message_timestamp=snapshot.latest_message_timestamp,
            lag_divergence_sec=time_lag_estimate.lag_divergence_sec,
            lag_velocity=lag_velocity,
            anomaly=anomaly.name,
            severity=anomaly.severity,
            service_health=None,
            confidence=anomaly.confidence,
            correlations=correlations,
            diagnostics={
                "raw_processing_rate": rate_sample.messages_per_second,
                "raw_producer_rate": producer_rate_sample.messages_per_second,
                "rate_window_size": self.settings.rate_window_size,
                "recent_rates": recent_rates[-self.settings.rate_window_size :],
                "recent_producer_rates": recent_producer_rates[-self.settings.rate_window_size :],
                "state_reset": rate_sample.state_reset,
                "no_offset_movement": no_offset_movement,
                "consecutive_zero_rate_intervals": consecutive_zero_rate_intervals,
                "consecutive_no_movement_intervals": consecutive_no_movement_intervals,
                "offset_time_lag_sec": time_lag_estimate.offset_based_seconds,
                "timestamp_time_lag_sec": time_lag_estimate.timestamp_based_seconds,
                "timestamp_sampling_state": snapshot.timestamp_sampling_state,
                "timestamp_sampled_at": snapshot.timestamp_sampled_at,
                "cold_start": cold_start,
                "catching_up": catching_up,
                "lag_decreasing": lag_decreasing,
                "producer_rate": producer_rate,
                "backlog_growth_rate": backlog_growth_rate,
                "lag_divergence_threshold_sec": self.settings.lag_divergence_threshold_sec,
                "backlog_head_timestamp": snapshot.backlog_head_timestamp,
                "latest_message_timestamp": snapshot.latest_message_timestamp,
            },
        )

    def _build_group_event(self, partition_events: list[IncidentEvent]) -> IncidentEvent:
        worst_event = max(partition_events, key=self._event_rank)
        max_time_lag = max(
            (event.time_lag_sec for event in partition_events if event.time_lag_sec is not None),
            default=None,
        )
        total_offset_lag = sum(event.offset_lag for event in partition_events)
        total_processing_rate = sum(
            event.processing_rate for event in partition_events if event.processing_rate is not None
        )
        if total_processing_rate == 0:
            total_processing_rate = None
        total_producer_rate = sum(
            event.producer_rate for event in partition_events if event.producer_rate is not None
        )
        if total_producer_rate == 0:
            total_producer_rate = None
        backlog_growth_rate = None
        if total_producer_rate is not None and total_processing_rate is not None:
            backlog_growth_rate = total_producer_rate - total_processing_rate

        max_lag_velocity = max(
            (event.lag_velocity for event in partition_events if event.lag_velocity is not None),
            default=None,
        )
        service_health = self._compute_service_health(partition_events)

        return IncidentEvent(
            timestamp=max(event.timestamp for event in partition_events),
            consumer_group=self.settings.consumer_group,
            scope="consumer_group",
            topic=None,
            partition=None,
            offset_lag=total_offset_lag,
            processing_rate=total_processing_rate,
            producer_rate=total_producer_rate,
            backlog_growth_rate=backlog_growth_rate,
            time_lag_sec=max_time_lag,
            time_lag_source=worst_event.time_lag_source,
            timestamp_type=worst_event.timestamp_type,
            backlog_head_timestamp=worst_event.backlog_head_timestamp,
            latest_message_timestamp=worst_event.latest_message_timestamp,
            lag_divergence_sec=max(
                (
                    event.lag_divergence_sec
                    for event in partition_events
                    if event.lag_divergence_sec is not None
                ),
                default=None,
            ),
            lag_velocity=max_lag_velocity,
            anomaly=worst_event.anomaly,
            severity=worst_event.severity,
            service_health=service_health,
            confidence=min(event.confidence for event in partition_events),
            correlations=sorted(
                {correlation for event in partition_events for correlation in event.correlations}
            ),
            diagnostics={
                "group_time_lag_sec": max_time_lag,
                "group_anomaly": worst_event.anomaly,
                "group_severity": worst_event.severity,
                "service_health": service_health,
                "partition_count": len(partition_events),
                "producer_rate": total_producer_rate,
                "processing_rate": total_processing_rate,
                "backlog_growth_rate": backlog_growth_rate,
                "affected_partitions": [
                    {
                        "topic": event.topic,
                        "partition": event.partition,
                        "offset_lag": event.offset_lag,
                        "producer_rate": event.producer_rate,
                        "processing_rate": event.processing_rate,
                        "backlog_growth_rate": event.backlog_growth_rate,
                        "time_lag_sec": event.time_lag_sec,
                        "time_lag_source": event.time_lag_source,
                        "timestamp_type": event.timestamp_type,
                        "lag_divergence_sec": event.lag_divergence_sec,
                        "lag_velocity": event.lag_velocity,
                        "anomaly": event.anomaly,
                        "severity": event.severity,
                    }
                    for event in partition_events
                ],
            },
        )

    def _collect_correlations(self, observed_at: float) -> list[str]:
        self._prune_correlation_signals(observed_at)
        active_signals = []
        for signal in self.correlation_signals:
            active_signals.append(signal.event_type)
        return sorted(set(active_signals))

    def _prune_correlation_signals(self, observed_at: float) -> None:
        valid_after = observed_at - self.settings.correlation_window_sec
        while self.correlation_signals and self.correlation_signals[0].created_at < valid_after:
            self.correlation_signals.popleft()

    def _apply_partition_skew(self, partition_events: list[IncidentEvent]) -> list[IncidentEvent]:
        if len(partition_events) < 2:
            return partition_events

        lag_median = median(event.offset_lag for event in partition_events)
        if lag_median <= 0:
            return partition_events

        result: list[IncidentEvent] = []
        for event in partition_events:
            if event.offset_lag >= lag_median * 3 and event.offset_lag > 0:
                result.append(
                    replace(
                        event,
                        anomaly="partition_skew",
                        severity="warning",
                        diagnostics={
                            **event.diagnostics,
                            "partition_skew": True,
                            "median_partition_lag": lag_median,
                        },
                    )
                )
            else:
                result.append(event)
        return result

    def _compute_service_health(self, partition_events: list[IncidentEvent]) -> str:
        worst_event = max(partition_events, key=self._event_rank)
        if worst_event.anomaly == "catching_up":
            return "recovering"
        if worst_event.severity == "critical":
            return "failing"
        if worst_event.severity == "warning":
            return "degraded"
        return "healthy"

    @staticmethod
    def _event_rank(event: IncidentEvent) -> tuple[int, float]:
        severity_rank = {"critical": 3, "warning": 2, "info": 1}.get(event.severity, 0)
        return (severity_rank, event.time_lag_sec or 0.0)
