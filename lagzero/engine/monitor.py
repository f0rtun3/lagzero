from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

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
    correlation_signals: list[CorrelationSignal] = field(default_factory=list)

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
        time_lag_sec = compute_time_lag(offset_lag=offset_lag, processing_rate=processing_rate)
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
                lag_velocity=lag_velocity,
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
            time_lag_sec=time_lag_sec,
            lag_velocity=lag_velocity,
            anomaly=anomaly.name,
            severity=anomaly.severity,
            confidence=anomaly.confidence,
            correlations=correlations,
            diagnostics={
                "raw_processing_rate": rate_sample.messages_per_second,
                "rate_window_size": self.settings.rate_window_size,
                "recent_rates": recent_rates[-self.settings.rate_window_size :],
                "state_reset": rate_sample.state_reset,
                "no_offset_movement": no_offset_movement,
                "consecutive_zero_rate_intervals": consecutive_zero_rate_intervals,
                "consecutive_no_movement_intervals": consecutive_no_movement_intervals,
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

        max_lag_velocity = max(
            (event.lag_velocity for event in partition_events if event.lag_velocity is not None),
            default=None,
        )

        return IncidentEvent(
            timestamp=max(event.timestamp for event in partition_events),
            consumer_group=self.settings.consumer_group,
            scope="consumer_group",
            topic=None,
            partition=None,
            offset_lag=total_offset_lag,
            processing_rate=total_processing_rate,
            time_lag_sec=max_time_lag,
            lag_velocity=max_lag_velocity,
            anomaly=worst_event.anomaly,
            severity=worst_event.severity,
            confidence=min(event.confidence for event in partition_events),
            correlations=sorted(
                {correlation for event in partition_events for correlation in event.correlations}
            ),
            diagnostics={
                "partition_count": len(partition_events),
                "affected_partitions": [
                    {
                        "topic": event.topic,
                        "partition": event.partition,
                        "offset_lag": event.offset_lag,
                        "time_lag_sec": event.time_lag_sec,
                        "lag_velocity": event.lag_velocity,
                        "anomaly": event.anomaly,
                        "severity": event.severity,
                    }
                    for event in partition_events
                ],
            },
        )

    def _collect_correlations(self, observed_at: float) -> list[str]:
        active_signals = []
        valid_after = observed_at - self.settings.correlation_window_sec

        kept_signals = []
        for signal in self.correlation_signals:
            if signal.created_at >= valid_after:
                kept_signals.append(signal)
                active_signals.append(signal.event_type)

        self.correlation_signals = kept_signals
        return sorted(set(active_signals))

    @staticmethod
    def _event_rank(event: IncidentEvent) -> tuple[int, float]:
        severity_rank = {"critical": 3, "warning": 2, "info": 1}.get(event.severity, 0)
        return (severity_rank, event.time_lag_sec or 0.0)
