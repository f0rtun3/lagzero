from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from dataclasses import replace
from statistics import median
import uuid

from lagzero.ai.client import DisabledLLMClient
from lagzero.ai.explainer import IncidentExplainer
from lagzero.correlation.engine import CorrelationEngine
from lagzero.correlation.schema import ExternalEvent
from lagzero.correlation.store import CorrelationEventStore
from lagzero.config.settings import Settings
from lagzero.events.emitter import EventEmitter
from lagzero.events.schema import IncidentEvent
from lagzero.kafka.offsets import OffsetFetcher, PartitionOffsets
from lagzero.monitoring.anomaly import (
    ANOMALY_PRIORITY,
    detect_anomaly,
    health_for_anomaly,
    severity_for_anomaly,
)
from lagzero.monitoring.lag_calculator import compute_lag
from lagzero.monitoring.lag_velocity import compute_lag_velocity
from lagzero.monitoring.rate_calculator import compute_rate, rate_variance_high, smooth_rate
from lagzero.monitoring.time_lag import compute_time_lag
from lagzero.state.store import DecisionState, InMemoryStateStore, PartitionState

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MonitorEngine:
    settings: Settings
    offset_fetcher: OffsetFetcher
    event_emitter: EventEmitter
    state_store: InMemoryStateStore = field(default_factory=InMemoryStateStore)
    correlation_store: CorrelationEventStore | None = None
    correlation_engine: CorrelationEngine | None = None
    incident_explainer: IncidentExplainer | None = None
    group_decision_state: DecisionState = field(default_factory=DecisionState)

    def __post_init__(self) -> None:
        if self.correlation_store is None:
            self.correlation_store = CorrelationEventStore(
                retention_sec=self.settings.correlation_retention_sec
            )
        if self.correlation_engine is None:
            self.correlation_engine = CorrelationEngine(
                self.correlation_store,
                deploy_window_sec=self.settings.deploy_window_sec,
                error_window_sec=self.settings.error_window_sec,
                rebalance_window_sec=self.settings.rebalance_window_sec,
                infra_window_sec=self.settings.infra_window_sec,
                max_correlations=self.settings.max_correlations,
            )
        if self.incident_explainer is None:
            self.incident_explainer = IncidentExplainer(
                llm_client=DisabledLLMClient(),
                enabled=self.settings.ai_enabled,
                cache_ttl_sec=self.settings.ai_cache_ttl_sec,
            )

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
        events = [self.correlation_engine.enrich(event) for event in events]
        events = [self.incident_explainer.explain(event) for event in events]

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
        *,
        source: str = "app",
        service: str | None = None,
        consumer_group: str | None = None,
        topic: str | None = None,
        partition: int | None = None,
        severity: str | None = None,
    ) -> None:
        event = ExternalEvent(
            event_id=str(uuid.uuid4()),
            timestamp=timestamp or time.time(),
            event_type=event_type,
            source=source,
            service=service,
            consumer_group=consumer_group or self.settings.consumer_group,
            topic=topic,
            partition=partition,
            severity=severity,
            metadata=attributes or {},
        )
        self.correlation_store.add(event)

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
        consumer_efficiency = None
        if (
            producer_rate is not None
            and producer_rate > 0
            and processing_rate is not None
        ):
            consumer_efficiency = processing_rate / producer_rate
        backlog_growth_rate = None
        if producer_rate is not None and processing_rate is not None:
            backlog_growth_rate = producer_rate - processing_rate
        backlog_growth_rate_improving = (
            previous_state is not None
            and backlog_growth_rate is not None
            and previous_state.last_backlog_growth_rate is not None
            and backlog_growth_rate < previous_state.last_backlog_growth_rate
        )
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
        spike_signal_active = (
            previous_state is not None
            and previous_state.offset_lag > 0
            and (offset_lag - previous_state.offset_lag) >= self.settings.min_lag_spike_delta
            and (
                offset_lag
                >= int(previous_state.offset_lag * self.settings.lag_spike_multiplier)
                or (
                    lag_velocity is not None
                    and lag_velocity >= float(self.settings.min_lag_spike_delta)
                )
            )
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
        recent_lag_spike_active = (
            previous_state is not None
            and previous_state.last_lag_spike_at is not None
            and (snapshot.observed_at - previous_state.last_lag_spike_at)
            <= self.settings.burst_grace_sec
        )
        catching_up = (
            processing_rate is not None
            and processing_rate > 0
            and lag_decreasing
            and (
                cold_start
                or (backlog_growth_rate is not None and backlog_growth_rate < 0)
                or (
                    consumer_efficiency is not None
                    and consumer_efficiency >= self.settings.catching_up_efficiency_threshold
                )
            )
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

        operator_offset_reset = (
            previous_state is not None
            and previous_state.offset_lag >= self.settings.min_incident_offset_lag
            and previous_state.offset_lag > 0
            and offset_lag == 0
            and rate_sample.messages_per_second is not None
            and rate_sample.processed_messages >= max(10, int(previous_state.offset_lag * 0.8))
            and (
                previous_state.consecutive_no_movement_intervals >= 1
                or previous_state.consecutive_zero_rate_intervals >= 1
            )

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
            state_reset=(rate_sample.state_reset or operator_offset_reset),
            lag_divergence_sec=time_lag_estimate.lag_divergence_sec,
            lag_divergence_threshold_sec=self.settings.lag_divergence_threshold_sec,
            time_lag_source=time_lag_estimate.source,
            timestamp_type=time_lag_estimate.timestamp_type,
            catching_up=catching_up,
            time_lag_sec=time_lag_sec,
            producer_rate=producer_rate,
            backlog_growth_rate=backlog_growth_rate,
            consumer_efficiency=consumer_efficiency,
            lag_decreasing=lag_decreasing,
            backlog_growth_rate_improving=backlog_growth_rate_improving,
            recent_lag_spike_active=recent_lag_spike_active,
            min_incident_offset_lag=self.settings.min_incident_offset_lag,
            min_incident_time_lag_sec=self.settings.min_incident_time_lag_sec,
            min_stalled_offset_lag=self.settings.min_stalled_offset_lag,
            min_stalled_time_lag_sec=self.settings.min_stalled_time_lag_sec,
            min_lag_spike_delta=self.settings.min_lag_spike_delta,
            slow_consumer_efficiency_threshold=self.settings.slow_consumer_efficiency_threshold,
        )
        final_anomaly, updated_decision_state, transition_pending = self._apply_hysteresis(
            decision_state=(
                previous_state.decision_state if previous_state is not None else DecisionState()
            ),
            observed_anomaly=anomaly.name,
        )
        final_severity = severity_for_anomaly(final_anomaly)
        final_service_health = health_for_anomaly(final_anomaly)
        final_confidence = anomaly.confidence if final_anomaly == anomaly.name else min(anomaly.confidence, 0.5)

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
                last_backlog_growth_rate=backlog_growth_rate,
                last_lag_spike_at=(
                    snapshot.observed_at
                    if spike_signal_active or anomaly.name == "lag_spike"
                    else (
                        previous_state.last_lag_spike_at
                        if previous_state is not None
                        else None
                    )
                ),
                decision_state=updated_decision_state,
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
            consumer_efficiency=consumer_efficiency,
            backlog_growth_rate=backlog_growth_rate,
            time_lag_sec=time_lag_sec,
            time_lag_source=time_lag_estimate.source,
            timestamp_type=time_lag_estimate.timestamp_type,
            backlog_head_timestamp=snapshot.backlog_head_timestamp,
            latest_message_timestamp=snapshot.latest_message_timestamp,
            lag_divergence_sec=time_lag_estimate.lag_divergence_sec,
            lag_velocity=lag_velocity,
            anomaly=final_anomaly,
            severity=final_severity,
            service_health=final_service_health,
            confidence=final_confidence,
            diagnostics={
                "observed_anomaly": anomaly.name,
                "observed_severity": anomaly.severity,
                "anomaly_priority": ANOMALY_PRIORITY.get(final_anomaly, 0),
                "transition_pending": transition_pending,
                "pending_anomaly": updated_decision_state.pending_anomaly,
                "pending_count": updated_decision_state.pending_count,
                "required_confirmations": self.settings.state_transition_confirmations,
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
                "spike_signal_active": spike_signal_active,
                "recent_lag_spike_active": recent_lag_spike_active,
                "backlog_growth_rate_improving": backlog_growth_rate_improving,
                "catching_up": catching_up,
                "lag_decreasing": lag_decreasing,
                "producer_rate": producer_rate,
                "consumer_efficiency": consumer_efficiency,
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
        observed_group_anomaly = worst_event.anomaly or "normal"

        # If backlog is overwhelmingly concentrated in a small minority of partitions,
        # surface that distribution truth at the group level.
        skew_metrics = self._detect_partition_skew(partition_events)
        if skew_metrics is not None:
            observed_priority = ANOMALY_PRIORITY.get(observed_group_anomaly, 0)
            if observed_priority < ANOMALY_PRIORITY.get("partition_skew", 0):
                observed_group_anomaly = "partition_skew"
        final_group_anomaly, updated_group_decision_state, transition_pending = self._apply_hysteresis(
            decision_state=self.group_decision_state,
            observed_anomaly=observed_group_anomaly,
        )
        self.group_decision_state = updated_group_decision_state
        final_group_severity = severity_for_anomaly(final_group_anomaly)
        service_health = health_for_anomaly(final_group_anomaly)
        final_group_confidence = (
            min(event.confidence for event in partition_events)
            if final_group_anomaly == observed_group_anomaly
            else min(min(event.confidence for event in partition_events), 0.5)
        )

        return IncidentEvent(
            timestamp=max(event.timestamp for event in partition_events),
            consumer_group=self.settings.consumer_group,
            scope="consumer_group",
            topic=None,
            partition=None,
            offset_lag=total_offset_lag,
            processing_rate=total_processing_rate,
            producer_rate=total_producer_rate,
            consumer_efficiency=(
                (total_processing_rate / total_producer_rate)
                if total_processing_rate is not None
                and total_producer_rate is not None
                and total_producer_rate > 0
                else None
            ),
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
            anomaly=final_group_anomaly,
            severity=final_group_severity,
            service_health=service_health,
            confidence=final_group_confidence,
            correlations=sorted(
                {correlation for event in partition_events for correlation in event.correlations}
            ),
            diagnostics={
                "group_time_lag_sec": max_time_lag,
                "group_anomaly": final_group_anomaly,
                "observed_group_anomaly": observed_group_anomaly,
                "group_severity": final_group_severity,
                "service_health": service_health,
                "anomaly_priority": ANOMALY_PRIORITY.get(final_group_anomaly, 0),
                "transition_pending": transition_pending,
                "pending_anomaly": updated_group_decision_state.pending_anomaly,
                "pending_count": updated_group_decision_state.pending_count,
                "required_confirmations": self.settings.state_transition_confirmations,
                "partition_count": len(partition_events),
                "producer_rate": total_producer_rate,
                "processing_rate": total_processing_rate,
                "consumer_efficiency": (
                    (total_processing_rate / total_producer_rate)
                    if total_processing_rate is not None
                    and total_producer_rate is not None
                    and total_producer_rate > 0
                    else None
                ),
                "backlog_growth_rate": backlog_growth_rate,
                "affected_partitions": [
                    {
                        "topic": event.topic,
                        "partition": event.partition,
                        "offset_lag": event.offset_lag,
                        "producer_rate": event.producer_rate,
                        "processing_rate": event.processing_rate,
                        "consumer_efficiency": event.consumer_efficiency,
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
                **({"partition_skew": True, **skew_metrics} if skew_metrics is not None else {}),
            },
        )

    def _detect_partition_skew(self, partition_events: list[IncidentEvent]) -> dict[str, object] | None:
        if len(partition_events) < 2:
            return None

        lags = [max(int(event.offset_lag or 0), 0) for event in partition_events]
        total_lag = sum(lags)
        if total_lag < self.settings.partition_skew_min_total_lag:
            return None

        nonzero_count = sum(1 for lag in lags if lag > 0)
        if nonzero_count == 0 or nonzero_count > self.settings.partition_skew_max_hot_partitions:
            return None

        max_lag = max(lags)
        max_share = (max_lag / total_lag) if total_lag > 0 else 0.0
        if max_share < float(self.settings.partition_skew_min_share):
            return None

        hotspots = sorted({event.partition for event in partition_events if (event.offset_lag or 0) == max_lag})
        return {
            "total_offset_lag": total_lag,
            "max_partition_lag": max_lag,
            "nonzero_lag_partitions": nonzero_count,
            "max_lag_share": max_share,
            "hot_partitions": hotspots,
        }

    def _apply_partition_skew(self, partition_events: list[IncidentEvent]) -> list[IncidentEvent]:
        if len(partition_events) < 2:
            return partition_events

        lag_median = median(event.offset_lag for event in partition_events)
        if lag_median <= 0:
            return partition_events

        result: list[IncidentEvent] = []
        for event in partition_events:
            if event.offset_lag >= lag_median * 3 and event.offset_lag > 0:
                if ANOMALY_PRIORITY.get(event.anomaly or "normal", 0) >= ANOMALY_PRIORITY["partition_skew"]:
                    result.append(
                        replace(
                            event,
                            diagnostics={
                                **event.diagnostics,
                                "partition_skew": True,
                                "median_partition_lag": lag_median,
                            },
                        )
                    )
                    continue
                result.append(
                    replace(
                        event,
                        anomaly="partition_skew",
                        severity="warning",
                        service_health=health_for_anomaly("partition_skew"),
                        diagnostics={
                            **event.diagnostics,
                            "observed_anomaly": "partition_skew",
                            "partition_skew": True,
                            "median_partition_lag": lag_median,
                        },
                    )
                )
            else:
                result.append(event)
        return result

    def _apply_hysteresis(
        self,
        *,
        decision_state: DecisionState,
        observed_anomaly: str,
    ) -> tuple[str, DecisionState, bool]:
        confirmed_anomaly = decision_state.confirmed_anomaly
        if observed_anomaly == confirmed_anomaly:
            return (
                confirmed_anomaly,
                DecisionState(confirmed_anomaly=confirmed_anomaly),
                False,
            )

        if decision_state.pending_anomaly == observed_anomaly:
            pending_count = decision_state.pending_count + 1
        else:
            pending_count = 1

        if pending_count >= self.settings.state_transition_confirmations:
            return (
                observed_anomaly,
                DecisionState(confirmed_anomaly=observed_anomaly),
                False,
            )

        return (
            confirmed_anomaly,
            DecisionState(
                confirmed_anomaly=confirmed_anomaly,
                pending_anomaly=observed_anomaly,
                pending_count=pending_count,
            ),
            True,
        )

    @staticmethod
    def _event_rank(event: IncidentEvent) -> tuple[int, float]:
        return (ANOMALY_PRIORITY.get(event.anomaly or "normal", 0), event.time_lag_sec or 0.0)
