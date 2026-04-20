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
from lagzero.monitoring.rate_calculator import compute_rate
from lagzero.monitoring.time_lag import compute_time_lag
from lagzero.state.store import InMemoryStateStore, PartitionState

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CorrelationSignal:
    kind: str
    created_at: float


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
        events = [self._build_event(snapshot) for snapshot in snapshots]

        for event in events:
            self.event_emitter.emit(event)

        return events

    def register_correlation_signal(self, kind: str, created_at: float | None = None) -> None:
        self.correlation_signals.append(
            CorrelationSignal(kind=kind, created_at=created_at or time.time())
        )

    def _build_event(self, snapshot: PartitionOffsets) -> IncidentEvent:
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
        processing_rate = rate_sample.messages_per_second
        time_lag_sec = compute_time_lag(offset_lag=offset_lag, processing_rate=processing_rate)

        consecutive_zero_rate_intervals = 0
        if processing_rate == 0 and offset_lag > 0:
            consecutive_zero_rate_intervals = (
                previous_state.consecutive_zero_rate_intervals + 1 if previous_state else 1
            )

        anomaly = detect_anomaly(
            current_lag=offset_lag,
            previous_lag=previous_state.offset_lag if previous_state else None,
            processing_rate=processing_rate,
            stalled_intervals=self.settings.stalled_intervals,
            consecutive_zero_rate_intervals=consecutive_zero_rate_intervals,
            lag_spike_multiplier=self.settings.lag_spike_multiplier,
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
            ),
        )

        return IncidentEvent(
            timestamp=snapshot.observed_at,
            topic=snapshot.topic,
            partition=snapshot.partition,
            consumer_group=self.settings.consumer_group,
            offset_lag=offset_lag,
            processing_rate=processing_rate,
            time_lag_sec=time_lag_sec,
            anomaly=anomaly.name,
            severity=anomaly.severity,
            confidence=anomaly.confidence,
            correlations=correlations,
        )

    def _collect_correlations(self, observed_at: float) -> list[str]:
        active_signals = []
        valid_after = observed_at - self.settings.correlation_window_sec

        kept_signals = []
        for signal in self.correlation_signals:
            if signal.created_at >= valid_after:
                kept_signals.append(signal)
                active_signals.append(signal.kind)

        self.correlation_signals = kept_signals
        return sorted(set(active_signals))

