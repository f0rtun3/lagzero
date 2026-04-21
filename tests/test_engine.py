from dataclasses import dataclass

from lagzero.config.settings import Settings
from lagzero.engine.monitor import MonitorEngine
from lagzero.events.schema import IncidentEvent
from lagzero.kafka.offsets import PartitionOffsets
from lagzero.state.store import PartitionState


@dataclass
class FakeOffsetFetcher:
    snapshots: list[PartitionOffsets]

    def fetch(self, topics: list[str]) -> list[PartitionOffsets]:
        return self.snapshots


class CollectingEmitter:
    def __init__(self) -> None:
        self.events: list[IncidentEvent] = []

    def emit(self, event: IncidentEvent) -> None:
        self.events.append(event)


def test_run_once_emits_group_event_and_partition_events() -> None:
    settings = Settings(
        bootstrap_servers="localhost:9092",
        consumer_group="payments",
        topics=["orders"],
    )
    fetcher = FakeOffsetFetcher(
        snapshots=[
            PartitionOffsets(
                topic="orders",
                partition=0,
                committed_offset=100,
                latest_offset=120,
                observed_at=10.0,
                backlog_head_timestamp=6.0,
                latest_message_timestamp=9.5,
                timestamp_type="log_append_time",
                timestamp_sampled_at=9.0,
                timestamp_sampling_state="timestamp",
            ),
            PartitionOffsets(
                topic="orders",
                partition=1,
                committed_offset=50,
                latest_offset=80,
                observed_at=10.0,
                backlog_head_timestamp=4.0,
                latest_message_timestamp=9.7,
                timestamp_type="create_time",
                timestamp_sampled_at=9.0,
                timestamp_sampling_state="timestamp",
            ),
        ]
    )
    emitter = CollectingEmitter()
    engine = MonitorEngine(settings=settings, offset_fetcher=fetcher, event_emitter=emitter)

    events = engine.run_once()

    assert len(events) == 3
    assert events[0].scope == "consumer_group"
    assert events[0].offset_lag == 50
    assert events[0].time_lag_sec == 6.0
    assert events[0].time_lag_source == "timestamp"
    assert events[0].timestamp_type == "create_time"
    assert events[1].scope == "partition"
    assert events[1].time_lag_source == "timestamp"
    assert events[2].scope == "partition"


def test_external_event_is_exposed_as_correlation() -> None:
    settings = Settings(
        bootstrap_servers="localhost:9092",
        consumer_group="payments",
        topics=["orders"],
    )
    fetcher = FakeOffsetFetcher(
        snapshots=[
            PartitionOffsets(
                topic="orders",
                partition=0,
                committed_offset=100,
                latest_offset=120,
                observed_at=10.0,
                backlog_head_timestamp=8.0,
                latest_message_timestamp=9.0,
                timestamp_type="log_append_time",
                timestamp_sampled_at=9.0,
                timestamp_sampling_state="timestamp",
            ),
        ]
    )
    emitter = CollectingEmitter()
    engine = MonitorEngine(settings=settings, offset_fetcher=fetcher, event_emitter=emitter)
    engine.add_external_event(event_type="deploy", timestamp=9.0)

    events = engine.run_once()

    assert events[0].correlations == ["deploy"]
    assert events[1].correlations == ["deploy"]


def test_partition_event_exposes_offset_and_timestamp_time_lag_diagnostics() -> None:
    settings = Settings(
        bootstrap_servers="localhost:9092",
        consumer_group="payments",
        topics=["orders"],
    )
    fetcher = FakeOffsetFetcher(
        snapshots=[
            PartitionOffsets(
                topic="orders",
                partition=0,
                committed_offset=100,
                latest_offset=120,
                observed_at=20.0,
                backlog_head_timestamp=12.0,
                latest_message_timestamp=19.0,
                timestamp_type="create_time",
                timestamp_sampled_at=18.0,
                timestamp_sampling_state="timestamp",
            ),
        ]
    )
    emitter = CollectingEmitter()
    engine = MonitorEngine(settings=settings, offset_fetcher=fetcher, event_emitter=emitter)

    events = engine.run_once()
    partition_event = events[1]

    assert partition_event.time_lag_sec == 8.0
    assert partition_event.time_lag_source == "timestamp"
    assert partition_event.diagnostics["timestamp_time_lag_sec"] == 8.0
    assert partition_event.diagnostics["backlog_head_timestamp"] == 12.0
    assert partition_event.timestamp_type == "create_time"
    assert partition_event.lag_divergence_sec is None


def test_cold_start_uses_estimated_fallback_and_marks_catching_up_on_next_cycle() -> None:
    settings = Settings(
        bootstrap_servers="localhost:9092",
        consumer_group="payments",
        topics=["orders"],
    )
    emitter = CollectingEmitter()
    engine = MonitorEngine(
        settings=settings,
        offset_fetcher=FakeOffsetFetcher(
            snapshots=[
                PartitionOffsets(
                    topic="orders",
                    partition=0,
                    committed_offset=0,
                    latest_offset=100,
                    observed_at=10.0,
                    latest_message_timestamp=9.0,
                    timestamp_sampled_at=9.0,
                    timestamp_sampling_state="cold_start",
                ),
            ]
        ),
        event_emitter=emitter,
    )

    first_events = engine.run_once()
    assert first_events[1].time_lag_source == "estimated_fallback"
    assert first_events[1].diagnostics["cold_start"] is True

    engine.offset_fetcher = FakeOffsetFetcher(
        snapshots=[
            PartitionOffsets(
                topic="orders",
                partition=0,
                committed_offset=40,
                latest_offset=100,
                observed_at=20.0,
                latest_message_timestamp=19.0,
                timestamp_sampled_at=19.0,
                timestamp_sampling_state="sampling_failed",
            ),
        ]
    )

    second_events = engine.run_once()
    assert second_events[1].anomaly == "catching_up"
    assert second_events[1].time_lag_source == "estimated_fallback"


def test_timestamp_divergence_is_exposed_in_partition_event() -> None:
    settings = Settings(
        bootstrap_servers="localhost:9092",
        consumer_group="payments",
        topics=["orders"],
        lag_divergence_threshold_sec=10.0,
    )
    fetcher = FakeOffsetFetcher(
        snapshots=[
            PartitionOffsets(
                topic="orders",
                partition=0,
                committed_offset=100,
                latest_offset=130,
                observed_at=20.0,
                backlog_head_timestamp=0.0,
                latest_message_timestamp=19.0,
                timestamp_type="create_time",
                timestamp_sampled_at=19.0,
                timestamp_sampling_state="timestamp",
            ),
        ]
    )
    emitter = CollectingEmitter()
    engine = MonitorEngine(settings=settings, offset_fetcher=fetcher, event_emitter=emitter)
    engine.state_store.set(
        ("orders", 0),
        PartitionState(
            committed_offset=90,
            latest_offset=120,
            observed_at=10.0,
            offset_lag=30,
            recent_rates=[1.0],
        ),
    )

    events = engine.run_once()
    partition_event = events[1]

    assert partition_event.lag_divergence_sec is not None
    assert partition_event.anomaly == "lag_estimation_mismatch"
