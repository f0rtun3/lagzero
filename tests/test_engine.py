from dataclasses import dataclass

from lagzero.config.settings import Settings
from lagzero.engine.monitor import MonitorEngine
from lagzero.events.schema import IncidentEvent
from lagzero.kafka.offsets import PartitionOffsets


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
            ),
            PartitionOffsets(
                topic="orders",
                partition=1,
                committed_offset=50,
                latest_offset=80,
                observed_at=10.0,
            ),
        ]
    )
    emitter = CollectingEmitter()
    engine = MonitorEngine(settings=settings, offset_fetcher=fetcher, event_emitter=emitter)

    events = engine.run_once()

    assert len(events) == 3
    assert events[0].scope == "consumer_group"
    assert events[0].offset_lag == 50
    assert events[1].scope == "partition"
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
            ),
        ]
    )
    emitter = CollectingEmitter()
    engine = MonitorEngine(settings=settings, offset_fetcher=fetcher, event_emitter=emitter)
    engine.add_external_event(event_type="deploy", timestamp=9.0)

    events = engine.run_once()

    assert events[0].correlations == ["deploy"]
    assert events[1].correlations == ["deploy"]
