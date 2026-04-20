from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol

from lagzero.kafka.client import build_consumer
from lagzero.kafka.metadata import build_topic_partitions


@dataclass(frozen=True, slots=True)
class PartitionOffsets:
    topic: str
    partition: int
    committed_offset: int
    latest_offset: int
    observed_at: float


class OffsetFetcher(Protocol):
    def fetch(self, topics: list[str]) -> list[PartitionOffsets]:
        """Fetch current committed and latest offsets for all partitions in the topic set."""


class KafkaOffsetFetcher:
    def __init__(self, bootstrap_servers: str, consumer_group: str) -> None:
        try:
            self._consumer = build_consumer(
                bootstrap_servers=bootstrap_servers,
                consumer_group=consumer_group,
            )
        except ImportError as exc:  # pragma: no cover - depends on optional dependency state.
            raise RuntimeError(
                "kafka-python is required for Kafka access. Install with: pip install -e '.[kafka]'"
            ) from exc

    def fetch(self, topics: list[str]) -> list[PartitionOffsets]:
        observed_at = time.time()
        offsets: list[PartitionOffsets] = []

        for topic in topics:
            partitions = sorted(self._consumer.partitions_for_topic(topic) or [])
            if not partitions:
                continue

            topic_partitions = build_topic_partitions(topic, list(partitions))
            latest_offsets = self._consumer.end_offsets(topic_partitions)

            for topic_partition in topic_partitions:
                committed_offset = self._consumer.committed(topic_partition)
                offsets.append(
                    PartitionOffsets(
                        topic=topic_partition.topic,
                        partition=topic_partition.partition,
                        committed_offset=committed_offset if committed_offset is not None else 0,
                        latest_offset=latest_offsets.get(topic_partition, 0),
                        observed_at=observed_at,
                    )
                )

        return offsets

    def close(self) -> None:
        self._consumer.close()
