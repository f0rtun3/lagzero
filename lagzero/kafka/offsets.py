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
    backlog_head_timestamp: float | None = None
    latest_message_timestamp: float | None = None


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
                normalized_committed_offset = committed_offset if committed_offset is not None else 0
                latest_offset = latest_offsets.get(topic_partition, 0)
                offsets.append(
                    PartitionOffsets(
                        topic=topic_partition.topic,
                        partition=topic_partition.partition,
                        committed_offset=normalized_committed_offset,
                        latest_offset=latest_offset,
                        observed_at=observed_at,
                        backlog_head_timestamp=self._fetch_backlog_head_timestamp(
                            topic_partition=topic_partition,
                            committed_offset=normalized_committed_offset,
                            latest_offset=latest_offset,
                        ),
                        latest_message_timestamp=self._fetch_latest_message_timestamp(
                            topic_partition=topic_partition,
                            latest_offset=latest_offset,
                        ),
                    )
                )

        return offsets

    def _fetch_backlog_head_timestamp(
        self,
        *,
        topic_partition: object,
        committed_offset: int,
        latest_offset: int,
    ) -> float | None:
        if latest_offset <= committed_offset:
            return None
        return self._read_message_timestamp(topic_partition=topic_partition, offset=committed_offset)

    def _fetch_latest_message_timestamp(
        self,
        *,
        topic_partition: object,
        latest_offset: int,
    ) -> float | None:
        if latest_offset <= 0:
            return None
        return self._read_message_timestamp(topic_partition=topic_partition, offset=latest_offset - 1)

    def _read_message_timestamp(self, *, topic_partition: object, offset: int) -> float | None:
        self._consumer.assign([topic_partition])
        self._consumer.seek(topic_partition, offset)
        records = self._consumer.poll(timeout_ms=500, max_records=1)
        messages = records.get(topic_partition, [])
        if not messages:
            return None

        message = messages[0]
        timestamp_ms = getattr(message, "timestamp", None)
        if timestamp_ms is None or timestamp_ms < 0:
            return None

        return timestamp_ms / 1000.0

    def close(self) -> None:
        self._consumer.close()
