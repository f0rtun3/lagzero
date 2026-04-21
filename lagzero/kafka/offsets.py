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
    timestamp_type: str | None = None
    timestamp_sampled_at: float | None = None
    timestamp_sampling_state: str = "unavailable"


@dataclass(frozen=True, slots=True)
class TimestampSample:
    backlog_head_timestamp: float | None
    latest_message_timestamp: float | None
    timestamp_type: str | None
    sampled_at: float | None
    sampling_state: str


@dataclass(frozen=True, slots=True)
class MessageTimestamp:
    timestamp: float
    timestamp_type: str | None


class OffsetFetcher(Protocol):
    def fetch(self, topics: list[str]) -> list[PartitionOffsets]:
        """Fetch current committed and latest offsets for all partitions in the topic set."""


class KafkaOffsetFetcher:
    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        timestamp_sample_interval_sec: float = 30.0,
    ) -> None:
        try:
            self._consumer = build_consumer(
                bootstrap_servers=bootstrap_servers,
                consumer_group=consumer_group,
            )
        except ImportError as exc:  # pragma: no cover - depends on optional dependency state.
            raise RuntimeError(
                "kafka-python is required for Kafka access. Install with: pip install -e '.[kafka]'"
            ) from exc
        self._timestamp_sample_interval_sec = timestamp_sample_interval_sec
        self._timestamp_cache: dict[tuple[str, int], TimestampSample] = {}

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
                timestamp_sample = self._get_timestamp_sample(
                    topic=topic_partition.topic,
                    partition=topic_partition.partition,
                    topic_partition=topic_partition,
                    committed_offset=normalized_committed_offset,
                    latest_offset=latest_offset,
                    observed_at=observed_at,
                )
                offsets.append(
                    PartitionOffsets(
                        topic=topic_partition.topic,
                        partition=topic_partition.partition,
                        committed_offset=normalized_committed_offset,
                        latest_offset=latest_offset,
                        observed_at=observed_at,
                        backlog_head_timestamp=timestamp_sample.backlog_head_timestamp,
                        latest_message_timestamp=timestamp_sample.latest_message_timestamp,
                        timestamp_type=timestamp_sample.timestamp_type,
                        timestamp_sampled_at=timestamp_sample.sampled_at,
                        timestamp_sampling_state=timestamp_sample.sampling_state,
                    )
                )

        return offsets

    def _get_timestamp_sample(
        self,
        *,
        topic: str,
        partition: int,
        topic_partition: object,
        committed_offset: int,
        latest_offset: int,
        observed_at: float,
    ) -> TimestampSample:
        key = (topic, partition)
        cached_sample = self._timestamp_cache.get(key)
        if (
            cached_sample is not None
            and cached_sample.sampled_at is not None
            and (observed_at - cached_sample.sampled_at) < self._timestamp_sample_interval_sec
        ):
            return cached_sample

        latest_sample = self._read_message_metadata(
            topic_partition=topic_partition,
            offset=latest_offset - 1,
        ) if latest_offset > 0 else None

        if latest_offset <= committed_offset:
            sample = TimestampSample(
                backlog_head_timestamp=None,
                latest_message_timestamp=latest_sample.timestamp if latest_sample else None,
                timestamp_type=latest_sample.timestamp_type if latest_sample else None,
                sampled_at=observed_at,
                sampling_state="no_backlog",
            )
            self._timestamp_cache[key] = sample
            return sample

        if committed_offset <= 0:
            sample = TimestampSample(
                backlog_head_timestamp=None,
                latest_message_timestamp=latest_sample.timestamp if latest_sample else None,
                timestamp_type=latest_sample.timestamp_type if latest_sample else None,
                sampled_at=observed_at,
                sampling_state="cold_start",
            )
            self._timestamp_cache[key] = sample
            return sample

        backlog_head_sample = self._read_message_metadata(
            topic_partition=topic_partition,
            offset=committed_offset,
        )
        if backlog_head_sample is None:
            sample = TimestampSample(
                backlog_head_timestamp=None,
                latest_message_timestamp=latest_sample.timestamp if latest_sample else None,
                timestamp_type=latest_sample.timestamp_type if latest_sample else None,
                sampled_at=observed_at,
                sampling_state="sampling_failed",
            )
            self._timestamp_cache[key] = sample
            return sample

        sample = TimestampSample(
            backlog_head_timestamp=backlog_head_sample.timestamp,
            latest_message_timestamp=latest_sample.timestamp if latest_sample else None,
            timestamp_type=backlog_head_sample.timestamp_type,
            sampled_at=observed_at,
            sampling_state="timestamp",
        )
        self._timestamp_cache[key] = sample
        return sample

    def _read_message_metadata(self, *, topic_partition: object, offset: int) -> MessageTimestamp | None:
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

        timestamp_type = getattr(message, "timestamp_type", None)
        normalized_timestamp_type = self._normalize_timestamp_type(timestamp_type)
        return MessageTimestamp(
            timestamp=timestamp_ms / 1000.0,
            timestamp_type=normalized_timestamp_type,
        )

    @staticmethod
    def _normalize_timestamp_type(timestamp_type: object) -> str | None:
        if timestamp_type == 0:
            return "create_time"
        if timestamp_type == 1:
            return "log_append_time"
        if isinstance(timestamp_type, str):
            lowered = timestamp_type.strip().lower()
            if lowered in {"create_time", "create time"}:
                return "create_time"
            if lowered in {"log_append_time", "log append time"}:
                return "log_append_time"
        return None

    def close(self) -> None:
        self._consumer.close()
