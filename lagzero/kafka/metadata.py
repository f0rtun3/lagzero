from __future__ import annotations

from typing import Any


def build_topic_partitions(topic: str, partitions: list[int]) -> list[Any]:
    from kafka import TopicPartition

    return [TopicPartition(topic, partition_id) for partition_id in partitions]
