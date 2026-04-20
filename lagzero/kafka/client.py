from __future__ import annotations

from typing import Any


def build_consumer(bootstrap_servers: str, consumer_group: str) -> Any:
    from kafka import KafkaConsumer

    return KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group,
        enable_auto_commit=False,
        consumer_timeout_ms=1_000,
    )
