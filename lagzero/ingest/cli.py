from __future__ import annotations

import argparse
import json
import time
import uuid

from lagzero.correlation.schema import ExternalEvent


def build_event_from_args(args: argparse.Namespace) -> ExternalEvent:
    return ExternalEvent(
        event_id=str(uuid.uuid4()),
        timestamp=time.time(),
        event_type=args.event_type,
        source="cli",
        service=args.service,
        consumer_group=args.consumer_group,
        topic=args.topic,
        partition=args.partition,
        severity=args.severity,
        metadata=args.metadata or {},
    )


def print_event_json(event: ExternalEvent) -> None:
    print(json.dumps(event.to_dict(), sort_keys=True))

