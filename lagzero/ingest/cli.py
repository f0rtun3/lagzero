from __future__ import annotations

import argparse
import json
import time
import urllib.request
import uuid
from typing import Any

from lagzero.correlation.schema import ExternalEvent


def build_ingest_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LagZero external event ingestion")
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8787/events",
        help="LagZero ingest HTTP endpoint.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=5.0,
        help="HTTP timeout in seconds.",
    )

    subparsers = parser.add_subparsers(dest="event_type", required=True)
    for event_type in ("deploy", "error", "infra", "rebalance"):
        event_parser = subparsers.add_parser(event_type, help=f"Send a {event_type} event.")
        _add_common_event_args(event_parser)

    return parser


def build_event_from_args(args: argparse.Namespace) -> ExternalEvent:
    metadata = _parse_metadata_args(args.metadata, args.metadata_json)
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
        metadata=metadata,
    )


def send_external_event(event: ExternalEvent, *, url: str, timeout_sec: float) -> dict[str, Any]:
    body = json.dumps(event.to_dict()).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        return json.loads(response.read().decode("utf-8"))


def print_event_json(event: ExternalEvent) -> None:
    print(json.dumps(event.to_dict(), sort_keys=True))


def _add_common_event_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--service")
    parser.add_argument("--consumer-group")
    parser.add_argument("--topic")
    parser.add_argument("--partition", type=int)
    parser.add_argument("--severity")
    parser.add_argument(
        "--metadata",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional metadata entries. Repeatable.",
    )
    parser.add_argument(
        "--metadata-json",
        help="Raw JSON object merged into metadata after --metadata flags.",
    )


def _parse_metadata_args(entries: list[str], metadata_json: str | None) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Invalid metadata entry '{entry}'. Expected KEY=VALUE.")
        key, value = entry.split("=", 1)
        metadata[key] = value
    if metadata_json:
        payload = json.loads(metadata_json)
        if not isinstance(payload, dict):
            raise ValueError("--metadata-json must decode to a JSON object.")
        metadata.update(payload)
    return metadata
