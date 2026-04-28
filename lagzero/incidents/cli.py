from __future__ import annotations

import argparse
import json
from typing import Iterable

from lagzero.incidents.schema import DeliveryRecord, IncidentRecord, TimelineEntry
from lagzero.incidents.service import IncidentHistoryService


def build_incidents_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LagZero incident history")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List incidents.")
    _add_common_list_args(list_parser)

    active_parser = subparsers.add_parser("active", help="List unresolved incidents.")
    _add_common_list_args(active_parser)

    show_parser = subparsers.add_parser("show", help="Show an incident.")
    show_parser.add_argument("incident_id")
    show_parser.add_argument("--json", action="store_true")

    timeline_parser = subparsers.add_parser("timeline", help="Show incident timeline.")
    timeline_parser.add_argument("incident_id")
    timeline_parser.add_argument("--json", action="store_true")

    deliveries_parser = subparsers.add_parser("deliveries", help="List lifecycle deliveries.")
    deliveries_parser.add_argument("--incident-id")
    deliveries_parser.add_argument("--delivery-state")
    deliveries_parser.add_argument("--limit", type=int, default=50)
    deliveries_parser.add_argument("--json", action="store_true")

    redeliver_parser = subparsers.add_parser("redeliver", help="Redeliver lifecycle webhooks.")
    redeliver_target = redeliver_parser.add_mutually_exclusive_group(required=True)
    redeliver_target.add_argument("--incident-id")
    redeliver_target.add_argument("--event-id")
    redeliver_target.add_argument("--delivery-state")
    redeliver_parser.add_argument("--json", action="store_true")
    return parser


def run_incidents_command(service: IncidentHistoryService, args: argparse.Namespace) -> int:
    if args.command == "list":
        incidents = service.list_incidents(
            status=getattr(args, "status", "all"),
            consumer_group=args.consumer_group,
            family=args.family,
            limit=args.limit,
        )
        _print_or_json(args.json, [incident.to_dict() for incident in incidents], _format_incidents(incidents))
        return 0

    if args.command == "active":
        incidents = service.list_incidents(
            status="active",
            consumer_group=args.consumer_group,
            family=args.family,
            limit=args.limit,
        )
        _print_or_json(args.json, [incident.to_dict() for incident in incidents], _format_incidents(incidents))
        return 0

    if args.command == "show":
        incident = service.get_incident(args.incident_id)
        if incident is None:
            raise KeyError(f"Unknown incident_id '{args.incident_id}'.")
        _print_or_json(args.json, incident.to_dict(), _format_incident_detail(incident))
        return 0

    if args.command == "timeline":
        entries = service.get_timeline(args.incident_id)
        _print_or_json(args.json, [entry.to_dict() for entry in entries], _format_timeline(entries))
        return 0

    if args.command == "deliveries":
        deliveries = service.list_deliveries(
            incident_id=args.incident_id,
            delivery_state=args.delivery_state,
            limit=args.limit,
        )
        _print_or_json(
            args.json,
            [record.to_dict() for record in deliveries],
            _format_deliveries(deliveries),
        )
        return 0

    if args.command == "redeliver":
        if args.event_id:
            deliveries = [service.redeliver_event(args.event_id)]
        elif args.incident_id:
            deliveries = service.redeliver_incident(args.incident_id)
        else:
            deliveries = service.redeliver_by_state(args.delivery_state)
        _print_or_json(
            args.json,
            [record.to_dict() for record in deliveries],
            _format_deliveries(deliveries),
        )
        return 0

    raise ValueError(f"Unsupported incidents command '{args.command}'.")


def _add_common_list_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--status", default="all")
    parser.add_argument("--consumer-group")
    parser.add_argument("--family")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--json", action="store_true")


def _print_or_json(as_json: bool, payload: object, text: str) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print(text)


def _format_incidents(incidents: Iterable[IncidentRecord]) -> str:
    lines = ["incident_id status family anomaly health updated_at"]
    for incident in incidents:
        lines.append(
            f"{incident.incident_id} {incident.status} {incident.family} "
            f"{incident.current_anomaly} {incident.current_health} {incident.updated_at:.3f}"
        )
    return "\n".join(lines)


def _format_incident_detail(incident: IncidentRecord) -> str:
    return (
        f"Incident {incident.incident_id}\n"
        f"status: {incident.status}\n"
        f"family: {incident.family}\n"
        f"scope: {incident.scope}\n"
        f"consumer_group: {incident.consumer_group}\n"
        f"topic: {incident.topic}\n"
        f"partition: {incident.partition}\n"
        f"anomaly: {incident.current_anomaly}\n"
        f"health: {incident.current_health}\n"
        f"severity: {incident.current_severity}\n"
        f"primary_cause: {incident.current_primary_cause}\n"
        f"opened_at: {incident.opened_at:.3f}\n"
        f"updated_at: {incident.updated_at:.3f}\n"
        f"resolved_at: {incident.resolved_at}"
    )


def _format_timeline(entries: Iterable[TimelineEntry]) -> str:
    lines = ["at entry_type summary"]
    for entry in entries:
        lines.append(f"{entry.at:.3f} {entry.entry_type} {entry.summary}")
    return "\n".join(lines)


def _format_deliveries(deliveries: Iterable[DeliveryRecord]) -> str:
    lines = ["event_id state attempts event_type incident_id"]
    for record in deliveries:
        lines.append(
            f"{record.event_id} {record.delivery_state} {record.delivery_attempts} "
            f"{record.event_type} {record.incident_id}"
        )
    return "\n".join(lines)
