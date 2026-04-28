import argparse

from lagzero.incidents.cli import build_incidents_parser, run_incidents_command
from lagzero.incidents.schema import DeliveryRecord, IncidentRecord, TimelineEntry


class FakeHistoryService:
    def list_incidents(self, **kwargs):
        return [
            IncidentRecord(
                incident_id="inc-1",
                incident_key="consumer_group:payments:pressure",
                family="pressure",
                status="open",
                scope="consumer_group",
                consumer_group="payments",
                topic=None,
                partition=None,
                opened_at=1.0,
                updated_at=2.0,
                resolved_at=None,
                current_anomaly="system_under_pressure",
                current_health="degraded",
                current_severity="warning",
                current_primary_cause=None,
                current_primary_cause_confidence=None,
                current_payload={"incident_id": "inc-1"},
            )
        ]

    def get_incident(self, incident_id: str):
        return self.list_incidents()[0]

    def get_timeline(self, incident_id: str):
        return [
            TimelineEntry(
                timeline_id="tl-1",
                incident_id=incident_id,
                entry_type="incident_opened",
                at=1.0,
                summary="opened",
                details={},
            )
        ]

    def list_deliveries(self, **kwargs):
        return [
            DeliveryRecord(
                event_id="evt-1",
                incident_id="inc-1",
                timeline_id="tl-1",
                event_type="incident.opened",
                event_version="1.0",
                delivery_state="failed",
                delivery_attempts=1,
                last_delivery_error="boom",
                last_delivery_at=1.0,
                payload_json='{"event_id":"evt-1"}',
            )
        ]

    def redeliver_event(self, event_id: str):
        return self.list_deliveries()[0]

    def redeliver_incident(self, incident_id: str):
        return self.list_deliveries()

    def redeliver_by_state(self, delivery_state: str):
        return self.list_deliveries()


def test_incidents_parser_supports_history_commands() -> None:
    parser = build_incidents_parser()
    args = parser.parse_args(["list", "--status", "active", "--json"])
    assert args.command == "list"
    assert args.status == "active"
    assert args.json is True


def test_run_incidents_command_prints_json(capsys) -> None:
    service = FakeHistoryService()
    args = argparse.Namespace(
        command="show",
        incident_id="inc-1",
        json=True,
    )
    code = run_incidents_command(service, args)
    assert code == 0
    output = capsys.readouterr().out
    assert '"incident_id": "inc-1"' in output


def test_run_deliveries_command_prints_human_output(capsys) -> None:
    service = FakeHistoryService()
    args = argparse.Namespace(
        command="deliveries",
        incident_id=None,
        delivery_state="failed",
        limit=10,
        json=False,
    )
    code = run_incidents_command(service, args)
    assert code == 0
    output = capsys.readouterr().out
    assert "event_id state attempts event_type incident_id" in output
    assert "evt-1 failed 1 incident.opened inc-1" in output
