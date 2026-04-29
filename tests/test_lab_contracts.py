from lagzero.lab.contracts import (
    ContractSection,
    ScenarioValidation,
    _SCENARIO_EXPECTATIONS,
    validate_delivery_contract,
    validate_lifecycle_contract,
    validate_scenario_contract,
)
from lagzero.lab.state import build_logical_incident_key
from lagzero.lab.webhook_capture import build_capture_record
from lagzero.sinks.signing import build_signature


def _group_event(
    *,
    anomaly: str,
    service_health: str,
    consumer_group: str = "payments",
    producer_rate: float | None = None,
    processing_rate: float | None = None,
    offset_lag: int = 100,
    primary_cause: str | None = None,
    primary_cause_confidence: float | None = None,
    correlations: list[dict[str, object]] | None = None,
    diagnostics: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "scope": "consumer_group",
        "consumer_group": consumer_group,
        "anomaly": anomaly,
        "service_health": service_health,
        "producer_rate": producer_rate,
        "processing_rate": processing_rate,
        "offset_lag": offset_lag,
        "primary_cause": primary_cause,
        "primary_cause_confidence": primary_cause_confidence,
        "correlations": correlations or [],
        "diagnostics": diagnostics or {"transition_pending": False},
    }


def _state_snapshot(
    *,
    incident_id: str = "inc-1",
    incident_key: str = "consumer_group:payments:pressure",
    family: str = "pressure",
    delivery_attempts: int = 1,
) -> dict[str, list[dict[str, object]]]:
    payload = {
        "event_id": "evt-1",
        "event_type": "incident.opened",
        "event_version": "1.0",
        "incident": {"incident_id": incident_id},
        "timeline_entry": {"timeline_id": "tl-1"},
        "current_state": {"anomaly": "system_under_pressure"},
    }
    return {
        "incidents": [
            {
                "incident_id": incident_id,
                "incident_key": incident_key,
                "family": family,
                "status": "open",
                "opened_at": 1.0,
                "updated_at": 2.0,
            }
        ],
        "timeline": [
            {
                "timeline_id": "tl-1",
                "incident_id": incident_id,
                "entry_type": "incident_opened",
                "at": 1.0,
                "summary": "Incident opened",
                "details_json": {"anomaly": "system_under_pressure"},
            }
        ],
        "deliveries": [
            {
                "event_id": "evt-1",
                "incident_id": incident_id,
                "timeline_id": "tl-1",
                "event_type": "incident.opened",
                "event_version": "1.0",
                "delivery_state": "delivered",
                "delivery_attempts": delivery_attempts,
                "payload_json": payload,
            }
        ],
    }


def _lifecycle_events() -> list[dict[str, object]]:
    return [
        {
            "logical_incident_key": build_logical_incident_key(
                incident_key="consumer_group:payments:pressure",
                family="pressure",
            ),
            "incident_id": "inc-1",
            "incident_key": "consumer_group:payments:pressure",
            "family": "pressure",
            "timeline_id": "tl-1",
            "entry_type": "incident_opened",
            "event_type": "incident.opened",
            "at": 1.0,
            "summary": "Incident opened",
            "details": {"anomaly": "system_under_pressure"},
        }
    ]


def test_logical_incident_key_combines_incident_key_and_family() -> None:
    assert (
        build_logical_incident_key(
            incident_key="consumer_group:payments:pressure",
            family="pressure",
        )
        == "consumer_group:payments:pressure|pressure"
    )


def test_phase_one_scenario_reports_all_contract_sections() -> None:
    validation = validate_scenario_contract(
        scenario_name="sustained_pressure",
        incidents=[
            _group_event(anomaly="system_under_pressure", service_health="degraded"),
            _group_event(anomaly="consumer_stalled", service_health="failing"),
        ],
        consumer_group="payments",
        lifecycle_events=_lifecycle_events(),
        state_snapshot=_state_snapshot(),
        captured_deliveries=[],
        current_phase=1,
    )

    assert isinstance(validation.snapshot_contract, ContractSection)
    assert isinstance(validation.lifecycle_contract, ContractSection)
    assert isinstance(validation.delivery_contract, ContractSection)
    assert validation.snapshot_contract.passed is True
    assert validation.lifecycle_contract.passed is True


def test_lifecycle_contract_rejects_duplicate_open_for_logical_incident() -> None:
    lifecycle = _lifecycle_events() * 2
    section = validate_lifecycle_contract(
        scenario_name="sustained_pressure",
        expectation=_SCENARIO_EXPECTATIONS["sustained_pressure"],
        incidents=[_group_event(anomaly="consumer_stalled", service_health="failing")],
        lifecycle_events=lifecycle,
        state_snapshot=_state_snapshot(),
        current_phase=1,
    )
    assert section.passed is False
    assert "more than one `incident.opened`" in section.reason


def test_restart_continuity_rejects_synthetic_update() -> None:
    lifecycle = _lifecycle_events() + [
        {
            "logical_incident_key": build_logical_incident_key(
                incident_key="consumer_group:payments:pressure",
                family="pressure",
            ),
            "incident_id": "inc-1",
            "incident_key": "consumer_group:payments:pressure",
            "family": "pressure",
            "timeline_id": "tl-2",
            "entry_type": "health_changed",
            "event_type": "incident.updated",
            "at": 2.0,
            "summary": "Changed",
            "details": {"changes": ["health_changed"]},
        }
    ]
    validation = validate_scenario_contract(
        scenario_name="restart_continuity",
        incidents=[_group_event(anomaly="system_under_pressure", service_health="degraded")],
        consumer_group="payments",
        lifecycle_events=lifecycle,
        state_snapshot=_state_snapshot(),
        captured_deliveries=[],
        current_phase=1,
    )
    assert validation.lifecycle_contract.passed is False
    assert "unexpected `incident.updated`" in validation.lifecycle_contract.reason


def test_delivery_contract_validates_hmac_and_redelivery_attempts() -> None:
    payload = '{"event_id":"evt-1","event_type":"incident.opened","event_version":"1.0","incident":{"incident_id":"inc-1"},"timeline_entry":{"timeline_id":"tl-1"},"current_state":{"anomaly":"system_under_pressure"}}'
    timestamp = "2026-04-29T00:00:00Z"
    captured = build_capture_record(
        headers={
            "X-LagZero-Timestamp": timestamp,
            "X-LagZero-Signature": build_signature("secret", timestamp=timestamp, raw_body=payload),
        },
        raw_body=payload,
        payload={
            "event_id": "evt-1",
            "event_type": "incident.opened",
            "event_version": "1.0",
            "incident": {"incident_id": "inc-1"},
            "timeline_entry": {"timeline_id": "tl-1"},
            "current_state": {"anomaly": "system_under_pressure"},
        },
        secret="secret",
    )
    state = _state_snapshot(delivery_attempts=2)
    section = validate_delivery_contract(
        scenario_name="webhook_redelivery",
        expectation=_SCENARIO_EXPECTATIONS["webhook_redelivery"],
        state_snapshot=state,
        captured_deliveries=[captured, captured],
        current_phase=1,
    )
    assert section.passed is True
    assert section.evidence["captured_attempts_by_event_id"]["evt-1"] == 2


def test_scenario_validation_serializes_sections() -> None:
    validation = ScenarioValidation(
        snapshot_contract=ContractSection(True, "snapshot ok"),
        lifecycle_contract=ContractSection(True, "lifecycle ok"),
        delivery_contract=ContractSection(True, "delivery ok"),
        overall_passed=True,
        final_incident={"scope": "consumer_group", "anomaly": "normal"},
    )

    payload = validation.to_dict()
    assert payload["overall_passed"] is True
    assert payload["snapshot_contract"]["passed"] is True
    assert payload["final_incident"]["anomaly"] == "normal"
