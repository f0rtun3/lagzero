from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
import time
from typing import Any, Callable

from lagzero.incidents.lifecycle import material_change_types
from lagzero.lab.events import IncidentPayload, load_incidents, load_jsonl
from lagzero.lab.state import build_logical_incident_key


HEALTHY_ANOMALIES = {"normal", "bounded_lag"}
RECOVERY_ANOMALIES = {"catching_up"}
PRESSURE_ANOMALIES = {"lag_spike", "system_under_pressure"}
DELAY_ANOMALIES = {"idle_but_delayed"}
SEVERE_ANOMALIES = {"consumer_stalled"}
REBALANCE_ANOMALIES = {"rebalance_in_progress"}
SKEW_ANOMALIES = {"partition_skew"}

HEALTHY_STATES = {"healthy"}
RECOVERY_STATES = {"recovering"}
DEGRADED_STATES = {"degraded"}
FAILING_STATES = {"failing"}

NEAR_ZERO_RATE = 0.1


@dataclass(frozen=True, slots=True)
class ContractSection:
    passed: bool
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ScenarioExpectation:
    phase: int
    require_snapshot_contract: bool = True
    require_lifecycle_contract: bool = False
    require_delivery_contract: bool = False
    require_open: str = "optional"  # required | optional | forbidden
    require_resolve: str = "optional"  # required | optional | forbidden
    allow_zero_incidents: bool = False
    expect_webhook_capture: bool = False
    expect_redelivery: bool = False


@dataclass(frozen=True, slots=True)
class ScenarioValidation:
    snapshot_contract: ContractSection
    lifecycle_contract: ContractSection
    delivery_contract: ContractSection
    overall_passed: bool
    final_incident: IncidentPayload | None = None

    @property
    def passed(self) -> bool:
        return self.overall_passed

    @property
    def reason(self) -> str:
        if not self.snapshot_contract.passed:
            return self.snapshot_contract.reason
        if not self.lifecycle_contract.passed:
            return self.lifecycle_contract.reason
        if not self.delivery_contract.passed:
            return self.delivery_contract.reason
        return "All required contracts passed."

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_contract": self.snapshot_contract.to_dict(),
            "lifecycle_contract": self.lifecycle_contract.to_dict(),
            "delivery_contract": self.delivery_contract.to_dict(),
            "overall_passed": self.overall_passed,
            "final_incident": self.final_incident,
            "reason": self.reason,
        }


def wait_for_snapshot_contract(
    path: str | Path,
    *,
    scenario_name: str,
    consumer_group: str,
    current_phase: int,
    timeout_sec: float,
    poll_interval_sec: float = 1.0,
) -> ContractSection:
    deadline = time.time() + timeout_sec
    latest = ContractSection(False, "No incidents observed yet.")
    while time.time() < deadline:
        incidents = load_incidents(path)
        expectation = _SCENARIO_EXPECTATIONS[scenario_name]
        latest = validate_snapshot_contract(
            scenario_name=scenario_name,
            incidents=incidents,
            consumer_group=consumer_group,
            expectation=expectation,
            current_phase=current_phase,
        )
        if latest.passed:
            return latest
        time.sleep(poll_interval_sec)
    return ContractSection(
        False,
        (
            f"Scenario `{scenario_name}` did not satisfy its snapshot contract within "
            f"{timeout_sec} seconds. {latest.reason}"
        ),
        latest.evidence,
    )


def validate_scenario_contract(
    *,
    scenario_name: str,
    incidents: list[IncidentPayload],
    consumer_group: str,
    lifecycle_events: list[dict[str, object]] | None = None,
    state_snapshot: dict[str, list[dict[str, object]]] | None = None,
    captured_deliveries: list[dict[str, object]] | None = None,
    current_phase: int = 1,
) -> ScenarioValidation:
    expectation = _SCENARIO_EXPECTATIONS[scenario_name]
    snapshot_contract = validate_snapshot_contract(
        scenario_name=scenario_name,
        incidents=incidents,
        consumer_group=consumer_group,
        expectation=expectation,
        current_phase=current_phase,
    )
    final_incident = snapshot_contract.evidence.get("final_incident")
    lifecycle_contract = validate_lifecycle_contract(
        scenario_name=scenario_name,
        expectation=expectation,
        incidents=incidents,
        lifecycle_events=lifecycle_events or [],
        state_snapshot=state_snapshot or {},
        current_phase=current_phase,
    )
    delivery_contract = validate_delivery_contract(
        scenario_name=scenario_name,
        expectation=expectation,
        state_snapshot=state_snapshot or {},
        captured_deliveries=captured_deliveries or [],
        current_phase=current_phase,
    )
    required_sections = [snapshot_contract]
    if _expect_lifecycle(expectation, current_phase):
        required_sections.append(lifecycle_contract)
    if _expect_delivery(expectation, current_phase):
        required_sections.append(delivery_contract)
    overall_passed = all(section.passed for section in required_sections)
    return ScenarioValidation(
        snapshot_contract=snapshot_contract,
        lifecycle_contract=lifecycle_contract,
        delivery_contract=delivery_contract,
        overall_passed=overall_passed,
        final_incident=final_incident if isinstance(final_incident, dict) else None,
    )


def validate_snapshot_contract(
    *,
    scenario_name: str,
    incidents: list[IncidentPayload],
    consumer_group: str,
    expectation: ScenarioExpectation,
    current_phase: int,
) -> ContractSection:
    del current_phase
    group_incidents = _group_incidents(incidents, consumer_group=consumer_group)
    if not group_incidents:
        if expectation.allow_zero_incidents:
            return ContractSection(True, "No consumer-group incidents emitted, which is allowed.", {})
        return ContractSection(False, "No consumer-group incidents emitted yet.", {})

    final_incident = _latest_stable_group_incident(group_incidents) or group_incidents[-1]
    validator = _SNAPSHOT_VALIDATORS.get(scenario_name)
    if validator is None:
        return ContractSection(
            False,
            f"No snapshot contract registered for scenario `{scenario_name}`.",
            {"final_incident": final_incident},
        )
    result = validator(group_incidents, final_incident, incidents)
    evidence = dict(result.evidence)
    evidence["final_incident"] = final_incident
    return ContractSection(result.passed, result.reason, evidence)


def validate_lifecycle_contract(
    *,
    scenario_name: str,
    expectation: ScenarioExpectation,
    incidents: list[IncidentPayload],
    lifecycle_events: list[dict[str, object]],
    state_snapshot: dict[str, list[dict[str, object]]],
    current_phase: int,
) -> ContractSection:
    if not _expect_lifecycle(expectation, current_phase):
        return ContractSection(True, "Lifecycle validation is not required in the current rollout phase.")

    if not lifecycle_events:
        if expectation.allow_zero_incidents:
            return ContractSection(True, "No lifecycle artifacts were emitted, which is allowed for this scenario.")
        return ContractSection(False, "Lifecycle artifacts are required but `lifecycle-events.jsonl` is empty.")

    logical_groups: dict[str, list[dict[str, object]]] = {}
    for event in lifecycle_events:
        logical_key = str(event.get("logical_incident_key"))
        logical_groups.setdefault(logical_key, []).append(event)

    if not logical_groups and expectation.allow_zero_incidents:
        return ContractSection(True, "No lifecycle events emitted, which is allowed for this scenario.")
    if not logical_groups:
        return ContractSection(False, "No lifecycle events were captured.")

    opened_count = sum(1 for event in lifecycle_events if event.get("event_type") == "incident.opened")
    updated_count = sum(1 for event in lifecycle_events if event.get("event_type") == "incident.updated")
    resolved_count = sum(1 for event in lifecycle_events if event.get("event_type") == "incident.resolved")
    incident_ids = {str(event.get("incident_id")) for event in lifecycle_events}
    issues: list[str] = []

    if expectation.require_open == "required" and opened_count == 0:
        issues.append("Expected at least one lifecycle open event.")
    if expectation.require_open == "forbidden" and opened_count > 0:
        issues.append("Lifecycle open events were forbidden for this scenario.")
    if expectation.require_resolve == "required" and resolved_count == 0:
        issues.append("Expected at least one lifecycle resolve event.")
    if expectation.require_resolve == "forbidden" and resolved_count > 0:
        issues.append("Lifecycle resolve events were forbidden for this scenario.")

    for logical_key, events_for_incident in logical_groups.items():
        opens = [event for event in events_for_incident if event.get("event_type") == "incident.opened"]
        resolves = [event for event in events_for_incident if event.get("event_type") == "incident.resolved"]
        updates = [event for event in events_for_incident if event.get("event_type") == "incident.updated"]
        if len(opens) > 1:
            issues.append(f"Logical incident `{logical_key}` emitted more than one `incident.opened`.")
        if len(resolves) > 1:
            issues.append(f"Logical incident `{logical_key}` emitted more than one `incident.resolved`.")
        for update in updates:
            changes = (update.get("details") or {}).get("changes", [])
            if not changes:
                issues.append(f"Lifecycle update `{update.get('timeline_id')}` has no normalized change list.")

    if _scenario_requires_no_synthetic_update(scenario_name) and updated_count > 0:
        issues.append("Restart continuity scenario emitted an unexpected `incident.updated` after restart.")

    pending_timestamps = {
        float(incident.get("timestamp"))
        for incident in incidents
        if incident.get("scope") == "consumer_group"
        and isinstance(incident.get("timestamp"), (int, float))
        and (incident.get("diagnostics") or {}).get("transition_pending")
    }
    for event in lifecycle_events:
        if isinstance(event.get("at"), (int, float)) and float(event["at"]) in pending_timestamps:
            issues.append("Lifecycle events were emitted while snapshot transitions were still pending.")
            break

    evidence = {
        "logical_incident_count": len(logical_groups),
        "logical_incidents": sorted(logical_groups.keys()),
        "incident_ids": sorted(incident_ids),
        "opened_count": opened_count,
        "updated_count": updated_count,
        "resolved_count": resolved_count,
        "state_incident_rows": len(state_snapshot.get("incidents", [])),
        "state_timeline_rows": len(state_snapshot.get("timeline", [])),
    }
    if issues:
        return ContractSection(False, " ".join(issues), evidence)
    return ContractSection(True, "Lifecycle contract passed.", evidence)


def validate_delivery_contract(
    *,
    scenario_name: str,
    expectation: ScenarioExpectation,
    state_snapshot: dict[str, list[dict[str, object]]],
    captured_deliveries: list[dict[str, object]],
    current_phase: int,
) -> ContractSection:
    if not _expect_delivery(expectation, current_phase):
        return ContractSection(True, "Delivery validation is not required in the current rollout phase.")

    persisted_deliveries = state_snapshot.get("deliveries", [])
    if expectation.expect_webhook_capture and not captured_deliveries:
        return ContractSection(False, "Webhook capture was expected but `deliveries.jsonl` is empty.")
    if not persisted_deliveries:
        return ContractSection(False, "Persisted delivery rows were required but none were captured.")

    issues: list[str] = []
    attempts_by_event_id: dict[str, int] = {}
    for captured in captured_deliveries:
        event_id = str(captured.get("event_id"))
        if event_id and event_id != "None":
            attempts_by_event_id[event_id] = attempts_by_event_id.get(event_id, 0) + 1
        if captured.get("signature_valid") is not True:
            issues.append(
                f"Captured delivery for event_id={captured.get('event_id')} failed signature validation "
                f"({captured.get('signature_error')})."
            )

    for delivery in persisted_deliveries:
        payload = delivery.get("payload_json")
        if not isinstance(payload, dict):
            issues.append(f"Persisted delivery `{delivery.get('event_id')}` does not contain schema-valid payload JSON.")
            continue
        required_fields = {"event_id", "event_type", "event_version", "incident", "timeline_entry", "current_state"}
        missing = sorted(field for field in required_fields if field not in payload)
        if missing:
            issues.append(
                f"Persisted delivery `{delivery.get('event_id')}` is missing required lifecycle envelope fields: {', '.join(missing)}."
            )

    if expectation.expect_redelivery:
        matching = [
            delivery
            for delivery in persisted_deliveries
            if int(delivery.get("delivery_attempts", 0) or 0) > 1
        ]
        if not matching:
            issues.append("Expected a redelivery attempt but no persisted delivery row shows attempts > 1.")
        for delivery in matching:
            event_id = str(delivery.get("event_id"))
            if attempts_by_event_id.get(event_id, 0) < 2:
                issues.append(
                    f"Expected multiple captured attempts for redelivered event_id={event_id}, "
                    "but capture evidence did not show repeated delivery attempts."
                )

    evidence = {
        "persisted_delivery_count": len(persisted_deliveries),
        "captured_delivery_count": len(captured_deliveries),
        "event_ids": sorted(
            {
                str(record.get("event_id"))
                for record in persisted_deliveries
                if record.get("event_id") is not None
            }
        ),
        "captured_attempts_by_event_id": attempts_by_event_id,
    }
    if issues:
        return ContractSection(False, " ".join(issues), evidence)
    return ContractSection(True, "Delivery contract passed.", evidence)


def _group_incidents(
    incidents: list[IncidentPayload], *, consumer_group: str
) -> list[IncidentPayload]:
    return [
        incident
        for incident in incidents
        if incident.get("scope") == "consumer_group"
        and incident.get("consumer_group") == consumer_group
    ]


def _latest_stable_group_incident(
    incidents: list[IncidentPayload],
) -> IncidentPayload | None:
    for incident in reversed(incidents):
        diagnostics = incident.get("diagnostics") or {}
        if not diagnostics.get("transition_pending", False):
            return incident
    return incidents[-1] if incidents else None


def _is_near_zero(value: object) -> bool:
    return isinstance(value, (int, float)) and abs(float(value)) <= NEAR_ZERO_RATE


def _is_positive(value: object) -> bool:
    return isinstance(value, (int, float)) and float(value) > NEAR_ZERO_RATE


def _has_meaningful_progress(incident: IncidentPayload) -> bool:
    if _is_positive(incident.get("processing_rate")):
        return True
    diagnostics = incident.get("diagnostics") or {}
    for partition in diagnostics.get("affected_partitions", []):
        if _is_positive(partition.get("processing_rate")):
            return True
    return False


def _producer_active(incident: IncidentPayload) -> bool:
    if _is_positive(incident.get("producer_rate")):
        return True
    diagnostics = incident.get("diagnostics") or {}
    for partition in diagnostics.get("affected_partitions", []):
        if _is_positive(partition.get("producer_rate")):
            return True
    return False


def _partition_skew_visible(
    final_incident: IncidentPayload,
    incidents: list[IncidentPayload],
) -> bool:
    if final_incident.get("anomaly") in SKEW_ANOMALIES:
        return True
    diagnostics = final_incident.get("diagnostics") or {}
    if diagnostics.get("partition_skew") is True:
        return True
    if any(partition.get("anomaly") in SKEW_ANOMALIES for partition in diagnostics.get("affected_partitions", [])):
        return True
    return any(
        incident.get("scope") == "partition" and incident.get("anomaly") in SKEW_ANOMALIES
        for incident in incidents
    )


def _expect_lifecycle(expectation: ScenarioExpectation, current_phase: int) -> bool:
    return expectation.require_lifecycle_contract and expectation.phase <= current_phase


def _expect_delivery(expectation: ScenarioExpectation, current_phase: int) -> bool:
    return expectation.require_delivery_contract and expectation.phase <= current_phase


def _scenario_requires_no_synthetic_update(scenario_name: str) -> bool:
    return scenario_name == "restart_continuity"


def _section(passed: bool, reason: str, final_incident: IncidentPayload | None) -> ContractSection:
    return ContractSection(passed, reason, {"final_incident": final_incident})


def _validate_baseline(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    final_anomaly = str(final_incident.get("anomaly"))
    final_health = final_incident.get("service_health")
    if any(incident.get("anomaly") in SEVERE_ANOMALIES for incident in group_incidents):
        return _section(False, "Baseline emitted a stable severe stall classification.", final_incident)
    if final_anomaly == "system_under_pressure":
        return _section(False, "Baseline ended in sustained pressure instead of a healthy family state.", final_incident)
    if final_anomaly in HEALTHY_ANOMALIES and final_health in HEALTHY_STATES:
        return _section(True, "Baseline ended in the healthy/non-incident family.", final_incident)
    return _section(False, f"Baseline final state `{final_anomaly}/{final_health}` is outside the healthy family.", final_incident)


def _validate_burst_spike(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    for incident in group_incidents:
        if incident.get("anomaly") in SEVERE_ANOMALIES and (_has_meaningful_progress(incident) or _producer_active(incident)):
            return _section(False, "Burst produced a stable `consumer_stalled` incident while progress or producer activity still existed.", final_incident)

    final_anomaly = str(final_incident.get("anomaly"))
    final_health = final_incident.get("service_health")
    if final_anomaly in HEALTHY_ANOMALIES and final_health in HEALTHY_STATES:
        return _section(True, "Burst recovered back into the healthy family.", final_incident)
    if final_anomaly in RECOVERY_ANOMALIES and final_health in RECOVERY_STATES:
        return _section(True, "Burst ended in an explicit recovery state.", final_incident)
    if final_anomaly in DELAY_ANOMALIES:
        if final_incident.get("offset_lag", 0) > 0 and _is_near_zero(final_incident.get("producer_rate")):
            return _section(True, "Burst ended as `idle_but_delayed` after producer activity stopped with backlog still present.", final_incident)
        return _section(False, "`idle_but_delayed` requires near-zero producer rate and remaining backlog.", final_incident)
    if final_anomaly in SEVERE_ANOMALIES:
        return _section(False, "Burst ended in severe failure instead of recovery, healthy, or justified dark-queue semantics.", final_incident)
    return _section(False, f"Burst final state `{final_anomaly}/{final_health}` is not in an allowed end-state family.", final_incident)


def _validate_sustained_pressure(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    final_anomaly = str(final_incident.get("anomaly"))
    final_health = final_incident.get("service_health")
    if final_anomaly in SEVERE_ANOMALIES and final_health in FAILING_STATES:
        return _section(True, "Sustained pressure ended in the severe failure family.", final_incident)
    if final_anomaly in HEALTHY_ANOMALIES and final_health in HEALTHY_STATES:
        return _section(False, "Sustained pressure incorrectly ended as healthy/non-incident.", final_incident)
    if final_anomaly in RECOVERY_ANOMALIES:
        return _section(False, "Sustained pressure incorrectly ended in recovery while backlog should still be unresolved.", final_incident)
    if any(
        incident.get("anomaly") in SEVERE_ANOMALIES and incident.get("service_health") in FAILING_STATES
        for incident in group_incidents
    ):
        return _section(True, "Sustained pressure reached eventual severe failure even before the latest artifact.", final_incident)
    return _section(False, "Sustained pressure has not yet escalated into a severe failure state.", final_incident)


def _validate_deploy_correlation(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    for incident in group_incidents:
        if incident.get("primary_cause") == "deploy_within_window" and _is_positive(incident.get("primary_cause_confidence")):
            if incident.get("anomaly") not in HEALTHY_ANOMALIES:
                return _section(True, "Deploy correlation was attached as the primary cause without changing detection semantics.", incident)
    return _section(False, "No incident carried `primary_cause=deploy_within_window` with non-zero confidence.", final_incident)


def _validate_error_correlation(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    for incident in group_incidents:
        if incident.get("primary_cause") == "error_nearby" and _is_positive(incident.get("primary_cause_confidence")):
            return _section(True, "Error correlation was attached as the primary cause.", incident)
    return _section(False, "No incident carried `primary_cause=error_nearby` with non-zero confidence.", final_incident)


def _validate_idle_but_delayed(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    del group_incidents
    final_anomaly = str(final_incident.get("anomaly"))
    if final_anomaly != "idle_but_delayed":
        return _section(False, f"Idle backlog final state was `{final_anomaly}` instead of `idle_but_delayed`.", final_incident)
    if final_incident.get("offset_lag", 0) <= 0:
        return _section(False, "`idle_but_delayed` requires unresolved positive lag.", final_incident)

    producer_rate = final_incident.get("producer_rate")
    group_level_quiet = producer_rate is None or _is_near_zero(producer_rate)
    diagnostics = final_incident.get("diagnostics") or {}
    partitions = diagnostics.get("affected_partitions", [])
    partition_level_quiet = any(
        (partition.get("offset_lag", 0) or 0) > 0
        and _is_near_zero(partition.get("producer_rate"))
        and partition.get("anomaly") == "idle_but_delayed"
        for partition in partitions
    )
    if not (group_level_quiet or partition_level_quiet):
        return _section(False, "`idle_but_delayed` requires producer inactivity visible at group level or on the affected backlog partition.", final_incident)
    if _has_meaningful_progress(final_incident):
        return _section(False, "`idle_but_delayed` should not report active draining progress.", final_incident)
    return _section(True, "Idle backlog was classified as `idle_but_delayed` with the expected supporting signals.", final_incident)


def _validate_offset_reset(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    del group_incidents
    final_anomaly = str(final_incident.get("anomaly"))
    if final_anomaly == "offset_reset" and float(final_incident.get("confidence", 0.0)) >= 0.5:
        return _section(True, "Offset reset was identified explicitly instead of being treated as natural recovery.", final_incident)
    return _section(False, "Offset reset did not end with explicit operator-action semantics.", final_incident)


def _validate_rebalance_noise(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    final_anomaly = str(final_incident.get("anomaly"))
    if final_anomaly in HEALTHY_ANOMALIES and final_incident.get("service_health") in HEALTHY_STATES:
        return _section(True, "Rebalance turbulence returned cleanly to a healthy state.", final_incident)
    if final_anomaly in REBALANCE_ANOMALIES:
        return _section(True, "Rebalance turbulence was surfaced explicitly.", final_incident)
    if final_anomaly in SEVERE_ANOMALIES:
        return _section(False, "Short rebalance turbulence ended as a severe failure.", final_incident)
    if any(
        match.get("correlation_type") == "rebalance_overlap"
        for incident in group_incidents
        for match in incident.get("correlations", [])
    ):
        return _section(True, "Rebalance turbulence was recognized through correlation without causing a false severe page.", final_incident)
    return _section(False, "Rebalance scenario did not return healthy or surface rebalance evidence.", final_incident)


def _validate_partition_skew(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    incidents: list[IncidentPayload],
) -> ContractSection:
    del group_incidents
    if _partition_skew_visible(final_incident, incidents):
        return _section(True, "Partition skew remained visible in the emitted evidence.", final_incident)
    return _section(False, "Partition skew was flattened into a generic group issue with no explicit skew evidence.", final_incident)


def _validate_consumer_freeze(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    del group_incidents
    if final_incident.get("anomaly") in SEVERE_ANOMALIES | PRESSURE_ANOMALIES and final_incident.get("service_health") in DEGRADED_STATES | FAILING_STATES:
        return _section(True, "Consumer freeze escalated beyond harmless lag.", final_incident)
    return _section(False, "Consumer freeze did not escalate into pressure or failure semantics.", final_incident)


def _validate_restart_continuity(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    del group_incidents
    final_anomaly = str(final_incident.get("anomaly"))
    final_health = final_incident.get("service_health")
    if final_anomaly in PRESSURE_ANOMALIES | SEVERE_ANOMALIES and final_health in DEGRADED_STATES | FAILING_STATES:
        return _section(True, "Restart continuity scenario preserved an unresolved incident state across restart.", final_incident)
    return _section(False, "Restart continuity scenario did not hold an unresolved degraded incident state.", final_incident)


def _validate_webhook_redelivery(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ContractSection:
    del group_incidents
    if final_incident.get("anomaly") in PRESSURE_ANOMALIES | SEVERE_ANOMALIES:
        return _section(True, "Webhook redelivery scenario produced a lifecycle-worthy incident.", final_incident)
    return _section(False, "Webhook redelivery scenario did not produce an incident suitable for delivery validation.", final_incident)


_SNAPSHOT_VALIDATORS: dict[
    str, Callable[[list[IncidentPayload], IncidentPayload, list[IncidentPayload]], ContractSection]
] = {
    "baseline_healthy_lag": _validate_baseline,
    "burst_spike": _validate_burst_spike,
    "sustained_pressure": _validate_sustained_pressure,
    "consumer_freeze": _validate_consumer_freeze,
    "idle_but_delayed": _validate_idle_but_delayed,
    "offset_reset": _validate_offset_reset,
    "rebalance_noise": _validate_rebalance_noise,
    "partition_skew": _validate_partition_skew,
    "deploy_correlation": _validate_deploy_correlation,
    "error_correlation": _validate_error_correlation,
    "restart_continuity": _validate_restart_continuity,
    "webhook_redelivery": _validate_webhook_redelivery,
}


_SCENARIO_EXPECTATIONS: dict[str, ScenarioExpectation] = {
    "baseline_healthy_lag": ScenarioExpectation(phase=1, require_lifecycle_contract=True, require_delivery_contract=False, require_open="optional", require_resolve="optional", allow_zero_incidents=True, expect_webhook_capture=False),
    "sustained_pressure": ScenarioExpectation(phase=1, require_lifecycle_contract=True, require_delivery_contract=True, require_open="required", require_resolve="forbidden", expect_webhook_capture=True),
    "deploy_correlation": ScenarioExpectation(phase=1, require_lifecycle_contract=True, require_delivery_contract=True, require_open="required", require_resolve="optional", expect_webhook_capture=True),
    "restart_continuity": ScenarioExpectation(phase=1, require_lifecycle_contract=True, require_delivery_contract=True, require_open="required", require_resolve="forbidden", expect_webhook_capture=True),
    "webhook_redelivery": ScenarioExpectation(phase=1, require_lifecycle_contract=True, require_delivery_contract=True, require_open="required", require_resolve="forbidden", expect_webhook_capture=True, expect_redelivery=True),
    "burst_spike": ScenarioExpectation(phase=2, require_lifecycle_contract=True, require_delivery_contract=False, require_open="optional", require_resolve="optional"),
    "consumer_freeze": ScenarioExpectation(phase=2, require_lifecycle_contract=True, require_delivery_contract=False, require_open="required", require_resolve="forbidden"),
    "idle_but_delayed": ScenarioExpectation(phase=2, require_lifecycle_contract=True, require_delivery_contract=False, require_open="required", require_resolve="forbidden"),
    "offset_reset": ScenarioExpectation(phase=2, require_lifecycle_contract=True, require_delivery_contract=False, require_open="required", require_resolve="optional"),
    "rebalance_noise": ScenarioExpectation(phase=2, require_lifecycle_contract=True, require_delivery_contract=False, require_open="optional", require_resolve="optional"),
    "partition_skew": ScenarioExpectation(phase=2, require_lifecycle_contract=True, require_delivery_contract=False, require_open="optional", require_resolve="optional"),
    "error_correlation": ScenarioExpectation(phase=2, require_lifecycle_contract=True, require_delivery_contract=False, require_open="required", require_resolve="optional"),
}
