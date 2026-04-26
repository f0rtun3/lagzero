from __future__ import annotations

from dataclasses import asdict
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from lagzero.lab.events import IncidentPayload, latest_group_incident, load_incidents


HEALTHY_ANOMALIES = {"normal", "bounded_lag"}
RECOVERY_ANOMALIES = {"catching_up"}
PRESSURE_ANOMALIES = {"lag_spike", "system_under_pressure"}
DELAY_ANOMALIES = {"idle_but_delayed"}
SEVERE_ANOMALIES = {"consumer_stalled"}
REBALANCE_ANOMALIES = {"rebalance_in_progress"}
SKEW_ANOMALIES = {"partition_skew"}
OPERATOR_ANOMALIES = {"offset_reset"}

HEALTHY_STATES = {"healthy"}
RECOVERY_STATES = {"recovering"}
DEGRADED_STATES = {"degraded"}
FAILING_STATES = {"failing"}

NEAR_ZERO_RATE = 0.1


@dataclass(frozen=True, slots=True)
class ScenarioValidation:
    passed: bool
    reason: str
    final_incident: IncidentPayload | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def wait_for_contract(
    path: str | Path,
    *,
    scenario_name: str,
    consumer_group: str,
    timeout_sec: float,
    poll_interval_sec: float = 1.0,
) -> ScenarioValidation:
    deadline = time.time() + timeout_sec
    latest_validation = ScenarioValidation(
        passed=False,
        reason="No incidents observed yet.",
        final_incident=None,
    )
    while time.time() < deadline:
        incidents = load_incidents(path)
        latest_validation = validate_scenario_contract(
            scenario_name=scenario_name,
            incidents=incidents,
            consumer_group=consumer_group,
        )
        if latest_validation.passed:
            return latest_validation
        time.sleep(poll_interval_sec)
    return ScenarioValidation(
        passed=False,
        reason=(
            f"Scenario `{scenario_name}` did not satisfy its contract within {timeout_sec} seconds. "
            f"{latest_validation.reason}"
        ),
        final_incident=latest_validation.final_incident,
    )


def validate_scenario_contract(
    *,
    scenario_name: str,
    incidents: list[IncidentPayload],
    consumer_group: str,
) -> ScenarioValidation:
    group_incidents = _group_incidents(incidents, consumer_group=consumer_group)
    if not group_incidents:
        return ScenarioValidation(False, "No consumer-group incidents emitted yet.", None)

    final_incident = _latest_stable_group_incident(group_incidents) or group_incidents[-1]
    validator = _SCENARIO_VALIDATORS.get(scenario_name)
    if validator is None:
        return ScenarioValidation(False, f"No contract registered for scenario `{scenario_name}`.", final_incident)
    return validator(group_incidents, final_incident, incidents)


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
    return latest_group_incident(incidents, consumer_group=str(incidents[-1].get("consumer_group")))


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


def _validate_baseline(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    final_anomaly = str(final_incident.get("anomaly"))
    final_health = final_incident.get("service_health")
    if any(incident.get("anomaly") in SEVERE_ANOMALIES for incident in group_incidents):
        return ScenarioValidation(False, "Baseline emitted a stable severe stall classification.", final_incident)
    if final_anomaly == "system_under_pressure":
        return ScenarioValidation(False, "Baseline ended in sustained pressure instead of a healthy family state.", final_incident)
    if final_anomaly in HEALTHY_ANOMALIES and final_health in HEALTHY_STATES:
        return ScenarioValidation(True, "Baseline ended in the healthy/non-incident family.", final_incident)
    return ScenarioValidation(False, f"Baseline final state `{final_anomaly}/{final_health}` is outside the healthy family.", final_incident)


def _validate_burst_spike(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    for incident in group_incidents:
        if incident.get("anomaly") in SEVERE_ANOMALIES and (_has_meaningful_progress(incident) or _producer_active(incident)):
            return ScenarioValidation(False, "Burst produced a stable `consumer_stalled` incident while progress or producer activity still existed.", final_incident)

    final_anomaly = str(final_incident.get("anomaly"))
    final_health = final_incident.get("service_health")
    if final_anomaly in HEALTHY_ANOMALIES and final_health in HEALTHY_STATES:
        return ScenarioValidation(True, "Burst recovered back into the healthy family.", final_incident)
    if final_anomaly in RECOVERY_ANOMALIES and final_health in RECOVERY_STATES:
        return ScenarioValidation(True, "Burst ended in an explicit recovery state.", final_incident)
    if final_anomaly in DELAY_ANOMALIES:
        if final_incident.get("offset_lag", 0) > 0 and _is_near_zero(final_incident.get("producer_rate")):
            return ScenarioValidation(True, "Burst ended as `idle_but_delayed` after producer activity stopped with backlog still present.", final_incident)
        return ScenarioValidation(False, "`idle_but_delayed` requires near-zero producer rate and remaining backlog.", final_incident)
    if final_anomaly in SEVERE_ANOMALIES:
        return ScenarioValidation(False, "Burst ended in severe failure instead of recovery, healthy, or justified dark-queue semantics.", final_incident)
    return ScenarioValidation(False, f"Burst final state `{final_anomaly}/{final_health}` is not in an allowed end-state family.", final_incident)


def _validate_sustained_pressure(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    final_anomaly = str(final_incident.get("anomaly"))
    final_health = final_incident.get("service_health")
    if final_anomaly in SEVERE_ANOMALIES and final_health in FAILING_STATES:
        return ScenarioValidation(True, "Sustained pressure ended in the severe failure family.", final_incident)
    if final_anomaly in HEALTHY_ANOMALIES and final_health in HEALTHY_STATES:
        return ScenarioValidation(False, "Sustained pressure incorrectly ended as healthy/non-incident.", final_incident)
    if final_anomaly in RECOVERY_ANOMALIES:
        return ScenarioValidation(False, "Sustained pressure incorrectly ended in recovery while backlog should still be unresolved.", final_incident)
    if any(
        incident.get("anomaly") in SEVERE_ANOMALIES and incident.get("service_health") in FAILING_STATES
        for incident in group_incidents
    ):
        return ScenarioValidation(True, "Sustained pressure reached eventual severe failure even before the latest artifact.", final_incident)
    return ScenarioValidation(False, "Sustained pressure has not yet escalated into a severe failure state.", final_incident)


def _validate_deploy_correlation(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    for incident in group_incidents:
        if incident.get("primary_cause") == "deploy_within_window" and _is_positive(incident.get("primary_cause_confidence")):
            if incident.get("anomaly") not in HEALTHY_ANOMALIES:
                return ScenarioValidation(True, "Deploy correlation was attached as the primary cause without changing detection semantics.", incident)
    return ScenarioValidation(False, "No incident carried `primary_cause=deploy_within_window` with non-zero confidence.", final_incident)


def _validate_error_correlation(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    for incident in group_incidents:
        if incident.get("primary_cause") == "error_nearby" and _is_positive(incident.get("primary_cause_confidence")):
            return ScenarioValidation(True, "Error correlation was attached as the primary cause.", incident)
    return ScenarioValidation(False, "No incident carried `primary_cause=error_nearby` with non-zero confidence.", final_incident)


def _validate_idle_but_delayed(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    final_anomaly = str(final_incident.get("anomaly"))
    if final_anomaly != "idle_but_delayed":
        return ScenarioValidation(False, f"Idle backlog final state was `{final_anomaly}` instead of `idle_but_delayed`.", final_incident)
    if final_incident.get("offset_lag", 0) <= 0:
        return ScenarioValidation(False, "`idle_but_delayed` requires unresolved positive lag.", final_incident)

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
        return ScenarioValidation(
            False,
            "`idle_but_delayed` requires producer inactivity visible at group level (null/near-zero producer_rate) or on the affected backlog partition.",
            final_incident,
        )
    if _has_meaningful_progress(final_incident):
        return ScenarioValidation(False, "`idle_but_delayed` should not report active draining progress.", final_incident)
    return ScenarioValidation(True, "Idle backlog was classified as `idle_but_delayed` with the expected supporting signals.", final_incident)


def _validate_offset_reset(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    final_anomaly = str(final_incident.get("anomaly"))
    if final_anomaly == "offset_reset" and float(final_incident.get("confidence", 0.0)) >= 0.5:
        return ScenarioValidation(True, "Offset reset was identified explicitly instead of being treated as natural recovery.", final_incident)
    return ScenarioValidation(False, "Offset reset did not end with explicit operator-action semantics.", final_incident)


def _validate_rebalance_noise(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    final_anomaly = str(final_incident.get("anomaly"))
    if final_anomaly in HEALTHY_ANOMALIES and final_incident.get("service_health") in HEALTHY_STATES:
        return ScenarioValidation(True, "Rebalance turbulence returned cleanly to a healthy state.", final_incident)
    if final_anomaly in REBALANCE_ANOMALIES:
        return ScenarioValidation(True, "Rebalance turbulence was surfaced explicitly.", final_incident)
    if final_anomaly in SEVERE_ANOMALIES:
        return ScenarioValidation(False, "Short rebalance turbulence ended as a severe failure.", final_incident)
    if any(
        match.get("correlation_type") == "rebalance_overlap"
        for incident in group_incidents
        for match in incident.get("correlations", [])
    ):
        return ScenarioValidation(True, "Rebalance turbulence was recognized through correlation without causing a false severe page.", final_incident)
    return ScenarioValidation(False, "Rebalance scenario did not return healthy or surface rebalance evidence.", final_incident)


def _validate_partition_skew(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    incidents: list[IncidentPayload],
) -> ScenarioValidation:
    if _partition_skew_visible(final_incident, incidents):
        return ScenarioValidation(True, "Partition skew remained visible in the emitted evidence.", final_incident)
    return ScenarioValidation(False, "Partition skew was flattened into a generic group issue with no explicit skew evidence.", final_incident)


def _validate_consumer_freeze(
    group_incidents: list[IncidentPayload],
    final_incident: IncidentPayload,
    _: list[IncidentPayload],
) -> ScenarioValidation:
    if final_incident.get("anomaly") in SEVERE_ANOMALIES | PRESSURE_ANOMALIES and final_incident.get("service_health") in DEGRADED_STATES | FAILING_STATES:
        return ScenarioValidation(True, "Consumer freeze escalated beyond harmless lag.", final_incident)
    return ScenarioValidation(False, "Consumer freeze did not escalate into pressure or failure semantics.", final_incident)


_SCENARIO_VALIDATORS: dict[
    str, Callable[[list[IncidentPayload], IncidentPayload, list[IncidentPayload]], ScenarioValidation]
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
}
