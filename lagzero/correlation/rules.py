from __future__ import annotations

from dataclasses import dataclass

from lagzero.correlation.schema import ExternalEvent
from lagzero.events.schema import IncidentEvent


@dataclass(frozen=True, slots=True)
class RuleCandidate:
    correlation_type: str
    event: ExternalEvent
    max_window_sec: float
    base_score: float
    time_diff_sec: float


def apply_rules(
    *,
    incident: IncidentEvent,
    events: list[ExternalEvent],
    deploy_window_sec: float,
    error_window_sec: float,
    rebalance_window_sec: float,
    infra_window_sec: float,
) -> list[RuleCandidate]:
    candidates: list[RuleCandidate] = []

    for event in events:
        time_diff_sec = incident.timestamp - event.timestamp
        if time_diff_sec < 0:
            continue

        if event.event_type == "deploy" and incident.anomaly in {
            "lag_spike",
            "system_under_pressure",
            "catching_up",
        }:
            if time_diff_sec <= deploy_window_sec and _scope_matches(incident, event):
                candidates.append(
                    RuleCandidate(
                        correlation_type="deploy_within_window",
                        event=event,
                        max_window_sec=deploy_window_sec,
                        base_score=0.55,
                        time_diff_sec=time_diff_sec,
                    )
                )

        if event.event_type == "error" and incident.anomaly in {
            "consumer_stalled",
            "lag_spike",
            "system_under_pressure",
        }:
            if time_diff_sec <= error_window_sec and _scope_matches(incident, event):
                candidates.append(
                    RuleCandidate(
                        correlation_type="error_nearby",
                        event=event,
                        max_window_sec=error_window_sec,
                        base_score=0.65,
                        time_diff_sec=time_diff_sec,
                    )
                )

        if event.event_type == "rebalance":
            if time_diff_sec <= rebalance_window_sec and _scope_matches(incident, event):
                candidates.append(
                    RuleCandidate(
                        correlation_type="rebalance_overlap",
                        event=event,
                        max_window_sec=rebalance_window_sec,
                        base_score=0.5,
                        time_diff_sec=time_diff_sec,
                    )
                )

        if event.event_type == "infra" and incident.severity in {"warning", "critical"}:
            if time_diff_sec <= infra_window_sec and _scope_matches(incident, event):
                candidates.append(
                    RuleCandidate(
                        correlation_type="infra_change_nearby",
                        event=event,
                        max_window_sec=infra_window_sec,
                        base_score=0.5,
                        time_diff_sec=time_diff_sec,
                    )
                )

    return candidates


def _scope_matches(incident: IncidentEvent, event: ExternalEvent) -> bool:
    if event.consumer_group and event.consumer_group == incident.consumer_group:
        return True
    if incident.topic is not None and event.topic and event.topic == incident.topic:
        if incident.partition is None or event.partition is None:
            return True
        return event.partition == incident.partition
    return False

