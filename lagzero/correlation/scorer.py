from __future__ import annotations

from lagzero.correlation.rules import RuleCandidate
from lagzero.correlation.schema import CorrelationMatch
from lagzero.events.schema import IncidentEvent


def score_match(candidate: RuleCandidate, incident: IncidentEvent) -> CorrelationMatch:
    time_score = max(0.0, 1.0 - (candidate.time_diff_sec / candidate.max_window_sec)) * 0.2
    scope_bonus = _scope_bonus(candidate, incident)
    confidence = min(candidate.base_score + scope_bonus + time_score, 1.0)

    if confidence >= 0.75:
        role = "probable_cause"
    elif confidence >= 0.5:
        role = "supporting_evidence"
    else:
        role = "contextual"

    return CorrelationMatch(
        correlation_type=candidate.correlation_type,
        event_id=candidate.event.event_id,
        event_type=candidate.event.event_type,
        source=candidate.event.source,
        service=candidate.event.service,
        confidence=round(confidence, 3),
        time_diff_sec=round(candidate.time_diff_sec, 3),
        role=role,
        metadata={
            "consumer_group": candidate.event.consumer_group,
            "topic": candidate.event.topic,
            "partition": candidate.event.partition,
            "severity": candidate.event.severity,
            **candidate.event.metadata,
        },
    )


def _scope_bonus(candidate: RuleCandidate, incident: IncidentEvent) -> float:
    bonus = 0.0
    event = candidate.event

    if event.consumer_group and event.consumer_group == incident.consumer_group:
        bonus += 0.15
    if incident.topic is not None and event.topic == incident.topic:
        bonus += 0.1
    if incident.partition is not None and event.partition == incident.partition:
        bonus += 0.1
    return bonus

