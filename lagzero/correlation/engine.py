from __future__ import annotations

from dataclasses import replace

from lagzero.correlation.rules import apply_rules
from lagzero.correlation.scorer import score_match
from lagzero.correlation.store import CorrelationEventStore
from lagzero.events.schema import IncidentEvent


class CorrelationEngine:
    def __init__(
        self,
        store: CorrelationEventStore,
        *,
        deploy_window_sec: float,
        error_window_sec: float,
        rebalance_window_sec: float,
        infra_window_sec: float,
        max_correlations: int,
    ) -> None:
        self._store = store
        self._deploy_window_sec = deploy_window_sec
        self._error_window_sec = error_window_sec
        self._rebalance_window_sec = rebalance_window_sec
        self._infra_window_sec = infra_window_sec
        self._max_correlations = max_correlations

    def enrich(self, incident: IncidentEvent) -> IncidentEvent:
        recent_events = self._store.query_recent(incident.timestamp)
        candidates = apply_rules(
            incident=incident,
            events=recent_events,
            deploy_window_sec=self._deploy_window_sec,
            error_window_sec=self._error_window_sec,
            rebalance_window_sec=self._rebalance_window_sec,
            infra_window_sec=self._infra_window_sec,
        )
        matches = sorted(
            (score_match(candidate, incident) for candidate in candidates),
            key=lambda match: match.confidence,
            reverse=True,
        )[: self._max_correlations]

        primary = matches[0] if matches else None

        return replace(
            incident,
            correlations=[match.to_dict() for match in matches],
            primary_cause=primary.correlation_type if primary else None,
            primary_cause_confidence=primary.confidence if primary else None,
            diagnostics={
                **incident.diagnostics,
                "correlation_candidates": len(candidates),
            },
        )

