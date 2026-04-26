from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Any

from lagzero.ai.client import DisabledLLMClient, LLMClient
from lagzero.ai.context import build_explanation_context
from lagzero.ai.formatter import fallback_explanation, parse_explanation_response
from lagzero.ai.prompt import SYSTEM_PROMPT, build_user_prompt
from lagzero.ai.schema import AIExplanation
from lagzero.events.schema import IncidentEvent


@dataclass(slots=True)
class CachedExplanation:
    explanation: AIExplanation
    created_at: float


@dataclass(slots=True)
class IncidentExplainer:
    llm_client: LLMClient
    enabled: bool
    cache_ttl_sec: float
    cache: dict[tuple[Any, ...], CachedExplanation] = field(default_factory=dict)

    def explain(self, incident: IncidentEvent) -> IncidentEvent:
        if not self.should_explain(incident):
            return incident

        context = build_explanation_context(incident)
        fingerprint = self.fingerprint(context)
        cached = self.cache.get(fingerprint)
        now = incident.timestamp or time.time()
        if cached is not None and (now - cached.created_at) <= self.cache_ttl_sec:
            return self._attach_explanation(incident, cached.explanation, cache_hit=True)

        system_prompt = SYSTEM_PROMPT
        user_prompt = build_user_prompt(context)

        try:
            response = self.llm_client.generate(system_prompt, user_prompt)
            explanation = parse_explanation_response(response)
        except Exception as exc:
            explanation = fallback_explanation(
                f"The AI explanation request failed; use deterministic fields for diagnosis. ({exc})"
            )

        self.cache[fingerprint] = CachedExplanation(explanation=explanation, created_at=now)
        self._prune_cache(now)
        return self._attach_explanation(incident, explanation, cache_hit=False)

    def should_explain(self, incident: IncidentEvent) -> bool:
        if not self.enabled:
            return False
        if isinstance(self.llm_client, DisabledLLMClient):
            return False
        if (incident.anomaly or "normal") == "normal":
            return False
        if incident.diagnostics.get("transition_pending") is True:
            return False
        if incident.scope == "partition" and incident.severity not in {"warning", "critical"}:
            return False
        return True

    def fingerprint(self, context: Any) -> tuple[Any, ...]:
        return (
            context.scope,
            context.consumer_group,
            context.topic,
            context.partition,
            context.anomaly,
            context.health,
            context.primary_cause,
            context.time_lag_source,
            int((context.time_lag_sec or 0) // 60),
            tuple(
                (match.get("correlation_type"), match.get("event_id"))
                for match in context.correlations
            ),
        )

    def _attach_explanation(
        self,
        incident: IncidentEvent,
        explanation: AIExplanation,
        *,
        cache_hit: bool,
    ) -> IncidentEvent:
        return replace(
            incident,
            ai_explanation=explanation.to_dict(),
            diagnostics={
                **incident.diagnostics,
                "ai_explanation_cache_hit": cache_hit,
            },
        )

    def _prune_cache(self, reference_time: float) -> None:
        expired_keys = [
            key
            for key, cached in self.cache.items()
            if (reference_time - cached.created_at) > self.cache_ttl_sec
        ]
        for key in expired_keys:
            self.cache.pop(key, None)
