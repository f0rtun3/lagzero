from __future__ import annotations

import json

from lagzero.ai.schema import AIExplanation


def parse_explanation_response(response: str) -> AIExplanation:
    try:
        payload = json.loads(response)
    except json.JSONDecodeError:
        return fallback_explanation(
            "The AI explanation response was malformed; use deterministic fields for diagnosis."
        )

    return AIExplanation(
        summary=str(payload.get("summary") or "Incident detected, but explanation could not be generated reliably."),
        probable_cause_explanation=str(
            payload.get("probable_cause_explanation")
            or "The structured evidence should be used directly to determine probable cause."
        ),
        impact=str(payload.get("impact") or "Refer to the deterministic incident metrics for impact."),
        recommended_actions=_coerce_str_list(
            payload.get("recommended_actions"),
            default=["Inspect the structured incident fields directly."],
        ),
        caveats=_coerce_str_list(
            payload.get("caveats"),
            default=["AI explanation output was incomplete."],
        ),
    )


def fallback_explanation(cause: str) -> AIExplanation:
    return AIExplanation(
        summary="Incident detected, but explanation could not be generated reliably.",
        probable_cause_explanation=cause,
        impact="Refer to the incident metrics and primary cause fields.",
        recommended_actions=["Inspect the structured incident fields directly."],
        caveats=["AI explanation parsing failed."],
    )


def _coerce_str_list(value: object, *, default: list[str]) -> list[str]:
    if not isinstance(value, list):
        return default
    items = [str(item) for item in value if str(item).strip()]
    return items or default

