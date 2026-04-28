from __future__ import annotations

import hashlib
import json
from typing import Any


def confidence_bucket(confidence: float | None) -> str:
    if confidence is None:
        return "none"
    if confidence >= 0.85:
        return "high"
    if confidence >= 0.5:
        return "medium"
    return "low"


def time_lag_bucket(time_lag_sec: float | None) -> str:
    if time_lag_sec is None or time_lag_sec <= 0:
        return "0"
    if time_lag_sec <= 30:
        return "1-30s"
    if time_lag_sec <= 120:
        return "30-120s"
    if time_lag_sec <= 600:
        return "2-10m"
    return "10m+"


def explanation_fingerprint(payload: dict[str, Any]) -> str | None:
    explanation = payload.get("ai_explanation")
    if explanation is None:
        return None
    raw = json.dumps(explanation, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def correlations_fingerprint(payload: dict[str, Any]) -> str:
    correlations = payload.get("correlations") or []
    normalized = [
        {
            "correlation_type": match.get("correlation_type"),
            "event_id": match.get("event_id"),
            "role": match.get("role"),
        }
        for match in correlations
    ]
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def material_change_types(
    previous_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> list[str]:
    changes: list[str] = []
    if previous_payload.get("anomaly") != current_payload.get("anomaly"):
        changes.append("anomaly_changed")
    if previous_payload.get("service_health") != current_payload.get("service_health"):
        changes.append("health_changed")
    if previous_payload.get("severity") != current_payload.get("severity"):
        changes.append("severity_changed")
    if previous_payload.get("primary_cause") != current_payload.get("primary_cause"):
        changes.append("cause_changed")
    if confidence_bucket(previous_payload.get("primary_cause_confidence")) != confidence_bucket(
        current_payload.get("primary_cause_confidence")
    ):
        changes.append("cause_confidence_changed")
    if time_lag_bucket(previous_payload.get("time_lag_sec")) != time_lag_bucket(
        current_payload.get("time_lag_sec")
    ):
        changes.append("lag_bucket_changed")
    if explanation_fingerprint(previous_payload) != explanation_fingerprint(current_payload):
        changes.append("ai_explanation_updated")
    if correlations_fingerprint(previous_payload) != correlations_fingerprint(current_payload):
        changes.append("correlation_changed")
    return changes
