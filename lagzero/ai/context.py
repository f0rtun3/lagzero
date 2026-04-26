from __future__ import annotations

from lagzero.ai.schema import ExplanationContext
from lagzero.events.schema import IncidentEvent


def build_explanation_context(incident: IncidentEvent) -> ExplanationContext:
    return ExplanationContext(
        timestamp=incident.timestamp,
        scope=incident.scope,
        consumer_group=incident.consumer_group,
        topic=incident.topic,
        partition=incident.partition,
        anomaly=incident.anomaly or "normal",
        health=incident.service_health or "healthy",
        severity=incident.severity,
        confidence=incident.confidence,
        offset_lag=incident.offset_lag,
        time_lag_sec=incident.time_lag_sec,
        time_lag_source=incident.time_lag_source,
        lag_divergence_sec=incident.lag_divergence_sec,
        producer_rate=incident.producer_rate,
        consumer_rate=incident.processing_rate,
        backlog_growth_rate=incident.backlog_growth_rate,
        consumer_efficiency=incident.consumer_efficiency,
        primary_cause=incident.primary_cause,
        primary_cause_confidence=incident.primary_cause_confidence,
        correlations=incident.correlations,
        diagnostics={
            "transition_pending": incident.diagnostics.get("transition_pending"),
            "timestamp_type": incident.timestamp_type,
            "backlog_head_timestamp": incident.backlog_head_timestamp,
            "latest_message_timestamp": incident.latest_message_timestamp,
            "observed_anomaly": incident.diagnostics.get("observed_anomaly"),
            "pending_anomaly": incident.diagnostics.get("pending_anomaly"),
            "correlation_candidates": incident.diagnostics.get("correlation_candidates"),
        },
    )

