from lagzero.ai.context import build_explanation_context
from lagzero.ai.prompt import SYSTEM_PROMPT, build_user_prompt
from lagzero.events.schema import IncidentEvent


def make_incident(**overrides: object) -> IncidentEvent:
    payload = {
        "timestamp": 100.0,
        "consumer_group": "orders-consumer",
        "scope": "consumer_group",
        "topic": None,
        "partition": None,
        "offset_lag": 18250,
        "processing_rate": 320.0,
        "producer_rate": 900.0,
        "consumer_efficiency": 0.36,
        "backlog_growth_rate": 580.0,
        "time_lag_sec": 662.0,
        "time_lag_source": "timestamp",
        "timestamp_type": "log_append_time",
        "backlog_head_timestamp": 38.0,
        "latest_message_timestamp": 99.0,
        "lag_divergence_sec": 12.0,
        "lag_velocity": 18.0,
        "anomaly": "system_under_pressure",
        "severity": "critical",
        "service_health": "degraded",
        "confidence": 0.91,
        "correlations": [
            {
                "correlation_type": "deploy_within_window",
                "event_id": "evt-1",
                "event_type": "deploy",
                "source": "cli",
                "service": "orders-service",
                "confidence": 0.82,
                "time_diff_sec": 42.0,
                "role": "probable_cause",
                "metadata": {},
            }
        ],
        "primary_cause": "deploy_within_window",
        "primary_cause_confidence": 0.82,
        "ai_explanation": None,
        "diagnostics": {
            "transition_pending": False,
            "observed_anomaly": "system_under_pressure",
            "pending_anomaly": None,
            "correlation_candidates": ["deploy_within_window"],
            "internal_only": "ignored",
        },
    }
    payload.update(overrides)
    return IncidentEvent(**payload)


def test_build_explanation_context_selects_operator_relevant_fields() -> None:
    incident = make_incident()

    context = build_explanation_context(incident)

    assert context.consumer_group == "orders-consumer"
    assert context.anomaly == "system_under_pressure"
    assert context.health == "degraded"
    assert context.consumer_rate == 320.0
    assert context.primary_cause == "deploy_within_window"
    assert context.diagnostics == {
        "transition_pending": False,
        "timestamp_type": "log_append_time",
        "backlog_head_timestamp": 38.0,
        "latest_message_timestamp": 99.0,
        "observed_anomaly": "system_under_pressure",
        "pending_anomaly": None,
        "correlation_candidates": ["deploy_within_window"],
    }


def test_build_user_prompt_embeds_json_context_and_guardrails() -> None:
    incident = make_incident()
    context = build_explanation_context(incident)

    user_prompt = build_user_prompt(context)

    assert "Incident context:" in user_prompt
    assert '"consumer_group": "orders-consumer"' in user_prompt
    assert '"anomaly": "system_under_pressure"' in user_prompt
    assert "What most likely caused it" in user_prompt
    assert "recommended_actions" in SYSTEM_PROMPT
    assert "Do not invent causes" in SYSTEM_PROMPT
