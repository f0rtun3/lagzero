from __future__ import annotations

import json

from lagzero.ai.schema import ExplanationContext

SYSTEM_PROMPT = """You are an incident explanation assistant for Kafka-based systems.

Your job is to explain incidents using only the structured evidence provided.
Do not invent causes, services, errors, infrastructure changes, or remediation steps not supported by the input.
Do not change the anomaly classification or health state.
If evidence is weak or ambiguous, say so clearly.
Prefer concise operational language.
Return valid JSON with keys:
summary, probable_cause_explanation, impact, recommended_actions, caveats."""


def build_user_prompt(context: ExplanationContext) -> str:
    context_json = json.dumps(context.to_dict(), sort_keys=True)
    return (
        f"Incident context:\n{context_json}\n\n"
        "Explain:\n"
        "1. What is happening\n"
        "2. What most likely caused it\n"
        "3. What impact it may have\n"
        "4. What the operator should check next\n"
        "5. Any caveats due to evidence quality\n"
    )

