from lagzero.ai.formatter import fallback_explanation, parse_explanation_response


def test_parse_explanation_response_reads_valid_json() -> None:
    explanation = parse_explanation_response(
        """
        {
          "summary": "Consumers are behind producer load.",
          "probable_cause_explanation": "A deploy is the strongest nearby cause.",
          "impact": "Processing is delayed.",
          "recommended_actions": ["Check the deploy", "Review error rate"],
          "caveats": ["Lag is estimated from timestamps."]
        }
        """
    )

    assert explanation.summary == "Consumers are behind producer load."
    assert explanation.recommended_actions == ["Check the deploy", "Review error rate"]
    assert explanation.caveats == ["Lag is estimated from timestamps."]


def test_parse_explanation_response_falls_back_on_malformed_json() -> None:
    explanation = parse_explanation_response("not-json")

    assert explanation.summary == "Incident detected, but explanation could not be generated reliably."
    assert explanation.caveats == ["AI explanation parsing failed."]


def test_fallback_explanation_is_conservative() -> None:
    explanation = fallback_explanation("Provider timeout")

    assert explanation.probable_cause_explanation == "Provider timeout"
    assert explanation.recommended_actions == ["Inspect the structured incident fields directly."]
