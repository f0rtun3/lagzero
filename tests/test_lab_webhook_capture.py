from lagzero.lab.webhook_capture import build_capture_record, verify_capture_signature
from lagzero.sinks.signing import build_signature


def test_verify_capture_signature_accepts_valid_signature() -> None:
    raw_body = '{"event_id":"evt-1"}'
    timestamp = "2026-04-29T00:00:00Z"
    signature = build_signature("secret", timestamp=timestamp, raw_body=raw_body)

    result = verify_capture_signature(
        raw_body=raw_body,
        timestamp=timestamp,
        signature=signature,
        secret="secret",
    )

    assert result["valid"] is True
    assert result["error"] is None


def test_build_capture_record_marks_invalid_signature() -> None:
    record = build_capture_record(
        headers={
            "X-LagZero-Timestamp": "2026-04-29T00:00:00Z",
            "X-LagZero-Signature": "sha256=bad",
        },
        raw_body='{"event_id":"evt-1"}',
        payload={"event_id": "evt-1", "event_type": "incident.opened", "incident": {"incident_id": "inc-1"}},
        secret="secret",
    )

    assert record["signature_valid"] is False
    assert record["signature_error"] == "signature_mismatch"
