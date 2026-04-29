import json
import threading
import time

from lagzero.lab.events import (
    append_jsonl,
    clear_incident_log,
    clear_jsonl,
    latest_group_incident,
    load_incidents,
    load_jsonl,
    wait_for_incident,
)


def test_clear_and_load_incidents(tmp_path) -> None:
    path = tmp_path / "incidents.jsonl"
    clear_incident_log(path)
    path.write_text(
        "\n".join(
            [
                json.dumps({"scope": "partition", "consumer_group": "payments", "anomaly": "lag_spike"}),
                json.dumps({"scope": "consumer_group", "consumer_group": "payments", "anomaly": "normal"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    incidents = load_incidents(path)

    assert len(incidents) == 2
    assert latest_group_incident(incidents, consumer_group="payments") == incidents[1]


def test_wait_for_incident_returns_matching_payload(tmp_path) -> None:
    path = tmp_path / "incidents.jsonl"
    clear_incident_log(path)

    def writer() -> None:
        time.sleep(0.2)
        path.write_text(
            json.dumps({"scope": "consumer_group", "consumer_group": "payments", "anomaly": "system_under_pressure"}) + "\n",
            encoding="utf-8",
        )

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        incident = wait_for_incident(
            path,
            lambda event: event.get("anomaly") == "system_under_pressure",
            timeout_sec=2.0,
            poll_interval_sec=0.1,
        )
    finally:
        thread.join()

    assert incident["consumer_group"] == "payments"


def test_generic_jsonl_helpers_append_and_load(tmp_path) -> None:
    path = tmp_path / "items.jsonl"
    clear_jsonl(path)
    append_jsonl(path, {"event_id": "evt-1"})
    append_jsonl(path, {"event_id": "evt-2"})

    items = load_jsonl(path)

    assert [item["event_id"] for item in items] == ["evt-1", "evt-2"]
