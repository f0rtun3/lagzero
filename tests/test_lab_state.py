from lagzero.lab.state import build_lifecycle_events, build_logical_incident_key


def test_build_lifecycle_events_maps_timeline_to_lifecycle_event_types() -> None:
    state_snapshot = {
        "incidents": [
            {
                "incident_id": "inc-1",
                "incident_key": "consumer_group:payments:pressure",
                "family": "pressure",
            }
        ],
        "timeline": [
            {
                "timeline_id": "tl-1",
                "incident_id": "inc-1",
                "entry_type": "incident_opened",
                "at": 1.0,
                "summary": "opened",
                "details_json": {"anomaly": "system_under_pressure"},
            },
            {
                "timeline_id": "tl-2",
                "incident_id": "inc-1",
                "entry_type": "health_changed",
                "at": 2.0,
                "summary": "updated",
                "details_json": {"changes": ["health_changed"]},
            },
            {
                "timeline_id": "tl-3",
                "incident_id": "inc-1",
                "entry_type": "incident_resolved",
                "at": 3.0,
                "summary": "resolved",
                "details_json": {"service_health": "healthy"},
            },
        ],
        "deliveries": [],
    }

    events = build_lifecycle_events(state_snapshot)

    assert [event["event_type"] for event in events] == [
        "incident.opened",
        "incident.updated",
        "incident.resolved",
    ]
    assert events[0]["logical_incident_key"] == build_logical_incident_key(
        incident_key="consumer_group:payments:pressure",
        family="pressure",
    )
