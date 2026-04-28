from lagzero.incidents.families import incident_family_for_anomaly


def test_incident_family_mapping_covers_core_anomalies() -> None:
    assert incident_family_for_anomaly("lag_spike") == "pressure"
    assert incident_family_for_anomaly("system_under_pressure") == "pressure"
    assert incident_family_for_anomaly("catching_up") == "pressure"
    assert incident_family_for_anomaly("consumer_stalled") == "stall"
    assert incident_family_for_anomaly("idle_but_delayed") == "delay"
    assert incident_family_for_anomaly("partition_skew") == "topology"
    assert incident_family_for_anomaly("offset_reset") == "operator_action"
    assert incident_family_for_anomaly("normal") is None
