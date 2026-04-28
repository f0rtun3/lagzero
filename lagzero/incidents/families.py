from __future__ import annotations


INCIDENT_FAMILIES: dict[str, str] = {
    "lag_spike": "pressure",
    "system_under_pressure": "pressure",
    "slow_consumer": "pressure",
    "catching_up": "pressure",
    "consumer_stalled": "stall",
    "idle_but_delayed": "delay",
    "rebalance_in_progress": "rebalance",
    "partition_skew": "topology",
    "offset_reset": "operator_action",
}


def incident_family_for_anomaly(anomaly: str | None) -> str | None:
    if anomaly is None:
        return None
    return INCIDENT_FAMILIES.get(anomaly)
