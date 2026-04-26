from lagzero.monitoring.anomaly import detect_anomaly, health_for_anomaly, resolve_anomaly


def _detect(**overrides):
    payload = {
        "current_lag": 50,
        "previous_lag": 40,
        "processing_rate": 10.0,
        "stalled_intervals": 2,
        "idle_intervals": 3,
        "consecutive_zero_rate_intervals": 0,
        "consecutive_no_movement_intervals": 0,
        "lag_spike_multiplier": 2.0,
        "rate_variance_high": False,
        "lag_velocity": None,
        "no_offset_movement": False,
        "state_reset": False,
        "lag_divergence_sec": None,
        "lag_divergence_threshold_sec": 120.0,
        "time_lag_source": "estimated",
        "timestamp_type": None,
        "catching_up": False,
        "time_lag_sec": 60.0,
        "producer_rate": None,
        "backlog_growth_rate": None,
        "consumer_efficiency": None,
        "lag_decreasing": False,
        "backlog_growth_rate_improving": False,
        "recent_lag_spike_active": False,
        "min_incident_offset_lag": 25,
        "min_incident_time_lag_sec": 15.0,
        "min_stalled_offset_lag": 50,
        "min_stalled_time_lag_sec": 30.0,
        "min_lag_spike_delta": 25,
        "slow_consumer_efficiency_threshold": 0.8,
    }
    payload.update(overrides)
    return detect_anomaly(**payload)


def test_detects_stalled_consumer() -> None:
    result = _detect(
        processing_rate=0.0,
        consecutive_zero_rate_intervals=2,
        no_offset_movement=True,
    )

    assert result.name == "consumer_stalled"
    assert result.confidence == 0.2


def test_detects_lag_spike() -> None:
    result = _detect(
        current_lag=200,
        previous_lag=80,
        processing_rate=20.0,
        lag_velocity=12.0,
        time_lag_source="timestamp",
        timestamp_type="log_append_time",
        producer_rate=15.0,
        backlog_growth_rate=-5.0,
        min_lag_spike_delta=25,
    )

    assert result.name == "lag_spike"
    assert result.confidence == 0.9


def test_returns_normal_when_lag_is_zero() -> None:
    result = _detect(
        current_lag=0,
        processing_rate=0.0,
        consecutive_zero_rate_intervals=3,
        time_lag_source="estimated_fallback",
    )

    assert result.name == "normal"


def test_detects_idle_but_delayed() -> None:
    result = _detect(
        current_lag=30,
        previous_lag=30,
        processing_rate=0.0,
        idle_intervals=3,
        consecutive_zero_rate_intervals=1,
        consecutive_no_movement_intervals=3,
        lag_velocity=0.0,
        no_offset_movement=True,
        time_lag_source="estimated_fallback",
    )

    assert result.name == "idle_but_delayed"


def test_detects_offset_reset() -> None:
    result = _detect(
        current_lag=10,
        processing_rate=None,
        state_reset=True,
    )

    assert result.name == "offset_reset"


def test_detects_lag_estimation_mismatch() -> None:
    result = _detect(
        current_lag=10,
        previous_lag=9,
        processing_rate=20.0,
        lag_velocity=1.0,
        lag_divergence_sec=180.0,
        time_lag_source="timestamp",
        timestamp_type="create_time",
        time_lag_sec=180.0,
        producer_rate=10.0,
        backlog_growth_rate=-10.0,
    )

    assert result.name == "lag_estimation_mismatch"
    assert result.confidence == 0.5


def test_prefers_catching_up_when_backlog_is_shrinking() -> None:
    result = _detect(
        current_lag=100,
        previous_lag=200,
        processing_rate=50.0,
        lag_velocity=-10.0,
        time_lag_source="estimated_fallback",
        catching_up=True,
        time_lag_sec=45.0,
        producer_rate=40.0,
        backlog_growth_rate=-10.0,
        consumer_efficiency=1.25,
        lag_decreasing=True,
    )

    assert result.name == "catching_up"
    assert result.severity == "info"


def test_detects_system_under_pressure() -> None:
    result = _detect(
        current_lag=100,
        previous_lag=90,
        processing_rate=20.0,
        lag_velocity=1.0,
        producer_rate=50.0,
        backlog_growth_rate=30.0,
    )

    assert result.name == "system_under_pressure"


def test_uses_bounded_lag_below_incident_floor() -> None:
    result = _detect(
        current_lag=10,
        previous_lag=8,
        processing_rate=1.0,
        time_lag_sec=5.0,
    )

    assert result.name == "bounded_lag"


def test_detects_slow_consumer_before_stalled() -> None:
    result = _detect(
        current_lag=80,
        previous_lag=70,
        processing_rate=5.0,
        producer_rate=None,
        backlog_growth_rate=1.0,
        consumer_efficiency=0.5,
    )

    assert result.name == "slow_consumer"


def test_lag_spike_requires_absolute_delta_floor() -> None:
    result = _detect(
        current_lag=20,
        previous_lag=10,
        processing_rate=20.0,
        lag_velocity=10.0,
        time_lag_sec=10.0,
    )

    assert result.name == "bounded_lag"


def test_burst_grace_suppresses_stalled_when_growth_is_improving() -> None:
    result = _detect(
        current_lag=120,
        previous_lag=140,
        processing_rate=0.0,
        consecutive_zero_rate_intervals=2,
        no_offset_movement=True,
        lag_decreasing=True,
        backlog_growth_rate=5.0,
        backlog_growth_rate_improving=True,
        recent_lag_spike_active=True,
    )

    assert result.name != "consumer_stalled"


def test_priority_model_prefers_more_severe_anomaly_over_recovery() -> None:
    assert resolve_anomaly(["catching_up", "lag_spike", "system_under_pressure"]) == "system_under_pressure"


def test_health_is_derived_explicitly_from_anomaly() -> None:
    assert health_for_anomaly("consumer_stalled") == "failing"
    assert health_for_anomaly("slow_consumer") == "degraded"
    assert health_for_anomaly("bounded_lag") == "healthy"
    assert health_for_anomaly("system_under_pressure") == "degraded"
    assert health_for_anomaly("catching_up") == "recovering"
    assert health_for_anomaly("normal") == "healthy"
