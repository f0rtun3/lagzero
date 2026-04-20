from lagzero.monitoring.anomaly import detect_anomaly


def test_detects_stalled_consumer() -> None:
    result = detect_anomaly(
        current_lag=50,
        previous_lag=40,
        processing_rate=0.0,
        stalled_intervals=2,
        idle_intervals=3,
        consecutive_zero_rate_intervals=2,
        consecutive_no_movement_intervals=0,
        lag_spike_multiplier=2.0,
        rate_variance_high=False,
        lag_velocity=None,
        no_offset_movement=False,
        state_reset=False,
    )

    assert result.name == "consumer_stalled"
    assert result.confidence == 0.2


def test_detects_lag_spike() -> None:
    result = detect_anomaly(
        current_lag=200,
        previous_lag=80,
        processing_rate=20.0,
        stalled_intervals=2,
        idle_intervals=3,
        consecutive_zero_rate_intervals=0,
        consecutive_no_movement_intervals=0,
        lag_spike_multiplier=2.0,
        rate_variance_high=False,
        lag_velocity=12.0,
        no_offset_movement=False,
        state_reset=False,
    )

    assert result.name == "lag_spike"
    assert result.confidence == 0.9


def test_returns_normal_when_lag_is_zero() -> None:
    result = detect_anomaly(
        current_lag=0,
        previous_lag=50,
        processing_rate=0.0,
        stalled_intervals=2,
        idle_intervals=3,
        consecutive_zero_rate_intervals=3,
        consecutive_no_movement_intervals=0,
        lag_spike_multiplier=2.0,
        rate_variance_high=False,
        lag_velocity=None,
        no_offset_movement=False,
        state_reset=False,
    )

    assert result.name == "normal"


def test_detects_idle_but_delayed() -> None:
    result = detect_anomaly(
        current_lag=30,
        previous_lag=30,
        processing_rate=0.0,
        stalled_intervals=5,
        idle_intervals=3,
        consecutive_zero_rate_intervals=1,
        consecutive_no_movement_intervals=3,
        lag_spike_multiplier=2.0,
        rate_variance_high=False,
        lag_velocity=0.0,
        no_offset_movement=True,
        state_reset=False,
    )

    assert result.name == "idle_but_delayed"


def test_detects_offset_reset() -> None:
    result = detect_anomaly(
        current_lag=10,
        previous_lag=50,
        processing_rate=None,
        stalled_intervals=2,
        idle_intervals=3,
        consecutive_zero_rate_intervals=0,
        consecutive_no_movement_intervals=0,
        lag_spike_multiplier=2.0,
        rate_variance_high=False,
        lag_velocity=None,
        no_offset_movement=False,
        state_reset=True,
    )

    assert result.name == "offset_reset"
