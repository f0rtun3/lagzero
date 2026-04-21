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
        lag_divergence_sec=None,
        lag_divergence_threshold_sec=120.0,
        time_lag_source="estimated",
        timestamp_type=None,
        catching_up=False,
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
        lag_divergence_sec=None,
        lag_divergence_threshold_sec=120.0,
        time_lag_source="timestamp",
        timestamp_type="log_append_time",
        catching_up=False,
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
        lag_divergence_sec=None,
        lag_divergence_threshold_sec=120.0,
        time_lag_source="estimated_fallback",
        timestamp_type=None,
        catching_up=False,
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
        lag_divergence_sec=None,
        lag_divergence_threshold_sec=120.0,
        time_lag_source="estimated_fallback",
        timestamp_type=None,
        catching_up=False,
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
        lag_divergence_sec=None,
        lag_divergence_threshold_sec=120.0,
        time_lag_source="estimated",
        timestamp_type=None,
        catching_up=False,
    )

    assert result.name == "offset_reset"


def test_detects_lag_estimation_mismatch() -> None:
    result = detect_anomaly(
        current_lag=10,
        previous_lag=9,
        processing_rate=20.0,
        stalled_intervals=2,
        idle_intervals=3,
        consecutive_zero_rate_intervals=0,
        consecutive_no_movement_intervals=0,
        lag_spike_multiplier=2.0,
        rate_variance_high=False,
        lag_velocity=1.0,
        no_offset_movement=False,
        state_reset=False,
        lag_divergence_sec=180.0,
        lag_divergence_threshold_sec=120.0,
        time_lag_source="timestamp",
        timestamp_type="create_time",
        catching_up=False,
    )

    assert result.name == "lag_estimation_mismatch"
    assert result.confidence == 0.5


def test_downgrades_cold_consumer_that_is_catching_up() -> None:
    result = detect_anomaly(
        current_lag=100,
        previous_lag=200,
        processing_rate=50.0,
        stalled_intervals=2,
        idle_intervals=3,
        consecutive_zero_rate_intervals=0,
        consecutive_no_movement_intervals=0,
        lag_spike_multiplier=2.0,
        rate_variance_high=False,
        lag_velocity=-10.0,
        no_offset_movement=False,
        state_reset=False,
        lag_divergence_sec=None,
        lag_divergence_threshold_sec=120.0,
        time_lag_source="estimated_fallback",
        timestamp_type=None,
        catching_up=True,
    )

    assert result.name == "catching_up"
    assert result.severity == "info"
