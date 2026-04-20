from lagzero.monitoring.anomaly import detect_anomaly


def test_detects_stalled_consumer() -> None:
    result = detect_anomaly(
        current_lag=50,
        previous_lag=40,
        processing_rate=0.0,
        stalled_intervals=2,
        consecutive_zero_rate_intervals=2,
        lag_spike_multiplier=2.0,
    )

    assert result.name == "consumer_stalled"


def test_detects_lag_spike() -> None:
    result = detect_anomaly(
        current_lag=200,
        previous_lag=80,
        processing_rate=20.0,
        stalled_intervals=2,
        consecutive_zero_rate_intervals=0,
        lag_spike_multiplier=2.0,
    )

    assert result.name == "lag_spike"


def test_returns_normal_when_lag_is_zero() -> None:
    result = detect_anomaly(
        current_lag=0,
        previous_lag=50,
        processing_rate=0.0,
        stalled_intervals=2,
        consecutive_zero_rate_intervals=3,
        lag_spike_multiplier=2.0,
    )

    assert result.name == "normal"
