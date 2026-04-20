from lagzero.monitoring.time_lag import compute_time_lag


def test_compute_time_lag_returns_estimate() -> None:
    assert compute_time_lag(offset_lag=120, processing_rate=30.0) == 4.0


def test_compute_time_lag_returns_none_for_zero_or_missing_rate() -> None:
    assert compute_time_lag(offset_lag=120, processing_rate=0.0) is None
    assert compute_time_lag(offset_lag=120, processing_rate=None) is None

