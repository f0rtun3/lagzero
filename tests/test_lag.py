from lagzero.monitoring.lag_calculator import compute_lag


def test_compute_lag_returns_difference() -> None:
    assert compute_lag(latest_offset=250, committed_offset=200) == 50


def test_compute_lag_never_goes_negative() -> None:
    assert compute_lag(latest_offset=100, committed_offset=120) == 0

