from lagzero.monitoring.rate_calculator import compute_rate, rate_variance_high, smooth_rate


def test_compute_rate_returns_none_without_previous_state() -> None:
    sample = compute_rate(
        previous_committed_offset=None,
        current_committed_offset=200,
        previous_timestamp=None,
        current_timestamp=10.0,
    )

    assert sample.messages_per_second is None


def test_compute_rate_calculates_messages_per_second() -> None:
    sample = compute_rate(
        previous_committed_offset=100,
        current_committed_offset=160,
        previous_timestamp=10.0,
        current_timestamp=15.0,
    )

    assert sample.messages_per_second == 12.0
    assert sample.processed_messages == 60


def test_compute_rate_marks_state_reset_when_offset_moves_backwards() -> None:
    sample = compute_rate(
        previous_committed_offset=150,
        current_committed_offset=10,
        previous_timestamp=10.0,
        current_timestamp=15.0,
    )

    assert sample.messages_per_second is None
    assert sample.state_reset is True


def test_smooth_rate_uses_recent_window_average() -> None:
    assert smooth_rate([10.0, 20.0, 30.0, 40.0], window_size=3) == 30.0


def test_rate_variance_high_detects_bursty_rates() -> None:
    assert rate_variance_high([1.0, 20.0, 1.0], window_size=3) is True
