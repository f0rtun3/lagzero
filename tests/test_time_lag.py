from lagzero.monitoring.time_lag import compute_offset_time_lag, compute_time_lag


def test_compute_time_lag_returns_estimate() -> None:
    assert compute_offset_time_lag(offset_lag=120, processing_rate=30.0) == 4.0


def test_compute_time_lag_returns_none_for_zero_or_missing_rate() -> None:
    assert compute_offset_time_lag(offset_lag=120, processing_rate=0.0) is None
    assert compute_offset_time_lag(offset_lag=120, processing_rate=None) is None


def test_compute_time_lag_prefers_timestamp_correction_when_available() -> None:
    estimate = compute_time_lag(
        offset_lag=120,
        processing_rate=30.0,
        observed_at=200.0,
        backlog_head_timestamp=150.0,
        timestamp_sampling_state="timestamp",
        timestamp_type="create_time",
        lag_divergence_threshold_sec=10.0,
    )

    assert estimate.seconds == 50.0
    assert estimate.source == "timestamp"
    assert estimate.offset_based_seconds == 4.0
    assert estimate.timestamp_based_seconds == 50.0
    assert estimate.timestamp_type == "create_time"
    assert estimate.lag_divergence_sec == 46.0
    assert estimate.is_divergent is True


def test_compute_time_lag_falls_back_to_offset_rate_when_needed() -> None:
    estimate = compute_time_lag(
        offset_lag=120,
        processing_rate=30.0,
        observed_at=200.0,
        backlog_head_timestamp=None,
        timestamp_sampling_state="unavailable",
        timestamp_type=None,
        lag_divergence_threshold_sec=10.0,
    )

    assert estimate.seconds == 4.0
    assert estimate.source == "estimated"


def test_compute_time_lag_marks_sampling_failure_as_estimated_fallback() -> None:
    estimate = compute_time_lag(
        offset_lag=120,
        processing_rate=30.0,
        observed_at=200.0,
        backlog_head_timestamp=None,
        timestamp_sampling_state="sampling_failed",
        timestamp_type=None,
        lag_divergence_threshold_sec=10.0,
    )

    assert estimate.seconds == 4.0
    assert estimate.source == "estimated_fallback"
