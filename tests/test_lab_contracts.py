from lagzero.lab.contracts import ScenarioValidation, validate_scenario_contract


def _group_event(
    *,
    anomaly: str,
    service_health: str,
    consumer_group: str = "payments",
    producer_rate: float | None = None,
    processing_rate: float | None = None,
    offset_lag: int = 100,
    primary_cause: str | None = None,
    primary_cause_confidence: float | None = None,
    correlations: list[dict[str, object]] | None = None,
    diagnostics: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "scope": "consumer_group",
        "consumer_group": consumer_group,
        "anomaly": anomaly,
        "service_health": service_health,
        "producer_rate": producer_rate,
        "processing_rate": processing_rate,
        "offset_lag": offset_lag,
        "primary_cause": primary_cause,
        "primary_cause_confidence": primary_cause_confidence,
        "correlations": correlations or [],
        "diagnostics": diagnostics or {"transition_pending": False},
    }


def _partition_event(
    *,
    anomaly: str,
    partition: int = 0,
    consumer_group: str = "payments",
) -> dict[str, object]:
    return {
        "scope": "partition",
        "consumer_group": consumer_group,
        "partition": partition,
        "anomaly": anomaly,
    }


def test_baseline_contract_accepts_healthy_final_state() -> None:
    result = validate_scenario_contract(
        scenario_name="baseline_healthy_lag",
        incidents=[
            _group_event(anomaly="bounded_lag", service_health="healthy"),
            _group_event(anomaly="normal", service_health="healthy"),
        ],
        consumer_group="payments",
    )

    assert result.passed is True


def test_burst_contract_rejects_stalled_with_active_producer() -> None:
    result = validate_scenario_contract(
        scenario_name="burst_spike",
        incidents=[
            _group_event(anomaly="system_under_pressure", service_health="degraded", producer_rate=20.0),
            _group_event(anomaly="consumer_stalled", service_health="failing", producer_rate=10.0, processing_rate=0.0),
        ],
        consumer_group="payments",
    )

    assert result.passed is False


def test_burst_contract_accepts_idle_but_delayed_end_state() -> None:
    result = validate_scenario_contract(
        scenario_name="burst_spike",
        incidents=[
            _group_event(anomaly="system_under_pressure", service_health="degraded", producer_rate=20.0),
            _group_event(anomaly="idle_but_delayed", service_health="degraded", producer_rate=0.0, processing_rate=0.0, offset_lag=50),
        ],
        consumer_group="payments",
    )

    assert result.passed is True


def test_sustained_pressure_contract_requires_eventual_failure() -> None:
    result = validate_scenario_contract(
        scenario_name="sustained_pressure",
        incidents=[
            _group_event(anomaly="system_under_pressure", service_health="degraded"),
            _group_event(anomaly="consumer_stalled", service_health="failing"),
        ],
        consumer_group="payments",
    )

    assert result.passed is True


def test_deploy_correlation_contract_requires_primary_cause() -> None:
    result = validate_scenario_contract(
        scenario_name="deploy_correlation",
        incidents=[
            _group_event(
                anomaly="system_under_pressure",
                service_health="degraded",
                primary_cause="deploy_within_window",
                primary_cause_confidence=0.82,
            ),
        ],
        consumer_group="payments",
    )

    assert result.passed is True


def test_partition_skew_contract_requires_visible_partition_truth() -> None:
    result = validate_scenario_contract(
        scenario_name="partition_skew",
        incidents=[
            _group_event(anomaly="system_under_pressure", service_health="degraded"),
            _partition_event(anomaly="partition_skew", partition=3),
        ],
        consumer_group="payments",
    )

    assert result.passed is True


def test_scenario_validation_can_be_serialized_for_artifacts() -> None:
    validation = ScenarioValidation(
        passed=True,
        reason="contract satisfied",
        final_incident={"scope": "consumer_group", "anomaly": "normal"},
    )

    assert validation.to_dict()["passed"] is True
    assert validation.to_dict()["final_incident"]["anomaly"] == "normal"
