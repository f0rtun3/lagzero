from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from lagzero.lab.contracts import ScenarioValidation
from lagzero.lab.events import load_incidents
from lagzero.lab.harness import ChaosLabHarness, Scenario

_ALL_SCENARIOS = (
    "baseline_healthy_lag",
    "burst_spike",
    "sustained_pressure",
    "consumer_freeze",
    "idle_but_delayed",
    "offset_reset",
    "rebalance_noise",
    "partition_skew",
    "deploy_correlation",
    "error_correlation",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LagZero local Kafka chaos lab")
    parser.add_argument(
        "--artifact-root",
        default=".chaos/artifacts",
        help="Directory for per-scenario artifacts and incident logs.",
    )
    parser.add_argument(
        "--compose-file",
        default="docker-compose.yml",
        help="Path to the Docker Compose file.",
    )
    parser.add_argument(
        "--ingest-url",
        default="http://127.0.0.1:8787/events",
        help="LagZero ingest endpoint used for correlation scenarios.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("smoke", help="Run the smoke subset of scenarios.")
    subparsers.add_parser("down", help="Tear down the chaos lab stack.")
    subparsers.add_parser("scorecard", help="Summarize the latest artifact results.")

    scenario_parser = subparsers.add_parser("scenario", help="Run one named scenario.")
    scenario_parser.add_argument(
        "name",
        choices=list(_ALL_SCENARIOS),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    harness = ChaosLabHarness(
        repo_root=repo_root,
        compose_file=(repo_root / args.compose_file).resolve(),
        artifact_root=(repo_root / args.artifact_root).resolve(),
        ingest_url=args.ingest_url,
    )

    if args.command == "down":
        harness.down_stack()
        return 0

    if args.command == "scorecard":
        rows = []
        for scenario_name in _ALL_SCENARIOS:
            slug = scenario_name.replace("_", "-")
            artifact_dir = (repo_root / args.artifact_root / slug).resolve()
            report_path = artifact_dir / "contract-report.json"
            incidents_path = artifact_dir / "incidents.jsonl"
            record: dict[str, object] = {
                "scenario": scenario_name,
                "artifact_dir": str(artifact_dir),
                "contract_report_path": str(report_path),
                "incident_log_path": str(incidents_path),
            }
            if report_path.exists():
                data = json.loads(report_path.read_text(encoding="utf-8"))
                record.update(
                    {
                        "passed": bool(data.get("passed")),
                        "reason": data.get("reason"),
                        "final_incident": data.get("final_incident"),
                        "source": "contract-report.json",
                    }
                )
            elif incidents_path.exists():
                # Fallback: show raw artifact presence without pretending it's a verdict.
                record.update(
                    {
                        "passed": None,
                        "reason": "No contract report present; run the scenario to generate one.",
                        "final_incident": None,
                        "source": "missing",
                    }
                )
            else:
                record.update(
                    {
                        "passed": None,
                        "reason": "No artifacts present for this scenario.",
                        "final_incident": None,
                        "source": "missing",
                    }
                )
            rows.append(record)
        print(json.dumps({"rows": rows}, indent=2, sort_keys=True))
        return 0

    harness.up_base_stack()
    if args.command == "smoke":
        results: list[dict[str, object]] = []
        for scenario_name in (
            "baseline_healthy_lag",
            "burst_spike",
            "sustained_pressure",
            "deploy_correlation",
        ):
            results.append(_run_named_scenario(harness, scenario_name))
        summary_path = harness.write_smoke_summary(results)
        print(f"Wrote smoke summary to {summary_path}")
        return 0 if all(result.get("passed") for result in results) else 1

    result = _run_named_scenario(harness, args.name)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("passed") else 1


def _run_named_scenario(harness: ChaosLabHarness, scenario_name: str) -> dict[str, object]:
    if scenario_name == "baseline_healthy_lag":
        return _run_baseline(harness)
    elif scenario_name == "burst_spike":
        return _run_burst_spike(harness)
    elif scenario_name == "sustained_pressure":
        return _run_sustained_pressure(harness)
    elif scenario_name == "consumer_freeze":
        return _run_consumer_freeze(harness)
    elif scenario_name == "idle_but_delayed":
        return _run_idle_but_delayed(harness)
    elif scenario_name == "offset_reset":
        return _run_offset_reset(harness)
    elif scenario_name == "rebalance_noise":
        return _run_rebalance_noise(harness)
    elif scenario_name == "partition_skew":
        return _run_partition_skew(harness)
    elif scenario_name == "deploy_correlation":
        return _run_deploy_correlation(harness)
    elif scenario_name == "error_correlation":
        return _run_error_correlation(harness)
    raise ValueError(f"Unsupported scenario: {scenario_name}")


def _result_payload(scenario: Scenario, validation: ScenarioValidation) -> dict[str, object]:
    return {
        "scenario": scenario.name,
        "artifact_dir": str(scenario.artifact_dir),
        "incident_log_path": str(scenario.incident_log_path),
        "contract_report_path": str(scenario.contract_report_path),
        "passed": validation.passed,
        "reason": validation.reason,
        "final_incident": validation.final_incident,
    }


def _run_baseline(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("baseline_healthy_lag")
    consumer = harness.start_slow_consumer(scenario, delay_sec=0.2)
    try:
        harness.produce_perf(scenario, num_records=120, throughput=5)
        validation = harness.assert_contract(scenario)
        return _result_payload(scenario, validation)
    finally:
        consumer.terminate()


def _run_burst_spike(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("burst_spike")
    consumer = harness.start_slow_consumer(scenario, delay_sec=0.5)
    try:
        harness.produce_perf(scenario, num_records=4000, throughput=-1)
        validation = harness.assert_contract(scenario, timeout_sec=60.0)
        return _result_payload(scenario, validation)
    finally:
        consumer.terminate()


def _run_sustained_pressure(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("sustained_pressure")
    consumer = harness.start_slow_consumer(scenario, delay_sec=1.0)
    try:
        harness.produce_perf(scenario, num_records=2000, throughput=20)
        # Ensure we reach a clear severe end-state for the scenario contract within the lab time budget.
        harness.pause_consumer_runner()
        validation = harness.assert_contract(scenario, timeout_sec=60.0)
        return _result_payload(scenario, validation)
    finally:
        try:
            harness.unpause_consumer_runner()
        except Exception:
            pass
        consumer.terminate()


def _run_consumer_freeze(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("consumer_freeze")
    consumer = harness.start_slow_consumer(scenario, delay_sec=1.0)
    try:
        harness.produce_perf(scenario, num_records=400, throughput=20)
        harness.pause_consumer_runner()
        validation = harness.assert_contract(scenario, timeout_sec=60.0)
        return _result_payload(scenario, validation)
    finally:
        harness.unpause_consumer_runner()
        consumer.terminate()


def _run_idle_but_delayed(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("idle_but_delayed")
    consumer = harness.start_slow_consumer(scenario, delay_sec=2.0)
    try:
        harness.produce_perf(
            scenario,
            num_records=800,
            throughput=-1,
            producer_props={"partitioner.class": "org.apache.kafka.clients.producer.RoundRobinPartitioner"},
        )
        harness.pause_consumer_runner()
        validation = harness.assert_contract(scenario, timeout_sec=60.0)
        return _result_payload(scenario, validation)
    finally:
        harness.unpause_consumer_runner()
        consumer.terminate()


def _run_offset_reset(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("offset_reset")
    # Offset resets are only allowed for inactive groups; model the operator-action scenario directly by
    # not running a consumer for this group.
    harness.produce_perf(scenario, num_records=300, throughput=20)
    # Give LagZero a couple of polls to observe backlog before the reset happens.
    harness.sleep(4.0)
    harness.reset_offsets_to_latest(scenario)
    validation = harness.assert_contract(scenario, timeout_sec=60.0)
    return _result_payload(scenario, validation)


def _run_rebalance_noise(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("rebalance_noise")
    consumer = harness.start_slow_consumer(scenario, delay_sec=0.5)
    try:
        harness.produce_perf(scenario, num_records=400, throughput=10)
        harness.ingest_event(
            "rebalance",
            consumer_group=scenario.consumer_group,
            topic=scenario.topic,
            metadata={"reason": "consumer_restarted"},
        )
        validation = harness.assert_contract(scenario, timeout_sec=60.0)
        return _result_payload(scenario, validation)
    finally:
        consumer.terminate()


def _run_partition_skew(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("partition_skew")
    consumer = harness.start_slow_consumer(scenario, delay_sec=1.5)
    try:
        harness.produce_hotkey(scenario, key="hotkey", count=5000)
        validation = harness.assert_contract(scenario, timeout_sec=60.0)
        return _result_payload(scenario, validation)
    finally:
        consumer.terminate()


def _run_deploy_correlation(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("deploy_correlation")
    consumer = harness.start_slow_consumer(scenario, delay_sec=2.0)
    producer = harness.produce_perf_async(scenario, num_records=2000, throughput=20)
    try:
        time.sleep(5)
        harness.ingest_event(
            "deploy",
            service="orders-service",
            consumer_group=scenario.consumer_group,
            topic=scenario.topic,
            metadata={"version": "v1.2.3"},
        )
        validation = harness.assert_contract(scenario, timeout_sec=60.0)
        return _result_payload(scenario, validation)
    finally:
        producer.terminate()
        consumer.terminate()


def _run_error_correlation(harness: ChaosLabHarness) -> dict[str, object]:
    scenario = harness.prepare_scenario("error_correlation")
    consumer = harness.start_slow_consumer(scenario, delay_sec=2.0)
    producer = harness.produce_perf_async(scenario, num_records=2000, throughput=20)
    try:
        time.sleep(5)
        harness.ingest_event(
            "error",
            service="orders-service",
            consumer_group=scenario.consumer_group,
            topic=scenario.topic,
            severity="error",
            metadata={"error_type": "deserialization_failure"},
        )
        validation = harness.assert_contract(scenario, timeout_sec=60.0)
        return _result_payload(scenario, validation)
    finally:
        producer.terminate()
        consumer.terminate()


def latest_scenario_incidents(artifact_root: Path, scenario_name: str) -> list[dict[str, object]]:
    scenario_path = artifact_root / scenario_name.replace("_", "-") / "incidents.jsonl"
    return load_incidents(scenario_path)


if __name__ == "__main__":
    raise SystemExit(main())
