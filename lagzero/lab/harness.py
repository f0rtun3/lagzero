from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from lagzero.lab.contracts import ScenarioValidation, wait_for_contract
from lagzero.lab.events import clear_incident_log, wait_for_incident

KAFKA_BIN_DIR = "/opt/bitnami/kafka/bin"


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    topic: str
    consumer_group: str
    artifact_dir: Path

    @property
    def incident_log_path(self) -> Path:
        return self.artifact_dir / "incidents.jsonl"

    @property
    def contract_report_path(self) -> Path:
        return self.artifact_dir / "contract-report.json"


class ChaosLabHarness:
    def __init__(
        self,
        *,
        repo_root: Path,
        compose_file: Path,
        artifact_root: Path,
        ingest_url: str,
    ) -> None:
        self._repo_root = repo_root
        self._compose_file = compose_file
        self._artifact_root = artifact_root
        self._ingest_url = ingest_url
        self._artifact_root.mkdir(parents=True, exist_ok=True)

    def up_base_stack(self) -> None:
        self._compose(["up", "-d", "kafka", "consumer-runner"])

    def down_stack(self) -> None:
        self._compose(["down", "-v", "--remove-orphans"])

    def prepare_scenario(self, name: str) -> Scenario:
        slug = name.replace("_", "-")
        scenario = Scenario(
            name=name,
            topic=f"orders-{slug}",
            consumer_group=f"payments-{slug}",
            artifact_dir=self._artifact_root / slug,
        )
        scenario.artifact_dir.mkdir(parents=True, exist_ok=True)
        clear_incident_log(scenario.incident_log_path)
        self._clear_contract_report(scenario)
        self._ensure_topic(scenario.topic)
        self._restart_lagzero(scenario)
        self._wait_for_ingest_health()
        return scenario

    def start_slow_consumer(self, scenario: Scenario, *, delay_sec: float) -> subprocess.Popen[str]:
        # kafka-console-consumer consumes and commits as fast as it can; slowing a downstream pipe only slows
        # printing, not consumption. Looping with --max-messages truly rate-limits progress.
        command = (
            "while true; do "
            f"{KAFKA_BIN_DIR}/kafka-console-consumer.sh "
            f"--bootstrap-server kafka:9092 --topic {scenario.topic} "
            f"--group {scenario.consumer_group} "
            "--property print.key=true --property print.timestamp=true "
            "--max-messages 1 --timeout-ms 1000 "
            ">/dev/null 2>&1; "
            f"sleep {delay_sec}; "
            "done"
        )
        return self._compose_popen(["exec", "-T", "consumer-runner", "bash", "-lc", command])

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def produce_perf(
        self,
        scenario: Scenario,
        *,
        num_records: int,
        throughput: int,
        record_size: int = 100,
        producer_props: dict[str, str] | None = None,
    ) -> None:
        props: dict[str, str] = {"bootstrap.servers": "kafka:9092"}
        if producer_props:
            props.update(producer_props)
        props_str = " ".join(f"{k}={v}" for k, v in sorted(props.items()))
        command = (
            f"{KAFKA_BIN_DIR}/kafka-producer-perf-test.sh "
            f"--topic {scenario.topic} "
            f"--num-records {num_records} "
            f"--record-size {record_size} "
            f"--throughput {throughput} "
            f"--producer-props {props_str}"
        )
        self._compose(["exec", "-T", "kafka", "bash", "-lc", command])

    def produce_perf_async(
        self,
        scenario: Scenario,
        *,
        num_records: int,
        throughput: int,
        record_size: int = 100,
        producer_props: dict[str, str] | None = None,
    ) -> subprocess.Popen[str]:
        props: dict[str, str] = {"bootstrap.servers": "kafka:9092"}
        if producer_props:
            props.update(producer_props)
        props_str = " ".join(f"{k}={v}" for k, v in sorted(props.items()))
        command = (
            f"{KAFKA_BIN_DIR}/kafka-producer-perf-test.sh "
            f"--topic {scenario.topic} "
            f"--num-records {num_records} "
            f"--record-size {record_size} "
            f"--throughput {throughput} "
            f"--producer-props {props_str}"
        )
        return self._compose_popen(["exec", "-T", "kafka", "bash", "-lc", command])

    def produce_hotkey(self, scenario: Scenario, *, key: str, count: int) -> None:
        # Generate the hot-key stream inside the container to avoid large host-side heredocs and quoting edge cases.
        command = (
            f"for i in $(seq 1 {count}); do echo '{key}:order-'\"$i\"; done | "
            f"{KAFKA_BIN_DIR}/kafka-console-producer.sh "
            f"--bootstrap-server kafka:9092 --topic {scenario.topic} "
            "--property parse.key=true --property key.separator=:"
        )
        self._compose(["exec", "-T", "kafka", "bash", "-lc", command])

    def pause_consumer_runner(self) -> None:
        container_id = self._compose_output(["ps", "-q", "consumer-runner"]).strip()
        subprocess.run(["docker", "pause", container_id], check=True)

    def unpause_consumer_runner(self) -> None:
        container_id = self._compose_output(["ps", "-q", "consumer-runner"]).strip()
        subprocess.run(["docker", "unpause", container_id], check=True)

    def reset_offsets_to_latest(self, scenario: Scenario) -> None:
        command = (
            f"{KAFKA_BIN_DIR}/kafka-consumer-groups.sh --bootstrap-server kafka:9092 "
            f"--group {scenario.consumer_group} "
            f"--topic {scenario.topic} "
            "--reset-offsets --to-latest --execute"
        )
        # Kafka refuses resets for active groups; retry briefly to allow the group to become inactive after stopping consumers.
        deadline = time.time() + 30.0
        last_output = ""
        while time.time() < deadline:
            proc = subprocess.run(
                ["docker", "compose", "-f", str(self._compose_file), "exec", "-T", "kafka", "bash", "-lc", command],
                cwd=self._repo_root,
                text=True,
                capture_output=True,
            )
            last_output = (proc.stdout or "") + (proc.stderr or "")
            if proc.returncode == 0 and "inactive" not in last_output.lower():
                return
            if "can only be reset if the group" in last_output:
                time.sleep(2)
                continue
            if proc.returncode == 0:
                return
            time.sleep(2)
        raise RuntimeError(f"Offset reset did not succeed in time. Last output:\n{last_output}")

    def ingest_event(
        self,
        event_type: str,
        *,
        service: str | None = None,
        consumer_group: str | None = None,
        topic: str | None = None,
        partition: int | None = None,
        severity: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        command = [sys.executable, "-m", "lagzero.main", "ingest", "--url", self._ingest_url, event_type]
        if service:
            command.extend(["--service", service])
        if consumer_group:
            command.extend(["--consumer-group", consumer_group])
        if topic:
            command.extend(["--topic", topic])
        if partition is not None:
            command.extend(["--partition", str(partition)])
        if severity:
            command.extend(["--severity", severity])
        for key, value in (metadata or {}).items():
            command.extend(["--metadata", f"{key}={value}"])
        subprocess.run(command, cwd=self._repo_root, check=True)

    def assert_incident(
        self,
        scenario: Scenario,
        predicate,
        *,
        timeout_sec: float = 45.0,
    ) -> dict[str, object]:
        return wait_for_incident(
            scenario.incident_log_path,
            predicate,
            timeout_sec=timeout_sec,
            poll_interval_sec=1.0,
        )

    def assert_contract(
        self,
        scenario: Scenario,
        *,
        timeout_sec: float = 45.0,
    ) -> ScenarioValidation:
        validation = wait_for_contract(
            scenario.incident_log_path,
            scenario_name=scenario.name,
            consumer_group=scenario.consumer_group,
            timeout_sec=timeout_sec,
            poll_interval_sec=1.0,
        )
        self.write_contract_report(scenario, validation)
        return validation

    def write_contract_report(
        self,
        scenario: Scenario,
        validation: ScenarioValidation,
    ) -> None:
        payload = {
            "scenario": scenario.name,
            "topic": scenario.topic,
            "consumer_group": scenario.consumer_group,
            "artifact_dir": str(scenario.artifact_dir),
            "passed": validation.passed,
            "reason": validation.reason,
            "final_incident": validation.final_incident,
        }
        scenario.contract_report_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def write_smoke_summary(self, results: list[dict[str, object]]) -> Path:
        summary_path = self._artifact_root / "smoke-summary.json"
        summary_path.write_text(
            json.dumps({"results": results}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return summary_path

    def _ensure_topic(self, topic: str) -> None:
        command = (
            f"{KAFKA_BIN_DIR}/kafka-topics.sh --bootstrap-server kafka:9092 "
            f"--create --if-not-exists --topic {topic} --partitions 4 --replication-factor 1"
        )
        self._compose(["exec", "-T", "kafka", "bash", "-lc", command])

    def _restart_lagzero(self, scenario: Scenario) -> None:
        env = os.environ.copy()
        env.update(
            {
                "LAGZERO_TOPICS": scenario.topic,
                "LAGZERO_CONSUMER_GROUP": scenario.consumer_group,
                "LAGZERO_EVENT_LOG_DIR": str(scenario.artifact_dir.resolve()),
            }
        )
        try:
            self._compose(["up", "-d", "--force-recreate", "lagzero"], env=env)
        except subprocess.CalledProcessError:
            # Docker Compose can occasionally fail to recreate the service due to stale container
            # IDs or name conflicts after abrupt stops. Clean up the service and retry once.
            subprocess.run(
                ["docker", "compose", "-f", str(self._compose_file), "rm", "-sf", "lagzero"],
                cwd=self._repo_root,
                check=False,
            )
            self._compose(["up", "-d", "--force-recreate", "lagzero"], env=env)

    def _wait_for_ingest_health(self, timeout_sec: float = 30.0) -> None:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            try:
                subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        (
                            "import json,urllib.request;"
                            f"print(json.load(urllib.request.urlopen('{self._ingest_url.rsplit('/', 1)[0]}/health', timeout=2)))"
                        ),
                    ],
                    cwd=self._repo_root,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return
            except subprocess.CalledProcessError:
                time.sleep(1)
        raise TimeoutError("LagZero ingest HTTP endpoint did not become healthy in time.")

    def _clear_contract_report(self, scenario: Scenario) -> None:
        if scenario.contract_report_path.exists():
            scenario.contract_report_path.unlink()

    def _compose(self, args: list[str], env: dict[str, str] | None = None) -> None:
        subprocess.run(
            ["docker", "compose", "-f", str(self._compose_file), *args],
            cwd=self._repo_root,
            env=env,
            check=True,
        )

    def _compose_output(self, args: list[str]) -> str:
        return subprocess.check_output(
            ["docker", "compose", "-f", str(self._compose_file), *args],
            cwd=self._repo_root,
            text=True,
        )

    def _compose_popen(self, args: list[str]) -> subprocess.Popen[str]:
        return subprocess.Popen(
            ["docker", "compose", "-f", str(self._compose_file), *args],
            cwd=self._repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
