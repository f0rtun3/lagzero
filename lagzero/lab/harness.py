from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from lagzero.lab.contracts import ScenarioValidation, validate_scenario_contract, wait_for_snapshot_contract
from lagzero.lab.events import clear_incident_log, clear_jsonl, load_jsonl, wait_for_incident
from lagzero.lab.state import build_lifecycle_events, snapshot_sqlite_state
from lagzero.lab.webhook_capture import WebhookCaptureServer

KAFKA_BIN_DIR = "/opt/bitnami/kafka/bin"
WEBHOOK_CAPTURE_HOST = "127.0.0.1"
WEBHOOK_CAPTURE_PORT = 8790
WEBHOOK_CAPTURE_PATH = "/webhooks"
WEBHOOK_SECRET = "lagzero-chaos-secret"


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

    @property
    def lifecycle_events_path(self) -> Path:
        return self.artifact_dir / "lifecycle-events.jsonl"

    @property
    def deliveries_path(self) -> Path:
        return self.artifact_dir / "deliveries.jsonl"

    @property
    def state_snapshot_path(self) -> Path:
        return self.artifact_dir / "state-snapshot.json"

    @property
    def sqlite_path(self) -> Path:
        return self.artifact_dir / "lagzero.db"


class ChaosLabHarness:
    def __init__(
        self,
        *,
        repo_root: Path,
        compose_file: Path,
        artifact_root: Path,
        ingest_url: str,
        contract_phase: int = 1,
    ) -> None:
        self._repo_root = repo_root
        self._compose_file = compose_file
        self._artifact_root = artifact_root
        self._ingest_url = ingest_url
        self._contract_phase = contract_phase
        self._artifact_root.mkdir(parents=True, exist_ok=True)
        self._webhook_capture: WebhookCaptureServer | None = None

    def up_base_stack(self) -> None:
        self._ensure_webhook_capture().start()
        self._compose(["up", "-d", "kafka", "consumer-runner"])

    def down_stack(self) -> None:
        if self._webhook_capture is not None:
            self._webhook_capture.stop()
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
        clear_jsonl(scenario.lifecycle_events_path)
        clear_jsonl(scenario.deliveries_path)
        self._clear_contract_report(scenario)
        if scenario.state_snapshot_path.exists():
            scenario.state_snapshot_path.unlink()
        if scenario.sqlite_path.exists():
            scenario.sqlite_path.unlink()
        self._ensure_webhook_capture().set_output_path(scenario.deliveries_path)
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
        wait_for_snapshot_contract(
            scenario.incident_log_path,
            scenario_name=scenario.name,
            consumer_group=scenario.consumer_group,
            current_phase=self._contract_phase,
            timeout_sec=timeout_sec,
            poll_interval_sec=1.0,
        )
        self.capture_state_artifacts(scenario)
        validation = validate_scenario_contract(
            scenario_name=scenario.name,
            incidents=load_jsonl(scenario.incident_log_path),
            consumer_group=scenario.consumer_group,
            lifecycle_events=load_jsonl(scenario.lifecycle_events_path),
            state_snapshot=json.loads(scenario.state_snapshot_path.read_text(encoding="utf-8")),
            captured_deliveries=load_jsonl(scenario.deliveries_path),
            current_phase=self._contract_phase,
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
            "snapshot_contract": validation.snapshot_contract.to_dict(),
            "lifecycle_contract": validation.lifecycle_contract.to_dict(),
            "delivery_contract": validation.delivery_contract.to_dict(),
            "overall_passed": validation.overall_passed,
            "final_incident": validation.final_incident,
        }
        scenario.contract_report_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def capture_state_artifacts(self, scenario: Scenario) -> dict[str, list[dict[str, object]]]:
        state_snapshot = snapshot_sqlite_state(scenario.sqlite_path)
        scenario.state_snapshot_path.write_text(
            json.dumps(state_snapshot, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        lifecycle_events = build_lifecycle_events(state_snapshot)
        clear_jsonl(scenario.lifecycle_events_path)
        if lifecycle_events:
            with scenario.lifecycle_events_path.open("a", encoding="utf-8") as handle:
                for event in lifecycle_events:
                    handle.write(json.dumps(event, sort_keys=True) + "\n")
        return state_snapshot

    def restart_lagzero(self, scenario: Scenario) -> None:
        self._restart_lagzero(scenario)

    def redeliver_event(self, scenario: Scenario, event_id: str) -> None:
        env = os.environ.copy()
        env.update(
            {
                "LAGZERO_INCIDENTS_ENABLED": "true",
                "LAGZERO_SQLITE_PATH": str(scenario.sqlite_path),
                "LAGZERO_WEBHOOK_ENABLED": "true",
                "LAGZERO_WEBHOOK_URL": self._capture_url,
                "LAGZERO_WEBHOOK_SECRET": WEBHOOK_SECRET,
            }
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "lagzero.main",
                "incidents",
                "redeliver",
                "--event-id",
                event_id,
            ],
            cwd=self._repo_root,
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def fail_next_webhook_delivery(self, count: int = 1) -> None:
        self._ensure_webhook_capture().fail_next(count)

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

    @property
    def _capture_url(self) -> str:
        return f"http://host.docker.internal:{WEBHOOK_CAPTURE_PORT}{WEBHOOK_CAPTURE_PATH}"

    def _restart_lagzero(self, scenario: Scenario, *, webhook_url: str | None = None) -> None:
        env = os.environ.copy()
        env.update(
            {
                "LAGZERO_TOPICS": scenario.topic,
                "LAGZERO_CONSUMER_GROUP": scenario.consumer_group,
                "LAGZERO_EVENT_LOG_DIR": str(scenario.artifact_dir.resolve()),
                "LAGZERO_INCIDENTS_ENABLED": "true",
                "LAGZERO_SQLITE_PATH": "/var/lagzero/lagzero.db",
                "LAGZERO_WEBHOOK_ENABLED": "true",
                "LAGZERO_WEBHOOK_URL": webhook_url or self._capture_url,
                "LAGZERO_WEBHOOK_SECRET": WEBHOOK_SECRET,
            }
        )
        try:
            self._compose(["up", "-d", "--build", "--force-recreate", "lagzero"], env=env)
        except subprocess.CalledProcessError:
            # Docker Compose can occasionally fail to recreate the service due to stale container
            # IDs or name conflicts after abrupt stops. Clean up the service and retry once.
            subprocess.run(
                ["docker", "compose", "-f", str(self._compose_file), "rm", "-sf", "lagzero"],
                cwd=self._repo_root,
                check=False,
            )
            self._compose(["up", "-d", "--build", "--force-recreate", "lagzero"], env=env)

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

    def _ensure_webhook_capture(self) -> WebhookCaptureServer:
        if self._webhook_capture is None:
            self._webhook_capture = WebhookCaptureServer(
                host=WEBHOOK_CAPTURE_HOST,
                port=WEBHOOK_CAPTURE_PORT,
                path=WEBHOOK_CAPTURE_PATH,
                secret=WEBHOOK_SECRET,
            )
        return cast(WebhookCaptureServer, self._webhook_capture)

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
