from __future__ import annotations

import argparse
import logging
import sys

from lagzero.config.logging import configure_logging
from lagzero.config.settings import Settings
from lagzero.engine.monitor import MonitorEngine
from lagzero.events.emitter import SlackEventEmitter, StdoutEventEmitter
from lagzero.incidents.cli import build_incidents_parser, run_incidents_command
from lagzero.incidents.manager import IncidentLifecycleManager
from lagzero.incidents.service import IncidentHistoryService
from lagzero.ingest.cli import build_event_from_args, build_ingest_parser, send_external_event
from lagzero.ingest.http import IngestHTTPServer
from lagzero.persistence.repository import IncidentRepository
from lagzero.persistence.sqlite import SQLiteIncidentStore
from lagzero.sinks.webhook import WebhookSink

logger = logging.getLogger(__name__)


def build_event_emitter(settings: Settings) -> StdoutEventEmitter | SlackEventEmitter:
    if settings.emitter == "stdout":
        return StdoutEventEmitter(capture_path=settings.event_log_path)

    if settings.emitter == "slack":
        if not settings.slack_webhook_url:
            raise ValueError("LAGZERO_SLACK_WEBHOOK_URL is required when LAGZERO_EMITTER=slack.")
        return SlackEventEmitter(settings.slack_webhook_url)

    raise ValueError(f"Unsupported emitter '{settings.emitter}'. Use 'stdout' or 'slack'.")


def build_monitor_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LagZero Kafka lag monitor")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one monitoring cycle and exit.",
    )
    return parser


def run_monitor(argv: list[str]) -> int:
    args = build_monitor_parser().parse_args(argv)
    try:
        settings = Settings.from_env(require_monitoring=False)
        configure_logging(settings.log_level)
        emitter = build_event_emitter(settings)
        from lagzero.kafka.offsets import KafkaOffsetFetcher

        fetcher = KafkaOffsetFetcher(
            bootstrap_servers=settings.bootstrap_servers,
            consumer_group=settings.consumer_group,
            timestamp_sample_interval_sec=settings.timestamp_sample_interval_sec,
        )
    except Exception as exc:
        print(f"Failed to initialize LagZero: {exc}", file=sys.stderr)
        return 1

    try:
        repository, webhook_sink, lifecycle_manager, history_service = build_incident_runtime(settings)
        engine = MonitorEngine(
            settings=settings,
            offset_fetcher=fetcher,
            event_emitter=emitter,
            incident_lifecycle_manager=lifecycle_manager,
        )

        servers: list[IngestHTTPServer] = []
        if settings.ingest_enabled:
            servers.append(
                IngestHTTPServer(
                    host=settings.ingest_host,
                    port=settings.ingest_port,
                    path=settings.ingest_path,
                    on_event=lambda event: engine.add_external_event(
                        event_type=event.event_type,
                        timestamp=event.timestamp,
                        attributes=event.metadata,
                        source=event.source,
                        service=event.service,
                        consumer_group=event.consumer_group,
                        topic=event.topic,
                        partition=event.partition,
                        severity=event.severity,
                    ),
                    health_payload={
                        "status": "ok",
                        "ingest_enabled": True,
                        "operator_api_enabled": False,
                    },
                )
            )
        if settings.operator_api_enabled and history_service is None:
            raise ValueError("LAGZERO_OPERATOR_API_ENABLED requires LAGZERO_INCIDENTS_ENABLED=true.")
        if settings.operator_api_enabled and history_service is not None:
            servers.append(
                IngestHTTPServer(
                    host=settings.operator_api_host,
                    port=settings.operator_api_port,
                    operator_service=history_service,
                    operator_path_prefix=settings.operator_api_path_prefix,
                    health_payload={
                        "status": "ok",
                        "ingest_enabled": False,
                        "operator_api_enabled": True,
                        "incidents_enabled": settings.incidents_enabled,
                        "persistence_backend": settings.persistence_backend,
                    },
                )
            )
        for server in servers:
            server.start()
    except Exception as exc:
        print(f"Failed to initialize LagZero: {exc}", file=sys.stderr)
        fetcher.close()
        return 1

    try:
        if args.once:
            engine.run_once()
        else:
            engine.run_forever()
    except KeyboardInterrupt:
        logger.info("LagZero interrupted, shutting down.")
    finally:
        for server in servers:
            server.stop()
        fetcher.close()

    return 0


def run_ingest(argv: list[str]) -> int:
    parser = build_ingest_parser()
    try:
        args = parser.parse_args(argv)
        event = build_event_from_args(args)
        response = send_external_event(event, url=args.url, timeout_sec=args.timeout_sec)
    except Exception as exc:
        print(f"Failed to ingest event: {exc}", file=sys.stderr)
        return 1

    print(
        f"Accepted {event.event_type} event {event.event_id} via {args.url}: "
        f"{response.get('status', 'unknown')}"
    )
    return 0


def run_incidents(argv: list[str]) -> int:
    parser = build_incidents_parser()
    try:
        args = parser.parse_args(argv)
        settings = Settings.from_env(require_monitoring=False)
        configure_logging(settings.log_level)
        _, _, _, history_service = build_incident_runtime(settings)
        if history_service is None:
            raise RuntimeError("Incident history requires LAGZERO_INCIDENTS_ENABLED=true.")
        return run_incidents_command(history_service, args)
    except Exception as exc:
        print(f"Failed to query incidents: {exc}", file=sys.stderr)
        return 1


def build_incident_runtime(
    settings: Settings,
) -> tuple[
    IncidentRepository | None,
    WebhookSink | None,
    IncidentLifecycleManager | None,
    IncidentHistoryService | None,
]:
    repository: IncidentRepository | None = None
    lifecycle_manager: IncidentLifecycleManager | None = None
    history_service: IncidentHistoryService | None = None
    if settings.incidents_enabled:
        if settings.persistence_backend != "sqlite":
            raise ValueError(f"Unsupported persistence backend '{settings.persistence_backend}'.")
        repository = IncidentRepository(SQLiteIncidentStore(settings.sqlite_path))
        history_service = IncidentHistoryService(repository=repository)

    webhook_sink: WebhookSink | None = None
    if settings.webhook_enabled:
        if not settings.webhook_url:
            raise ValueError("LAGZERO_WEBHOOK_URL is required when LAGZERO_WEBHOOK_ENABLED=true.")
        if not settings.webhook_secret:
            raise ValueError(
                "LAGZERO_WEBHOOK_SECRET is required when LAGZERO_WEBHOOK_ENABLED=true."
            )
        webhook_sink = WebhookSink(
            url=settings.webhook_url,
            secret=settings.webhook_secret,
            timeout_sec=settings.webhook_timeout_sec,
            max_retries=settings.webhook_max_retries,
            event_version=settings.webhook_event_version,
        )

    if repository is not None:
        lifecycle_manager = IncidentLifecycleManager(
            repository=repository,
            webhook_sink=webhook_sink,
            resolve_confirmations=settings.incident_resolve_confirmations,
            enabled=settings.incidents_enabled,
        )
        history_service = IncidentHistoryService(
            repository=repository,
            webhook_sink=webhook_sink,
        )

    return repository, webhook_sink, lifecycle_manager, history_service


def main() -> int:
    argv = sys.argv[1:]
    if argv and argv[0] == "ingest":
        return run_ingest(argv[1:])
    if argv and argv[0] == "incidents":
        return run_incidents(argv[1:])
    return run_monitor(argv)


if __name__ == "__main__":
    raise SystemExit(main())
