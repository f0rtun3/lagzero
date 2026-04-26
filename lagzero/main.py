from __future__ import annotations

import argparse
import logging
import sys

from lagzero.config.logging import configure_logging
from lagzero.config.settings import Settings
from lagzero.engine.monitor import MonitorEngine
from lagzero.events.emitter import SlackEventEmitter, StdoutEventEmitter
from lagzero.ingest.cli import build_event_from_args, build_ingest_parser, send_external_event
from lagzero.ingest.http import IngestHTTPServer

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
        settings = Settings.from_env()
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

    engine = MonitorEngine(settings=settings, offset_fetcher=fetcher, event_emitter=emitter)
    ingest_server: IngestHTTPServer | None = None
    if settings.ingest_enabled:
        ingest_server = IngestHTTPServer(
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
        )
        ingest_server.start()

    try:
        if args.once:
            engine.run_once()
        else:
            engine.run_forever()
    except KeyboardInterrupt:
        logger.info("LagZero interrupted, shutting down.")
    finally:
        if ingest_server is not None:
            ingest_server.stop()
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


def main() -> int:
    argv = sys.argv[1:]
    if argv and argv[0] == "ingest":
        return run_ingest(argv[1:])
    return run_monitor(argv)


if __name__ == "__main__":
    raise SystemExit(main())
