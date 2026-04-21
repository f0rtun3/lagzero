from __future__ import annotations

import argparse
import logging
import sys

from lagzero.config.logging import configure_logging
from lagzero.config.settings import Settings
from lagzero.engine.monitor import MonitorEngine
from lagzero.events.emitter import SlackEventEmitter, StdoutEventEmitter

logger = logging.getLogger(__name__)


def build_event_emitter(settings: Settings) -> StdoutEventEmitter | SlackEventEmitter:
    if settings.emitter == "stdout":
        return StdoutEventEmitter()

    if settings.emitter == "slack":
        if not settings.slack_webhook_url:
            raise ValueError("LAGZERO_SLACK_WEBHOOK_URL is required when LAGZERO_EMITTER=slack.")
        return SlackEventEmitter(settings.slack_webhook_url)

    raise ValueError(f"Unsupported emitter '{settings.emitter}'. Use 'stdout' or 'slack'.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LagZero Kafka lag monitor")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one monitoring cycle and exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

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

    try:
        if args.once:
            engine.run_once()
        else:
            engine.run_forever()
    except KeyboardInterrupt:
        logger.info("LagZero interrupted, shutting down.")
    finally:
        fetcher.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
