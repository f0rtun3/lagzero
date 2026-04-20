# LagZero

LagZero is an AI-powered incident intelligence system for event-driven architectures, starting with Kafka.

It answers one urgent operational question first:

> Why is my Kafka consumer lag increasing?

Traditional observability tooling tells you that lag is growing. LagZero focuses on explaining what that means in real time by turning offsets, rates, and simple operational signals into structured incident events.

## What LagZero Does Today

The current MVP monitors a Kafka consumer group and emits per-partition incident signals that include:

- Current offset lag
- Estimated processing rate
- Estimated time lag
- Basic anomaly classification
- Optional correlation metadata for errors and deploy events

Example output:

```json
{
  "timestamp": 1713571200.0,
  "topic": "orders",
  "partition": 3,
  "consumer_group": "payments-consumer",
  "offset_lag": 18250,
  "processing_rate": 320.0,
  "time_lag_sec": 57.03,
  "anomaly": "lag_spike",
  "severity": "warning",
  "confidence": 0.75,
  "correlations": [
    "deploy_within_window"
  ]
}
```

## Why This Exists

Modern event-driven systems fail in ways that are asynchronous, silent, and hard to debug. Kafka lag is often the first visible symptom, but lag alone does not explain causality.

LagZero is built around a simple idea:

> Treat operational signals as events, derive meaning continuously, and emit incidents that humans and downstream systems can act on.

## MVP Architecture

```text
Kafka Cluster
   ↓
Offset Fetcher
   ↓
Lag Calculator
   ↓
Rate Calculator (stateful)
   ↓
Time Lag Estimator
   ↓
Anomaly Detector
   ↓
Event Emitter
   ↓
stdout / Slack
```

## Features

- Kafka-first design with a narrow, useful wedge
- Continuous polling loop for consumer lag monitoring
- Sliding rate estimation using previous offsets and timestamps
- Time-lag approximation via `offset_lag / processing_rate`
- Heuristic anomaly detection for stalled consumers and lag spikes
- Structured incident event schema for downstream automation
- Pure monitoring functions with focused unit tests

## Project Layout

```text
lagzero/
├── README.md
├── pyproject.toml
├── .env.example
├── lagzero/
│   ├── config/
│   ├── engine/
│   ├── events/
│   ├── kafka/
│   ├── monitoring/
│   ├── state/
│   └── main.py
├── tests/
└── scripts/
```

## Quick Start

### 1. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,kafka]"
```

### 2. Configure environment

```bash
cp .env.example .env
```

Set at least:

```bash
export LAGZERO_BOOTSTRAP_SERVERS=localhost:9092
export LAGZERO_CONSUMER_GROUP=my-consumer-group
export LAGZERO_TOPICS=orders,shipments
```

### 3. Run the monitor

```bash
lagzero
```

Or:

```bash
python -m lagzero.main
```

## Docker

Build and run LagZero in a container:

```bash
docker build -t lagzero .
docker run --rm \
  -e LAGZERO_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e LAGZERO_CONSUMER_GROUP=my-consumer-group \
  -e LAGZERO_TOPICS=orders \
  lagzero
```

For a local demo stack with Kafka included:

```bash
docker compose up --build
```

This starts:

- `zookeeper`
- `kafka`
- `lagzero`

## Configuration

LagZero is configured through environment variables:

| Variable | Description | Default |
| --- | --- | --- |
| `LAGZERO_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `LAGZERO_CONSUMER_GROUP` | Consumer group to inspect | required |
| `LAGZERO_TOPICS` | Comma-separated topic list | required |
| `LAGZERO_POLL_INTERVAL_SEC` | Polling interval in seconds | `10` |
| `LAGZERO_EMITTER` | `stdout` or `slack` | `stdout` |
| `LAGZERO_LOG_LEVEL` | Logging level | `INFO` |
| `LAGZERO_LAG_SPIKE_MULTIPLIER` | Spike threshold vs previous lag | `2.0` |
| `LAGZERO_STALLED_INTERVALS` | Consecutive zero-rate intervals before stalled | `2` |
| `LAGZERO_SLACK_WEBHOOK_URL` | Slack webhook when emitter is `slack` | empty |

## How Detection Works

For each topic partition, the engine:

1. Fetches the latest broker offset and the consumer group committed offset.
2. Computes offset lag as `high_watermark - committed_offset`.
3. Estimates processing rate from the previous committed offset and elapsed time.
4. Estimates time lag as `offset_lag / processing_rate` when rate is positive.
5. Applies simple heuristics to classify the partition state.
6. Emits a structured incident event.

Initial heuristics:

- `consumer_stalled`: no progress for repeated intervals while lag remains positive
- `lag_spike`: lag jumps sharply relative to the previous observation
- `normal`: no anomaly detected

## Slack Output

Set:

```bash
export LAGZERO_EMITTER=slack
export LAGZERO_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

LagZero will post a concise JSON-backed alert summary to Slack while still preserving the full structured payload in logs.

## Testing

```bash
pytest
```

The tests cover the core pure logic:

- lag computation
- rate estimation
- time-lag estimation
- anomaly detection behavior

## Design Principles

- Start simple and iterate fast
- Prefer heuristics over premature ML
- Separate transport, state, logic, and emission cleanly
- Optimize for actionable incident signals, not dashboards

## Roadmap

- Timestamp-based lag correction
- Rich correlation with deploys, errors, and infra signals
- AI-generated root-cause explanations
- Kafka incident timelines
- More sinks: PagerDuty, Kafka topics, webhooks
- More backends: SQS, Pub/Sub, and beyond

## Status

LagZero is currently an MVP foundation focused on Kafka consumer lag intelligence. It is intentionally narrow so the core event model, detection logic, and incident schema can harden before broader platform expansion.
