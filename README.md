# LagZero

LagZero is an AI-powered incident intelligence system for event-driven architectures, starting with Kafka.

It answers one urgent operational question first:

> Why is my Kafka consumer lag increasing?

Traditional observability tooling tells you that lag is growing. LagZero focuses on explaining what that means in real time by turning offsets, rates, and simple operational signals into structured incident events.

## What LagZero Does Today

The current MVP monitors a Kafka consumer group and emits per-partition incident signals that include:

- Consumer-group incident truth derived from partition state
- Current offset lag
- Smoothed processing rate
- Estimated time lag with timestamp-based correction
- Explicit timestamp semantics and lag divergence diagnostics
- Lag velocity
- Basic anomaly classification
- Explicit handling for offset resets and idle-but-delayed partitions
- Optional correlation metadata from external events like deploys

Example output:

```json
{
  "timestamp": 1713571200.0,
  "scope": "partition",
  "topic": "orders",
  "partition": 3,
  "consumer_group": "payments-consumer",
  "offset_lag": 18250,
  "processing_rate": 320.0,
  "time_lag_sec": 92.0,
  "time_lag_source": "timestamp",
  "timestamp_type": "create_time",
  "backlog_head_timestamp": 1713571108.0,
  "latest_message_timestamp": 1713571195.0,
  "lag_divergence_sec": 34.97,
  "lag_velocity": 41.2,
  "anomaly": "lag_spike",
  "severity": "warning",
  "confidence": 0.75,
  "correlations": [
    "deploy_within_window"
  ],
  "diagnostics": {
    "raw_processing_rate": 400.0,
    "rate_window_size": 3,
    "offset_time_lag_sec": 57.03,
    "timestamp_time_lag_sec": 92.0
  }
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
- Sliding rate estimation with short-window smoothing
- Hybrid time-lag estimation using `offset_lag / processing_rate` plus timestamp correction
- Timestamp sampling cache to avoid per-partition fetch cost on every poll
- Lag velocity derived from lag deltas over time
- Group-level incident synthesis with partition diagnostics
- Heuristic anomaly detection for stalled consumers, lag spikes, idle-but-delayed states, estimation mismatch, cold-start catch-up, and offset resets
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
| `LAGZERO_IDLE_INTERVALS` | Consecutive no-movement intervals before idle detection | `3` |
| `LAGZERO_RATE_WINDOW_SIZE` | Number of intervals used for rate smoothing | `3` |
| `LAGZERO_TIMESTAMP_SAMPLE_INTERVAL_SEC` | Minimum interval between timestamp sampling attempts per partition | `30` |
| `LAGZERO_LAG_DIVERGENCE_THRESHOLD_SEC` | Threshold for flagging measured vs estimated lag mismatch | `120` |
| `LAGZERO_SLACK_WEBHOOK_URL` | Slack webhook when emitter is `slack` | empty |

## How Detection Works

For each topic partition, the engine:

1. Fetches the latest broker offset and the consumer group committed offset.
2. Computes offset lag as `high_watermark - committed_offset`.
3. Estimates raw processing rate from the previous committed offset and elapsed time.
4. Smooths rate with a short moving average to reduce burst noise.
5. Estimates offset-based lag as `offset_lag / processing_rate` when rate is positive.
6. Samples Kafka record timestamps for the backlog head when available and uses that to correct time lag.
7. Computes lag velocity from the change in lag over time.
8. Applies explicit heuristics to classify the partition state.
9. Synthesizes a consumer-group incident using worst-case partition truth.
10. Emits structured incident events.

Timestamp correction works like this:

- If the oldest unprocessed message timestamp is available, `time_lag_sec` becomes `observed_at - backlog_head_timestamp`
- If Kafka timestamps are unavailable, sparse-partition sampling fails, or the consumer is cold-starting, LagZero falls back to the offset/rate estimate
- Sampling is throttled with `LAGZERO_TIMESTAMP_SAMPLE_INTERVAL_SEC` so timestamp measurement is cheaper than the main poll loop
- Timestamp semantics are preserved with `timestamp_type` so `create_time` and `log_append_time` remain distinguishable
- Both values are preserved, together with `lag_divergence_sec`, so operators can compare measured delay vs estimated delay

Initial heuristics:

- `consumer_stalled`: no progress for repeated intervals while lag remains positive
- `idle_but_delayed`: lag persists while offsets stop moving for multiple intervals
- `lag_spike`: lag jumps sharply relative to the previous observation
- `lag_estimation_mismatch`: measured timestamp lag and estimated lag diverge materially
- `offset_reset`: committed offset moved backward, so state was reset intentionally
- `catching_up`: consumer is cold-starting and reducing lag at a healthy rate
- `normal`: no anomaly detected

Confidence is rule-based rather than opaque:

- `0.2` when rate is zero
- `0.5` when rate variance is high, offsets are not moving, or measured and estimated lag diverge
- `0.9` for timestamp lag derived from `log_append_time`
- `0.75` for timestamp lag derived from `create_time`
- `0.6` for estimated lag
- `0.4` for fallback estimation after sparse-partition sampling failure or cold start
- `0.95` for explicit zero-lag and offset-reset cases

## External Correlation Events

LagZero includes a simple ingestion hook for external operational context:

```python
engine.add_external_event(event_type="deploy", timestamp=time.time())
```

This keeps correlation grounded in a defined MVP interface instead of leaving it as a placeholder concept.

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
- rate estimation and smoothing
- time-lag estimation and timestamp correction
- anomaly detection behavior
- group event aggregation

## Design Principles

- Start simple and iterate fast
- Prefer heuristics over premature ML
- Separate transport, state, logic, and emission cleanly
- Optimize for actionable incident signals, not dashboards

## Roadmap

- Rich correlation with deploys, errors, and infra signals
- AI-generated root-cause explanations
- Kafka incident timelines
- More sinks: PagerDuty, Kafka topics, webhooks
- More backends: SQS, Pub/Sub, and beyond

## Status

LagZero is currently an MVP foundation focused on Kafka consumer lag intelligence. It is intentionally narrow so the core event model, detection logic, and incident schema can harden before broader platform expansion.
