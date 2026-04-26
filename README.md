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
- Producer rate and backlog growth rate
- Consumer efficiency (`consumer_rate / producer_rate`)
- Estimated time lag with timestamp-based correction
- Explicit timestamp semantics and lag divergence diagnostics
- Service-health classification for the consumer group
- Lag velocity
- Basic anomaly classification
- Explicit handling for offset resets, cold-start catch-up, and partition skew
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
  "producer_rate": 410.0,
  "consumer_efficiency": 0.78,
  "backlog_growth_rate": 90.0,
  "time_lag_sec": 92.0,
  "time_lag_source": "timestamp",
  "timestamp_type": "create_time",
  "backlog_head_timestamp": 1713571108.0,
  "latest_message_timestamp": 1713571195.0,
  "lag_divergence_sec": 34.97,
  "lag_velocity": 41.2,
  "anomaly": "system_under_pressure",
  "severity": "warning",
  "service_health": null,
  "confidence": 0.75,
  "correlations": [
    {
      "correlation_type": "deploy_within_window",
      "event_id": "evt-123",
      "event_type": "deploy",
      "source": "cli",
      "service": null,
      "confidence": 0.883,
      "time_diff_sec": 42.0,
      "role": "probable_cause",
      "metadata": {
        "consumer_group": "payments-consumer",
        "topic": "orders",
        "partition": null,
        "severity": null
      }
    }
  ],
  "primary_cause": "deploy_within_window",
  "primary_cause_confidence": 0.883,
  "diagnostics": {
    "raw_processing_rate": 400.0,
    "raw_producer_rate": 450.0,
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
- Producer-rate estimation from broker offset movement
- Consumer-efficiency estimation for catch-up vs steady-state vs falling-behind interpretation
- Hybrid time-lag estimation using `offset_lag / processing_rate` plus timestamp correction
- Timestamp sampling cache to avoid per-partition fetch cost on every poll
- Backlog growth and system-pressure detection
- Lag velocity derived from lag deltas over time
- Group-level incident synthesis with partition diagnostics
- Anomaly priority and conflict resolution across competing signals
- Stable state transitions with confirmation-based hysteresis
- Partition-skew detection for Kafka hotspots
- Heuristic anomaly detection for stalled consumers, lag spikes, idle-but-delayed states, estimation mismatch, cold-start catch-up, pressure, and offset resets
- Explicit consumer-group service health: `healthy`, `degraded`, `failing`, `recovering`
- In-memory correlation window for recent operational events
- Deterministic correlation engine with primary-cause selection
- Public `lagzero ingest ...` CLI for deploy, error, infra, and rebalance events
- Grounded AI explanation layer for stable incidents
- Local Kafka chaos lab with JSONL artifact capture and smoke scenarios
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
│   ├── ai/
│   ├── correlation/
│   ├── engine/
│   ├── events/
│   ├── ingest/
│   ├── kafka/
│   ├── lab/
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

### 4. Ingest external events

```bash
lagzero ingest deploy \
  --service orders-service \
  --consumer-group payments-consumer \
  --topic orders \
  --metadata version=v1.2.3
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

For the local Kafka chaos lab stack:

```bash
docker compose up --build
```

This starts:

- `kafka`
- `lagzero`
- `consumer-runner`

The stack uses a pinned Kafka `4.0.0` KRaft image (`bitnamilegacy/kafka:4.0.0-debian-12-r10`) and exposes a local ingest endpoint on `http://127.0.0.1:8787/events`.

Run the smoke subset of chaos scenarios with:

```bash
./scripts/run_chaos_smoke.sh
```

Run an individual scenario with:

```bash
./scripts/run_chaos_lab.sh scenario burst_spike
```

Each scenario now writes:

- `incidents.jsonl`: raw emitted incident stream
- `contract-report.json`: pass/fail decision plus final incident evidence

The smoke suite also writes:

- `smoke-summary.json`: one structured verdict per smoke scenario

The chaos lab validates scenario contracts against the full artifact, not an exact transition sequence. That makes it more resilient to future decision-layer tuning while still failing on semantic regressions such as:

- healthy baseline ending in `consumer_stalled`
- burst scenarios stabilizing as severe failure without justification
- sustained pressure never reaching a severe path
- missing or incorrect deploy/error primary causes

The full local chaos suite now has contract coverage for:

- `baseline_healthy_lag`
- `burst_spike`
- `sustained_pressure`
- `consumer_freeze`
- `idle_but_delayed`
- `offset_reset`
- `rebalance_noise`
- `partition_skew`
- `deploy_correlation`
- `error_correlation`

At the time of the latest README update, all of those scenarios have passing local contract artifacts.

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
| `LAGZERO_MIN_INCIDENT_OFFSET_LAG` | Minimum offset lag before warning-level incident classes are eligible | `25` |
| `LAGZERO_MIN_INCIDENT_TIME_LAG_SEC` | Minimum time lag before warning-level incident classes are eligible | `15` |
| `LAGZERO_MIN_STALLED_OFFSET_LAG` | Minimum offset lag before `consumer_stalled` is eligible | `50` |
| `LAGZERO_MIN_STALLED_TIME_LAG_SEC` | Minimum time lag before `consumer_stalled` is eligible | `30` |
| `LAGZERO_MIN_LAG_SPIKE_DELTA` | Minimum absolute lag jump required for `lag_spike` | `25` |
| `LAGZERO_STALLED_INTERVALS` | Consecutive zero-rate intervals before stalled | `2` |
| `LAGZERO_IDLE_INTERVALS` | Consecutive no-movement intervals before idle detection | `3` |
| `LAGZERO_RATE_WINDOW_SIZE` | Number of intervals used for rate smoothing | `3` |
| `LAGZERO_SLOW_CONSUMER_EFFICIENCY_THRESHOLD` | Efficiency threshold below which positive but insufficient progress is treated as `slow_consumer` | `0.8` |
| `LAGZERO_CATCHING_UP_EFFICIENCY_THRESHOLD` | Efficiency threshold that can help recovery states qualify as `catching_up` | `1.0` |
| `LAGZERO_BURST_GRACE_SEC` | Short grace window after a lag spike that helps burst recovery avoid premature `consumer_stalled` classification | `20` |
| `LAGZERO_TIMESTAMP_SAMPLE_INTERVAL_SEC` | Minimum interval between timestamp sampling attempts per partition | `30` |
| `LAGZERO_LAG_DIVERGENCE_THRESHOLD_SEC` | Threshold for flagging measured vs estimated lag mismatch | `120` |
| `LAGZERO_STATE_TRANSITION_CONFIRMATIONS` | Consecutive confirmations required before a new state becomes stable | `2` |
| `LAGZERO_CORRELATION_RETENTION_SEC` | Retention window for external events kept in memory | `900` |
| `LAGZERO_DEPLOY_WINDOW_SEC` | Matching window for deploy correlation | `300` |
| `LAGZERO_ERROR_WINDOW_SEC` | Matching window for error correlation | `120` |
| `LAGZERO_REBALANCE_WINDOW_SEC` | Matching window for rebalance correlation | `90` |
| `LAGZERO_INFRA_WINDOW_SEC` | Matching window for infrastructure correlation | `300` |
| `LAGZERO_MAX_CORRELATIONS` | Maximum matches attached to an incident | `3` |
| `LAGZERO_AI_ENABLED` | Enable grounded AI explanations for stable incidents | `false` |
| `LAGZERO_AI_PROVIDER` | AI provider identifier | `openai` |
| `LAGZERO_AI_MODEL` | Model used for incident explanations | `gpt-5-mini` |
| `LAGZERO_AI_MAX_TOKENS` | Maximum explanation output tokens | `400` |
| `LAGZERO_AI_TEMPERATURE` | Explanation sampling temperature | `0.1` |
| `LAGZERO_AI_CACHE_TTL_SEC` | Cache TTL for repeated equivalent incidents | `120` |
| `LAGZERO_EVENT_LOG_PATH` | Optional JSONL capture file for emitted incidents | empty |
| `LAGZERO_INGEST_ENABLED` | Enable the local ingest HTTP server | `false` |
| `LAGZERO_INGEST_HOST` | Bind host for the ingest HTTP server | `127.0.0.1` |
| `LAGZERO_INGEST_PORT` | Port for the ingest HTTP server | `8787` |
| `LAGZERO_INGEST_PATH` | HTTP path for event ingestion | `/events` |
| `LAGZERO_INGEST_REQUEST_TIMEOUT_SEC` | CLI timeout for ingest requests | `5` |
| `LAGZERO_SLACK_WEBHOOK_URL` | Slack webhook when emitter is `slack` | empty |

## How Detection Works

For each topic partition, the engine:

1. Fetches the latest broker offset and the consumer group committed offset.
2. Computes offset lag as `high_watermark - committed_offset`.
3. Estimates consumer processing rate from committed offset movement.
4. Estimates producer rate from latest broker offset movement.
5. Smooths both rates with a short moving average to reduce burst noise.
6. Derives consumer efficiency as `processing_rate / producer_rate` when producer rate is known.
7. Derives backlog growth rate as `producer_rate - processing_rate`.
8. Estimates offset-based lag as `offset_lag / processing_rate` when rate is positive.
9. Samples Kafka record timestamps for the backlog head when available and uses that to correct time lag.
10. Computes lag velocity from the change in lag over time.
11. Applies explicit heuristics to classify the partition state.
12. Resolves conflicting anomaly signals using an explicit priority model.
13. Applies hysteresis so a new anomaly must be confirmed across multiple polls before it becomes the stable emitted state.
14. Detects partition skew from cross-partition lag imbalance.
15. Synthesizes a consumer-group incident using worst-case stable anomaly truth and explicit service-health rules.
16. Runs deterministic correlation against recent external events.
17. Builds a grounded explanation for stable incidents when AI is enabled.
18. Emits enriched incident events.

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
- `system_under_pressure`: producer rate exceeds consumer rate and backlog is growing
- `partition_skew`: one partition is disproportionately behind the rest
- `offset_reset`: committed offset moved backward, so state was reset intentionally
- `catching_up`: consumer is cold-starting and reducing lag at a healthy rate
- `normal`: no anomaly detected

When multiple signals are true at once, LagZero resolves them by priority before emitting a final anomaly. Examples:

- `consumer_stalled` outranks `partition_skew`
- `system_under_pressure` outranks `lag_spike`
- `catching_up` only wins when higher-priority failure signals are absent

LagZero also applies hysteresis to avoid flapping:

- a newly observed anomaly must be confirmed across `LAGZERO_STATE_TRANSITION_CONFIRMATIONS` consecutive polls
- until then, the previously stable anomaly and health state remain in place
- pending transitions are exposed in event diagnostics

Consumer-group service health is derived explicitly:

- `healthy`: no meaningful incident signal at the group level
- `degraded`: warning-level anomalies such as pressure, spike, skew, or divergence
- `failing`: critical partition behavior such as a stalled consumer
- `recovering`: the group is catching up after a cold start

Confidence is rule-based rather than opaque:

- `0.2` when rate is zero
- `0.5` when rate variance is high, offsets are not moving, or measured and estimated lag diverge
- `0.9` for timestamp lag derived from `log_append_time`
- `0.75` for timestamp lag derived from `create_time`
- `0.6` for estimated lag
- `0.4` for fallback estimation after sparse-partition sampling failure or cold start
- `0.95` for explicit zero-lag and offset-reset cases

## External Correlation Events

LagZero includes both an in-process ingestion hook and a public CLI for external operational context:

```python
engine.add_external_event(event_type="deploy", timestamp=time.time())
```

```bash
lagzero ingest error \
  --service orders-service \
  --consumer-group payments-consumer \
  --topic orders \
  --severity error \
  --metadata error_type=deserialization_failure
```

Recent external events are retained in an in-memory time window and attached to emitted incidents as correlation context. This keeps correlation grounded in a defined MVP interface instead of leaving it as a placeholder concept.

The correlation engine:

1. Retains recent external events in a bounded in-memory FIFO store.
2. Filters by relevance using consumer group, topic, and partition alignment.
3. Applies deterministic rules such as `deploy_within_window`, `error_nearby`, `rebalance_overlap`, and `infra_change_nearby`.
4. Scores matches by time proximity, scope match, and rule relevance.
5. Selects a `primary_cause` and keeps the next strongest matches as supporting evidence.

## AI Explanation Layer

The AI layer runs after detection, stabilization, and correlation. It explains incidents; it does not detect them.

It:

- consumes only final stable incident fields
- uses anomaly, health, lag metrics, primary cause, and supporting correlations as evidence
- returns structured output with `summary`, `probable_cause_explanation`, `impact`, `recommended_actions`, and `caveats`
- caches explanations for repeated equivalent incidents within a short TTL
- fails safely without blocking deterministic incident emission

It does not:

- change anomaly classification
- override service health
- invent causes outside the structured evidence
- suppress deterministic incident output

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
- group event aggregation and service health
- producer pressure, partition skew, and correlation retention
- correlation rule matching, scoring, and primary-cause selection
- AI context building, prompt grounding, caching, and fallback parsing
- ingest CLI transport and JSONL incident capture helpers

For the Docker-backed chaos lab, the smoke subset currently covers:

- healthy bounded lag
- burst-induced lag spike
- sustained pressure
- deploy correlation

## Design Principles

- Start simple and iterate fast
- Prefer heuristics over premature ML
- Separate transport, state, logic, and emission cleanly
- Optimize for actionable incident signals, not dashboards

## Roadmap

Completed in the current foundation:

- Kafka-first lag monitoring with group-level incident synthesis
- Smoothed producer/consumer rates, backlog growth, lag velocity, and consumer efficiency
- Timestamp-based lag correction with timestamp semantics and divergence diagnostics
- Deterministic anomaly resolution with hysteresis and explicit service health
- Correlation engine with deploy, error, infra, and rebalance evidence
- Public `lagzero ingest ...` CLI and local HTTP ingest endpoint
- Grounded AI explanation layer for stable incidents
- Docker-backed local chaos lab with contract-based scenario validation

Next:

- Incident lifecycle and stateful incident tracking
- Incident timelines built from stable transitions and correlated evidence
- More sinks: PagerDuty, Kafka topics, webhooks
- Persistent event/correlation storage beyond in-memory retention
- More backends: SQS, Pub/Sub, and beyond

## Status

LagZero is now a working Kafka incident-intelligence foundation with deterministic detection, correlation, grounded explanation, and a local chaos lab that validates the core incident scenarios end to end. The next step is not more raw signal collection; it is turning stable incidents into first-class lifecycle objects so operators can reason about open, changing, and resolved incidents over time.
