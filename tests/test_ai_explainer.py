from dataclasses import dataclass

from lagzero.ai.client import DisabledLLMClient, StaticLLMClient
from lagzero.ai.context import build_explanation_context
from lagzero.ai.explainer import IncidentExplainer
from lagzero.config.settings import Settings
from lagzero.engine.monitor import MonitorEngine
from lagzero.events.schema import IncidentEvent
from lagzero.kafka.offsets import PartitionOffsets
from lagzero.state.store import PartitionState


def make_incident(**overrides: object) -> IncidentEvent:
    payload = {
        "timestamp": 100.0,
        "consumer_group": "orders-consumer",
        "scope": "consumer_group",
        "topic": None,
        "partition": None,
        "offset_lag": 18250,
        "processing_rate": 320.0,
        "producer_rate": 900.0,
        "consumer_efficiency": 0.36,
        "backlog_growth_rate": 580.0,
        "time_lag_sec": 662.0,
        "time_lag_source": "timestamp",
        "timestamp_type": "log_append_time",
        "backlog_head_timestamp": 38.0,
        "latest_message_timestamp": 99.0,
        "lag_divergence_sec": 12.0,
        "lag_velocity": 18.0,
        "anomaly": "system_under_pressure",
        "severity": "critical",
        "service_health": "degraded",
        "confidence": 0.91,
        "correlations": [
            {
                "correlation_type": "deploy_within_window",
                "event_id": "evt-1",
                "event_type": "deploy",
                "source": "cli",
                "service": "orders-service",
                "confidence": 0.82,
                "time_diff_sec": 42.0,
                "role": "probable_cause",
                "metadata": {},
            }
        ],
        "primary_cause": "deploy_within_window",
        "primary_cause_confidence": 0.82,
        "ai_explanation": None,
        "diagnostics": {
            "transition_pending": False,
            "observed_anomaly": "system_under_pressure",
            "pending_anomaly": None,
            "correlation_candidates": ["deploy_within_window"],
        },
    }
    payload.update(overrides)
    return IncidentEvent(**payload)


@dataclass
class FakeOffsetFetcher:
    snapshots: list[PartitionOffsets]

    def fetch(self, topics: list[str]) -> list[PartitionOffsets]:
        return self.snapshots


class CollectingEmitter:
    def __init__(self) -> None:
        self.events: list[IncidentEvent] = []

    def emit(self, event: IncidentEvent) -> None:
        self.events.append(event)


def test_explainer_skips_normal_incidents_without_model_call() -> None:
    client = StaticLLMClient(
        '{"summary":"unused","probable_cause_explanation":"unused","impact":"unused","recommended_actions":["unused"],"caveats":["unused"]}'
    )
    explainer = IncidentExplainer(llm_client=client, enabled=True, cache_ttl_sec=120.0)

    explained = explainer.explain(make_incident(anomaly="normal", service_health="healthy"))

    assert explained.ai_explanation is None
    assert client.calls == 0


def test_explainer_skips_when_ai_is_disabled() -> None:
    client = StaticLLMClient(
        '{"summary":"unused","probable_cause_explanation":"unused","impact":"unused","recommended_actions":["unused"],"caveats":["unused"]}'
    )
    explainer = IncidentExplainer(llm_client=client, enabled=False, cache_ttl_sec=120.0)

    explained = explainer.explain(make_incident())

    assert explained.ai_explanation is None
    assert client.calls == 0


def test_explainer_treats_disabled_client_as_noop() -> None:
    explainer = IncidentExplainer(
        llm_client=DisabledLLMClient(),
        enabled=True,
        cache_ttl_sec=120.0,
    )

    explained = explainer.explain(make_incident())

    assert explained.ai_explanation is None


def test_explainer_caches_identical_incidents() -> None:
    client = StaticLLMClient(
        """
        {
          "summary": "Orders consumer group is degraded.",
          "probable_cause_explanation": "A nearby deploy is the strongest likely cause.",
          "impact": "Order processing is delayed.",
          "recommended_actions": ["Check the latest deploy"],
          "caveats": ["Time lag is timestamp-derived."]
        }
        """
    )
    explainer = IncidentExplainer(llm_client=client, enabled=True, cache_ttl_sec=120.0)
    incident = make_incident()

    first = explainer.explain(incident)
    second = explainer.explain(incident)

    assert first.ai_explanation is not None
    assert second.ai_explanation is not None
    assert first.ai_explanation["summary"] == "Orders consumer group is degraded."
    assert second.diagnostics["ai_explanation_cache_hit"] is True
    assert client.calls == 1


def test_explainer_uses_fallback_when_model_response_is_malformed() -> None:
    client = StaticLLMClient("not-json")
    explainer = IncidentExplainer(llm_client=client, enabled=True, cache_ttl_sec=120.0)

    explained = explainer.explain(
        make_incident(primary_cause=None, primary_cause_confidence=None)
    )

    assert explained.ai_explanation is not None
    assert explained.ai_explanation["summary"] == (
        "Incident detected, but explanation could not be generated reliably."
    )
    assert explained.ai_explanation["caveats"] == ["AI explanation parsing failed."]


def test_explainer_fingerprint_is_stable_within_same_time_bucket() -> None:
    client = StaticLLMClient(
        '{"summary":"stable","probable_cause_explanation":"stable","impact":"stable","recommended_actions":["stable"],"caveats":["stable"]}'
    )
    explainer = IncidentExplainer(llm_client=client, enabled=True, cache_ttl_sec=120.0)

    first_context = build_explanation_context(make_incident(time_lag_sec=121.0))
    second_context = build_explanation_context(make_incident(time_lag_sec=179.0))
    first = explainer.fingerprint(first_context)
    second = explainer.fingerprint(second_context)

    assert first_context.time_lag_sec == 121.0
    assert first == second


def test_monitor_engine_attaches_ai_explanation_after_correlation() -> None:
    client = StaticLLMClient(
        """
        {
          "summary": "Payments consumer group is degraded.",
          "probable_cause_explanation": "A deploy is the strongest nearby cause.",
          "impact": "Processing is delayed.",
          "recommended_actions": ["Check the deploy"],
          "caveats": ["Time lag is timestamp-derived."]
        }
        """
    )
    explainer = IncidentExplainer(llm_client=client, enabled=True, cache_ttl_sec=120.0)
    settings = Settings(
        bootstrap_servers="localhost:9092",
        consumer_group="payments",
        topics=["orders"],
        state_transition_confirmations=1,
        ai_enabled=True,
    )
    engine = MonitorEngine(
        settings=settings,
        offset_fetcher=FakeOffsetFetcher(
            snapshots=[
                PartitionOffsets(
                    topic="orders",
                    partition=0,
                    committed_offset=100,
                    latest_offset=200,
                    observed_at=10.0,
                    backlog_head_timestamp=8.0,
                    latest_message_timestamp=9.0,
                    timestamp_type="log_append_time",
                    timestamp_sampled_at=9.0,
                    timestamp_sampling_state="timestamp",
                ),
            ]
        ),
        event_emitter=CollectingEmitter(),
        incident_explainer=explainer,
    )
    engine.add_external_event(event_type="deploy", timestamp=9.0)
    engine.state_store.set(
        ("orders", 0),
        PartitionState(
            committed_offset=90,
            latest_offset=120,
            observed_at=0.0,
            offset_lag=30,
            recent_rates=[1.0],
            recent_producer_rates=[1.0],
        ),
    )

    events = engine.run_once()

    assert events[0].primary_cause == "deploy_within_window"
    assert events[0].ai_explanation is not None
    assert events[0].ai_explanation["summary"] == "Payments consumer group is degraded."
    assert client.calls >= 1
