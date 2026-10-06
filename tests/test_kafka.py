"""Producing keyed, idempotent writes; and a real round trip through Kafka when a broker is available."""

from __future__ import annotations

import os
from typing import Any

import pytest
from helpers import click

from streaming_feature_pipeline.engine import WindowSpec, run
from streaming_feature_pipeline.generator import GeneratorConfig, generate
from streaming_feature_pipeline.kafka_io import PRODUCER_CONFIG, consume, produce


class FakeProducer:
    """Records what would be sent; can simulate undelivered messages."""

    def __init__(self, undelivered: int = 0) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.undelivered = undelivered

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        self.sent.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return self.undelivered


def test_events_are_keyed_by_item_with_json_values() -> None:
    fake = FakeProducer()
    assert produce([click(5, "i1"), click(6, "i2")], "unused:9092", "clicks", producer=fake) == 2
    assert [(t, k) for t, k, _ in fake.sent] == [("clicks", b"i1"), ("clicks", b"i2")]
    assert fake.sent[0][2].startswith(b'{"event_id":')


def test_the_producer_is_idempotent_and_fully_acknowledged() -> None:
    assert PRODUCER_CONFIG["acks"] == "all"
    assert PRODUCER_CONFIG["enable.idempotence"] is True


def test_undelivered_messages_fail_loudly() -> None:
    with pytest.raises(RuntimeError, match="undelivered"):
        produce([click(1)], "unused:9092", "clicks", producer=FakeProducer(undelivered=1))


@pytest.mark.kafka
@pytest.mark.skipif(
    not os.environ.get("KAFKA_BOOTSTRAP"), reason="set KAFKA_BOOTSTRAP to a broker (CI starts one)"
)
def test_round_trip_through_a_real_broker_preserves_order_and_results() -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    events = [a.event for a in generate(GeneratorConfig(seed=21, events=5_000, late_fraction=0.02))]
    topic = f"clicks-{os.getpid()}"
    assert produce(events, bootstrap, topic) == len(events)
    back = consume(bootstrap, topic, expected=len(events), timeout_s=120)
    assert back == events, "a single-partition topic keeps arrival order exactly"
    spec = WindowSpec(600_000, 60_000)
    assert (
        run(back, spec, 5_000, 30_000).final_counts()
        == run(events, spec, 5_000, 30_000).final_counts()
    )
