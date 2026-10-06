"""Write generated events to Kafka and read them back (needs the `kafka` extra)."""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Any

from streaming_feature_pipeline.events import ClickEvent

# Idempotent, fully acknowledged writes: a retry can never duplicate or reorder a partition.
PRODUCER_CONFIG = {
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,
    "compression.type": "zstd",
}


def produce(events: Iterable[ClickEvent], bootstrap: str, topic: str, producer: Any = None) -> int:
    """Send events keyed by item (so one item's events stay in order in one partition)."""
    if producer is None:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": bootstrap, **PRODUCER_CONFIG})
    errors: list[str] = []

    def on_delivery(err: Any, _msg: Any) -> None:
        if err is not None:
            errors.append(str(err))

    sent = 0
    for e in events:
        producer.produce(
            topic, key=e.item_id.encode(), value=e.to_json().encode(), on_delivery=on_delivery
        )
        producer.poll(0)
        sent += 1
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"{remaining} undelivered, errors: {errors[:3]}")
    return sent


def consume(
    bootstrap: str, topic: str, expected: int, timeout_s: float = 60, group: str | None = None
) -> list[ClickEvent]:
    """Read from the start of the topic until `expected` events arrive or the timeout passes."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group or f"reader-{time.time_ns()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    out: list[ClickEvent] = []
    deadline = time.monotonic() + timeout_s
    try:
        while len(out) < expected and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(f"consumer error: {msg.error()}")
            value = msg.value()
            if value is None:
                raise RuntimeError("unexpected tombstone in the click topic")
            out.append(ClickEvent.from_json(value))
    finally:
        consumer.close()
    return out
