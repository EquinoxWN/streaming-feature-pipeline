"""
PyFlink user functions for flink_job, kept at module level: PyFlink ships them to its Python
workers with cloudpickle, which pickles module-level classes by reference but classes defined
inside a function by value, together with state that cannot be pickled.

Imports PyFlink at the top, so only flink_job imports this module, and only when PyFlink is installed.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from typing import Any

from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction

from streaming_feature_pipeline.events import ClickEvent

Row = tuple[str, str, str, str, int]


class Parse(MapFunction):  # type: ignore[misc]
    """JSON click to a row, holding back the events listed in pauses."""

    def __init__(self, pauses: frozenset[str], pause_ms: int) -> None:
        self.pauses, self.pause_ms = pauses, pause_ms

    def map(self, value: str) -> Row:
        """Parse one event."""
        e = ClickEvent.from_json(value)
        if e.event_id in self.pauses:
            time.sleep(self.pause_ms / 1000)
        return (e.event_id, e.user_id, e.item_id, e.kind, e.event_time)


class EventTime(TimestampAssigner):  # type: ignore[misc]
    """Event time from the row's timestamp field."""

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        """The event's own time, in milliseconds."""
        return int(value[4])


class Count(ProcessWindowFunction):  # type: ignore[misc]
    """Click count of one item in one window, as JSON."""

    def process(self, key: str, context: Any, elements: Iterable[Any]) -> Iterable[str]:
        """Emit the window's count."""
        w = context.window()
        yield json.dumps(
            {
                "t": "window",
                "item": key,
                "start": w.start,
                "end": w.end,
                "count": len(list(elements)),
            }
        )
