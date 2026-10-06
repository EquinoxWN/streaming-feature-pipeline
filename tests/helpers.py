"""Small builders shared by the engine tests."""

from __future__ import annotations

from streaming_feature_pipeline.events import ClickEvent


def click(ts: int, item: str = "a", n: int = 0) -> ClickEvent:
    """A click at ts milliseconds."""
    return ClickEvent(f"e{ts}-{item}-{n}", "u1", item, "click", ts)


def view(ts: int, item: str = "a") -> ClickEvent:
    """A view (advances time, is not counted)."""
    return ClickEvent(f"v{ts}-{item}", "u1", item, "view", ts)


def watermark_before(events: list[ClickEvent], bound: int) -> list[int]:
    """The reference watermark in effect when each event arrived."""
    out = []
    newest = None
    for e in events:
        out.append(-(2**63) if newest is None else newest - bound - 1)
        newest = e.event_time if newest is None else max(newest, e.event_time)
    return out
