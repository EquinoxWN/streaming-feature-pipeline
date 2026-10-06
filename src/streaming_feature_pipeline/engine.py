"""
A deterministic event-time window engine with Flink's semantics, used as the reference model.

It follows Flink's WindowOperator rules for sliding event-time windows: window assignment, a
bounded-out-of-orderness watermark (newest timestamp minus the bound minus 1 ms), firing when the
watermark passes a window's last millisecond, re-firing for late elements within the allowed
lateness, purging at the cleanup time, and sending elements whose every window is too late to a
side output. The one deliberate difference: the watermark advances after every event
("punctuated"), the earliest moment Flink's periodic watermark could, so Flink can only ever
treat fewer elements as late than this engine does.
"""

from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field

from streaming_feature_pipeline.events import ClickEvent

MIN_WATERMARK = -(2**63)
MAX_WATERMARK = 2**63 - 1


@dataclass(frozen=True)
class WindowSpec:
    """Sliding windows of size_ms every slide_ms (Flink's SlidingEventTimeWindows, offset 0)."""

    size_ms: int
    slide_ms: int

    def __post_init__(self) -> None:
        if self.size_ms <= 0 or self.slide_ms <= 0 or self.size_ms % self.slide_ms:
            raise ValueError("size and slide must be positive, and size a multiple of slide")

    def assign(self, ts: int) -> list[tuple[int, int]]:
        """Every [start, end) window that contains ts, latest first (Flink's order)."""
        last_start = ts - (ts + self.slide_ms) % self.slide_ms
        out = []
        start = last_start
        while start > ts - self.size_ms:
            out.append((start, start + self.size_ms))
            start -= self.slide_ms
        return out


@dataclass(frozen=True)
class Emission:
    """A window result: the click count for one item and window, and why it was emitted."""

    item_id: str
    start: int
    end: int
    count: int
    kind: str  # "on_time" (watermark passed the window end) or "late_firing" (late element within lateness)


@dataclass
class RunResult:
    """Everything one run produced."""

    emissions: list[Emission] = field(default_factory=list)
    late: list[ClickEvent] = field(default_factory=list)

    def final_counts(self) -> dict[tuple[str, int, int], int]:
        """The last emitted count of every window."""
        out: dict[tuple[str, int, int], int] = {}
        for e in self.emissions:
            out[(e.item_id, e.start, e.end)] = e.count
        return out


class EventTimeEngine:
    """Counts click events per item in sliding windows; feed events in arrival order."""

    def __init__(
        self, spec: WindowSpec, out_of_orderness_ms: int, allowed_lateness_ms: int
    ) -> None:
        if out_of_orderness_ms < 0 or allowed_lateness_ms < 0:
            raise ValueError("bounds must be non-negative")
        self.spec = spec
        self.bound = out_of_orderness_ms
        self.lateness = allowed_lateness_ms
        self.watermark = MIN_WATERMARK
        self._max_ts = MIN_WATERMARK
        self._state: dict[tuple[str, int, int], int] = defaultdict(int)
        self._timers: list[tuple[int, int, str, tuple[str, int, int]]] = []
        self._seq = 0
        self.result = RunResult()

    def _cleanup_time(self, window_end: int) -> int:
        return window_end - 1 + self.lateness

    def _timer(self, time: int, kind: str, key: tuple[str, int, int]) -> None:
        self._seq += 1
        heapq.heappush(self._timers, (time, self._seq, kind, key))

    def process(self, event: ClickEvent) -> None:
        """Apply one event with the current watermark, then advance the watermark."""
        if event.kind == "click":
            accepted = False
            for start, end in self.spec.assign(event.event_time):
                if self._cleanup_time(end) <= self.watermark:
                    continue  # this window is past its allowed lateness
                accepted = True
                key = (event.item_id, start, end)
                if key not in self._state:
                    self._timer(end - 1, "fire", key)
                    self._timer(self._cleanup_time(end), "cleanup", key)
                self._state[key] += 1
                if end - 1 <= self.watermark:
                    self.result.emissions.append(Emission(*key, self._state[key], "late_firing"))
            if not accepted and event.event_time + self.lateness <= self.watermark:
                self.result.late.append(event)
        self._max_ts = max(self._max_ts, event.event_time)
        self._advance(self._max_ts - self.bound - 1)

    def finish(self) -> RunResult:
        """End of input: the watermark jumps to the maximum and every window fires."""
        self._advance(MAX_WATERMARK)
        return self.result

    def _advance(self, watermark: int) -> None:
        if watermark <= self.watermark:
            return
        self.watermark = watermark
        while self._timers and self._timers[0][0] <= watermark:
            _, _, kind, key = heapq.heappop(self._timers)
            if kind == "fire" and key in self._state:
                self.result.emissions.append(Emission(*key, self._state[key], "on_time"))
            elif kind == "cleanup":
                self._state.pop(key, None)


def run(
    events: Iterable[ClickEvent],
    spec: WindowSpec,
    out_of_orderness_ms: int,
    allowed_lateness_ms: int,
) -> RunResult:
    """Feed events in the given (arrival) order and finish."""
    engine = EventTimeEngine(spec, out_of_orderness_ms, allowed_lateness_ms)
    for e in events:
        engine.process(e)
    return engine.finish()


def batch_counts(events: Iterable[ClickEvent], spec: WindowSpec) -> dict[tuple[str, int, int], int]:
    """What a batch job over the complete data computes: every click counted in every window."""
    out: dict[tuple[str, int, int], int] = defaultdict(int)
    for e in events:
        if e.kind == "click":
            for start, end in spec.assign(e.event_time):
                out[(e.item_id, start, end)] += 1
    return dict(out)
