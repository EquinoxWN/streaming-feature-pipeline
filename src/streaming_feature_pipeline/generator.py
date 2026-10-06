"""Generate click streams the way they really arrive: mostly a little out of order, a few very late."""

from __future__ import annotations

import bisect
import itertools
import random
from dataclasses import dataclass

from streaming_feature_pipeline.events import ClickEvent


@dataclass(frozen=True)
class GeneratorConfig:
    """Shape of the stream. Delays are arrival time minus event time, in milliseconds."""

    seed: int = 7
    events: int = 10_000
    start_ms: int = 1_767_225_600_000  # 2026-01-01T00:00:00Z
    rate_per_s: float = 50.0
    users: int = 500
    items: int = 200
    zipf_s: float = 1.1
    mean_delay_ms: float = 400.0
    max_delay_ms: int = 4_000
    # Stragglers: later than the watermark bound but within a window plus lateness (re-fire windows).
    straggler_fraction: float = 0.02
    straggler_delay_ms: tuple[int, int] = (10_000, 60_000)
    late_fraction: float = 0.01
    # Longer than a whole 10-minute window plus lateness: a phone that was offline, then syncs.
    late_delay_ms: tuple[int, int] = (720_000, 1_800_000)
    view_fraction: float = 0.3


@dataclass(frozen=True)
class Arrival:
    """An event and the moment it reached the pipeline (processing time)."""

    event: ClickEvent
    arrival_ms: int
    late_by_design: bool
    straggler: bool = False


def _zipf_cdf(n: int, s: float) -> list[float]:
    weights = list(itertools.accumulate(1 / (k**s) for k in range(1, n + 1)))
    return [w / weights[-1] for w in weights]


def generate(config: GeneratorConfig) -> list[Arrival]:
    """Events in arrival order; the same config always gives the same stream."""
    rng = random.Random(config.seed)
    cdf = _zipf_cdf(config.items, config.zipf_s)
    out: list[Arrival] = []
    arrival = float(config.start_ms)
    for i in range(config.events):
        arrival += rng.expovariate(config.rate_per_s / 1000)
        roll = rng.random()
        late = roll < config.late_fraction
        straggler = not late and roll < config.late_fraction + config.straggler_fraction
        if late:
            delay = rng.randint(*config.late_delay_ms)
        elif straggler:
            delay = rng.randint(*config.straggler_delay_ms)
        else:
            delay = min(int(rng.expovariate(1 / config.mean_delay_ms)), config.max_delay_ms)
        item = bisect.bisect_left(cdf, rng.random())
        event = ClickEvent(
            event_id=f"e{i:07d}",
            user_id=f"u{rng.randrange(config.users):05d}",
            item_id=f"i{item:05d}",
            kind="view" if rng.random() < config.view_fraction else "click",
            event_time=int(arrival) - delay,
        )
        out.append(Arrival(event, int(arrival), late, straggler))
    return out


def disorder_stats(arrivals: list[Arrival]) -> dict[str, float]:
    """How out of order a stream is: share behind the newest event seen, and the largest gap."""
    newest = -1
    behind = 0
    worst = 0
    for a in arrivals:
        t = a.event.event_time
        if t < newest:
            behind += 1
            worst = max(worst, newest - t)
        newest = max(newest, t)
    return {
        "events": len(arrivals),
        "out_of_order_share": behind / max(1, len(arrivals)),
        "max_behind_ms": worst,
    }
