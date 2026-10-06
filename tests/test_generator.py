"""The generator reproduces a realistic, controllable mix of on-time, disordered and late events."""

from __future__ import annotations

import itertools
from collections import Counter

from streaming_feature_pipeline.generator import GeneratorConfig, disorder_stats, generate

CONFIG = GeneratorConfig(seed=11, events=20_000)


def test_the_same_seed_gives_the_same_stream_and_another_seed_does_not() -> None:
    assert generate(CONFIG) == generate(CONFIG)
    assert generate(CONFIG)[:50] != generate(GeneratorConfig(seed=12, events=20_000))[:50]


def test_events_arrive_in_processing_order_with_bounded_or_designed_delays() -> None:
    arrivals = generate(CONFIG)
    assert len(arrivals) == CONFIG.events
    assert all(a.arrival_ms <= b.arrival_ms for a, b in itertools.pairwise(arrivals))
    for a in arrivals:
        delay = a.arrival_ms - a.event.event_time
        if a.late_by_design:
            assert CONFIG.late_delay_ms[0] <= delay <= CONFIG.late_delay_ms[1]
        elif a.straggler:
            assert CONFIG.straggler_delay_ms[0] <= delay <= CONFIG.straggler_delay_ms[1]
        else:
            assert 0 <= delay <= CONFIG.max_delay_ms


def test_the_mix_matches_the_configuration() -> None:
    arrivals = generate(CONFIG)
    late_share = sum(a.late_by_design for a in arrivals) / len(arrivals)
    assert 0.007 < late_share < 0.013, late_share
    stragglers = sum(a.straggler for a in arrivals) / len(arrivals)
    assert 0.016 < stragglers < 0.024, stragglers
    views = sum(a.event.kind == "view" for a in arrivals) / len(arrivals)
    assert 0.28 < views < 0.32, views
    stats = disorder_stats(arrivals)
    assert stats["out_of_order_share"] > 0.2, "plenty of mild disorder"
    assert stats["max_behind_ms"] >= CONFIG.late_delay_ms[0] - CONFIG.max_delay_ms


def test_item_popularity_is_skewed_like_real_traffic() -> None:
    counts = Counter(a.event.item_id for a in generate(CONFIG))
    top = counts.most_common(1)[0][1]
    assert top > 10 * (CONFIG.events / CONFIG.items), (
        "the most popular item gets far more than its fair share"
    )
    assert len(counts) > CONFIG.items // 2


def test_ids_are_unique() -> None:
    ids = [a.event.event_id for a in generate(CONFIG)]
    assert len(set(ids)) == len(ids)
