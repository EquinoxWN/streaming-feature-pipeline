"""Event-time semantics, checked by hand on small streams and as properties on generated ones."""

from __future__ import annotations

import pytest
from helpers import click, view, watermark_before

from streaming_feature_pipeline.engine import (
    Emission,
    EventTimeEngine,
    WindowSpec,
    batch_counts,
    run,
)
from streaming_feature_pipeline.generator import GeneratorConfig, generate

TEN_MIN = WindowSpec(600_000, 60_000)


def test_sliding_window_assignment_follows_flinks_formula() -> None:
    spec = WindowSpec(10, 5)
    assert spec.assign(12) == [(10, 20), (5, 15)]
    assert spec.assign(10) == [(10, 20), (5, 15)]
    assert spec.assign(9) == [(5, 15), (0, 10)]
    assert WindowSpec(5, 5).assign(7) == [(5, 10)], "tumbling: one window"
    windows = TEN_MIN.assign(1_767_225_654_321)
    assert len(windows) == 10
    assert all(s <= 1_767_225_654_321 < e and e - s == 600_000 for s, e in windows)


@pytest.mark.parametrize(("size", "slide"), [(0, 1), (10, 0), (10, 3), (-10, 5)])
def test_invalid_window_specs_are_rejected(size: int, slide: int) -> None:
    with pytest.raises(ValueError, match="size"):
        WindowSpec(size, slide)


def test_watermark_is_newest_timestamp_minus_bound_minus_one_and_never_goes_back() -> None:
    engine = EventTimeEngine(WindowSpec(10, 10), out_of_orderness_ms=3, allowed_lateness_ms=0)
    engine.process(click(20))
    assert engine.watermark == 16
    engine.process(click(5))
    assert engine.watermark == 16, "an older event does not move the watermark back"
    engine.process(view(30))
    assert engine.watermark == 26, "views advance time even though they are not counted"


def test_a_window_fires_once_the_watermark_passes_its_last_millisecond() -> None:
    engine = EventTimeEngine(WindowSpec(10, 10), out_of_orderness_ms=0, allowed_lateness_ms=0)
    for ts in (1, 4, 9):
        engine.process(click(ts))
    assert engine.result.emissions == [], "watermark 8 has not passed 9 yet"
    engine.process(click(10))
    assert engine.result.emissions == [Emission("a", 0, 10, 3, "on_time")]


def test_late_elements_within_allowed_lateness_refire_the_window_and_later_ones_go_to_the_side_output() -> (
    None
):
    engine = EventTimeEngine(WindowSpec(10, 10), out_of_orderness_ms=0, allowed_lateness_ms=5)
    for e in (click(2), click(12)):  # watermark 11: window [0,10) fired with 1
        engine.process(e)
    engine.process(click(3, n=1))  # late but within lateness (cleanup at 9 + 5 = 14)
    engine.process(click(16))  # watermark 15 >= 14: window [0,10) purged
    engine.process(click(4, n=2))  # too late for every window
    result = engine.finish()
    assert result.emissions[:2] == [
        Emission("a", 0, 10, 1, "on_time"),
        Emission("a", 0, 10, 2, "late_firing"),
    ]
    assert [e.event_time for e in result.late] == [4]
    assert result.final_counts() == {("a", 0, 10): 2, ("a", 10, 20): 2}


def test_with_sliding_windows_a_late_event_still_counts_in_the_windows_that_are_open() -> None:
    engine = EventTimeEngine(WindowSpec(20, 10), out_of_orderness_ms=0, allowed_lateness_ms=0)
    engine.process(click(5))
    engine.process(click(21))  # watermark 20: window [0,20) has closed, [10,30) has not
    engine.process(click(15, n=1))  # belongs to [0,20) and [10,30)
    result = engine.finish()
    assert result.late == [], "not every window is closed, so it is not a late element"
    counts = result.final_counts()
    assert counts[("a", 0, 20)] == 1, "silently missing from the closed window"
    assert counts[("a", 10, 30)] == 2, "but counted in the open one"


def test_end_of_input_fires_every_open_window() -> None:
    result = run([click(1), click(15, "b")], WindowSpec(10, 10), 0, 0)
    assert result.final_counts() == {("a", 0, 10): 1, ("b", 10, 20): 1}
    assert all(e.kind == "on_time" for e in result.emissions)


def test_items_are_windowed_independently() -> None:
    result = run([click(1, "a"), click(2, "b"), click(3, "a")], WindowSpec(10, 10), 0, 0)
    assert result.final_counts() == {("a", 0, 10): 2, ("b", 0, 10): 1}


def test_negative_bounds_are_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        EventTimeEngine(TEN_MIN, -1, 0)


SEEDS = [1, 2, 3, 4, 5]


def stream(seed: int, events: int = 4_000) -> list:  # type: ignore[type-arg]
    return generate(GeneratorConfig(seed=seed, events=events, late_fraction=0.02))


@pytest.mark.parametrize("seed", SEEDS)
def test_a_perfectly_ordered_stream_gives_exactly_the_batch_result(seed: int) -> None:
    events = sorted((a.event for a in stream(seed)), key=lambda e: e.event_time)
    result = run(events, TEN_MIN, 5_000, 30_000)
    assert result.late == []
    assert all(e.kind == "on_time" for e in result.emissions)
    assert result.final_counts() == batch_counts(events, TEN_MIN)


@pytest.mark.parametrize("seed", SEEDS)
def test_every_click_is_counted_or_sent_to_the_side_output_never_lost(seed: int) -> None:
    arrivals = stream(seed)
    events = [a.event for a in arrivals]
    result = run(events, TEN_MIN, 5_000, 30_000)
    final, batch = result.final_counts(), batch_counts(events, TEN_MIN)
    assert set(final) <= set(batch)
    assert all(final[k] <= batch[k] for k in final), (
        "a window never counts more than really happened"
    )
    late_ids = {e.event_id for e in result.late}
    wm = watermark_before(events, 5_000)
    accepted = 0
    for i, e in enumerate(events):
        if e.kind != "click":
            continue
        open_windows = [w for w in TEN_MIN.assign(e.event_time) if w[1] - 1 + 30_000 > wm[i]]
        if e.event_id in late_ids:
            assert not open_windows, "only clicks with no open window go to the side output"
            assert e.event_time + 30_000 <= wm[i]
        else:
            assert open_windows, "a click with no open window must not vanish silently"
            accepted += len(open_windows)
    assert sum(final.values()) == accepted, "final counts add up to exactly the accepted clicks"


@pytest.mark.parametrize("seed", SEEDS)
def test_designed_late_events_are_caught_and_ordinary_disorder_never_is(seed: int) -> None:
    arrivals = stream(seed)
    result = run([a.event for a in arrivals], TEN_MIN, 5_000, 30_000)
    late_ids = {e.event_id for e in result.late}
    designed = {a.event.event_id for a in arrivals if a.late_by_design and a.event.kind == "click"}
    assert late_ids == designed, (
        "delays past a whole window are late; delays of at most 4 s never are"
    )
    assert designed, "the stream contains late events"


@pytest.mark.parametrize("seed", SEEDS)
def test_stragglers_refire_windows_and_without_them_nothing_refires(seed: int) -> None:
    with_stragglers = run([a.event for a in stream(seed)], TEN_MIN, 5_000, 30_000)
    assert any(e.kind == "late_firing" for e in with_stragglers.emissions)
    calm = generate(GeneratorConfig(seed=seed, events=4_000, straggler_fraction=0, late_fraction=0))
    assert all(
        e.kind == "on_time" for e in run([a.event for a in calm], TEN_MIN, 5_000, 30_000).emissions
    )


@pytest.mark.parametrize("seed", SEEDS)
def test_more_tolerance_never_makes_more_events_late(seed: int) -> None:
    events = [a.event for a in stream(seed)]

    def late(bound: int, lateness: int) -> set[str]:
        return {e.event_id for e in run(events, TEN_MIN, bound, lateness).late}

    assert late(5_000, 120_000) <= late(5_000, 30_000) <= late(5_000, 0)
    assert late(60_000, 30_000) <= late(5_000, 30_000)
