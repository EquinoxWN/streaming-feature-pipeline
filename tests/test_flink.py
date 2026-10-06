"""The PyFlink job against the reference engine (Linux or macOS with Java; runs in CI)."""

from __future__ import annotations

import importlib.util

import pytest
from helpers import click

from streaming_feature_pipeline.engine import WindowSpec, batch_counts, run
from streaming_feature_pipeline.generator import GeneratorConfig, generate

pytestmark = [
    pytest.mark.flink,
    pytest.mark.skipif(
        importlib.util.find_spec("pyflink") is None, reason="PyFlink is not installed (runs in CI)"
    ),
]

TEN_MIN = WindowSpec(600_000, 60_000)


def test_without_disorder_flink_the_reference_and_batch_agree_exactly() -> None:
    from streaming_feature_pipeline.flink_job import run_flink

    events = sorted(
        (a.event for a in generate(GeneratorConfig(seed=31, events=1_500))),
        key=lambda e: e.event_time,
    )
    flink = run_flink(events, TEN_MIN, 5_000, 30_000)
    assert flink.late == []
    assert (
        flink.final_counts()
        == batch_counts(events, TEN_MIN)
        == run(events, TEN_MIN, 5_000, 30_000).final_counts()
    )


def test_with_disorder_flink_stays_between_the_reference_and_batch() -> None:
    from streaming_feature_pipeline.flink_job import run_flink

    events = [a.event for a in generate(GeneratorConfig(seed=32, events=2_000, late_fraction=0.03))]
    reference = run(events, TEN_MIN, 5_000, 30_000)
    flink = run_flink(events, TEN_MIN, 5_000, 30_000)
    flink_late = [e.event_id for e in flink.late]
    assert len(flink_late) == len(set(flink_late)), "no element is reported late twice"
    assert set(flink_late) <= {e.event_id for e in reference.late}, (
        "Flink's lagging watermark can only mark fewer late"
    )
    ref, fl, batch = reference.final_counts(), flink.final_counts(), batch_counts(events, TEN_MIN)
    for key, count in fl.items():
        assert ref.get(key, 0) <= count <= batch[key], key


def test_a_late_element_after_the_watermark_advanced_goes_to_flinks_side_output() -> None:
    from streaming_feature_pipeline.flink_job import run_flink

    events = [click(ts) for ts in range(0, 700_001, 5_000)]
    straggler = click(1_000, n=9)
    flink = run_flink(
        [*events, straggler], TEN_MIN, 5_000, 30_000, pause_before=[straggler.event_id]
    )
    assert [e.event_id for e in flink.late] == [straggler.event_id]
    assert [e.event_id for e in run([*events, straggler], TEN_MIN, 5_000, 30_000).late] == [
        straggler.event_id
    ]
