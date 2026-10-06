"""
The same windows on Apache Flink (PyFlink 2.3): event time, bounded-out-of-orderness watermarks,
sliding windows with allowed lateness, and late elements in a side output.

Flink publishes PyFlink wheels for Linux and macOS only, so this module runs in CI.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence

from streaming_feature_pipeline.engine import Emission, RunResult, WindowSpec
from streaming_feature_pipeline.events import ClickEvent


def run_flink(
    events: Sequence[ClickEvent],
    spec: WindowSpec,
    out_of_orderness_ms: int,
    allowed_lateness_ms: int,
    pause_before: Iterable[str] = (),
    pause_ms: int = 1_500,
) -> RunResult:
    """
    Run the job on a bounded source in arrival order and collect window results and late
    elements. Events whose id is in pause_before are held back by pause_ms first, which gives
    Flink's periodic watermark time to advance (used to make lateness deterministic in tests).
    """
    from pyflink.common import Duration, Types, WatermarkStrategy
    from pyflink.common.time import Time
    from pyflink.datastream import OutputTag, StreamExecutionEnvironment
    from pyflink.datastream.window import SlidingEventTimeWindows

    from streaming_feature_pipeline.flink_functions import Count, EventTime, Parse

    row = Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()]
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    late_tag = OutputTag("late", row)
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_millis(out_of_orderness_ms)
    ).with_timestamp_assigner(EventTime())
    clicks = (
        env.from_collection([e.to_json() for e in events], type_info=Types.STRING())
        .map(Parse(frozenset(pause_before), pause_ms), output_type=row)
        .disable_chaining()  # a separate task, so the watermark timer can fire during a pause
        # Views advance the watermark too (as in the reference engine); only clicks are counted.
        .assign_timestamps_and_watermarks(watermarks)
        .filter(lambda r: r[3] == "click")
    )
    windows = (
        clicks.key_by(lambda r: r[2], key_type=Types.STRING())
        .window(
            # PyFlink's window assigners still take its own Time class, not Duration.
            SlidingEventTimeWindows.of(
                Time.milliseconds(spec.size_ms), Time.milliseconds(spec.slide_ms)
            )
        )
        .allowed_lateness(allowed_lateness_ms)
        .side_output_late_data(late_tag)
        .process(Count(), output_type=Types.STRING())
    )
    late = windows.get_side_output(late_tag).map(
        lambda r: json.dumps(
            {"t": "late", "id": r[0], "user": r[1], "item": r[2], "kind": r[3], "ts": r[4]}
        ),
        output_type=Types.STRING(),
    )
    result = RunResult()
    with windows.union(late).execute_and_collect() as rows:
        for raw in rows:
            r = json.loads(raw)
            if r["t"] == "window":
                result.emissions.append(
                    Emission(r["item"], r["start"], r["end"], r["count"], "flink")
                )
            else:
                result.late.append(ClickEvent(r["id"], r["user"], r["item"], r["kind"], r["ts"]))
    return result
