"""clickgen: generate a click stream to a JSON-lines file or to Kafka, and summarise its disorder."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from streaming_feature_pipeline.engine import WindowSpec, run
from streaming_feature_pipeline.generator import GeneratorConfig, disorder_stats, generate


def main(argv: list[str] | None = None) -> int:
    """Run the command line."""
    p = argparse.ArgumentParser(prog="clickgen", description=__doc__)
    p.add_argument("--events", type=int, default=10_000)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--late-fraction", type=float, default=0.01)
    p.add_argument("--out", type=Path, help="write JSON lines here")
    p.add_argument("--kafka", metavar="BOOTSTRAP", help="produce to this Kafka cluster")
    p.add_argument("--topic", default="clicks")
    p.add_argument("--window-min", type=int, default=10)
    p.add_argument("--slide-min", type=int, default=1)
    p.add_argument("--bound-s", type=int, default=5)
    p.add_argument("--lateness-s", type=int, default=30)
    args = p.parse_args(argv)
    arrivals = generate(
        GeneratorConfig(seed=args.seed, events=args.events, late_fraction=args.late_fraction)
    )
    events = [a.event for a in arrivals]
    if args.out:
        args.out.write_text("".join(e.to_json() + "\n" for e in events), encoding="utf-8")
    if args.kafka:
        from streaming_feature_pipeline.kafka_io import produce

        produce(events, args.kafka, args.topic)
    spec = WindowSpec(args.window_min * 60_000, args.slide_min * 60_000)
    result = run(events, spec, args.bound_s * 1000, args.lateness_s * 1000)
    summary = {
        **disorder_stats(arrivals),
        "stragglers": sum(a.straggler for a in arrivals),
        "late_by_design": sum(a.late_by_design for a in arrivals),
        "windows": len(result.final_counts()),
        "emissions": len(result.emissions),
        "late_firings": sum(e.kind == "late_firing" for e in result.emissions),
        "side_output": len(result.late),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
