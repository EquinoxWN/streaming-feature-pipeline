# ADR 0002: Check the Flink job against a deterministic reference engine, with provable bounds

- **Status:** Accepted

## Context

Flink emits watermarks periodically (every 200 ms by default), not after each event. Which
elements count as late therefore depends on how fast the job happens to run: the same input can
yield different late sets on a fast and a slow machine. Asserting exact window counts or exact late
sets for a disordered stream would make the tests flaky, and asserting only "it ran" proves
nothing. Flink also cannot run on the author's Windows machine.

## Decision

Implement Flink's window rules in a small, deterministic Python engine whose watermark advances
after every event, the earliest moment Flink's could. Test that engine exhaustively (hand-worked
cases and properties over generated streams). In CI, run the PyFlink job on the same input and
assert what must hold for any watermark schedule:

- Without disorder, Flink, the engine and a batch computation agree exactly.
- With disorder, Flink's watermark is never ahead of the engine's, so every element Flink marks
  late, the engine marks late too (subset), and each final window count lies between the
  engine's count and the complete batch count.
- To show Flink's side output working, a test pauses the source before one very late element (the
  map runs as its own task), so the periodic watermark has advanced when it arrives.

## Consequences

- The Flink tests are deterministic despite periodic watermarks, and they would catch a wrong
  window size, slide, bound, lateness or keying.
- Exact late sets are only asserted for the reference engine; Flink's exact behaviour under load
  is left to M3 measurements.
- The engine is extra code to maintain; its docstring names the Flink classes it mirrors so it can
  be re-checked when Flink changes.
