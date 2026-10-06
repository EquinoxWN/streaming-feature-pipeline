# ADR 0003: Allowed lateness plus a side output, and generated data that exercises both

- **Status:** Accepted

## Context

Late data is where streaming features most often go wrong, and it is easy to test nothing: if the
generated stream only contains mild disorder below the watermark bound, no window ever re-fires and
nothing is ever late, so the tests pass whatever the late-data code does. With sliding windows
there is also a subtle middle case: an element can be too late for its older windows and still in
time for newer ones.

## Decision

- Windows allow 30 s of lateness: an element arriving after its window fired but before the
  cleanup time updates the window and re-fires it with the corrected count.
- Elements whose every window is past cleanup go to a side output instead of being dropped.
- The generator produces three classes on purpose: ordinary disorder (at most 4 s, below the 5 s
  bound), stragglers (10 to 60 s, which re-fire windows) and late events (12 to 30 minutes,
  longer than a window plus lateness, which reach the side output).

## Consequences

- Tests prove that exactly the designed-late clicks reach the side output and ordinary disorder
  never does, that stragglers cause re-firings (and that a calm stream causes none), and that
  nothing is lost: every click is either counted in at least one window or in the side output.
- Consumers of the feature receive updated counts after a re-firing, so the online store must
  overwrite by window key (M3); the last emission per window is the final value.
- An element dropped from some windows but not others is not in the side output; that case is
  tested and documented, and M2 adds a metric for it.
