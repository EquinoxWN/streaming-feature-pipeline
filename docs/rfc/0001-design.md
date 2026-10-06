# RFC 0001: streaming-feature-pipeline design

- **Status:** Accepted (M1 implemented)
- **Author:** EquinoxWN
- **Created:** 2026

## Problem

A recommendation model trained on "clicks per item in the last 10 minutes" needs that number
computed the same way at training time (from historical data) and at serving time (from a live
stream). Real click streams do not arrive in order: phones batch events, networks delay them,
and an offline device can upload half an hour of clicks at once. If the streaming job counts by
arrival time, or silently drops late events, the live feature disagrees with the training data and
the model quietly gets worse. The job has to process by **event time**, decide explicitly when a
window is complete, treat late data deliberately, and later survive crashes without double
counting.

## Goals

- A generator produces realistic click streams in arrival order, with three controlled kinds of
  delay: ordinary disorder (up to 4 s), stragglers (10 to 60 s) and late events (12 to 30
  minutes), plus skewed item popularity; the same seed always gives the same stream. It writes
  JSON lines or produces to Kafka with idempotent, fully acknowledged, item-keyed writes.
- Processing is by event time with Flink's semantics: bounded-out-of-orderness watermarks, sliding
  windows (10 minutes every minute per item), allowed lateness that re-fires windows, and a side
  output for elements whose windows have all closed, so nothing disappears silently.
- A deterministic reference engine implements those semantics in plain Python, and the PyFlink job
  is checked against it in CI.
- Later: RocksDB state and the keyed feature in production shape (M2), checkpoints with Kafka
  transactions for exactly-once results through a killed task manager (M2), Valkey and Iceberg
  sinks from the same code (M3), the Flink Kubernetes Operator and savepoints (M3).

## Non-goals

- Running Flink or Kafka on the author's Windows machine: Flink publishes PyFlink wheels for Linux
  and macOS only, and Docker is not used locally; both run in CI.
- Session windows, joins and multiple sources.
- A feature store product; this repo produces the feature and its correctness evidence.

## Proposed design

![architecture](../architecture.png)

```
clickgen ─► events in arrival order ──┬─► JSON lines
 (seeded)    on-time / straggler /    └─► Kafka topic "clicks" (key = item, acks=all, idempotent)
             late, Zipf item mix
                                       ┌─► reference engine (Python, punctuated watermark) ─┐
events ─► watermark = newest - 5 s - 1 ┤                                                    ├─► window counts
          sliding 10 min / 1 min       └─► PyFlink job (periodic watermark, CI)            ─┘   + late side output
          allowed lateness 30 s
```

| Part | M1 implementation |
|---|---|
| Event | `ClickEvent(event_id, user_id, item_id, kind, event_time)`; strict JSON validation of untrusted input (exact keys, id pattern, kind, non-negative integer milliseconds) |
| Generator | Poisson arrivals at 50 events/s, Zipf(1.1) items, exponential disorder capped at 4 s, 2% stragglers (10 to 60 s), 1% late (12 to 30 min), 30% views |
| Kafka | `confluent-kafka` producer with `acks=all`, idempotence, zstd, keyed by item; consumer reads from the earliest offset with strict parsing |
| Reference engine | Flink's `SlidingEventTimeWindows` assignment formula; `BoundedOutOfOrdernessWatermarks` (newest minus bound minus 1); `WindowOperator` rules: skip windows past cleanup time, fire at the window's last millisecond, re-fire late elements within allowed lateness, purge at cleanup, side output when every window is late; the watermark advances after every event |
| Flink job | PyFlink 2.3 DataStream: timestamp assigner, `for_bounded_out_of_orderness`, key by item, sliding windows, `allowed_lateness`, `side_output_late_data`, a `ProcessWindowFunction` emitting counts with window bounds |
| Checks | Local: 57 tests on the event model, generator and engine. CI: the same, plus a Kafka round trip and three Flink-versus-reference tests |

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Count by processing (arrival) time | Simple and needs no watermarks, but the count then depends on network delays, so training and serving disagree; exactly the bug this repo exists to avoid. |
| Kafka Streams or ksqlDB | Good event-time support, but Java or SQL only and no Python job; Flink's Python API, savepoints and Kubernetes operator match the roadmap (M3). |
| Spark Structured Streaming | Micro-batches with watermarks work, but late data is dropped without a side output and latency is higher; Flink makes the late-data path explicit. |
| Only test the Flink job, with no reference engine | Flink's periodic watermark makes lateness depend on timing, so exact assertions would be flaky; a deterministic reference turns them into provable bounds. See ADR 0002. |
| Drop late events quietly | The default in many pipelines; it hides data loss. The side output keeps every fully late event visible and countable. See ADR 0003. |

## Measurement plan

- M1: 57 local tests (event validation, generator statistics and determinism, hand-worked engine
  cases, and five properties over generated streams, each with five seeds) plus CI-only tests:
  a Kafka round trip that must preserve order and results, and the Flink job checked against the
  reference (exact agreement without disorder; with disorder, Flink's late set is a subset of the
  reference's and every window count lies between the reference and the batch result; a
  deliberate late event lands in Flink's side output).
- M2: kill a task manager mid-stream and compare output counts with an uninterrupted run.
- M3: throughput and end-to-end latency at several event rates; Valkey and Iceberg agreement.

## Milestones

- **M1 (done):** generator with three delay classes, Kafka producer and consumer, reference
  event-time engine, PyFlink job with watermarks, allowed lateness and late side output, 57 local
  tests plus Kafka and Flink tests in CI.
- **M2:** RocksDB state backend, checkpoints and Kafka transactions, kill-the-worker test.
- **M3:** Valkey online and Iceberg offline sinks from one code path, Flink Kubernetes Operator,
  savepoint upgrades, proof table.

## Risks and open questions

- With sliding windows, an event a few minutes late is dropped from the windows that have closed
  but counted in those still open, without reaching the side output; this is Flink's behaviour and
  is now tested and documented, but a production job may want a metric for partially dropped
  events (M2).
- One Kafka partition keeps the M1 round trip exact; with several partitions Flink keeps a
  watermark per partition and the slowest one wins, which M2 tests.
- The reference engine is single-threaded Python (about 200,000 events per second here); it is a
  correctness oracle, not the production path.
