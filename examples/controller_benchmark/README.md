# Controller and CPU benchmark

This example measures a bounded local operating envelope. It measures API and
SQLite overhead separately from real CPU experiments. It does not establish a
maximum worker count, multi-host scaling, GPU efficiency or scientific progress.

```sh
uv run python examples/controller_benchmark/controller_benchmark.py \
  --state ./controller-benchmark-state \
  --output ./controller-benchmark-results
```

Use a private state directory and at least two observed CPU slots. The runner
registers one native worker with two slots and uses a task-owned machine registry.
All CPU cells reuse that registration, runtime and execution fingerprint. Each
cell has a separate campaign and durable command intents. No cloud credentials,
external model or accelerator framework is required.

The default panel has 14 cells:

| Scenario | Allocation | What it measures |
| --- | --- | --- |
| Controller traffic | 1, 2, 4 and 8 connections; 64 heartbeats each | Accepted commands/second, p50/p95/p99 latency, queries every fourth heartbeat and exact replay every eighth |
| Budget contention | 2, 4 and 8 independent database connections | A race for the last affordable reservation, expected rejections and exactly-once settlement |
| Reopen and replay | One discarded command acknowledgment | Native runtime reopen plus exact replay latency and preserved command identity |
| Native throughput | Eight trials per cell; one versus two driver lanes; three paired repetitions | Eligible trials/hour, budget closure, stage intervals, optimizer steps, evaluator receipts and overlap |

Controller connections are synthetic traffic against the in-process API, not
separate physical workers or an HTTP load test. Their identities cannot launch
native work. Reservation latency includes expected `BUDGET_EXHAUSTED` rejections;
these are recorded as rejected samples and do not invalidate correct accounting.
The recovery cell does not simulate a killed controller or measure live-process
recovery. Separate native failure-injection tests cover those correctness cases.

CPU trials use the ordinary `cpu_bigram` project, its protected independent
evaluator and its declared 300 ms active-training budget. Training subprocess
startup, supervision, artifact publication and evaluation are included in the
drain timer. Runtime/enrollment setup and source capture/admission are reported
separately. Trial failures stay in the denominator and evidence. A result is
valid only when all planned receipts are eligible, every stage has one attempt,
and no budget reservation or uncertainty remains.

Both lane counts use the same seed panel within each repetition. Execution order
alternates by repetition. Ratios require matching actual source-tree, protocol,
environment, evaluator, data, stages, seed panel and execution fingerprint.
The runner checks source hashes before and after each capture. Native execution
then uses the captured immutable artifact. Setup caches and OS caches are not
cleared; the report states this limitation. Other activity and thermal conditions
on the host remain uncontrolled.

`result.json` retains every cell, latency sample, result identity, failed outcome
and evaluator receipt. `report.md` is a concise rendering of the same cells and
marks invalid or censored measurements explicitly. The implementation, runner,
workload and system profile are frozen before execution. Changing frozen inputs
requires a new state directory.

Run the same command to reopen unfinished state. Completed cells are reused.
An interrupted native cell resumes its original campaigns and attempts, but its
elapsed time and rate remain missing because an uninterrupted timer no longer
exists. Interrupted traffic cells are censored rather than rerun under the same
identity. Failed cells are retained. A native failure prevents later CPU cells
from starting. A censored, invalid or unfinished panel exits with status 2 and
cannot produce a complete paired average.

For a smaller diagnostic panel, pass `--definition` with a JSON file:

```json
{"connections": [1, 2], "commands": 8, "trials": 2, "repeats": 1}
```

CI runs the small correctness and interrupted-publication tests without speed
thresholds. Full measurements are opt-in. The included
[local report](results/local-cpu/report.md) and
[machine-readable results](results/local-cpu/result.json) describe one measured
host and the exact frozen inputs in those files.

## Included measurement

The included panel completed all 14 cells and all 48 CPU trials with eligible
receipts, 96 stage attempts, and no outstanding budget reservation or uncertainty.
Paired two-lane speedups were 1.77×, 2.02× and 1.80×; the median was 1.80×, or
about 90% parallel efficiency. The slightly superlinear second pair is a noisy
short-workload observation, not evidence of sustained superlinear scaling.

Synthetic heartbeat throughput ranged from 514 to 882 accepted commands/second
across the four connection counts, with no failed command/query/replay samples.
Each budget race admitted exactly one reservation. Runtime reopen and exact replay
took 63.9 ms in its single sample. These are descriptive local measurements;
three CPU pairs and one recovery sample do not establish confidence bounds or
production capacity. The JSON contains the exact system and implementation hashes.
