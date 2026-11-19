# Run a campaign

Use a local CPU campaign to check the trial interface, search loop and
selection rule. The program example needs Python 3.11 or newer, uv and a
source checkout. Run these commands from the repository root.

## Run the supplied campaign

~~~sh
uv sync --locked --group dev
uv run --no-sync python -m research_swarm swarm run benchmarks/program.yaml --db .research/program.sqlite
~~~

The spec declares three programs and two worker slots. Each program receives
the same unit tests for its search score. The panel policy calibrates before
dispatch. It checks a challenger against the incumbent on separate selection
and confirmation cases before changing the selected trial.

Inspect the JSON output. `trials` counts submitted trials. `attempts` includes
retries. `failed` and `lost` count unsuccessful observations. `selected`
identifies the chosen recipe, its search score and the number of replicates.
`confirmed_score` reports the selected recipe's stored held-out measurement
when the panel policy supplies one. The normalized program should pass all
search cases. To inspect its promotion decision and exposure counts, use the
[operator guide's panel query](operator-guide.md#inspect-the-panel-study).

After the example finishes, run the same command again to resume. The queue
restores completed trials, and the panel study reuses recorded confirmation
scores. Choose a new campaign ID for a changed spec. Use a new panel study
when you change a split or scorer revision.

If the process stopped during calibration, inspect the stored looks before resuming.
[Panel recovery check](operator-guide.md#inspect-the-panel-study)

## Report what the run qualifies

Record the campaign ID, workload, data split, device, fidelity and budget
with the results. Use the [operator guide](operator-guide.md#read-campaign-kpis)
to read the campaign KPIs from the existing queue.

| Acceptance check | Recorded outcome |
| --- | --- |
| Search quality | **Failed.** Each workload must beat random and greedy at the same trial budget. Only nanoGPT beat both on mean confirmed score in the installed CLI comparison. The separate v2 comparison missed on nanoGPT. [CLI results](search-quality.md) · [v2 results](../benchmarks/protocol-v2.md#recorded-comparison) |
| Evaluation integrity | **Passed on synthetic cases.** The challenger improved on search and selection, then lost on independent confirmation. The study kept the baseline. [Overfit regression](../tests/benchmarks/test_panels.py) |
| Campaign restart, KPI reporting and incident alerts | **Passed locally.** The supervised campaign resumed and its status view showed recorded KPIs. The watcher delivered an alert while the hourly reporter stalled. The notifier change passed the full local gate in 35 seconds. [KPI guide](operator-guide.md#read-campaign-kpis) · [Watcher and agent deadlines](campaign-ops.md) · [Notifier qualification](https://github.com/vincentkoc/autoresearch-swarm/pull/133) |
| Physical GPU utilization and provider billing | **Unmeasured.** The generated device records exercise accounting. The target above 80% requires busy and allocated device-seconds for a complete hardware run. Actual charges require billing records. [GPU qualification](gpu-qualification.md) |
| Workload acceptance | **Generated rehearsal passed. Real workload unmeasured.** Source edits, short and full fits, and recovery ran on generated signals. Their scores describe fictional subjects. [Native workload fidelity](https://github.com/vincentkoc/autoresearch-swarm/pull/117) |

Compare hill-climb scores within one fidelity. For a multi-fidelity campaign,
find the rung in the snapshot's `observed_cohorts`. Pass its `comparison_digest`
as `--comparison-digest` to `research report view` when exporting that curve.
If an older report groups all rungs together, use the per-rung trial records.
[Recorded example](../benchmarks/results/v1-cli-cpu-small.jsonl)

The local CPU MLP comparison used all 144 trials. The challenger tied the
control on all three seeds, so it failed the strict superiority test.
It measured full-fidelity search accuracy only. Selection and confirmation
require separate authorization. [Paired results](search-quality.md#prospective-mlp-coordinate-comparison) · [Frozen protocol](mlp-local-search-protocol.md)

### Before a real-workload acceptance run

Check the original training revision, allowed edit paths, readable training
inputs for that revision, and the intended device and runtime. Obtain the
owner's explicit budget and independent evaluation procedure. If any item is
missing, stop before creating or resuming the queue. Record the missing item
and leave the real result unmeasured. An approved zero-cost budget is valid.
Use a separate campaign ID for any generated-input rehearsal.

Once authorized, check fast-path training, calibrate the proxy on real
inputs, and run the approved input-path edits at the declared fidelities.
Record the hill-climb curve and compare the selected recipe against the
declared threshold on independent confirmation. If the protocol requires an
external portal check, record the observed result under its own authorization.

## Write a task

1. Specify the metric name, its direction and the comparison unit.
2. Set the trial command and its working directory in YAML.
3. Read the trial envelope from the path in `SWARM_TRIAL`.
4. Read `candidate`, `seed` and `fidelity` from that JSON object.
5. Write finite numeric metrics to the path in `SWARM_METRICS`.
6. Exit with a nonzero status when training or evaluation fails.

A task can also write measured token counts and peak VRAM to `SWARM_USAGE`.
Keep log messages on stdout or stderr. Use a trial-specific path for
artifacts that must outlive the process.

A minimal score file looks like this.

~~~json
{"accuracy": 0.875}
~~~

Set `metric: accuracy` and `direction: maximize` for this example.
Specify `task_dir` relative to the YAML file. The worker runs the command
there. Supply the command as arguments and make the interpreter and task
dependencies available in the worker environment.

## Choose candidates and budgets

Use `candidates` for a short list of named recipes. Use `search_space` for a
finite grid. Set `proposer` to an importable `module:factory` for a search
policy. A proposer returns up to the requested number of candidates. It can
use previous search scores and failure diagnoses.

Set `max_trials` for distinct candidate, seed and fidelity combinations.
Set `max_workers` for concurrent work. Set `max_attempts` for the number of
tries per trial. Give each trial and proposer a timeout. Set `max_hours` for
a wall-clock budget that survives restarts. Set price fields and `max_cost`
when the campaign uses a priced resource.

Start with a small trial. Check the metric, time and device before increasing
the budget. For the built-in training tasks, `--size small` selects the short
profile. `--size full` increases training and evaluation work.

## Configure evaluation

Research campaigns use `evaluation_mode: calibrated` by default. Set
`panel_policy` to the scorer factory. For a dispatch smoke test, specify
`evaluation_mode: smoke` and omit the panels. The summary records the mode.

Prepare the training, calibration, search, selection and confirmation member
IDs. Use the same identity for a subject across every split. The panel policy
rejects reused IDs. Keep evaluation case details out of proposer input.

Declare a baseline and at least two calibration variants. Use at least two
distinct seeds per variant. Inspect the proxy slope, target seed noise and
seconds per trial. A flat proxy yields a null slope. A negative slope means
the candidate ordering may reverse between proxy and target.

The example uses `benchmarks.study:worker_policy`. A custom factory accepts
the campaign spec and returns a study for its metric and split. A study counts
search looks at dispatch. It compares selection replicates and consults the
confirmation panel once per candidate recipe. The operator can inspect look
counts and decisions after the run.

## Compare policies

Use the benchmark CLI for equal-budget experiments. It reports each arm's
raw scores, cumulative best, elapsed time and held-out decision.

~~~sh
uv run --no-sync python -m benchmarks program --budget 12 --seed 11
uv sync --locked --group dev --extra torch
uv run --no-sync python -m benchmarks mlp --device cpu --size small --budget 12 --seed 11
uv run --no-sync python -m benchmarks nanogpt --device cpu --size small --budget 12 --seed 11
~~~

For the default v1 protocol, repeat on seeds 23 and 47. For the larger
preregistered comparison, pass `--protocol v2` and use its three registered
seeds. Keep the workload profile, device and trial budget fixed across arms.
Share one study file per workload and protocol. Report the selected recipe's
`confirmed_score`, including ties and baseline wins. The benchmark README
includes the protocols, measured results and local Ollama commands.

[Benchmark suite](../benchmarks/README.md) · [Operator guide](operator-guide.md) · [Design](design.md)
