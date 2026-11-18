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
The normalized program should pass all search cases.

Run the same command again to resume. The queue restores completed trials,
and the panel study reuses recorded confirmation scores. Choose a new
campaign ID for a changed spec. Use a new panel study when you change a split
or scorer revision.

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
tries per trial. Give each trial and proposer a timeout. Set price fields and
`max_cost` when the campaign uses a priced resource.

Start with a small trial. Check the metric, time and device before increasing
the budget. For the built-in training tasks, `--size small` selects the short
profile. `--size full` increases training and evaluation work.

## Configure evaluation

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

Repeat on seeds 23 and 47. Keep the workload profile, device and trial budget
fixed across arms. Share the study file for each workload. Report failures
and ties alongside successful improvements. The benchmark README includes
the local Ollama commands.

[Benchmark suite](../benchmarks/README.md) · [Operator guide](operator-guide.md) · [Design](design.md)
