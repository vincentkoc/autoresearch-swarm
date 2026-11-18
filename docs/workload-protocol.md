# Project command protocol

A campaign runs a manifest project by setting its command to
`python -m research_swarm.swarm.native` and its task directory to the project
root. The local worker supplies `SWARM_TRIAL`, `SWARM_ATTEMPT_ID` and
`SWARM_METRICS`. The runner captures the project, resolves `research.toml` and
executes the stages in dependency order. Each stage reads its own sealed JSON
context from `RESEARCH_CONTEXT` and writes its report to the path in that context.

## Input context

`WorkloadContext` contains:

- The trial ID, stage-specific attempt ID, stage name and native trial digest.
- The explicit random seed and project training budget in milliseconds.
- Named input artifacts with local paths, byte counts, and content digests.
- A dedicated output directory and report path.
- The candidate parameters, plus registered engine or training controls when configured.
- A context digest over the complete normalized payload, excluding only itself.

Context paths describe a particular execution. They are not scientific protocol
identity and must not enter a global comparison key. The context accepts absolute
POSIX and Windows paths so that another platform can inspect the wire record.

The runner writes a fresh context for each stage of an attempt. A command uses the supplied seed
for its actual initialization and stochastic operations. Copying a seed into the
report alone does not establish correct replication.

## Output report

`WorkloadReport` identifies its original context digest and observed seed. It can
contain named output files, descriptive measurements, observed panel members, phase
timings in milliseconds, and structured diagnostics.

Every declared stage output must appear exactly once. Paths are portable paths
relative to the dedicated output directory. A report cannot use traversal, absolute
output paths, or duplicate names. The worker rejects stale contexts, changed seeds,
duplicate JSON keys, non-finite measurements, oversized reports, and redirected
report or output files.

The report size limit is one MiB. The runner verifies declared output names,
sizes and content hashes before passing those files to dependent stages. Each
required artifact must have exactly one producer among the stage's direct
dependencies. The stage output allowance
is 256 MiB. By default the attempt directory is temporary. Pass `--outputs DIR`
in the campaign command to retain the per-attempt subdirectory, stage contexts,
reports and files.

Native evaluation requires `measurement_trust = "authenticated"` and declared
`expected_members` in the manifest. Each evaluation stage must report precisely
those members. Its numeric measurements must match their declared names, units
and directions. Across evaluation stages, every declared metric must appear
once. The runner writes those values to `SWARM_METRICS`. The campaign owns
selection and any configured calibration and held-out comparisons.

## Timing and failures

The manifest's training budget appears in the context as `time_budget_ms`.
Each stage has its own `timeout_ms`. Set `trial_timeout_seconds` in the campaign
to cover capture, preparation and all stages. Project-reported training time and
worker elapsed time measure different intervals.

A stage failure, missing report or invalid evaluation ends the attempt with a
diagnostic. The queue preserves the failure and its logs. A failed attempt
contributes no score.

## Original CPU example

`examples/cpu_bigram` implements a character transition model with real gradient
updates and a separate evaluator. It uses only the Python standard library. The
fixed synthetic grammar is original project content and requires no download.

The mutable file is `train.py`. The dataset manifest, evaluator, common helpers,
environment declaration, and project manifest are protected. A checkpoint records
its training seed and dataset identity. Evaluation checks the artifact digest,
checkpoint dimensions, finite weights, seed, and dataset before calculating bits
per predicted ASCII byte.

Save the following as `native-bigram.yaml` at the repository root. It runs one
CPU trial with the default resource requirements and the runner's interpreter.
The manifest's primary metric is `val_bpb`, measured in bits per predicted byte.

~~~yaml
campaign_id: native-bigram
task_dir: examples/cpu_bigram
command: [python, -m, research_swarm.swarm.native]
metric: val_bpb
direction: minimize
evaluation_mode: smoke
max_trials: 1
max_workers: 1
candidates:
  - candidate_id: baseline
~~~

~~~sh
uv sync --locked --group dev
uv run --no-sync research swarm run native-bigram.yaml --db .research/native-bigram.sqlite
~~~

The `train` stage writes the model. The `evaluate` stage reads the verified
model and returns `val_bpb`. Repeating the command resumes the same queue.
This example uses smoke mode to exercise execution. For a research comparison,
configure calibration and separate panels as described in the
[local campaign guide](run-a-campaign.md).

The earlier two-seed CPU qualification used immutable capture, per-checkpoint
transfer and a uniform-model comparison. Its result belongs to the
[archived workload protocol](https://github.com/vincentkoc/autoresearch-swarm/blob/b8b44174b925483c8b202163fef12b733517fa49/docs/workload-protocol.md#original-cpu-example).
