# Project command protocol

An ordinary research command reads a JSON context and writes a JSON report. It does
not need to import the research SDK. The worker supplies the context file through
`RESEARCH_CONTEXT` and verifies the report before collecting outputs.

## Input context

`WorkloadContext` contains:

- Trial, attempt, stage, and sealed experiment identities.
- The explicit random seed and project training budget in milliseconds.
- Named input artifacts with local paths, byte counts, and content digests.
- A dedicated output directory and report path.
- Versioned project parameters.
- A context digest over the complete normalized payload, excluding only itself.

Context paths describe a particular execution. They are not scientific protocol
identity and must not enter a global comparison key. The context accepts absolute
POSIX and Windows paths so that another platform can inspect the wire record.

The worker creates a new context for each attempt. A command uses the supplied seed
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

The report size limit is one MiB. Output verification uses a separate byte allowance,
streams content hashes, and rejects symlinks and hardlinks through the native source
reader. The reader opens POSIX files in nonblocking mode before checking their type,
so a FIFO cannot stall verification. Artifact publication verifies the bytes again
before accepting them into the artifact catalog.

A workload report is descriptive evidence. It is not an `EvaluationReceipt`, does
not grant evaluator identity, and cannot promote itself. An authenticated evaluator
and the declared promotion policy establish those separate claims.

## Timing and failures

The project's `time_budget_ms` can describe active training. The native supervisor's
timeout includes execution startup and cleanup observations. Record these clocks
separately.

Project-reported training time is an explicit trust level. A project cannot acquire
a trusted timing harness by writing a field in its report. The same rule applies to
protected evaluation.

A process failure or absent report produces no valid primary metric. The controller
must preserve the failed attempt and its logs. It must not insert zero into a metric
where lower values represent better results.

## Original CPU example

`examples/cpu_bigram` implements a character transition model with real gradient
updates and a separate evaluator. It uses only the Python standard library. The
fixed synthetic grammar is original project content and requires no download.

The mutable file is `train.py`. The dataset manifest, evaluator, common helpers,
environment declaration, and project manifest are protected. A checkpoint records
its training seed and dataset identity. Evaluation checks the artifact digest,
checkpoint dimensions, finite weights, seed, and dataset before calculating bits
per predicted ASCII byte.

The integration test captures an immutable source bundle, materializes a separate
execution directory, trains with two distinct seeds, transfers each checkpoint
through its content identity, and runs the evaluator as a separate process. It
checks improvement against the uniform model's analytical loss. This demonstrates
a working training/evaluation protocol, not an autonomous search improvement or
comparability with upstream autoresearch datasets.
