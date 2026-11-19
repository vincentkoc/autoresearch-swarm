# Adopt an ordinary research project

Keep training, data formats, evaluation formulas and receipt readers in the
project. Its manifest declares the workload contract. Its stage commands read
`RESEARCH_CONTEXT` and write their reports and declared outputs.

## Adoption sequence

1. Identify the source revision, dependency environment, data manifest,
   partitions, seeds, evaluator, timer and metric definitions. Record gaps in
   the archive alongside the original files.
2. Write `research.toml`. Identify mutable training files and protected evaluator
   dependencies. Declare metric units, direction, aggregation and panel coverage.
3. Capture the project with `research project capture PROJECT --db DB`. Use
   `project register` with the same arguments to register the captured revision.
4. Replay a known recipe with a fresh trial ID. Compare checkpoint bytes, draw
   order, coverage, units and sufficient statistics. Record elapsed time for each
   run separately. Preserve original receipts when interpreting older formats.

Native stages receive the dispatched trial fidelity in the sealed workload
context at `parameters.fidelity`. Multi-fidelity training code maps this value
to an amount of work, such as an update count or a fraction of the training set.

For a native campaign using the `llm-code-diff` operator, set its `revision`
to the captured source tree digest or, for a Git capture, the captured commit.
The native worker uses Git to apply each candidate's unified diff to a private
copy and checks every changed path against the manifest's mutation policy.
Training uses the edited source. Evaluation uses the original source and reads
the artifacts produced by that candidate.

The [synthetic pose example](../examples/synthetic_pose/README.md) runs real
CPU training and protected evaluation on fictional data. Its fit receipt binds the
checkpoint, injected seed and input identities. Its two endpoint formats retain
per-person errors and denominators. The project reader verifies that the full
panels agree and checks the central panel separately.

Run `uv run pytest tests/projects/test_project_adoption.py` to replay the recipe.
The test compares checkpoint bytes and recomputes both macro and pooled error
under a new trial ID. Negative cases cover seed, partition, checkpoint, units,
coverage, and inconsistent archived aggregates.

For a campaign with a queue, panel selection and resume, follow the
[local campaign guide](run-a-campaign.md).
