# Ordinary pose-project adoption

This is a complete, standard-library CPU research project. Its scalar linear
predictor learns from fictional signal/angle pairs. The project owns its training,
data, evaluator, receipt readers, and formulas. The engine only needs the manifest
and the workload file protocol; there is no pose-project plugin.

From the repository root:

```sh
uv run research project register examples/synthetic_pose --db .research/pose.sqlite
uv run pytest tests/projects/test_project_adoption.py
```

Registration captures immutable source. The test runs the training and
protected evaluator commands twice with seed 7 under distinct trial IDs.

The workload performs 120 deterministic updates with a one-second active-training
cap. This small integration example does not estimate performance or reproduce
the upstream five-minute benchmark. Its project-reported timer is explicitly
declared in the manifest.

## Receipt meanings

`train.py` writes `model` and `fit_receipt`. The fit receipt binds the checkpoint,
injected seed, draw order, dataset, partition, manifest, evaluator, update count,
and trial identity. Timings are observations and need not repeat byte-for-byte.

`evaluate.py` verifies those identities and emits two endpoint formats:

| Output | Preserved interpretation |
|---|---|
| `endpoint_schema2` | Per-person absolute-error sums in radians, valid element counts, and window counts; explicit units and pinned identities |
| `endpoint_candidates_v2` | Separate `central` and `full` panels; per-person MAE in degrees, pooled sufficient statistics, and both reported aggregates |

The full panel contains two fictional people with different observation counts.
The evaluator reports both:

```text
macro_mae_deg  = mean(person absolute-error sum / person valid elements) × 180/π
pooled_mae_deg = sum(all absolute errors) / sum(all valid elements) × 180/π
```

Those are deliberately different numbers. The central panel has only the first
row per person and cannot replace the full panel. The endpoint formats omit a
standalone seed; the fit receipt and fresh workload/evaluator records carry that
identity. Consult the matching fit receipt to recover the seed.

## Replay archived receipts

`receipts.normalize` accepts a decoded endpoint, its declared schema, the
expected participant list and its pinned trial, batch and checkpoint identities.
It rejects unknown formats, mismatched identities, missing participants, invalid
units, nonfinite statistics, invalid counts and inconsistent aggregates.

The integration test compares checkpoint bytes, draw order and both endpoint
formats across runs. It independently recomputes the macro and pooled scores
from per-person errors and counts. The data and receipts are fictional.

See [the adoption guide](../../docs/project-adoption.md) for the project contract.
