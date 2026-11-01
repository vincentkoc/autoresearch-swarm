# Ordinary pose-project adoption

This is a complete, standard-library CPU research project. Its scalar linear
predictor learns from fictional signal/angle pairs. The project owns its training,
data, evaluator, receipt readers, and formulas. The engine only needs the manifest
and the workload file protocol; there is no pose-project plugin.

From the repository root:

```sh
uv run research run examples/synthetic_pose --key pose-baseline --seed 7 \
  --primary-metric macro_mae_deg --max-trials 1
```

The command captures immutable source, runs training and a separate protected
evaluator, and returns an authenticated evaluation receipt. It leaves selection and
campaign completion to the operator or agent. Repeating the same key resumes the
existing run. A new key starts a separately budgeted campaign.

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
identity. Importing an old endpoint cannot manufacture missing seed authority.

## Import and replay

`history.py` is an optional project-local SDK helper. Pass an existing
`ResearchClient`, registered project ID, endpoint file, explicit source schema,
original record ID, expected participant list, known identity fields, and stable
command key to `import_endpoint`. It preserves the exact raw bytes, records its
normalizer code digest, and submits generic `evidence.import_historical` metadata.
Neither training nor evaluation imports the SDK.

The helper qualifies only these two declared shapes. It rejects ambiguous JSON,
unknown formats, mismatched identities, missing participants, invalid units,
nonfinite statistics, invalid counts, or aggregates inconsistent with their
sufficient statistics. It does not authenticate an original event chain or infer
native closure. An incomplete archive belongs in a generic import with null
measurements and an explicit missing reason.

The integration test creates a disposable historical archive, registers this
ordinary project, imports both endpoints and an incomplete record, and verifies
that import creates no campaign, attempt, or evaluator authority. A fresh native
run then reproduces checkpoint bytes, draw-order identity, and endpoint statistics
under a new trial. Its independent evaluator creates a new eligible receipt;
historical records remain ineligible. Reopening the controller reuses the original
run without another execution.

```sh
uv run pytest tests/projects/test_project_adoption.py
```

The data and archived receipts are fictional. This example qualifies integration
and recipe replay, not real-cohort accuracy or scientific improvement.
See [the adoption guide](../../docs/project-adoption.md) for the project contract.
