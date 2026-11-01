# Adopt an ordinary research project

An existing research repository stays an ordinary repository. Add a manifest and
small command wrappers that read `RESEARCH_CONTEXT` and write the workload report.
Keep model code, data formats, evaluation formulas, and source-specific historical
readers in that project. The engine supplies execution, evidence, scheduling,
budgets, and agent control through its existing API.

The runnable [synthetic pose project](../examples/synthetic_pose/README.md)
demonstrates this boundary independently of the original language-model example.
Its training and evaluator require only Python's standard library.

## Adoption sequence

1. Inventory the existing source, dependency environment, data manifest, split,
   seeds, evaluator, timer, metrics, and native ownership. Record absent facts as
   absent. Do not derive execution permission from an old log or score.
2. Write `research.toml`. Declare the mutable source surface and protect evaluator
   dependencies. Name metric units, direction, aggregation, and expected panel
   coverage. Declare actual timer authority and engine requirements.
3. Register a frozen source bundle. Use the generic historical-evidence command
   for retained metadata. A project-local reader can interpret each known schema;
   it must preserve raw bytes and retain missing measurements, contradictory
   closure observations, and provenance limits.
4. Run a disposable replay under fresh admission. Compare checkpoint or recipe
   identity, sufficient statistics, coverage, units, and evaluator semantics.
   Separate expected numerical variation from changed contracts. New trial IDs,
   native evidence, and elapsed times are not historical identities.
5. Start a bounded campaign using the same project contract. External agents use
   the ordinary proposal, capture, submission, comparison, and promotion API.
   Imported metadata informs hypotheses but cannot settle new attempts or serve
   as authenticated confirmation evidence.

## Pose-estimation example

The synthetic pose project demonstrates this sequence with a linear model and
two archived receipt formats. Model code, data logic and receipt readers remain
in the project.

| Existing evidence | Project responsibility | Engine record |
|---|---|---|
| Candidate manifest and source/data pins | Preserve exact bytes and distinguish schema-specific identities | Frozen project revision and retained historical artifact |
| Fit receipt and checkpoint | Bind seed, draw order, fitting coverage, checkpoint, and declared timing | Stage outputs and authenticated fresh run result |
| Per-person sums and counts | Preserve radians and valid-element denominators; derive macro and pooled values separately | Named metrics with explicit units and aggregation |
| Central and full panel scores | Keep both panels and per-person degree mappings; validate stored aggregates | Historical observations labeled by panel |
| Native archive and settlement history | Preserve original custody, clock domains, and contradictory observations | Historical evidence without new budget or execution authority |
| Fresh authorized endpoint | Verify checkpoint, seed, data, split, evaluator, protocol, and coverage | Current evaluation receipt, eligible only under its campaign contract |

The example uses fictional data and qualifies recipe replay and metric handling.
Real-project adoption requires a frozen environment and authorized data and
compute. A changed source, data or evaluator contract creates a new comparison
identity.

## Verified behavior

`tests/projects/test_project_adoption.py` performs historical import before creating any
campaign. It then executes real CPU training and independent evaluation through
the local engine, compares both receipt formats against the original archive,
recomputes macro and pooled error independently, and reopens the controller with
no repeated execution. Negative subprocess cases reject seed, unit, fitting
coverage, partition, and checkpoint mismatches. Reader tests reject malformed or
inconsistent archived metadata before it gains authority.

This qualifies local integration and recipe replay. Statistical replication,
accelerator performance, and policy improvement require their own experiments.
