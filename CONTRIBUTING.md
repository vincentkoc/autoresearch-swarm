# Contributing

Autoresearch Swarm keeps research policy, execution, and evidence separate.

## Setup

~~~sh
uv sync --group dev
uv run pytest
uv run ruff check .
uv run mypy src
~~~

Optional engine and provider dependencies belong in extras. A core installation
must not install an accelerator framework.

Tests are grouped by component in [tests/README.md](tests/README.md). Run the
affected folders first, then the relevant integration or provider checks.

## Changes

Describe the behavior and its observable contract. Include tests for state
transitions, recovery, and invalid input where relevant. Avoid tests that merely
repeat implementation details.

Preserve schema compatibility or document the migration. Changes to hashing,
budget arithmetic, evaluator trust, or execution authority require golden
fixtures and concurrency/recovery coverage.

Use substantial, cohesive commits. Do not combine unrelated changes merely to
increase commit size.

## Evidence

Label evidence as implemented, locally verified, native/provider qualified, or
scientifically demonstrated. Keep hardware and workload fingerprints with
performance results. Do not infer a scientific win from a smoke test.

## License and privacy

Contributions use the MIT license. Do not copy source without compatible terms.
Public examples must not contain private datasets, session histories, personal
filesystem paths, internal endpoints, or credentials.
