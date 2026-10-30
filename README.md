# Autoresearch Swarm

A local-first research engine with structured agent control, isolated experiments,
registered compute workers, and explicit scientific promotion.

This project builds on the experiment-loop idea in
[Andrej Karpathy's autoresearch](https://github.com/karpathy/autoresearch).
The platform implementation is original and MIT-licensed. Upstream source is
not bundled or relicensed.

## Development status

Implementation is in progress. The design distinguishes implemented behavior,
local verification, native/provider qualification, and demonstrated scientific
results. An interface declaration is not a hardware or scientific qualification.

The core contract separates immutable experiments from mutable dispatch authority.
Every attempt, evaluator receipt, budget settlement, and promotion retains its
own identity. The local implementation starts with Python, SQLite, and subprocesses.
Temporal and Cloudflare use the same domain semantics through adapters.

## Development

Install Python 3.11 or newer and uv.

~~~sh
uv sync --group dev
uv run pytest
uv run ruff check .
uv run mypy src
~~~

See [CONTRIBUTING.md](CONTRIBUTING.md) for development boundaries.
