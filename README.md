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
Temporal and Cloudflare adapters are planned against the same domain semantics.

Implemented foundations include the transactional ledger, exact budget arithmetic,
evaluation receipt validation, verified artifact lifecycle, and immutable source
bundles. The authenticated HTTP API, Python SDK, JSON CLI, and stdio MCP tools
share one command service. The complete campaign runner remains in progress.

Start with `uv sync --group dev`, then `uv run research init` and
`uv run research capabilities`. Run `uv run research mcp` to expose agent tools.
See [agent control](docs/agent-control.md), the [Python SDK](docs/sdk.md), and
the [HTTP API](docs/api.md) for their contracts.
See [evaluation authority](docs/evaluation.md) for the boundary between measurements and receipts.

See the [implementation ledger](docs/implementation.md) for qualification status,
the [design](docs/design.md) for the complete contract, and
[artifacts and source](docs/artifacts-and-source.md) for the current storage behavior.

## Development

Install Python 3.11 or newer and uv.

~~~sh
uv sync --group dev
uv run pytest
uv run ruff check .
uv run mypy src
~~~

See [CONTRIBUTING.md](CONTRIBUTING.md) for development boundaries.
