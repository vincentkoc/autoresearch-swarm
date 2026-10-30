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
An optional [Temporal adapter](docs/temporal.md) delivers saved actions and waits
against the same domain authority. The [Cloudflare direction](docs/cloudflare.md)
has locally qualified hybrid Workflow delivery and SQL/signing primitives;
The native notification bridge is implemented; R2 storage and agent forwarding remain in progress.

Implemented foundations include the transactional ledger, exact budget arithmetic,
evaluation receipt validation, verified artifact lifecycle, and immutable source
bundles. The authenticated HTTP API, Python SDK, JSON CLI, and stdio MCP tools
share one command service. The local runner executes a frozen baseline and exposes
the campaign to external agents. Greedy and asynchronous portfolio policies
coordinate their responses; learned proposal policies remain in progress.

Start with `uv sync --group dev`, then `uv run research init` and
`uv run research capabilities`. Run `uv run research mcp` to expose agent tools.
See [agent control](docs/agent-control.md), the [Python SDK](docs/sdk.md), and
the [HTTP API](docs/api.md) for their contracts.
See [evaluation authority](docs/evaluation.md) for the boundary between measurements and receipts.
See [campaign control](docs/campaigns.md) for pause, completion, and evidence reports.
See [durable orchestration](docs/orchestration.md) for saved action retries and
dedicated driver ownership without changing worker custody.
See [event recovery](docs/api.md#recover-an-expired-event-cursor) for frozen
snapshots and bounded catch-up when a client reconnects after stream expiry.

Run the original CPU example with `uv run research run examples/cpu_bigram`.
The same reference scorer also qualifies a [native MLX Metal workload](docs/mlx-workload.md).
Run a second ordinary workload with [the synthetic pose project](examples/synthetic_pose/README.md),
including historical receipt import and fresh authorized replay.
Run the [empirical policy benchmark](examples/policy_benchmark/README.md) to compare
funded greedy/portfolio campaigns and timed no-op confirmation panels.
Adopt a supplied pinned upstream checkout with the [learning-rate study adapter](docs/upstream.md);
source/contract adoption is CPU-tested, while native CUDA parity remains unqualified.
See [local runs](docs/local-runs.md) for resume, budgets, and agent control.
See [remote workers](docs/remote-workers.md) for explicit CPU enrollment, pinned
HTTP connections, and persistent asynchronous execution.
See [worker control](docs/worker-control.md) for presence, draining, and cancellation evidence.
See [engine profiles](docs/engines.md) for bounded CPU, PyTorch, and MLX environment
probes and explicit private registrations. Accelerator worker qualification remains separate.
See [historical evidence](docs/historical-evidence.md) to retain an existing project's
receipts without treating imported claims as current execution or validated results.

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
