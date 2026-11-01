# Autoresearch Swarm

Run reproducible research loops through an API, CLI or coding agent. Propose a
change, execute it in isolation, evaluate it, then keep or reject it using recorded
evidence. Multiple agents and workers can explore candidates concurrently.

The default runtime uses Python, asyncio, SQLite and local artifact storage.
Temporal, Cloudflare and remote workers extend the same contracts. A UI is optional.

Inspired by [Andrej Karpathy's autoresearch](https://github.com/karpathy/autoresearch).
The platform implementation is original and MIT-licensed. Upstream source is not
bundled or relicensed.

## Start locally

From this checkout, with Python 3.11 or newer and [uv](https://docs.astral.sh/uv/):

~~~sh
uv sync --group dev
uv run research init
uv run research run examples/cpu_bigram
~~~

The CPU example trains a small model and evaluates it independently. See
[local runs](docs/local-runs.md) for budgets, resumption and campaign reports.

Expose the same control surface to an agent:

~~~sh
uv run research capabilities
uv run research mcp
~~~

Agents can propose candidates, submit trials, inspect results and request promotion.
The engine owns execution state, budgets and evaluator authority.
Start with [agent control](docs/agent-control.md), the [Python SDK](docs/sdk.md)
or the [HTTP API](docs/api.md).

## Examples

| Example | Purpose |
|---|---|
| [CPU bigram](examples/cpu_bigram) | A small training and evaluation loop. |
| [Synthetic pose](examples/synthetic_pose/README.md) | Adopt an ordinary research repository and replay archived receipts. |
| [MLX workload](docs/mlx-workload.md) | Native Apple Metal training with the reference scorer. |
| [Pinned upstream workload](docs/upstream.md) | Preserve the original fixed-budget experiment contract. |
| [Policy learning](examples/learning_cycle) | Train, evaluate and explicitly adopt a research policy. |

Examples distinguish integration checks from measured scientific improvement.
[Qualification status](docs/implementation.md) records the tested platforms,
workloads and provider runtimes. Native CUDA parity and deployed Cloudflare
operation require separate qualification.

## Guides

| Task | Guide |
|---|---|
| Add a research repository | [Project adoption](docs/project-adoption.md), [manifest](docs/project-manifests.md), [workload protocol](docs/workload-protocol.md) |
| Control a research loop | [Campaigns](docs/campaigns.md), [evaluation and promotion](docs/evaluation.md) |
| Register compute | [Remote workers](docs/remote-workers.md), [engine profiles](docs/engines.md), [worker control](docs/worker-control.md) |
| Change infrastructure | [Orchestration](docs/orchestration.md), [Temporal](docs/temporal.md), [Cloudflare](docs/cloudflare.md), [R2](docs/r2.md) |
| Preserve and recover evidence | [Artifacts and source](docs/artifacts-and-source.md), [historical import](docs/historical-evidence.md), [backup](docs/backup.md) |
| Extend the platform | [Adapter contracts](adapters/README.md), [design contract](docs/design.md), [protocol conformance](docs/protocol-conformance.md) |
| Prepare a release | [Release previews and PyPI publishing](docs/releasing.md) |

## Development

~~~sh
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
~~~

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, scoped testing and review.
