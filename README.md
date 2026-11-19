# Autoresearch Swarm

Run a bounded search over prompts, programs or training recipes. A Python
coordinator fills worker slots, records each attempt in SQLite and selects
candidates using the campaign's metric. A panel policy measures whether a
search improvement holds on independent examples.

Inspired by [Andrej Karpathy's autoresearch](https://github.com/karpathy/autoresearch).
The implementation is original and MIT-licensed.

## Five-minute CPU quickstart

From the root of a source checkout on Linux or macOS, use uv and Python 3.11
or newer. This example scores three text-normalization programs on local CPU
workers. It uses the base dependencies and needs no model download or API key.

~~~sh
uv sync --locked --no-dev
uv run --no-sync python -m research_swarm swarm run benchmarks/program.yaml --db .research/program.sqlite
~~~

The JSON summary should show `trials: 3`, `attempts: 3` and `failed: 0`.
Look under `selected.result.attempt.trial.candidate.candidate_id` for
`normalized`, the program that passes every search case. Run the same command
again to resume. The winner and attempt count stay the same.

To see the scores and selection history, export the campaign to a new file
and open `.research/program.html` in a browser.

~~~sh
uv run --no-sync research report view --db .research/program.sqlite \
  --campaign program-normalization --output .research/program.html
~~~

`research --help` lists 12 top-level commands. Including subcommands, there are
25 executable paths, including `swarm notify` and `swarm advise`.
The campaign database has six tables for projects,
campaigns, candidates, trials, attempts and events. A panel study keeps two
tables in its own database. An optional reactive session also has a separate
two-table journal. The optional watcher uses one `notifications` table in
`<campaign-db>.notifications.sqlite` for hourly digests and metric alerts.

[Run a campaign](docs/run-a-campaign.md) · [Operator guide](docs/operator-guide.md) · [Campaign ops](docs/campaign-ops.md) · [Design](docs/design.md)

## Benchmarks

| Workload | Measurement |
| --- | --- |
| Program optimization | Fraction of fixed unit tests passed |
| MLP sweep | Classification accuracy on held-out synthetic points |
| Causal language model | Next-token validation loss after a training-code edit |

Each workload has random and greedy baselines at the same trial budget.
Run the training tasks on CPU or MPS with the small profile. The full
profile supports a configured CUDA device. Program and training-code
proposers can call local Ollama.

[Benchmark commands and comparison protocol](benchmarks/README.md)

## Project and operator references

| Task | Reference |
| --- | --- |
| Write a campaign and interpret selection | [Run a campaign](docs/run-a-campaign.md) |
| Inspect attempts, budgets and panel exposures | [Operator guide](docs/operator-guide.md) |
| Extend a proposer, backend or evaluator | [Swarm design](docs/design.md) |
| Adopt a manifest-based research project | [Project adoption](docs/project-adoption.md) · [Manifest](docs/project-manifests.md) · [Workload protocol](docs/workload-protocol.md) |
| Configure a native training environment | [Engine profiles](docs/engines.md) · [MLX workload](docs/mlx-workload.md) |
| Configure artifact storage | [R2 object storage](docs/r2.md) |
| Read campaign snapshots, events and budgets | [HTTP API](docs/api.md) · [Qualification history](docs/implementation.md) |
| Publish a release | [Release guide](docs/releasing.md) |

## Explore recorded experiments

The controller campaign workspace displays score history, lineage and
activity. The experiment inventory lays out explicit parent links and
supports pan, zoom and pinning. The example below contains 600 synthetic
records across six research areas.

![Symmetric ring layout of synthetic experiment records](docs/images/symmetric-ring.png)

[Campaign workspace](docs/campaign-view.md) · [Experiment inventory](docs/experiment-graph.md) · [Generate the sample](examples/graph_inventory)

## Development

~~~sh
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run python -m pytest
~~~

[Contributing](CONTRIBUTING.md)
