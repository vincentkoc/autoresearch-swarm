# Test layout

Tests follow the component they exercise. From the repository root, run
`uv run pytest tests/benchmarks` for the benchmark tests or `uv run pytest`
for the complete suite.

| Folder | Coverage |
|---|---|
| `domain/` | Records, identity, signatures and statistics |
| `storage/` | Artifacts, retention and storage transports |
| `projects/` | Manifests, source capture and workload protocols |
| `workers/` | Enrollment, native processes, clocks, resources and journals |
| `control/` | Campaigns, evaluation, promotion and reports |
| `swarm/` | SQLite queue, campaign loop, workers and search policies |
| `benchmarks/` | Benchmark tasks, baselines, calibration and evaluation panels |
| `reactive/` | Training-time decisions, local sessions and application evidence |
| `api/` | HTTP, SDK, CLI and MCP |
| `integration/` | Complete research cycles and installed-package checks |
| `visual/` | Real Chrome smoke test of the synthetic offline campaign view (Node 22+) |

Root fixtures and support modules are shared. Use `tests.paths.REPO_ROOT` and
`TEST_ROOT` for repository and fixture paths. Do not derive these paths from a
test module's directory depth.

Runtime and hardware checks retain their explicit opt-in conditions; collection
alone does not qualify those environments.
