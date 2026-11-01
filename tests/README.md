# Test layout

Tests follow the component they exercise. Run a folder for a focused check or run
`uv run pytest` for the complete suite.

| Folder | Coverage |
|---|---|
| `domain/` | Records, identity, signatures and statistics |
| `state/` | Transactions, budgets, snapshots and recovery |
| `storage/` | Artifacts, retention and storage transports |
| `projects/` | Manifests, source capture and workload protocols |
| `workers/` | Enrollment, native processes, clocks, resources and journals |
| `control/` | Campaigns, evaluation, promotion and reports |
| `search/` | Proposals, populations and search policies |
| `learning/` | Policy training, assessment and adoption |
| `orchestration/` | Saved actions, waits, delivery and local drivers |
| `api/` | HTTP, SDK, CLI and MCP |
| `adapters/` | Cloudflare and Temporal contracts and runtime qualification |
| `integration/` | Complete research cycles and installed-package checks |

Root fixtures and support modules are shared. Use `tests.paths.REPO_ROOT` and
`TEST_ROOT` for repository and fixture paths. Do not derive these paths from a
test module's directory depth.

`conformance/` and `fixtures/temporal/` contain frozen protocol evidence. A directory
move must not rewrite their bytes. Runtime and hardware checks retain their explicit
opt-in conditions; collection alone does not qualify those environments.
