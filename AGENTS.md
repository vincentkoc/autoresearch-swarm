# Project instructions

Read the [design](docs/design.md) and the tests for the contract you change.
Keep dispatch, retries and selection in code. Keep model logic in the workload.
Record failed attempts. On heartbeat expiry, requeue eligible work under the same trial ID.
Accept one terminal result per trial ID and reject results from stale attempts.
Calibrate panel studies before search. Keep held-out measurements out of proposal history.
Use synthetic public fixtures and optional dependencies for framework integrations.
Test subprocess behavior with real children and queue races with independent connections.
Run scoped pytest, Ruff and mypy. Record user-visible changes in CHANGELOG.md.
Label the platform, workload and evidence behind qualification claims.
