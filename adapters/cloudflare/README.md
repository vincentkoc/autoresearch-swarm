# Cloudflare runtime qualification

This directory qualifies infrastructure seams for the Cloudflare adapter. It is
not the research API, a production controller, or a deployment template. The
complete Durable Object command service, R2 custody, authority clock and Workflow
delivery integration remain separate implementation work.

The fixture imports the actual Python command kernel through `Database.from_store`
and the `StateStore` contract. The Durable Object adapter owns SQL cursor conversion,
synchronous transactions and schema-version metadata. Shared Python code owns
command replay, revision checks, records, events, the outbox and rejection behavior.
It calls a prebundled synchronous Ed25519 implementation through local JavaScript FFI.
A failed inner operation rolls back before its fence and rejected acknowledgment
commit; a failed rejection hook rolls back the entire command. Parallel duplicate
deliveries and process restart exercise the actual saved command acknowledgments.

## Local setup

Use Node 22 or newer and a host Python 3.12 or newer. All commands run from this
directory unless shown otherwise. Dependencies are isolated here; no native core
dependency pin is weakened. `uv.lock`, `package-lock.json` and `pylock.toml` freeze
host tooling, JavaScript dependencies and Worker runtime packages respectively.

```sh
uv sync --python 3.12
npm ci --ignore-scripts --no-audit --no-fund
npm run test:crypto
npm run build
WRANGLER_SEND_METRICS=false .venv/bin/uv run --frozen pywrangler sync
```

The local environment includes uv 0.13.4 because pywrangler 1.17.9 requires it.
Wrangler 4.156.0 is a local npm dependency; no global CLI upgrade is required.
The compatibility date selects the Worker runtime separately from host Python.
The checked runtime lock records Pyodide's Python 3.14.2 / `v314.0.7` package wheels,
Pydantic 2.12.5 and pydantic-core 2.41.5. Do not substitute arbitrary CPython wheels
for the WASM extensions.

From the repository root, using its core development environment:

```sh
RESEARCH_CLOUDFLARE_TESTS=1 .venv/bin/pytest -q tests/test_cloudflare_native.py
```

The native fixture currently requires POSIX process groups. It starts only owned
local processes, passes `--local`, binds loopback, uses temporary persisted state,
and verifies closure of its exact process group before reopening state. It rebuilds
current sources after `npm ci`, so stale generated code cannot satisfy the test.
It needs no Cloudflare login, account,
namespace or hosted mutation. The endpoint additionally rejects non-loopback Host
names, but that fixture guard is not production authentication. Do not deploy it.

## Boundaries and cryptography

`build.py` copies the shared domain and state packages unchanged into an owned
generated directory. The native fixture asserts that the SQLite adapter and
`sqlite3` are never imported. It does not maintain a second implementation of those Python rules.
The JavaScript bundle uses MIT-licensed `@noble/curves` 2.4.0, with strict Ed25519
verification (`zip215: false`). Node checks an RFC 8032 vector. The Worker repeats
that vector inside its transaction; the host's existing cryptography library
independently verifies a signature over the shared canonical payload.

Only public RFC test keys are present. The fixture deliberately has no production
key store. Loading the JavaScript module may suspend through JSPI, so loading
finishes before the transaction starts. The subsequent signing call must return a
plain string synchronously. No RPC, Web Crypto Promise or network request belongs
inside an atomic callback.

`atomic.py` preserves original Python values and typed exceptions across the FFI
rollback boundary. The native fixture catches the actual `ResearchError` after
inner rollback. CPU tests separately preserve foreign-wrapper exception identity,
distinguish commit failure and reject asynchronous or repeated callbacks.
`durable_sql.py` implements that storage boundary for the unchanged command kernel.
It fully consumes raw cursors before returning, preserves column order and binary
values, maps constraint failures, and invalidates sessions when callbacks finish.
It uses `UPDATE ... RETURNING` for CAS, never billing-oriented `rowsWritten`.

SQL bindings reject non-finite or inexact values. Authority counters share an exact
`2**53 - 1` ceiling; their increment fails before any state becomes inexact. The
Workers RPC bridge requires explicit numeric conversion for large Python integers
and returns binary columns as buffer objects. Empty and non-UTF-8 bytes, SQL NULL,
duplicate column names and shared cursor consumption have native contract probes.

The adapter stores schema versions in a singleton table because the runtime does
not support the native `PRAGMA user_version` mechanism. Both adapters apply the
same migration statements. Unknown versions and unversioned application tables
are refused. Interrupted migration tests preserve existing acknowledgments and
roll back both the schema changes and version update. Foreign keys remain enabled.

## Sources

- [Python Workers setup and runtime](https://developers.cloudflare.com/workers/languages/python/)
- [Python package support](https://developers.cloudflare.com/workers/languages/python/packages/)
- [Local Python/JavaScript FFI](https://developers.cloudflare.com/workers/languages/python/ffi/)
- [Durable Object SQLite transactions](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Bundled module rules](https://developers.cloudflare.com/workers/wrangler/bundling/)
- [Noble curves and Ed25519](https://github.com/paulmillr/noble-curves)

Local workerd evidence does not establish hosted recovery, regional failover,
production signing-key custody, R2 durability, or the complete M5 deployment profile.

The opt-in native test exercises the shared kernel, storage boundary, full owned
process restart and eight contending HTTP deliveries. CPU tests cover callback
failure propagation and native counter exhaustion. These are infrastructure
observations, not provider throughput or research-performance measurements.
