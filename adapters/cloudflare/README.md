# Cloudflare runtime qualification

This directory contains two distinct configurations. `wrangler.jsonc` is the
local-only SQL/signing qualification fixture described below; do not deploy it.
`wrangler.hybrid.jsonc` selects the Workflow delivery adapter around a native
research authority. See [hybrid setup and recovery](../../docs/cloudflare.md).
R2 storage, notification bridging and agent forwarding remain separate work.
The authoritative hosted-controller clock contract remains unqualified.

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
RESEARCH_CLOUDFLARE_TESTS=1 .venv/bin/pytest -q tests/test_cloudflare_witness_ordering.py
RESEARCH_CLOUDFLARE_TESTS=1 .venv/bin/pytest -q tests/test_cloudflare_router.py
```

The native fixture currently requires POSIX process groups. It starts only owned
local processes, passes `--local`, binds loopback, uses temporary persisted state,
and verifies closure of its exact process group before reopening state. It rebuilds
current sources after `npm ci`, so stale generated code cannot satisfy the test.
It needs no Cloudflare login, account,
namespace or hosted mutation. The endpoint additionally rejects non-loopback Host
names, but that fixture guard is not production authentication. Do not deploy it.

## Boundaries and cryptography

`build.py` copies the shared Python package unchanged into an owned generated
directory. The native fixture asserts that the SQLite and cryptography adapters
are never imported. It does not maintain a second implementation of those rules.
The JavaScript bundle uses MIT-licensed `@noble/curves` 2.4.0, with strict Ed25519
verification (`zip215: false`). Node checks an RFC 8032 vector. The Worker repeats
that vector inside its transaction; the host's existing cryptography library
independently verifies a signature over the shared canonical payload.

`NobleEd25519` implements the shared synchronous `Ed25519Provider` port. The same
assignment, clock-reply and assignment-time codecs verify native signatures in
workerd and produce byte-identical envelopes for native verification. Historical
timing arithmetic also runs unchanged. Binding, signature and domain tampering
must fail through the shared authority checks. This qualifies the codecs, not a
Cloudflare clock source or hosted research controller.

Only public synthetic test keys are present. The fixture deliberately has no production
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

The router fixture uses an explicit provisioning operation with public synthetic
credentials and timestamps. Ordinary requests then call `open_existing()` and the
real `ResearchService` through `ExistingResearchRouter`. Its test holds an external
HTTP witness while another RPC is submitted, checks shared worker commands and
signed queries, and restarts the owned workerd process before replay. Revocation
and expiring credentials remain checked after replay records exist. This is a
local provider test, not production provisioning or a native clock qualification.

## Sources

- [Python Workers setup and runtime](https://developers.cloudflare.com/workers/languages/python/)
- [Python package support](https://developers.cloudflare.com/workers/languages/python/packages/)
- [Local Python/JavaScript FFI](https://developers.cloudflare.com/workers/languages/python/ffi/)
- [Durable Object SQLite transactions](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Bundled module rules](https://developers.cloudflare.com/workers/wrangler/bundling/)
- [Noble curves and Ed25519](https://github.com/paulmillr/noble-curves)

Local workerd evidence does not establish hosted recovery, regional failover,
production signing-key custody, R2 durability, or the complete M5 deployment profile.

The witness fixture additionally imports the actual provider lifecycle and
one-use operation context. Public synthetic timestamps exercise accepted
observations, source-change rollback/fencing, the injected Noble verifier and
acknowledgment replay after a full process restart. These are runtime contract
checks, not native-clock, exclusive-ordering or hosted deployment qualification.
The command fixture also keeps witnessed time available in a legitimate domain
rejection hook while rolling back its handler effects. Synthetic bootstrap is
atomic and has an injected failure/retry probe.
The `Witness runtime qualification` workflow runs this fixture on a clean Linux
runner and the core witness/recovery tests on Linux, macOS and Windows. It uses
local workerd only and has no Cloudflare account credentials.

The awaited-witness fixture uses `exclusive()` around the actual DO operation.
An independent loopback HTTP server holds A's response and separately records
B's RPC submission from the outer Worker. Durable event sequence and prior-effect
counts must show A committed before B. A fixture-only gate bypass must reverse
that order under the same delivery path. The fixture also rejects child-task
witness use and aborts a bounded fetch. It observes the expired response attempt
while holding the retry response, then requires one added effect/acknowledgment
and a fresh nonce. Accepted and rejected acknowledgments replay
after a full owned workerd restart without another witness request.

`fixture_transport.mjs` accepts only loopback HTTP and fixed fixture paths, bounds
request/response bytes, refuses redirects and aborts the whole exchange after its
short timeout. Its credential is a public test string. This is qualification
transport, not deployment authentication or a native clock source. The runtime's
30-second reset path and production routing still require separate qualification.

The earlier synchronous witness/lifecycle suite passed at `ba66e98` on Linux,
macOS, Windows and local workerd in
[runtime qualification 41189985236](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41189985236).
That evidence predates the awaited-witness fixture and does not qualify its gate.
The awaited probe subsequently passed at `595af9f` in
[workerd qualification 41190864271](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41190864271):
both runtime tests passed in 41.82 seconds, including the gate-bypass negative
control and expired-response completion check. This qualifies the local fixture's
ordering boundary, not native-clock rate or deployed hosted authority.

The opt-in native test exercises the shared kernel, storage boundary, full owned
process restart and eight contending HTTP deliveries. CPU tests cover callback
failure propagation and native counter exhaustion. These are infrastructure
observations, not provider throughput or research-performance measurements.

## Agent API facade

`wrangler.facade.jsonc` selects the separate credential-free research API facade.
It forwards the caller's token to one configured native API origin and uses the
command names generated from the shared Python catalog. It has no delivery
gateway or adapter control credentials. See [configuration and client use](../../docs/cloudflare.md#agent-api-facade).

Run its local native transport qualification from the repository root:

```sh
RESEARCH_CLOUDFLARE_TESTS=1 .venv/bin/pytest -q tests/test_cloudflare_facade.py
```
