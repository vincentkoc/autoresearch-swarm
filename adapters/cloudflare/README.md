# Cloudflare adapter

This adapter runs the shared research command kernel in a Durable Object and
provides Workflow delivery, R2 storage and an agent API facade. Provider objects
stay outside the domain schemas. See the [adapter contracts](../README.md).

## Layout

| Path | Responsibility |
|---|---|
| platform/ | SQL, atomic callbacks, cryptography, I/O bindings and provisioning |
| research/ | Research commands, artifacts, evaluation and worker evidence |
| delivery/ | Workflow delivery, timers, orchestration and the agent facade |
| recovery/ | Checkpoints, elections, activation and recovery history |
| qualification/ | Local-only runtime fixtures and test gateways |
| tests/ | Node unit and transport tests |
| entry.py | Local qualification entrypoint |
| production.py | Production authority entrypoint |
| build.py | Explicit source-to-generated-module mapping |

The build preserves flat generated module names and copies the shared Python
package unchanged. Duplicate output names fail before replacing an existing build.
Production output excludes qualification gateways and public fixture keys.

## Profiles

| Configuration | Use |
|---|---|
| wrangler.production.jsonc | Durable Object research authority |
| wrangler.hybrid.jsonc | Workflow delivery around a configured authority |
| wrangler.facade.jsonc | Agent API forwarding with caller credentials |
| wrangler.jsonc | Local SQL, cryptography and authority qualification |
| wrangler.r2-test.jsonc | Local object-storage qualification |
| wrangler.operator-test.jsonc | Local recovery test gateway |

Qualification profiles are local-only. Production configuration, protected key
custody and deployment qualification are separate operator tasks.
Use [Cloudflare setup](../../docs/cloudflare.md) and the
[hosted-authority contract](../../docs/hosted-authority.md) for deployment details.

## Build and check

Use Node 22 or newer and host Python 3.12 or newer. From this directory:

~~~sh
uv sync --locked --python 3.12
npm ci --ignore-scripts --no-audit --no-fund
npm run build
npm run build:production
npm run test:crypto
npm run check:hybrid
npm run test:hybrid
WRANGLER_SEND_METRICS=false .venv/bin/uv run --frozen pywrangler sync
~~~

The host-tool lock, npm lock and Worker runtime lock have separate roles.
Keep uv.lock, package-lock.json and pylock.toml consistent. Worker Python packages
must match the declared WASM runtime; host CPython wheels are not substitutes.

From the repository root, run native contract tests with the core development
environment. Actual local workerd tests require explicit opt-in:

~~~sh
RESEARCH_CLOUDFLARE_TESTS=1 uv run --no-sync pytest -q \
  tests/adapters/cloudflare/test_cloudflare_native.py \
  tests/adapters/cloudflare/test_cloudflare_witness_ordering.py \
  tests/adapters/cloudflare/test_cloudflare_production.py \
  tests/adapters/cloudflare/test_cloudflare_checkpoint.py
~~~

The workerd fixtures require POSIX process groups. They use loopback endpoints,
temporary persisted state and owned processes, then verify closure before restart.
They rebuild current source and need no Cloudflare account.

## Runtime boundaries

SQL adapters consume cursors and invalidate sessions when callbacks finish.
Atomic callbacks cannot await network I/O. Cryptography uses a preloaded,
synchronous JavaScript implementation with the shared signature codecs.

Witness transport returns signed bytes; the shared kernel verifies source,
nonce, generation and operation scope. R2 timeouts retain uncertain write outcomes.
Recovery preserves original identities, accounting and rejected-command history.

See the [qualification register](../../docs/implementation.md) for tested revisions
and runtime coverage. Local workerd checks do not qualify an account deployment,
physical accelerator or scientific improvement.
