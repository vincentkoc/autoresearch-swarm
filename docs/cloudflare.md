# Cloudflare adapters and qualification

The supported implementation direction keeps the native controller as the
research authority and places Cloudflare delivery services around its API.
This preserves the same experiment identities, budgets, worker protocol,
evaluation rules and promotion decisions used by local and Temporal delivery.

Current implementation includes the native delivery gateway, a hybrid Workflow
driver with a durable transport registry, and locally qualified Cloudflare
storage/signing primitives. A native bridge delivers the durable wakeup outbox.
R2 storage and the agent facade remain separate work. Hosted deployment is not part of the local
qualification.

## Authority clock boundary

Deployed Workers update `Date.now()` and `performance.now()` after I/O; neither
advances during computation. Local development intentionally behaves differently.
Therefore local workerd timing tests cannot establish the deployed runtime's
fitness for the existing fresh, bounded continuous-time authority contract.
[Cloudflare timing documentation](https://developers.cloudflare.com/workers/runtime-apis/performance/)

The current controller samples qualified time inside a synchronous state decision.
A cached timestamp could accept a result after its acceptance deadline. Fetching
time before the transaction, extending a guard interval or relabeling a hosted
clock as a native profile does not establish the missing guarantee. This is a
contract mismatch, not a missing SDK dependency.

Authoritative Cloudflare execution admission remains unsupported. There is no
implicit `local-utc-v1` fallback for a hosted controller. The hybrid profile uses
the qualified native controller for assignment and acceptance authority; Workflow
timeouts affect delivery only. Native compute remains outside Workers.

The original design's authoritative Durable Object deployment is consequently
unqualified. Its SQL and signing adapters remain useful, but a complete hosted
authority requires a separately designed and qualified time contract. The hybrid
route must not be reported as completion of that original deployment profile.

## Native delivery gateway

`create_delivery_app` exposes four bounded operations around the existing
`DeliveryKernel`. It does not provide a second command implementation.

| Endpoint | Request | Result |
|---|---|---|
| `POST /v1/delivery/bootstrap` | Binding and stable recovery ID | Active, retired or expired startup |
| `POST /v1/delivery/inspect` | Exact binding | One saved work reference, idle, retired or terminal |
| `POST /v1/delivery/execute` | Exact saved turn reference | Settled outcome and receipt/closure identity |
| `POST /v1/delivery/wait` | Exact saved wait reference | Pending or settled condition evidence |

The request records are `GatewayBinding`, `GatewayStartup` and `GatewayWork` in
`orchestration/gateway_models.py`. Requests and results use schema version `1.0`
and have a 16 KiB encoded limit. Only bounded references belong here. Research
payloads, snapshots, source, logs, credentials and arbitrary URLs do not.

Register a distinct gateway principal with `orchestration.deliver` scope against
one exact `DriverConnection`. That connection already contains the private control
and evaluator research clients. Registration is trusted application configuration;
HTTP callers cannot register themselves or replace routes. Each request resolves
the credential again, so revocation applies to subsequent admissions.

```python
from research_swarm.api.delivery import GATEWAY_SCOPE, create_delivery_app
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id

# credentials, admin and connection are explicitly provisioned by the operator.
actor = Principal(
    id=new_id(),
    workspace_id=connection.binding.workspace_id,
    scopes=frozenset({GATEWAY_SCOPE}),
)
issued = credentials.issue(admin, actor)
app = create_delivery_app(credentials, {actor.id: connection})
# Provision issued.token privately to the delivery adapter; do not put it in history.
```

Serve this optional ASGI application behind an authenticated private TLS route.
The local qualification fixture uses loopback HTTP. Gateway credentials are not
agent credentials and cannot be reused as a facade's upstream identity.

The gateway checks the complete workspace/campaign/epoch/configuration/actor
binding before invoking the kernel. The kernel then verifies the saved work's
route and current domain authority. A stale delivery cannot select a new action,
replan an ordinal, bypass a fence or grant a worker more execution time.

## Retry and ownership behavior

Persist the selected work reference before invoking `execute` or `wait`. After an
uncertain reply, retry that exact reference. The existing command acknowledgment
or closure record determines the outcome. Preserve one recovery ID while retrying
startup; an explicit expired outcome permits a new recovery generation.

Errors contain a stable code and retryability flag without exception messages,
causes, request bodies or private details. Transport uncertainty remains retryable
even if the underlying exception omitted its retry flag. An unknown failure may
follow a committed action, so it is reported as `DELIVERY_UNCERTAIN`. A cancelled
HTTP request drains its admitted native API call before relinquishing ownership.

Local tests exercise real HTTP acknowledgment loss, concurrent replay, wait
reconciliation, fencing, credential revocation, route/binding refusal, bounded
input parsing, sanitized failures and cancellation custody. They do not qualify
a deployed Cloudflare Workflow or authorize account deployment.

See the [local runtime qualification fixture](../adapters/cloudflare/README.md)
for the actual Durable Object SQL and cross-runtime signed-envelope tests.

## Hybrid Workflow driver

`adapters/cloudflare/wrangler.hybrid.jsonc` selects the TypeScript delivery adapter.
It has no Python Workers dependency. `ResearchWorkflow` calls the native gateway;
`DeliveryRegistry` stores transport identities and continuation inputs in a
SQLite-backed Durable Object. Neither implements research decisions.

Configure `NATIVE_DELIVERY_ORIGIN` as an HTTPS origin and keep
`AUTHORITY_PROFILE=hybrid-native-v1`. Provision `GATEWAY_TOKEN` with the native
gateway credential and `CONTROL_TOKEN` with a separate, at least 32-character
adapter administration secret. Keep those values out of Workflow parameters and
history. The committed configuration uses a placeholder origin and has
`workers_dev=false`. Agent tools continue to use the research API.

The adapter administration API accepts authenticated POST requests:

| Path | Body | Behavior |
|---|---|---|
| `/adapter/start` | `{binding, options}` | Reserve or verify the original start; ensure and nudge its current generation |
| `/adapter/status` | `{binding}` | Read current generation, provider status and its immutable initial pending reference |
| `/adapter/wake` | `{binding, actor_id, wait_id}` | Notify an existing registered Workflow; never create one |

`options` requires integer `poll_seconds` (1–60), `rotate_after` (2–128), and
`request_timeout_seconds` (1–120). Responses and parameters contain bounded
references. A start retry must repeat the same binding and options; changed input
conflicts. The returned start ID stays stable when the current generation advances.

Each successful inspect step freezes its selected work reference. Delivery
retries use that exact reference. Settled work clears the pending reference;
uncertain work retains it. At a generation boundary the registry atomically
reserves the complete successor input before provider creation. A lost creation
or handoff reply recovers that reservation. Every native callback checks current
generation again; a cached startup check cannot authorize a later retry.

Observed-generation markers outlive provider history. A missing or temporarily
inaccessible observed history is never recreated. Lookup failures remain
retryable without interpreting them as permanent deletion. An unobserved creation
can retry the exact ID/input; Workflow startup records observation before its
first native call. Native domain records remain the permanent research ledger.

## Native bridge and wakeup ownership

`CloudflareBridge` is an async client for the private adapter administration API.
It needs no Cloudflare SDK or account credential. Register the adapter origin and
its control credential in trusted native startup configuration, separately from
the gateway and research route credentials. Reuse the exact `DriverConnection`
binding and route clients established above.

```python
from research_swarm.orchestration.cloudflare import CloudflareBridge, WorkflowOptions
from research_swarm.orchestration.waits import DurableWaitDriver

async with CloudflareBridge(adapter_origin, adapter_control_token) as bridge:
    identifier = await bridge.start(connection.binding, WorkflowOptions())
    status = await bridge.status(connection.binding)
    for route, client in connection.clients.items():
        waits = DurableWaitDriver(
            client,
            connection.binding.campaign_id,
            epoch=connection.binding.epoch,
            actor_id=connection.binding.actors[route],
        )
        delivered_ids = await bridge.deliver(connection.binding, waits)
```

Call `deliver` periodically for each registered route. It is one bounded batch,
not a background task or an implicit controller. A failure leaves that route's
notifications pending; an application may continue servicing other routes.
Explicit startup remains separate from notification delivery. `notify` never
starts, resets or resumes a Workflow.

The bridge verifies the complete native binding before reading its
`wakeup_delivery` records. For an earlier epoch it follows verified immutable
transfer checkpoints, bounded to 128 hops; an unavailable or longer chain retains
the notifications. It does not use the generic event outbox. It acknowledges only after the adapter returns exactly
`{"accepted": true}`. A timeout, malformed/oversized reply, redirect or provider
refusal retains the pending notification. Retrying can send a duplicate hint,
which is safe because readiness and execution remain native decisions. The
acknowledgment does not consume the research wait. Cancellation drains admitted
native read/acknowledgment calls before returning custody to the caller.

Start and status responses are checked against the same normalized binding and
provider ID used by the Worker. Status reports the generation's immutable
`initial_pending` reference, not a live view of its current instruction. Errors
are sanitized, responses are bounded to 16 KiB, redirects are disabled, and
non-loopback adapter connections require HTTPS with certificate verification.

## Local recovery qualification

The pinned local emulator persists a running status across process restart but
does not resume execution on a status read. An explicit `/adapter/start` retry
ensures the current reserved instance and sends a wake hint to its existing
history. It does not call provider `restart()`, clear cached steps, create another
research action, or resume a paused native campaign. Paused, terminated and
errored provider instances are not reset by this operation.

Wake hints do not establish readiness. Normal polling rechecks the native
condition when hints are lost. The local fixture proves both resumed history
after an explicit start retry and a later settled native wait without an
evaluation wakeup. It separately verifies actual CPU training, authenticated
evaluation, resource closure, lost-response retry, concurrent identical starts,
generation handoff, fencing and refusal of a hosted-authority profile.

Workerd requires manual redirect handling for this fetch path. The adapter
rejects every 3xx response and the native test verifies that gateway credentials
never reach the redirect destination. Event timeout handling accepts the pinned
runtime's named timeout or its exact RPC timeout shape; other errors remain
failures. This runtime-specific guard requires requalification on upgrades.

Run the local suites from the repository root:

```sh
RESEARCH_CLOUDFLARE_TESTS=1 .venv/bin/pytest -q \
  tests/test_cloudflare_hybrid.py tests/test_cloudflare_native.py
```

Both fixtures use owned local process groups and temporary persisted state.
The hybrid fixture uses only loopback native services and ephemeral credentials.
The standard CI job checks TypeScript, formatting, crypto and delivery boundary
tests without starting a hosted service. Cloudflare documents local Workflows as
an emulator, so these results do not establish deployed restart behavior.
[Local development](https://developers.cloudflare.com/workflows/build/local-development/)
