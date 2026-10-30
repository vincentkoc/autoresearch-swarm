# Cloudflare adapters and qualification

The supported implementation direction keeps the native controller as the
research authority and places Cloudflare delivery services around its API.
This preserves the same experiment identities, budgets, worker protocol,
evaluation rules and promotion decisions used by local and Temporal delivery.

Current implementation includes a native delivery gateway and locally qualified
Cloudflare storage/signing primitives. The Workflow driver, R2 adapter and agent
facade are separate work; their presence is not implied by the primitive tests.

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
