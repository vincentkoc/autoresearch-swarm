# Durable orchestration actions

An adapter retries a saved action, not a fresh invocation of a research loop.
`SearchDriver.advance()` can perform several commands and replan between them.
Wrapping that method in a provider retry could perform different research after
an accepted reply is lost. Use a durable turn as the retry boundary instead.

The current implementation supplies initial ownership, one saved command per
turn, transaction-time fencing, acknowledgment recovery, and durable waits with
destination-bound wakeups. A paused campaign can transfer to fresh driver
principals through a verified same-workspace checkpoint. The optional
[Temporal adapter](temporal.md) delivers these saved intents using real Workflows
and Activities. An adapter label alone is descriptive metadata, not qualification.

## Local delivery by default

`LocalDeliveryDriver` runs the same API delivery kernel used by the Temporal
Activities. It needs asyncio, the ordinary research clients, and one small private
checkpoint directory. Importing or running it does not load the Temporal SDK.

```python
import asyncio
from pathlib import Path

from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.orchestration.delivery import DriverConnection, binding_reference
from research_swarm.orchestration.local_delivery import LocalDeliveryDriver

binding = OrchestrationBinding.model_validate(
    control_client.get("orchestration_binding", campaign_id)["data"]
)
connection = DriverConnection(
    binding_reference(binding),
    {"control": control_client, "evaluator": evaluator_client},
)
delivery = LocalDeliveryDriver(Path(".research-delivery"), connection)
stop = asyncio.Event()
await delivery.run(stop)
```

Use only the clients declared in the binding. An external agent prepares turns
and registers waits through the normal API. `step()` or `step_async()` performs
one bounded delivery step; `run()` polls until stopped or the binding retires.
The kernel supplies identity validation, checkpoint startup, exact turn execution,
and wait reconciliation. Each adapter supplies its own retry and wait mechanism.

The local checkpoint stores the binding, startup recovery generation, and pending
work reference before calling the API. If an accepted reply is lost, reopening
the same directory retries that exact reference. Snapshot creation uncertainty
also reuses the same identity. A proven expired snapshot advances a recorded
recovery generation. The checkpoint contains no credentials or research payloads.

A native lock prevents simultaneous steps against one checkpoint. Another
binding cannot adopt its directory. Transport or domain errors propagate while
the pending reference remains available for explicit retry. Stopping the asyncio
loop does not cancel native work, consume a ready wait, release budgets, or
change the campaign's state. Async cancellation drains its already-admitted API
step before releasing the checkpoint or allowing the caller to close clients.
Domain records remain the authoritative evidence.

## Register gateway delivery after campaign creation

The shared `orchestration.register_delivery` command freezes delegation for an
existing binding. It requires `orchestration.manage`, `auth.manage`, and
`expected_revision=0`. The request carries no tokens:

```python
receipt = operator.command(
    "orchestration.register_delivery",
    {
        "campaign_id": campaign_id,
        "epoch": 1,
        "configuration_digest": configuration_digest,
        "gateway_credential_id": gateway_credential_id,
        "route_credentials": {
            "control": control_credential_id,
            "evaluator": evaluator_credential_id,
        },
    },
    idempotency_key="register-delivery",
    expected_revision=0,
)
```

The command derives workspace and actors from current server-owned credentials.
The gateway needs `orchestration.deliver`; each route needs `read` and
`orchestration.drive`, and must match the active binding and its dedicated owner
record. Revoked, missing, expiring or foreign credentials refuse. The gateway must
be distinct from all route actors. Registration does not grant additional scopes.
The operator must hold every scope of each delegated principal, matching the
credential issuer's existing scope ceiling.

One immutable `delivery_registration` record is keyed by gateway actor. Its event
and acknowledgment commit in the same transaction. Concurrent identical requests
converge on that record; a different credential set or binding cannot overwrite it.
Moving to another epoch requires a separate gateway registration.

The exact original command replays its historical receipt after the binding is
fenced or a target credential is revoked. The caller must still authenticate and
hold both required scopes. A new idempotency key performs fresh live checks before
returning even an identical registration. Neither receipt authorizes execution:
delivery must resolve current credentials and recheck current binding ownership.

This shared command defines the durable record. Each delivery adapter must
explicitly support resolving it and provide protected token custody; it does not
replace an adapter's existing in-memory registrations automatically.

## Bind dedicated principals

An operator uses `orchestration.bind` with `orchestration.manage`, the current
campaign revision, and a configuration digest. The campaign must be sealed and
nonterminal. Bind a fresh dedicated control principal and optionally a separate
evaluator principal. Neither can be the binding operator. Principals with prior
command history or an existing ownership record cannot be adopted or reassigned.
Credential issuance remains a separate operator action; a binding grants no
scopes, tokens, compute capacity, or native execution rights.

~~~python
from research_swarm.control.orchestration_models import BindOrchestration
from research_swarm.domain.canonical import record_digest

configuration_digest = record_digest(
    "example.search-driver-v1",
    {
        "policy": "greedy-v1",
        "baseline_trial_id": baseline_trial_id,
    },
)
binding = operator.command(
    "orchestration.bind",
    BindOrchestration(
        campaign_id=campaign_id,
        adapter="local",
        configuration_digest=configuration_digest,
        routes={"control": control_actor_id, "evaluator": evaluator_actor_id},
    ),
    idempotency_key="bind-research-driver",
    expected_revision=campaign_revision,
)["result"]
~~~

The binding has its own epoch, separate from the workspace's worker-control
epoch. It also records its workspace, routes, next ordinal, and pending turn.
One turn may remain unresolved per binding. Native workers and external proposal
agents retain independent identities and can continue concurrently.

The control route can use the supported search, promotion, confirmation,
evaluation-authorization, pause/completion, and reporting commands. Only the
evaluator route can complete an evaluation. Each actor still needs the original
command's scopes. The service never impersonates the evaluator or grants a
driver the operator's credential.

## Prepare, execute, reconcile

`orchestration.prepare_turn` freezes an operation, normalized typed payload,
target idempotency key, expected target revision, configuration digest, ordinal,
and planning watermark. The action must belong to the binding's campaign and
fit within 64 KiB. Preparation requires the binding revision. Repeated delivery
of an identical ordinal returns its original record; changed intent conflicts.
Target and delivery keys must differ. Reserved target keys cannot be consumed
by another lifecycle command or an unguarded write.

`orchestration.execute_turn` executes that immutable action as the authenticated
route actor. It checks ownership inside the target transaction after exact
acknowledgment replay. Dedicated drivers cannot bypass this route by omitting a
fence from a direct command. Internal artifact reads/publications derive exact
server-only capabilities; those capabilities are not accepted in API JSON.

Execution returns a nested `response` from the original command plus an immutable
`receipt` and the settled `turn`. Inspect `response.disposition`: a durably
rejected target is still a successfully reconciled turn. Its receipt binds the
original command ID, request digest, and response digest. A later delivery key
cannot cause the effect to run again or create another receipt.

Preflight refusal, unavailable transport, or missing target permissions can
leave no authoritative command acknowledgment. Such a turn stays unresolved.
It does not become a scientific rejection, and it does not permit another
ordinal. `orchestration.reconcile_turn` reads the original command ledger; it
accepts no caller-supplied acknowledgment or cost. If no acknowledgment exists,
it returns an unresolved error without caching that error as a final result.
An operator with management, drive, and read permission can reconcile an already
acknowledged turn after its original route becomes unavailable.

## Close a fenced action with no acknowledgment

When the original binding is fenced and its pending action never committed a
target acknowledgment, an operator can use `orchestration.close_turn`. Supply
the original epoch, turn ID, turn revision, and an explanatory finding. The
transaction checks the authoritative target command ledger before closing it.
An accepted or rejected acknowledgment requires normal reconciliation instead.

The resulting immutable `drive_closure` has reason
`fenced_without_target_ack`; the turn becomes `closed_unacknowledged`. It contains
no fabricated target command ID, score, or rejection. Closure clears the pending
pointer while keeping the binding fenced and preserving its permanent reserved
action key. It changes no native worker custody, budget, proposal, or evaluation.
The finding proves absence of a committed target acknowledgment. It does not
prove that preflight reads, pins, staged bytes, or provider calls never happened.

Late original dispatch cannot commit after this closure or write a substitute
rejected acknowledgment under its reserved target key. Execute and reconcile
return the same closure with `response: null` and `receipt: null`; the outer
command acknowledgment confirms only that this terminal record was recovered.
`DurableTurnDriver` recovers a closed ordinal without invoking its planner.
Callers must distinguish this outcome from a settled accepted/rejected action.

Exact closure retries and identical intent under new command keys preserve the
original receipt. Changed epoch, revision, or finding conflicts. Dedicated
drivers cannot close themselves, even with wildcard scopes. This operation is
part of retiring ownership; it does not transfer ownership or resume a campaign.

## Use the API-only runner

`DurableTurnDriver` works with a local or HTTP `ResearchClient`. Supply a pure
planner that returns one `DriveAction`. The runner looks up the deterministic
turn ID before calling that planner, including after the turn has settled.
Retry the same ordinal after any interrupted delivery. Advance the ordinal only
after inspecting its authoritative result and making the next planning decision.

~~~python
from research_swarm.domain.orchestration import DriveAction
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.search.common import SearchWait

driver = DurableTurnDriver(
    control_client,
    campaign_id,
    epoch=1,
    configuration_digest=configuration_digest,
)


def plan_one_action():
    action = search_driver.plan()
    if isinstance(action, SearchWait):
        # No turn is prepared. The caller handles the declared wait condition.
        raise RuntimeError(action.reason)
    return DriveAction(
        operation=action.operation,
        payload=action.payload,
        idempotency_key=action.command_key(campaign_id),
        expected_revision=action.expected_revision,
    )


result = driver.advance_turn(1, plan_one_action)
~~~

The async method uses the same boundary. Cancelling the awaiting task is not
proof that an in-flight RPC or native process stopped. Reopen the runner and
recover the same ordinal. Planning callbacks must not perform LLM calls,
filesystem mutations, or experiment execution; those effects have their own
durable request and worker protocols. The initial turn runner does not schedule
timers or automatically invoke the next ordinal. Use the wait runner below when
the planner needs new evidence before preparing another action.

## Persist a wait before sleeping

`orchestration.record_wait` records a stable wait ID, configuration digest,
binding epoch, and condition under the current binding revision. The caller is
the destination actor; an operator cannot silently choose another recipient.
Only one action or wait owns the binding at a time. The condition contains:

- `observed_watermark`: the event position actually inspected by the planner.
- `event_types`: up to 32 exact domain event names; empty means any domain event
  in this campaign. Orchestration bookkeeping cannot wake itself.
- `wake_at`: an optional absolute UTC timestamp, preserved across restarts.

Async delivery submits the same reconciliation command as native delivery; it
does not decide timer readiness itself. The hosted `research-timers-witness-v1`
profile uses its transaction's admitted native witness to compare this threshold
and records the same instant in readiness. Earlier hosted profiles remain
event-only. An absolute UTC timer is not a minimum-duration guarantee across clock
requalification. The original threshold is preserved; native execution deadlines
and acceptance leases keep their separate contracts.

Registration checks already committed events after the supplied watermark in
the same transaction. Matching events that commit later create readiness and a
pending delivery in their own domain transaction. This closes the gap between
reading state and registering a wait. Parent campaigns receive
`funding.child_opened`, `funding.child_changed`, and `funding.child_closed` events
when descendant accounting changes their immediate allocation. Zero-valued
child closure still emits an event. Other campaign events do not implicitly
subscribe the parent to every descendant's history.

~~~python
from research_swarm.control.wait_models import RecordOrchestrationWait
from research_swarm.domain.orchestration_waits import WaitCondition
from research_swarm.orchestration.waits import DurableWaitDriver

waiting = control_client.command(
    "orchestration.record_wait",
    RecordOrchestrationWait(
        wait_id=wait_id,
        campaign_id=campaign_id,
        epoch=1,
        configuration_digest=configuration_digest,
        condition=WaitCondition(
            observed_watermark=planning_watermark,
            event_types=(),
            wake_at=next_check_at,
        ),
    ),
    idempotency_key=wait_command_key,
    expected_revision=binding_revision,
)["result"]

waits = DurableWaitDriver(control_client, campaign_id, epoch=1, actor_id=control_actor_id)
outcome = await waits.wait_ready(wait_id, timeout_seconds=30)
~~~

`orchestration.reconcile_wait` discovers a due timer after restart. A condition
that is still pending returns a retryable error without permanently caching a
rejection under the reconciliation key. Timers do not run inside a disconnected
database: a local runner or provider timer must call reconciliation. The async
runner uses bounded monotonic polling; a polling timeout leaves the durable wait
intact.

`DurableWaitDriver.deliver_once(callback)` sends a bounded batch for its exact
campaign, epoch, and actor. The callback must durably accept the stable delivery
ID before returning. A lost reply can deliver the same ID again. Deduplicate at
the receiver; acknowledgment records delivery, not exactly-once transport.
Other routes' pending messages cannot starve this destination's batch.

Acknowledging a wakeup does not consume research evidence or advance a cursor.
After rereading state, preparation of the next action (or a replacement wait)
must supply a watermark covering the readiness record. Consumption is atomic
with that preparation. A stale rejected preparation does not poison the next
valid attempt at the same ordinal. A fence supersedes the wait and preserves its
original destination and pending notification; it cannot redirect old work to a
new actor.

Query `orchestration_wait`, `wait_readiness`, `wakeup_delivery`, and
`wakeup_acknowledgment` to inspect conditions and delivery independently.

## Fence ownership

`orchestration.fence` requires the binding revision, original epoch, management
scope, and an explicit reason. It blocks new actions by the assigned drivers.
It does not kill workers, close native resources, release budgets, erase staged
artifacts, or clear unresolved turns. Exact accepted and rejected acknowledgments
remain recoverable with current permissions. Fenced principals remain retired;
initial binding is not a live migration API.

Bare bound actors cannot begin, append, or finalize upload staging through the
public artifact endpoints, even if their token has wildcard scopes. Internal
staging I/O checks its original saved-turn authority before admission. An I/O
operation already admitted can finish after fencing; publication and scientific
acceptance still require the transaction fence. Cleanup can release only the
original actor's exact read pin after a fence. It cannot acquire a new read or
publish a result.

Use `orchestration_binding`, `drive_turn`, and `drive_receipt` queries for recovery
and inspection. Turn list summaries omit the full payload; fetch a named turn
when its exact action is needed. The existing operator API remains available for
explicit human intervention. Provider task tokens and coroutine histories do
not appear in these records.

Local tests cover independent connections, real HTTP recovery, separate native
evaluation credentials, source-pinned proposal admission, preflight refusal,
fencing between preflight and commit, owned-pin cleanup, reserved-key collisions,
and unchanged staging bytes after denied HTTP mutations. Wait tests include a
controller process exiting immediately after commit, reopening the runtime,
receiver acknowledgment loss, persisted timer recovery, parent-child wakeups,
and rollback when notification storage fails. Temporal's real-server qualification
is described separately; Cloudflare execution remains pending.
Closure tests also cover a delayed original dispatch, competing operators,
controller reopen, real HTTP recovery, accepted/rejected acknowledgment gaps,
and atomic rollback if storing the closure event fails.

## Transfer a paused campaign

`orchestration.transfer` changes the research driver inside the same workspace.
It requires `read`, `orchestration.manage`, and `orchestration.drive`, a sealed
paused campaign, its current revision, the original binding epoch, and the
binding revision as `expected_revision`. The operator cannot be a bound driver.
Successor actors must be fresh dedicated principals without earlier commands or
ownership. The campaign remains paused until an explicit `campaign.start`.

Native attempts must have proven closure. Unknown resource custody, unsettled
attempt reservations, and work still pinned to the retiring evaluator block
transfer. A completed failed scoring job does not remain a blocker merely because
its authorization record survives. Pending confirmation panels keep their
original evaluator identity and block transfer until that work is resolved.

```python
from research_swarm.control.orchestration_models import TransferOrchestration

transfer = operator.command(
    "orchestration.transfer",
    TransferOrchestration(
        transfer_id=transfer_id,
        campaign_id=campaign_id,
        campaign_revision=paused_campaign["revision"],
        epoch=old_binding["data"]["epoch"],
        adapter="local",
        configuration_digest=configuration_digest,
        routes={"control": new_control_id, "evaluator": new_evaluator_id},
        reason="Move the paused campaign to replacement research drivers",
    ),
    idempotency_key=transfer_id,
    expected_revision=old_binding["revision"],
)["result"]
```

The controller follows required artifact retention owners, verifies their bytes
under read pins, then rechecks the full manifest in the write transaction.
Changed or corrupt evidence refuses transfer before ownership changes. The
transaction records an immutable checkpoint and old binding, fences old turns
and waits, reconciles a pending target acknowledgment or records its distinct
absent-ack closure, advances the orchestration epoch, and creates a pending start
intent for each successor route. All effects commit together.

The checkpoint records campaign contract and record digests, full budget balances
and reservations, child-funding projections, required artifact identities and
generations, workspace authority, and the observed event watermark. It is an
audit of the transfer, not a portable backup or a frozen copy of every historical
payload. It has no expiry and does not depend on recovery-snapshot storage.
The local profile limits the manifest to 100,000 selected records, 4,096 unique
required artifacts, 131,072 entries, and 64 MiB of canonical manifest bytes.
Chunks contain at most 128 entries and 256 KiB each, with at most 1,024 chunks.
Oversized evidence returns `RESOURCE_UNAVAILABLE`; it is never silently omitted.

After assignment, each successor reads `orchestration_start` using its ID from
the binding's `start_ids`, verifies the referenced checkpoint and chunks, and
recovers current state through a fresh snapshot and event catch-up. It then sends
`orchestration.acknowledge_start` with the exact start ID, checkpoint digest, an
observed watermark at least as new as the checkpoint, and the start revision as
`expected_revision`. Only the named actor can acknowledge its route. New turns
and waits are blocked until that acknowledgment commits. A stored start intent
does not itself provision a process or prove provider delivery.

Replaying the transfer identity returns its original immutable receipt. A
different intent cannot reuse that identity. Competing transfers select one
successor epoch. Old deliveries retain their original destinations and accepted
effects remain recoverable. Workspace worker authority, queued trials, admitted
scientific contracts, budgets, and funded children are unchanged.

Local tests cover competing transfers, absent/accepted/rejected pending turns,
corrupt and changing artifacts, evaluator and native-custody blockers, large
manifest entries, and transactional rollback. The HTTP successor test recovers a
fresh snapshot, acknowledges both routes, and resumes the original queued CPU
trial on its existing worker without another attempt or workspace-authority change.
