# Durable orchestration actions

An adapter retries a saved action, not a fresh invocation of a research loop.
`SearchDriver.advance()` can perform several commands and replan between them.
Wrapping that method in a provider retry could perform different research after
an accepted reply is lost. Use a durable turn as the retry boundary instead.

The current implementation supplies initial ownership, one saved command per
turn, transaction-time fencing, and acknowledgment recovery. It does not yet
implement provider Workflows, durable wait delivery, or checkpoint ownership
transfer. An adapter label is descriptive metadata, not a qualification claim.

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
timers or automatically invoke the next ordinal.

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
and unchanged staging bytes after denied HTTP mutations. Temporal replay,
Cloudflare execution, durable wakeup delivery, and ownership transfer require
their own implementation and qualification.
