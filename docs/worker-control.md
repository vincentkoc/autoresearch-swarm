# Worker operational control

The controller records three different facts: admission policy, worker presence,
and cancellation progress. A presence-only heartbeat is an observation. Explicit
[acceptance lease renewals](acceptance-leases.md) can extend result-publication
grace, while native process closure and budget settlement remain separate.

The shared command catalog exposes these operations through the Python SDK,
HTTP command endpoint, JSON CLI, and MCP. The scoped worker query endpoints also
accept the new read and list kinds. No new worker credential scope is required.

| Operation | Authority | Revision / ordering | Result |
| --- | --- | --- | --- |
| `worker.set_admission` | `worker.manage` | CAS on `worker_admission.revision` | Active or draining admission record |
| `worker.heartbeat` | `worker.publish`, exact enrollment owner | Strictly increasing heartbeat sequence | Presence and admission snapshots |
| `worker.cache_advertise` | `worker.publish`, exact enrollment owner | CAS on `worker_cache.revision` | Expiring, worker-reported cache observation |
| `attempt.request_cancel` | `campaign.manage` | CAS on the attempt revision | Disposition and sticky control record, or `already_terminal` |
| Worker get, `worker_admission` / `worker_presence` | `worker.query`, exact enrollment owner | Current read | Own operational record |
| Worker get, `worker_cache` | `worker.publish`, exact enrollment owner | Current read | Own session's cache observation, or revision zero |
| Worker list, `controls` | `worker.query`, exact enrollment owner | Insertion-watermark cursor | Requested cancellations, at most 100 per page |

## Independent records

Immutable enrollment continues to describe the worker's principal, binding,
profiles, and environment contracts. It does not change when the worker reports
presence or an operator drains it.

`worker_admission` has the full binding, operator identity, reason, and receipt
time. Its state is `active` or `draining`. An enrollment with no admission record
is active at revision zero. Its first state change inserts revision one. Setting
the same state is an accepted no-op and preserves its original reason and
revision. Operators can list admission records; absence means revision zero.

`worker_presence` records the full binding, last heartbeat sequence, driver
instance ID, and controller receipt time. Before the first heartbeat, the scoped
read returns `unobserved` at revision zero. Reads do not create either record.
Every accepted heartbeat advances presence; explicit renewals also update their
named acceptance leases. An operator's admission CAS
therefore remains valid during unrelated heartbeat traffic.

The sequence is monotone across driver restarts within an enrollment. Gaps are
allowed. New command keys carrying old or equal sequences are rejected. Exact
idempotency replay returns the original acknowledgment without refreshing time.
A replayed admission snapshot can be old; read current admission and controls
before relying on them. A new driver instance does not reset the sequence.

Receipt timestamps are informational. Missing heartbeats do not establish that
a worker, supervisor, child, accelerator context, or reservation is gone.

## Cache observations

`AdvertiseWorkerCache` carries the full enrollment binding, a snapshot ID, up to
128 distinct complete `ArtifactRef` values, inspected byte count and an explicit
truncation flag. The worker inspects local bytes; the controller checks each
reference against the current artifact catalog and labels the observation
`worker_reported`. It does not independently attest residency.

The first observation uses expected revision zero. Further observations use the
current revision returned by `WorkerRead(kind="worker_cache", id=session_id)`.
Publication records qualified controller time and its clock epoch. Hints expire
after 60 seconds; a changed epoch, backwards time or unavailable time authority
makes them unusable. Exact command replay preserves the original timestamp and
expiry. Heartbeats do not refresh them. A newly verified snapshot needs a new
command key and the current revision.
Resubmitting the current snapshot ID with identical contents returns its old
observation without refreshing time; changing its contents is a conflict.

These observations grant no artifact access, retention, execution lease, resource
reservation or scientific eligibility. Cache eviction is allowed after publication.
Input materialization still checks its ordinary live authority. Eligible worker
offer pages use these hints for stable locality preference; claims retain all
existing admission checks.

## Drain linearization

Fresh claim admission reads worker policy inside the same transaction that
reserves attempt budget and issues its assignment. A committed drain blocks
fresh claims and removes the worker's offers. A claim committed first remains
valid and can finish. Replaying that accepted command still returns its original
grant after a drain.

Draining does not revoke credentials, mutate signed assignments, extend clocks,
or prevent input access, pending-work recovery, output upload, or publication.
Returning admission to active is an explicit operator command with the current
admission revision. It does not alter unresolved native work.

## Cancellation lifecycle

`attempt.request_cancel` creates one immutable intent identity for one admitted
attempt. The record contains its full original worker binding, dispatch digest,
sequence one, requester, reason, and receipt time. Repeated requests preserve
that original intent. There is no resume/uncancel operation in this version.

The request leaves the attempt, job, trial, signed grant, native capacity,
execution deadline, and budget reservation unchanged. A terminal attempt without
a prior control returns `already_terminal` and creates no pending work.

Workers fetch requested records through `WorkerQuery(kind="controls")`. Each
heartbeat may acknowledge up to 100 distinct controls using attempt ID, dispatch
digest, and sequence. Every acknowledgment must match the current worker's full
binding. The transaction validates the entire batch and commits acknowledgment,
presence, events, and outbox together. A failed item rolls back the batch.

The acknowledgment means the worker has persisted the cancellation locally.
The controller cannot observe that write directly; authenticated worker code is
responsible for honoring this contract. Acknowledgment does not claim closure.
It changes the record to `acknowledged` and removes it from requested delivery.

Only accepted native completion resolves the control, in the same transaction
that settles the attempt. Resolution records the accepted `attempt_outcome` and one of
`cancelled`, `not_started`, or `completed_without_cancellation`. Success racing a
cancel remains success, including its measurements and normal stage dependency
progression. An invalid workload report can make an attempt fail even when its native process
exits zero; the attempt outcome does not reinterpret the native receipt. A late acknowledgment
updates its timestamp without reopening completed control. UNKNOWN publication
keeps the control and resource uncertainty unresolved.

This command cancels one attempt. It does not stop a trial, campaign, descendants,
or another worker. A broader research stop requires its own explicit policy.

## Bounded delivery and recovery

Control pages are bound to actor, workspace, complete worker binding, and query
kind. A fixed row-ID watermark makes the scan finite. Acknowledgment during a
scan may remove pending rows; the cursor tracks scanned rows. Newly created
controls beyond the watermark appear in a fresh scan. A cursor is not an
acknowledgment. The requested-control index excludes completed history.

The controller can retain requests while a worker is offline. Drivers must
persist a received control before acknowledging it, synchronize before starting
new work, and poll independently while supervisors execute. They must replay
uncertain heartbeat delivery before advancing their sequence. Native journal
and supervisor gates remain the authority for preventing launch and stopping
owned process scopes.

## Durable worker receipt

`WorkerJournal.apply_control(binding, directive)` accepts the installation-pinned
binding and a `CancellationDirective`. The directive contains only immutable
binding, attempt, dispatch digest, action, and sequence fields. Controller audit
metadata and changing acknowledgment/completion timestamps are excluded from
its identity.

The inbox and attempt journal share a SQLite write transaction. If the assignment
is already present, its complete binding and digest must match before cancellation
becomes sticky. If it has not arrived, the inbox keeps the directive and `receive()`
merges it before returning the new entry. A mismatched later assignment is refused.
Cancellation does not change the immutable worker-input digest.

`pending_control_acks()` reads at most 100 exact acknowledgments. After an accepted
heartbeat response, `acknowledge_controls()` validates the complete batch and marks
its delivery. It retains the cancellation record: an acknowledged cancellation
must still prevent a later assignment from launching. Duplicate receipt and
acknowledgment are idempotent across restart.

For CLOSED and NOT_STARTED attempts, late controls remain in the separate inbox.
They do not modify journal payload, revision, events, outcome, or artifacts that
may already have been published. UNKNOWN remains unresolved. Local preparation
and clock errors use `request_local_cancel()` without consuming remote sequence
numbers; legacy journal sequence values cannot suppress a new remote receipt.

Existing native claim/release gates and supervision read the sticky cancellation
flag. Tests cover cancellation before assignment, during materialization, before
and after release authorization, and while a parent and descendants run. Capacity
and budget settlement still require accepted closure or proven absence.

## Remote driver synchronization

`ScopedWorkerDriver` synchronizes controls before scheduling pending work and
after a claim reply. An independent polling task continues while native work
executes, including during orderly shutdown. Two reserved control threads keep
long experiment execution from exhausting the executor used for control calls.
The default control poll interval is 500 ms. Heartbeats target 15 seconds or send
sooner when there are acknowledgments. These are polling targets, not delivery
latency guarantees; network calls retain their configured timeout.

One private `driver/heartbeat.json` stores the full binding, last accepted sequence,
and at most one pending request with its original idempotency key. It is written
before delivery. Accepted controller reply, local acknowledgment, and persisted
sequence advance occur in that order. A crash between them replays the original
request and repeats local acknowledgment safely. There is no file per heartbeat.

On initialization, the driver reads current presence. Existing sequence and local
state must agree after replay of a pending request. Missing local heartbeat state
for an already observed enrollment blocks explicitly; it never silently resets a
sequence or replaces an uncertain envelope. A new driver instance ID does not
change this rule. Preserve the private driver directory when resuming.

Each synchronization pass scans at most 128 control pages of 100 records and
acknowledges progress during the scan. A bound or transport failure prevents fresh
native launch until a complete synchronization succeeds. Existing owned supervisors
continue with their original deadlines. A claim accepted during a control outage
remains reserved for recovery. A drained worker remains observable and can finish
previously admitted assignments; fresh offers resume after explicit reactivation.

Cancelling an awaited synchronization does not release its writer lock while its
thread is still persisting a heartbeat. Driver shutdown first drains owned native
tasks, then stops the control pump, waits for outstanding control calls, and releases
its ownership lock. Forced driver death leaves native supervision independent;
on restart, queued cancellation is applied to the original process scope.

Tests use HTTP, separate controller/driver/supervisor processes, real native
cancellation, interrupted heartbeat acknowledgment, and bounded paging. Physical
remote-host deployment remains a separate qualification. The controller API alone
still does not prove receipt: inspect acknowledgment and accepted closure separately.


## Original session evidence ownership

Enrollment records the complete original `WorkerData` as a `worker_session`,
including its principal, full binding, engine profiles, and assignment public
authority. That identity cannot be reused for different enrollment data. Existing
legacy enrollments remain readable as virtual revision-zero session records; a
read does not migrate them. Future replacement must archive the original before
changing the current worker row.

Workers with `worker.query` can explicitly read their own `worker_session` and
`historical_attempt`, and page `historical_pending`. These operations authenticate
the original principal and complete original binding. They do not require that
the old session is still the current execution authority. Their cursors retain
the same workspace, actor, binding, and query-kind isolation as current views.

Attempt-bound output begin, append, finalization, cleanup, and receipt checks use
that historical ownership. A valid original worker can preserve journals, logs,
and declared outputs after an epoch change or execution fence. A replacement
principal cannot borrow the old session or its upload receipts. Credential
revocation remains authoritative. An unavailable archived session, or explicit
revocation of the matching current session, denies fresh historical access.

Offers, claims, input acquisition, clock samples, ordinary heartbeats, and normal
result publication continue to require current authority. Transporting evidence
therefore does not settle budget, create a scientific observation, advance stages,
or select an incumbent. Accepted native closure can release its worker-owned
machine claim independently; the controller reservation remains pending until
an authorized reconciliation proves the appropriate disposition.

The [recovery-only command](remote-workers.md#recover-original-evidence-without-execution-authority) opens original custody for evidence delivery. Historical evidence access does not reconnect an old worker for execution.


## Quarantine and explicit reconciliation

`worker.submit_evidence` accepts the same `PublishAttempt` payload as ordinary
publication, with `worker.publish` scope. The original session authenticates the
submission. Scoped workers must supply finalized upload receipts for the exact
attempt, artifact, and role. Submission records and retained artifact references
are immutable. Repeating the same publication returns the same submission; a later
closed observation can create a distinct submission after an UNKNOWN observation.

A submission is evidence custody, not acceptance. It does not settle a reservation,
change a job, create a `RunResult`, authorize evaluation, or select an incumbent.
The `attempt_submission` record remains `quarantined` even after reconciliation;
its separate reconciliation receipt records the acceptance decision. This preserves
the original submission instead of rewriting its history.

An operator uses `attempt.reconcile` with:

~~~json
{
  "attempt_id": "<original-attempt-id>",
  "submission_id": "<immutable-submission-id>",
  "expected_controller_id": "<current-controller-id>",
  "expected_control_epoch": 2,
  "reason": "Recover the original native closure after controller replacement"
}
~~~

The command envelope must include the current attempt `expected_revision`. The
operator requires `attempt.reconcile`. The bounded campaign context query exposes
`authority.controller_id` and `authority.control_epoch` in the same read snapshot.
The command does not impersonate the original worker
or need broad artifact administration. This operation is available through the
same command catalog used by the Python SDK, HTTP, CLI, and MCP tools. Use record
queries for `attempt_submission` and `attempt_reconciliation` to inspect evidence
and receipts. Read access remains independently scoped.

Reconciliation rechecks current controller authority, the original archived session,
complete dispatch binding, assignment and timing evidence, native closure, prepared
inputs, seed, log identity, report, output names, artifact availability, and current
job ownership. It uses the ordinary publication validator and settlement path.
It cannot substitute a new attempt or change the historical epoch or result digest.
It records original and current authority in an immutable receipt. Accounting,
accepted execution, dependency changes, cancellation resolution, and the receipt
commit in one transaction. The native machine registry remains worker-owned;
controller acceptance never proves physical resource release by itself.

A reconciled execution still needs its separately authorized evaluator receipt
before scientific comparison or promotion. An UNKNOWN observation, changed job
owner, invalid evidence, unavailable original session, or terminal campaign prevents
this acceptance. Failed reconciliation retains the submission and existing budget
uncertainty. There is no force-accept option. Operational closure uses the separate
`attempt.reconcile_closure` command described below.

Fresh submissions continue to require valid original worker credentials. A current
operator can reconcile evidence already submitted before that credential was
revoked, because the operator acts under its own recorded authority. Revoking the
archived session evidence authority also blocks fresh reconciliation.

Exact command replay checks actor, scope, and request digest before current authority
or artifact access. If another identical request commits while preflight is in
progress, a subsequent preflight failure returns that exact committed acknowledgment.
No execution or settlement is retried. New keys still undergo current checks;
changed payloads on old keys conflict.

`DispatchEnvelope.expires_at` remains a launch cutoff. It is not a result delivery
lease. Epoch-change recovery is verified; an independent acceptance-lease policy
must be specified and tested before claiming that separate qualification.


## Operational closure without scientific acceptance

`attempt.reconcile_closure` takes the same `ReconcileAttempt` payload, current
attempt revision, and `attempt.reconcile` permission as scientific reconciliation.
It validates original native ownership and closure, then records the final
`closed_unaccepted` disposition. It does not create a `RunResult` or evaluator
receipt. A later ordinary publication or scientific reconciliation cannot upgrade
that attempt. New research requires an explicitly admitted new trial.

This command pins and reads the journal, not the scientific report or model output
blobs. A missing or malformed report must not make known process closure disappear.
The journal must still match the original dispatch, specification, signed assignment,
clock evidence, command, native identity, and current job ownership. UNKNOWN state,
an unclosed scope, or unproven exit refuses settlement. The immutable `attempt_closure`
receipt keeps the actual native exit/reason, original authority, original publication
digest, measured consumption, and current operator authority. Successful native exit
and scientific nonacceptance remain distinct facts.

The transaction settles original execution reservations once. Launched scoring
consumes its reserved evaluation exposure even when its output is unusable. Proven
no-start releases that exposure; actual measured native time remains accounted for.
The closed job exposes no output to successors. Undispatched jobs in its trial become
`not_started`, with `trial_closed_unaccepted` as their stopping reason; only their
unused exposure reservations are released.

Already admitted siblings keep their reservations and native custody until their
own evidence arrives. A sibling UNKNOWN report leaves resource state unknown. A
later sibling completion can close resource state, but cannot erase the trial's
`closed_unaccepted` outcome or incomplete evaluation status. Comparisons and promotion
continue to reject the trial. Cancellation receipts record the closed-unaccepted
attempt outcome while deriving cancellation resolution from the actual native proof.

A fully settled closed-unaccepted trial can be included in ordinary campaign
completion. Closure reconciliation also works against a campaign already marked
terminal without changing its state, revision, incumbent, or original final report.
New reports reflect the appended accounting and closure evidence. This does not yet
add the separate operation of completing a campaign while resource uncertainty remains.

Worker and local driver recovery recognize the new terminal attempt status. The
local driver skips evaluator completion for a scientifically closed trial, including
closure that races receipt delivery or a previously persisted receipt intent. It
suppresses only the corresponding comparison-ineligibility rejection; credential,
transport, and unrelated evaluation failures retain their existing handling. If
operator closure wins during terminal evidence upload or publication, the worker
returns that disposition only after rereading the exact original dispatch and
confirming its local journal is terminal. An UNKNOWN local journal cannot borrow
controller closure as native proof.

Controller settlement never releases a native machine registry claim. That remains
conditional on the worker's independently verified native closure. Other quarantined
scientific artifacts retain their own evidence and storage lifecycle.


## Explicit worker fencing and session replacement

`worker.fence` requires `worker.manage`, the exact original `WorkerBinding`, a
reason, the expected current controller ID/epoch, and the current **worker row**
revision. It records an immutable `worker_fence` receipt and changes the current
worker state to `fenced` in one transaction. An operator can fence an exactly
identified older-epoch session using the current controller authority.

Fencing denies fresh server authority: claims, input acquisition, clock replies,
normal publication, and current operational control. It preserves original
session evidence access. It does not revoke credentials, terminate processes,
request cancellation, expire signed grants, release capacity, change job state,
or refund budget. Already issued bounded grants retain their original local
limits; a disconnected process cannot receive instant server-side revocation.

`worker.replace_session` names that exact fence and old binding, fresh session and
principal IDs, expected current controller authority, and the fenced worker's
revision. It preserves worker ID, profiles, project scope, environment contracts,
and concurrency. Reused archived session/principal IDs and identities held by
legacy current workers are rejected. A fresh principal prevents old credentials
from acquiring the replacement's authority using its public session ID.

The command archives both enrollments and records a `worker_replacement` receipt.
That receipt preserves the previous presence/admission values and revisions.
Current operational projections advance their revisions, switch to the fresh
binding, and start with `unobserved` presence and `draining` admission. Heartbeat
sequence starts at zero. Explicit `worker.set_admission` activates the installed
replacement. Old operational commands cannot mutate these new projections.

~~~python
from research_swarm.control.worker_turnover import FenceWorker, ReplaceWorkerSession
from research_swarm.control.worker_records import WorkerData
from research_swarm.domain.types import new_id

row = client.get("worker", worker_id)
old = WorkerData.model_validate(row["data"])
authority = client.context(campaign_id)["authority"]
request = FenceWorker(
    binding=old.binding,
    expected_controller_id=authority["controller_id"],
    expected_control_epoch=authority["control_epoch"],
    reason="Install a replacement worker session",
)
fenced = client.command(
    "worker.fence",
    request,
    idempotency_key=fence_command_key,
    expected_revision=row["revision"],
)["result"]
replacement = ReplaceWorkerSession(
    **request.model_dump(exclude={"schema_version"}),
    fence_id=fenced["fence"]["id"],
    session_id=new_id(),
    principal_id=replacement_principal_id,
)
result = client.command(
    "worker.replace_session",
    replacement,
    idempotency_key=replacement_command_key,
    expected_revision=fenced["worker"]["revision"],
)["result"]
~~~

Persist fresh IDs and exact request/command keys before delivery. Exact command
replay returns its original acknowledgment even after authority changes. Two
replacement commands competing for one revision produce one replacement. A stale
old binding or fence cannot affect a later session.

Controller concurrency counts unresolved attempts across all sessions of the
stable worker ID. The machine registry independently retains physical claims.
Original recovery can release a proven closed native claim; controller capacity
remains occupied until explicit reconciliation settles the original attempt.
Replacement is neither a scientific result nor a retry of that experiment.

These domain commands are available through every shared transport. Use the
[resumable provisioning and installation flow](remote-workers.md#provision-and-install-a-replacement-session)
for remote workers. Preserve the old installation and its occupied machine
registry. Credential revocation remains an independent authentication operation
and also denies historical delivery.
