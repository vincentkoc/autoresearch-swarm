# Sealed native backups

The `sealed-native-v1` profile exports a consistent SQLite snapshot and every
verified filesystem artifact generation. It requires a sealed workspace and full
operator authority (`*`). R2 and other state or storage adapters are not supported
by this profile.

Sealing stops new research mutations. It does not stop native processes, release
capacity or settle their reservations. The source remains sealed after export.
This is a checkpoint for explicit recovery, not a rolling online backup.

## Establish the cut

Use the shared `workspace.seal` command through the SDK, HTTP API, CLI command
dispatcher or agent tools. The command requires the current controller ID and
control epoch, a new backup ID, a reason and expected revision zero. Pending
uploads, artifact deletions and retained readers must close first. If admission
is rejected, use a new command key after resolving the cause; exact rejected
commands keep their original reply.

For a native operator:

```python
from pathlib import Path

from research_swarm.domain.backup import SealWorkspace
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime

runtime = LocalRuntime.open(Path("state"))
context = CommandContext(
    principal=runtime.principal(), idempotency_key=new_id(), expected_revision=0
)
snapshot = runtime.database.snapshot(context)
request = SealWorkspace(
    backup_id=new_id(),
    expected_controller_id=runtime.identity.controller_id,
    expected_control_epoch=snapshot["control_epoch"],
    reason="Planned inactive restoration",
)
runtime.service.seal_workspace(context, request)
print(request.backup_id)
```

Retain the original worker installations, journals, resource registries and
controller directory. A seal is not evidence that their execution has ended.

## Export and verify

```sh
research --state-dir state backup export backup-directory --backup-id BACKUP_UUID
research backup verify backup-directory --digest sha256:MANIFEST_DIGEST
```

The destination must be outside the runtime, its parent must already exist, and
it must not contain another bundle.
The exporter uses SQLite's backup API, checks its schema/integrity/foreign keys,
and derives the artifact inventory from the copied database. It verifies the
operator credential and assignment key before publishing. It streams and checks
every exact artifact generation, including unreferenced verified content that
still occupies storage.

The manifest binds the database and private-file hashes, ordered generation
inventory, exact seal, both evidence watermarks and source custody public key.
The seal watermark precedes its own event; the snapshot watermark includes that
event and the source custody registration. First export persists the native key,
then registers its public identity in an immutable `backup_custody` record for
this control epoch. That root-only metadata command is permitted after sealing;
it cannot reopen research or replace an existing pin. A failed export can leave
this registration in place. Completed export retries do not register it again.
Once pinned, fresh seal/registration commands also refuse before recording an
acknowledgment. Previously committed command keys still replay their exact reply.
A signature over the manifest digest detects inconsistent content. To establish
provenance, compare the manifest digest or source custody key with a trusted
record; a self-contained public key is not independent proof of origin.

The bundle layout is fixed:

```text
backup-directory/
  manifest.json
  research.sqlite
  private/
    identity.json
    operator.token
    dispatch.key
    assignment.key
  blobs/
    GENERATION_UUID
```

The private directory contains credentials and signing keys. A principal with
only `service.manage` cannot export it. Private file protections apply on POSIX
and Windows. Verification rejects unexpected files, substituted paths, linked
files, changed bytes, missing retained evidence and unsupported schemas. The
profile bounds the database at 1 GiB and the inventory at 10,000 generations.

The source keeps its key and eventual target election together in
`backup-custody.json`. This file is never included in the bundle.
Preserve it with the original sealed controller: copied research keys
must not become authority to choose which restored installation may activate.
Worker journals, machine resource registries and supervisor state are excluded.

The manifest is published last. A destination without it is incomplete and
verification refuses it. An ordinary failed export removes its staging directory;
an interrupted destination remains incomplete. Export to a new destination after
inspection. Retrying a completed destination verifies and returns the same
manifest instead of recopying it or changing the cut.

## Restore inactive

```sh
research backup restore backup-directory --destination restored-state \
  --restore-id RESTORE_UUID --digest sha256:MANIFEST_DIGEST
```

Choose a new destination with an existing parent and retain the restore UUID.
The command verifies the whole bundle before creating that destination. It
publishes a private admission barrier before creating the runtime directory.
The barrier uses the native directory entry and conservatively normalized name
to cover case aliases. Both runtime initialization and reopening refuse while
the restore is incomplete.

The importer materializes the original artifact generations through the native
byte store. It copies no worker journal, machine resource registry or supervisor.
One offline transaction advances the workspace control epoch exactly once,
archives original worker sessions, fences current workers and records a
`restoration` receipt. The workspace gate becomes `restoring`. The original
workspace/controller IDs and historical dispatch bindings remain unchanged.

Source and target tables are compared as streams. The only permitted changes are
the new control epoch, worker fences/session archives, workspace gate, restore
receipt and corresponding events/outbox entries. Prior acknowledgments, budgets,
reservations, costs, evidence and other records must match the verified source.
Restore events do not wake existing research workflows.

Repeat the exact command after interruption. The validated intent owns a narrow
staging namespace; incomplete private writes can be removed and recopied without
adopting unrelated files. SQLite recovers its own interrupted transaction. A
committed restore receipt prevents a second epoch increment. The importer checks
the preserved database content and artifacts again before removing its admission
barrier. A changed intent, lost custody key or changed ledger is a refusal, not a
request to reset the state. Keep an incomplete destination at its original path.

After completion, normal clients can inspect the inactive runtime and replay
previous acknowledgments. New research mutations, credentials, enrollment and
ordinary artifact reads still refuse. The native backup verifier and importer
have direct access to verified bytes; that access does not admit research work.
The target keeps a distinct private `restore-custody.json` key for its identity.

Original worker processes may still be running and remain charged. Activation
requires their original closure evidence and the completed recovery checks below.
A source claim elects one target but does not open it.
Do not remove the gate, reset budgets or copy machine registries to force progress.

## Elect one target

Request a signed proof from the completed inactive destination, then submit that
public JSON to the original sealed source:

```sh
research backup request-claim backup-directory --destination restored-state \
  --restore-id RESTORE_UUID --digest sha256:MANIFEST_DIGEST > request.json
research --state-dir original-state backup claim backup-directory \
  --request-file request.json > claim.json
```

The Python equivalents are `request_restore_claim` and `claim_restore` in
`research_swarm.checkpoints.claims`. These are native custody operations; they
are not remote controller commands. Their JSON outputs contain no private key.

The target proof binds its private key, restoration ID, source identity, exact
manifest and next epoch. Source issuance verifies that proof, its immutable key
pin, the still-sealed source and unchanged checkpoint ledger. Under the source
lock it persists one signed claim in the same private file as the source key
before returning. Concurrent requests for different targets have one winner.
An exact retry returns the original claim ID, time and signature after completing
the same file durability barrier. It authenticates the original pinned custody
and request before replay; a missing or damaged bundle cannot hide an already
committed claim. A claim never expires and cannot be reassigned. After election,
export can only replay the elected bundle. Copy that bundle when another location
is needed; a new manifest could not receive another election.

Missing or mismatched native custody is a refusal. Export cannot regenerate a key
after its public identity has been pinned. A copied inactive controller has no
original source custody and cannot elect itself. Older bundles without a custody
pin remain verifiable and importable, but require re-export and a fresh import
before claim issuance.

This profile trusts the operator to preserve the unique native custody file.
Cloning that file, or rolling it back to its pre-claim contents, defeats the
single-source assumption; the database public-key pin cannot detect a same-key
rollback. It is not a consensus service or an online disaster-recovery quorum.
Do not copy or rewind source custody to recover a failed elected destination.
An elected destination must resume with its original target key and claim.

Private writes flush the staged file before atomic publication. POSIX also
syncs the containing directory. Windows uses
[`MoveFileExW` with replacement and write-through flags](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)
and propagates failure without a weaker fallback. These are native OS durability
operations; process-interruption tests do not establish physical power-loss
behavior on every filesystem or storage device.

## Admit recovery on the elected target

```sh
research --state-dir restored-state backup admit backup-directory --claim-file claim.json
```

The Python entry point is `checkpoints.admission.admit_recovery`. Like import and
claim issuance, this operation requires the native files and full operator
authority. The general HTTP/MCP command catalog cannot invoke it. Normal agent
research commands continue to use the shared API.

Admission verifies the complete inactive import, its original ledger, source
claim and possession of the selected target key. An internal, non-wire proof
binds that verification to the exact database authority, request digest, source
custody pin, restoration receipt, gate revision, target epoch and watermark.
The transaction rechecks the proof before changing any state. An operation name
alone does not grant admission. The original restore lock spans native
verification and the transaction.

One transaction records the signed `recovery_admission`, changes the gate from
`restoring` to `recovering`, fences active orchestration bindings and requests a
drain pause for running campaigns. Already-fenced bindings, dedicated actor
ownership and saved pending turns remain recorded. Waiting/ready workflows are
fenced through the existing wait lifecycle. Recovery events do not wake unrelated
research waits. A running or UNKNOWN trial keeps its campaign `pausing` and its
original reservations intact. Ready, awaiting-input, paused and terminal
campaigns retain their states. Admission does not increment the control epoch.

The exact command key replays its committed reply even after the backup bundle
becomes unavailable. A completed import can also be inspected again after
admission: its signed lifecycle receipt replaces the pristine-ledger comparison,
while original artifact and private-file checks remain in place. A fresh command
key cannot restart admission or elect another target.
Rejected native admission rolls back the whole transition, including any proposed
rejection acknowledgment, so the original pristine import remains retryable with
corrected authority.

## Deliver original evidence during recovery

The `recovering` gate admits a narrow subset of the existing worker API. The
original worker principal can upload a journal of at most 1 MiB for an attempt in
the restoration's frozen unresolved-attempt inventory. The journal must retain
its original dispatch, signed assignment, clock evidence, experiment and native
command. Reports, named outputs and log uploads remain unavailable. Invalid
unpublished journals can be aborted through `worker.abort_upload`, including after
the attempt settles. Storage quota remains reserved until physical cleanup ends.

Use `worker_upload_file` with `role="journal"`, then `worker.submit_evidence` with
only its journal reference. These operations share the SDK, HTTP and agent command
contracts. The target verifies its signed source election and admission before
minting an internal capability for that actor, attempt, session and request. Raw
transfer operations check authority under the upload lock; publication checks
again in the write transaction. The capability is not a wire field. Generic
artifact upload routes cannot bypass it, even for a full operator.

Submission records quarantine evidence. Received, running and UNKNOWN journals
remain observations of unresolved custody: they do not expire the original
acceptance lease, settle budgets or grant execution. A principal with
`attempt.reconcile` can call `attempt.reconcile_closure` against a quarantined
submission and the target's current epoch and attempt revision. The existing
native closure checks remain mandatory. Proven closure settles the original
reservations once, produces `closed_unaccepted` and lets a drain pause finish.
It creates no scientific result, evaluator receipt or successor job.

Exact committed acknowledgments replay before fresh authority checks. Partial
uploads resume after reopening the target. Canonical artifact deduplication uses
the original worker receipt; internal pin, unpin and cleanup capabilities bind to
the exact artifact or upload. No generic artifact mutation is granted. Cleanup
of an existing unpublished transfer remains possible after native settlement.

An explicit recovery route binds the original private worker installation to the
elected destination before the recovery driver can deliver there. Until explicit
activation, the target stays `recovering`; ordinary
research mutation, new dispatch and scientific acceptance remain unavailable.
Neither a source claim nor an admission receipt proves native process closure.

## Route an original worker

Create a public request containing a stable `route_id`, `attempt_id`, the original
worker's `credential_id`, its `source_controller_url`, and the target
`destination_url`. Issue the route from the admitted target, then install it at
the original worker:

```sh
research --state-dir restored-state backup route --file route-request.json > route.json
research worker install-recovery-route --worker-dir original-worker \
  --file route.json --source-pin source-pin.json
research worker recover --worker-dir original-worker --attempt ATTEMPT_UUID \
  --route-id ROUTE_UUID
```

`source-pin.json` must contain the `data` object from the independently trusted
source's `backup_custody` record. The original worker enrollment does not pin the
backup key, so the key embedded in a route cannot establish its own trust. Route
installation verifies that source pin, the source election, target admission and
the target's signature over the exact destination and original worker authority.
Public routes contain IDs and proofs, with no credential or private key.
Installation authorizes sending the original worker credential to the signed
destination. HTTPS is required except for loopback HTTP; redirects and ambient
proxy settings remain disabled by the normal client transport.

The Python entry points are `checkpoints.routes.issue_recovery_route`,
`workers.recovery_routes.install_recovery_route`, and
`workers.recovery.recover_worker_attempt(..., route_id=...)`. Issuance is a native
operator action with the existing target custody key. The route ID is immutable;
an identical request replays its deterministic signed file, while changed content
requires another route ID. Installation checks the full original worker digest,
credential ID, selected journal, and existing journal/registry identities under
the original enrollment, driver and attempt locks. Reopening a route repeats
those checks before any connection. Missing native stores are not recreated.

Route construction reads the original attempt, archived worker session and
credential in one SQL snapshot. Session records must be immutable at revision
one; revoked, differently owned or insufficiently scoped credentials refuse even
when a route file already exists. Attempt and session payloads are bounded to
one MiB each, credential principal data to 16 KiB. This shared construction path
does not sign, persist or establish current deployment authority itself.

Signed routes may additionally carry `logical_election`. This complete source
election envelope must match the digest signed into the target admission. Native
route serialization omits the field and retains its existing bytes and digest.
The native verifier rejects logical proofs; worker installation explicitly selects
the logical verifier when the envelope is present. Removing it cannot downgrade a
logical admission into native trust.

Offline logical verification checks all four election signatures against the
independently supplied source custody pin, then admission and route signatures.
The source authenticates the target intent and deployment identity as a delegation;
these are not independently observed runtime facts. The receiving controller must
also verify its actual deployment and current recovery admission. The route still
binds the original worker session, credential, attempt, installation and destination,
and cannot authorize execution. Installing and reopening such proofs is supported;
hosted issuance and journal delivery remain separate implementation steps.

The original enrollment URL, token, driver identity and pending recovery intent
remain unchanged. Each route gets a separate delivery directory, identity digest,
upload key and submission key. A target acknowledgment resolves only that target's
delivery; an uncertain request to the old controller retains its original bytes
and keys. Each route also freezes its own journal until acknowledgment, then can
submit a later observation. Without `--route-id`, recovery uses its original
destination as before. Recovery never launches or enrolls a worker.

## Reconcile an interrupted evidence reader

An interrupted target process can leave read pins after a journal upload or
closure check. Use `artifact.reconcile_reader_session` with
`{"session_id":"READER_SESSION_UUID"}` through the SDK, HTTP, CLI or agent tools.
The command requires `artifact.admin` and `read`; `artifact_read_session` records
are available through ordinary bounded queries. A still-active reader session
with no retained pins needs no cleanup for activation.

Cleanup first proves that the exact reader incarnation cannot resume. Native
ownership checks the original host, PID and process start time. A live,
inaccessible, foreign-host or opaque incarnation remains unresolved. Neither
elapsed time nor a controller restart proves closure.

During recovery, a separate internal maintenance capability binds that proof to
the elected target, current gate and epoch, operator, request, session revision
and complete retained-reference set. The transaction rechecks each pin's original
actor, artifact, retention owner and reader session, releases only its read pins,
and closes the session atomically. One reader may have pins for several actors.
Changes or additions after preflight refuse the write; partial cleanup failures
roll back all releases. Cleanup changes neither execution accounting nor scientific
evidence retention, and grants no generic artifact release or deletion authority.
The original accepted command replays without obtaining another native proof.

## Reconcile original saved agent turns

An admitted target permits two existing orchestration commands for a principal
with `orchestration.manage`, the command's ordinary scopes, and `read`:

- `orchestration.reconcile_turn` records the exact existing target acknowledgment,
  whether accepted or rejected. It never dispatches the saved action.
- `orchestration.close_turn` explicitly retires an original prepared turn when
  no target acknowledgment exists. Supply its original binding epoch, current
  turn revision and a finding. The resulting closure states that no reply was
  recorded; it does not invent a target success or failure.

A separate internal capability binds the request to an admitted campaign, its
original turn/action, dedicated actor, fenced binding and pending pointer, and
the exact target acknowledgment. All are checked again in the write transaction.
The existing settlement or closure helper then records the outcome and clears
the pending pointer atomically. A changed acknowledgment or ownership revision
invalidates preflight. Exact committed replies survive a later preflight refusal.

The old dedicated driver cannot grant itself maintenance authority. Execution,
preparation, transfer and new research remain fenced. Campaign state, native
attempt custody and budgets do not change when retiring this bookkeeping.
Bindings retain their old identities and remain fenced after the pending turn
is resolved; final activation must separately check all outstanding obligations.

## Activate the reconciled target

Activation is a native operator command and Python API. It requires the explicit
restored directory, its private target custody, the original signed admission,
and the current `workspace_gate` revision. It is not a remote wire command.

```sh
research --state-dir target-state backup activate \
  --file activation.json --revision 3
```

`activation.json` contains `restore_id`, `expected_control_epoch`, and `reason`.
Read the epoch and gate revision from the target; do not infer them from this
example. The Python entry point is
`checkpoints.activation.activate_recovery(runtime, context, ActivateRecovery(...))`.
The CLI derives a stable command key from the restore ID. An exact committed
request replays before reading mutable private files; a temporary blocker does
not consume the command key.

Settlement verification uses the shared `control.settlements` boundary. Each
reservation must map to exactly one settlement with matching ownership, dimensions,
request digest and terminal actual consumption. Actual values retain the ledger's
nonnegative Decimal consumption range, including legitimate overshoot; they are
not rounded or clamped to a reservation limit. The verifier exposes a separate
`research.budget-settlement-response.v1` digest for evidence consumers. It does not
rewrite historical settlement bytes or their original request digests.

Generated settlement IDs can be resolved through the exact reservation. Indexed
lookup guards malformed JSON: corrupt evidence refuses verification without making
unrelated database initialization depend on parsing that response.

Before activation, the controller requires:

- Every originally unresolved attempt has an immutable operational closure
  receipt, consistent original dispatch and settled acceptance authority. Source
  attempts that were already accepted retain that disposition. Pending or UNKNOWN
  execution anywhere in the workspace blocks activation.
- Dispatch reservations and confirmation exposures have their exact settlement
  identities and amounts. Budget balances match their ledger entries, including
  child-funding projections. Unknown custody and unreclaimed consumed capacity
  block activation. Cumulative consumption and legitimate queued reservations stay.
- Original driver bindings remain fenced, pending pointers are clear, and every
  saved turn has its actual acknowledgment or explicit unacknowledged closure.
- Uploads and worker upload receipts are terminal and consistent. No deletion or
  retained reader pin remains. A reader session without pins does not block.
- Every retained artifact is available and every verified generation passes a
  native byte check, including journals uploaded during recovery.

Native verification captures a bounded digest of record revisions, payloads,
accounting, credentials, driver replies and workspace authority. The transaction
rechecks that cut after byte verification; a concurrent state change refuses the
entire transition. Private identity and keys are checked on both sides of byte
verification. The native profile bounds the view to 100,000 rows and 64 MiB,
and the artifact inventory to 10,000 generations.

One transaction writes a target-signed immutable `recovery_activation` receipt
and changes the gate to `active`. It does not increment the restored epoch.
Campaigns remain paused and old driver identities remain fenced. A drain pause
can now finish when an accepted training stage has left evaluation queued; that
trial, queued stage and funding remain intact. Activation does not accept new
scientific evidence, complete trials, enroll workers or resume campaigns.

After activation, normal API mutation is available under existing authorization.
Old worker sessions remain fenced; resumption requires explicitly qualified
workers and control decisions. Re-inspecting the original import verifies its
signed lifecycle history instead of comparing a legitimately changed workspace
with pristine backup bytes. The original source remains sealed.

## Seal an activated target again

An active target can use `workspace.seal` with a fresh backup ID, its current
controller epoch, and the current gate revision. Revision zero applies only to
a workspace that has never had a gate. A competing seal must win the same
revision check; two callers cannot publish different cuts for the active gate.

Sealing retains immutable `workspace_seal` records for both the previous cut and
the new cut. Older imported seals without that history record are archived as
part of this transaction. Reusing any historical backup ID refuses. Pending
uploads, inconsistent worker upload receipts, deletion and retained reader pins
block sealing through the same byte-operation barrier used by activation.

Export then creates and pins a new source-custody key at the target's current
epoch. Neither the ancestor's election key nor the target's restore-identity key
becomes this new election key. The target can elect one successor, whose inactive
import advances the epoch once. The earlier admissions, activations, settlements,
seal history and worker fences remain in the ledger. Each ancestor stays sealed.
Use restore-specific activation keys and backup-specific seal keys: the command
ledger deliberately retains old idempotency keys across generations.

## Qualification boundary

Native tests cover a real SQLite snapshot, artifact and key corruption, missing
files, hardlinks, scoped-authority refusal and incomplete export publication.
Restore tests cover real process exit before the root marker, during a private
file write and inside the SQLite transaction; concurrent exact replay; unchanged
pending dispatch/budget custody; and rejection of lost acknowledgments or changed
reservations. These qualify inactive import.
Claim tests additionally cover competing independent openers, restart, lost
publication replies, actual process exit after publication, missing/substituted
custody and forged target proofs. Windows CI checks protected-file replacement;
platform-independent unit tests verify the required Windows flags and refusal
path without treating them as native qualification.
Admission tests cover actual process exit within the transaction and after
commit, concurrent replay, rollback of fences and pauses, native-proof mismatch,
target/source-key rejection, preserved UNKNOWN custody, pending driver turns and
both waiting and ready workflow fences.
Recovery evidence tests use a real native CPU attempt and HTTP delivery after
target reopen. They cover duplicate-generation cleanup, once-only operational
settlement without scientific acceptance, unfinished/UNKNOWN quarantine, partial
transfer restart, malformed or altered journals, cleanup interruption, authority
changes between preflight and commit, foreign artifacts and generic-I/O refusal.
Route tests additionally exercise a real running native scope across checkpoint
import and target admission. They verify retained capacity and refused closure
while running, then delivery and settlement after the same native scope exits,
without changing the original pending intent. They also cover signature/source-pin
substitution, missing stores, immutable route IDs, driver-lock contention, CLI
routing and lost target acknowledgments.
Reader tests use actual child-process exit and a live child handshake. They cover
multiple pin actors, exact-set races, atomic rollback, opaque and foreign-host
refusal, and authenticated HTTP replay.
Activation tests cover original closed and not-started execution, exact settlement
and ledger consistency, queued evaluation stages, reserved and consumed confirmation
exposures, open child funding, reader pins, saved turns, byte-verification races,
explicit CLI targeting and real process exit inside and after the activation
transaction. A composed native fixture keeps one real CPU process alive across
seal, export, import, single-target election and routed recovery. Activation refuses
while the original scope runs; after that same scope exits, closure settles once,
activation succeeds, and the target seals and exports under a new custody key.
The original uncertain-delivery intent remains unchanged throughout.
Successor tests cover a second import/election/activation, competing seal CAS,
transaction rollback and explicit campaign resumption. A newly enrolled worker
finishes a queued evaluation and an independent evaluator accepts its receipt;
the original worker remains fenced. These are local native proofs, not a claim
of cross-host disaster recovery, hosted-provider deployment or accelerator parity.
Saved-turn tests cover absent, accepted and rejected target replies, unchanged
execution accounting, preserved actor fences, target-acknowledgment races,
interrupted transaction rollback and exact acknowledgment replay through a
competing database connection.
