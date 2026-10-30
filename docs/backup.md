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

There is no activation command in this slice. Original worker processes may
still be running and remain charged. Live worker reconciliation is required
before activation can exist. A source claim elects one target but does not open it.
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

The original worker installation is not automatically redirected. A separate
explicit recovery route must bind its original private installation to the
elected destination before the recovery driver can deliver there. That route,
crashed recovery-reader reconciliation, saved-turn reconciliation and final
activation remain required work. The target stays `recovering`; ordinary
research mutation, new dispatch and scientific acceptance remain unavailable.
Neither a source claim nor an admission receipt proves native process closure.

## Qualification boundary

Native tests cover a real SQLite snapshot, artifact and key corruption, missing
files, hardlinks, scoped-authority refusal and incomplete export publication.
Restore tests cover real process exit before the root marker, during a private
file write and inside the SQLite transaction; concurrent exact replay; unchanged
pending dispatch/budget custody; and rejection of lost acknowledgments or changed
reservations. These qualify inactive import. A running-process reconciliation and
single-target activation demonstration remains required for release recovery.
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
They do not qualify automatic worker redirection or final target activation.
