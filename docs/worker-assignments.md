# Signed worker assignments

The controller signs the complete execution assignment. A worker receives a pinned
public key scoped to its workspace, controller, epoch, worker, and session. The
worker receives no private signing key.

`AssignmentPayload` includes the sealed experiment, dispatch and reservation
identities, source archive reference and tree digest, exact named inputs, admitted
engine profile, evaluator request, network policy, offline policy, and issue time.
The signature covers all of these fields. A valid signature for a different worker
session does not grant execution authority.

`AssignmentSigner` uses Ed25519 through the
[Cryptography Ed25519 API](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/ed25519/).
Its input is RFC 8785 canonical JSON with the `research.worker-assignment` domain,
algorithm `ed25519-jcs-v1`, pinned key ID, and payload. The key ID is a separate
domain digest of the public key and algorithm. Public keys use 32-byte lowercase
hex; signatures use 64-byte lowercase hex. `tests/test_assignment_signatures.py`
provides deterministic synthetic known-answer fixtures.

Verification checks the pinned key, signature, enrollment, sealed source/spec
consistency, exact stage inputs, capabilities, issue time, and launch deadline.
The caller must supply qualified controller time. A caller cannot use this
primitive to establish clock agreement with a remote controller.

`allow_expired_observation` permits reconstruction of an expired authenticated
assignment. It grants no new launch authority. The journal and native release gate
must still enforce the original deadlines and prevent repeat execution.

Local runtimes persist a separate private `assignment.key` and pin its public key
ID in `identity.json`. Restart does not replace a missing or changed key. An older
runtime without signed enrollments creates this key once. Existing HMAC
enrollments keep their original authority; new enrollments use signatures.

Admission signs and stores the assignment in the same transaction as its attempt,
budget reservation, and dispatch event. A changed or absent signing key rolls back
admission. Command replay returns the original signature and grant.

The native worker verifies the signature and all presented assignment fields
before journal admission. The journal persists the full signed assignment and
includes it in duplicate-delivery identity. Preparation must match its input
digests and execution profile. Restart checks the same assignment again; result
publication must preserve it. A signed enrollment cannot fall back to HMAC.

Local execution uses the controller host's clock. Remote enrollment provisioning,
clock qualification, and remote execution remain pending. Signatures
alone do not qualify a remote worker or prove native process closure.

## Scoped discovery and recovery

`worker.enroll` accepts an optional nonempty `project_ids` tuple. It verifies these
projects and requires signing authority. Admission checks this restriction before
reserving budget, even when the caller submits a job ID without obtaining an
offer. `None` preserves the unrestricted legacy/local behavior; an explicit empty
list is invalid. Enrollment is immutable, so changing this scope requires an
explicit replacement workflow.

The `worker.query` scope permits two bounded operations without workspace `read`:

- `worker_list(WorkerQuery)` lists eligible `offers` or the worker's `pending`
  assignments. It checks actor, workspace, controller, epoch, and session.
- `worker_get(WorkerRead)` returns that enrollment or an attempt/trial assigned to
  that exact authority. It cannot read another worker's assignments.

Pages scan at most `limit` records and return a continuation cursor when more
remain. A filtered page can be empty and still have a cursor. Follow the cursor
until it is absent. The insertion boundary is fixed, while eligibility and record
state are current at each page read. Begin a fresh scan to discover later work.
The cursor binds the actor, enrollment, and query kind.
Worker/session and pending-state indexes keep empty polls and failed ownership
lookups from scanning completed attempt history. A SQLite operation-count test
covers 2,000 completed attempts.

Offers respect project scope, campaign admission, engine capabilities, and
evaluation authorization. They do not reserve capacity or budget. Claim the job
with the offered revision and preserve the command key through uncertain delivery.
The atomic claim can reject an offer that became unavailable.

The same views are available through HTTP, Python/async SDKs, and MCP. Independent
remote provisioning, clock qualification, and a persistent remote driver remain
pending; these endpoints alone do not qualify execution on another host.

## Attempt-bound inputs

`worker.inputs` permits metadata and content access to the exact source archive
and named input references in an admitted signed attempt. The request identifies
the worker, session, attempt, and artifact. Project membership or knowledge of an
artifact ID does not grant access to other project data.

Input acquisition requires a pending attempt and an unexpired launch grant.
This restriction applies to acquiring bytes; closure evidence publication has a
separate authority contract and must remain possible after the deadline.

The controller rechecks assignment authority and the exact reference in the same
transaction that creates a reader pin. The stream uses the pinned storage
generation. Each new HTTP request resolves its credential and enrollment again.
An already admitted stream can finish after revocation; its exact pin is released
when the stream closes or fails, without requiring new enrollment authority.

Use `worker_input(WorkerArtifactRead)` for metadata and
`worker_download(request, destination)` for verified bytes. The destination must
not exist. Both SDK transports enforce declared size and digest; incomplete new
files are removed. MCP exposes metadata through `research_worker_input`.

## Attempt-bound outputs and observations

`worker.outputs` permits uploads for one signed attempt owned by the current
worker session. Each upload declares a role: `journal`, `report`, `stdout`,
`stderr`, or a named `output` from the assigned stage. Undeclared outputs are
rejected before storage is reserved. A launch deadline does not prevent uploading
closure evidence. Revoked enrollment cannot create or continue new transfers.

Use `worker_upload_file(WorkerOutputIntent, path, idempotency_key=...)` on either
SDK transport. It shares the resumable file transfer algorithm with ordinary
artifact uploads. `worker_output(WorkerUploadTarget)` returns transfer progress,
the canonical artifact reference when finalized, and the current cleanup state.
HTTP exposes bounded byte-range parts; MCP exposes metadata and the typed begin,
finalize, and cleanup commands. Binary content stays on the HTTP/SDK path.

Finalization stores an immutable receipt binding the full enrollment, attempt,
role, optional output name, and canonical artifact reference. Content deduplication
does not prove authorship: the artifact's first publisher may be another actor,
and the canonical media type may differ from the upload request. The receipt
preserves this worker's transfer proof separately. Empty stdout and stderr can
share bytes while keeping distinct role receipts.

Project-scoped publication requires these receipts before opening evidence. The
controller rechecks ownership and provenance when pinning each storage generation,
then again in the result transaction. That transaction retains attempt evidence
before releasing the corresponding unpublished-upload pins. Unknown execution
also preserves evidence and its uncertain resource accounting; a later closure
may have a new journal receipt. Terminal acceptance transfers all finalized
uploads, including duplicate and superseded observations, to the attempt's
evidence lifecycle. It preserves their history without leaving orphan upload pins.
Uploading evidence does not itself accept a result
or create an authenticated evaluator receipt.

Each upload has a lifecycle guard shared by append, finalize, and abort. State is
rechecked under the guard; byte I/O holds no database transaction. Immutable
ownership is checked before creating a guard, so rejected IDs create no files.
An abort cannot
release quota and then have a delayed append recreate staging. Finalization keeps
the guard through verified catalog publication.

The accepted finalize acknowledgment survives a duplicate-generation cleanup
failure. The duplicate remains charged in `cleanup` state and can be retried with
`worker.cleanup_upload` using its own stable command key. An operator can use the
ordinary artifact abort lifecycle to remove a pending duplicate after worker
revocation. This removes only that upload generation; canonical retained evidence
remains available. Replaying an accepted finalize or result command returns its
original acknowledgment even after enrollment revocation, provided the caller
still has the required command scope. Credential revocation still denies access.

The native CPU worker uses scoped queries, input downloads, uploads, and result
publication when enrollment names explicit projects. A real loopback HTTP test
runs training and an authorized evaluation, then completes its receipt under a
different actor. This qualifies the local HTTP execution path, not remote clock
agreement, persistent remote enrollment, accelerator execution, or a physical swarm.
