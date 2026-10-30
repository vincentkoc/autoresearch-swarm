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

Local execution uses the controller host's clock. Restricted remote enrollment,
polling, clock qualification, and remote execution remain pending. Signatures
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

The same views are available through HTTP, Python/async SDKs, and MCP. Restricted
artifact transfer and a remote worker driver remain pending; these query endpoints
alone do not make the worker runnable on another host.
