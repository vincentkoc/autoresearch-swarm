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

Default local execution uses the controller host's clock. Explicit calibrated
execution is described below. [Remote enrollment](remote-workers.md) uses explicit
operator bundles; the persistent remote driver remains pending. Signatures
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
physical clock qualification and a persistent remote driver remain pending;
these endpoints alone do not qualify execution on another host.

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

## Native deadline clocks

Execution and cleanup use a suspend-inclusive elapsed clock. Linux uses
`CLOCK_BOOTTIME`; its paired awake clock is `CLOCK_MONOTONIC`. Python documents
the distinction in its [clock constants](https://docs.python.org/3.11/library/time.html#time.CLOCK_BOOTTIME).
macOS uses `mach_continuous_time` and `mach_absolute_time` with integer timebase
conversion, following [Apple's native declarations](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/mach/mach_time.h).
Windows uses [QueryPerformanceCounter](https://learn.microsoft.com/en-us/windows/win32/sysinfo/acquiring-high-resolution-time-stamps),
which includes sleep, paired with [QueryUnbiasedInterruptTime](https://learn.microsoft.com/en-us/windows/win32/api/realtimeapiset/nf-realtimeapiset-queryunbiasedinterrupttime),
which excludes it. Missing primitives are an explicit capability failure.

The supervisor freezes its local UTC-to-elapsed conversion once and passes the
absolute cap into native execution. Preparing a held process or delaying release
cannot restart that allowance. The effective deadline is the earlier of the
stage timeout and the absolute cap. An expired cap never becomes a one-millisecond
positive allowance.

The watchdog checks the absolute cap after bounded waits, including after resume
from suspension. Userspace cannot terminate a process while the host is asleep;
it requests termination when execution resumes. This is not a realtime scheduling
guarantee. Native closure remains independently observed.

Clock samples retain continuous, awake, and wall readings plus read uncertainty.
Their continuity check distinguishes suspension, counter reversal, and wall-clock
steps. These are prerequisites for remote calibration. They do not authenticate
controller time or grant remote launch authority. The protocol below supplies
authenticated time evidence. Live supervisor-origin qualification and wiring
through every journal release gate remain separate work.

## Signed authority-time evidence

`SignedClockReply` covers a fresh challenge nonce, the full worker binding,
controller clock epoch, authority timestamp in integer milliseconds, clock
profile, and `ClockPolicy`. Its signature domain is `research.worker-clock`.
`SignedAssignmentTime` uses the separate `research.assignment-clock` domain and
binds that epoch and policy to the exact existing signed assignment. Existing
assignment signatures and their canonical fixtures do not change.

The time-binding verifier also authenticates the original assignment. This is a
static check: it deliberately permits expired observations and uses the declared
issue time only while checking signature and structure. It does not establish
current launch authority.

`ClockSampler` creates a process-scoped origin ID and pins its native counter
profile. Each `begin()` generates a fresh nonce and records the send sample.
Completing that challenge records the receive sample once. Invalid completion
consumes the challenge; retry requires a new nonce. Exact completion replay
returns the original estimate and cannot reset its age. A changed process or
counter profile rejects new sampling. The origin ID is not a process-liveness
capability for a supervisor; that handshake remains required before remote launch.

Let `A` be sampled authority time, `R` the full worker roundtrip, `e` elapsed
worker time since receipt, `g` timestamp/read uncertainty, and `rho` the admitted
relative drift bound. Integer arithmetic computes:

~~~text
lower = A - g + floor(e * (1 - rho))
upper = A + g + ceil((R + e) * (1 + rho))
~~~

The ratio assumption is `1-rho <= controller_elapsed/worker_elapsed <= 1+rho`.
It is not a claim that each clock has that individual accuracy. The default is
an explicit operating assumption, not measured hardware qualification. No
symmetric-network assumption or estimated server-processing subtraction is used.

The estimate rejects unsupported profiles, excessive roundtrip, wide read
samples, stale age, excess interval width, counter reversal, suspension, and wall
steps. Assignment deadlines require matching worker, clock epoch, assignment
digest, and policy. The lower bound must reach issue time; the upper bound must
remain strictly before expiry. Deadlines beyond the admitted horizon are rejected.

The absolute cap divides remaining allowance by the upper rate bound and rounds
down. It includes the admitted maximum read uncertainty for future checks. Launch
is additionally capped by sample-age expiry. Repeated computation cannot reset
the receive anchor, and intersecting refreshed caps can only tighten them.
Intersections require the same assignment, authority epoch, local origin, and
counter profile. A native release consumer must still check a fresh interval,
current authority, cancellation, and its live origin; a cap alone grants nothing.

## Controller time admission

Enrollment freezes `clock_mode`. The default `local-utc-v1` preserves the existing
local execution path. Opt-in `bounded-continuous-v1` requires pinned Ed25519
assignment authority. A qualified enrollment cannot downgrade to local time by
omitting its assignment's timing sidecar. The stock worker supports explicit
calibrated enrollment through the live-origin native protocol described below.
It does not infer calibrated mode from an HTTP connection.

`POST /v1/worker/clock` accepts `ClockChallenge` under `worker.query`. It verifies
the principal, current enrollment/session, mode, and pinned signing key before
returning `SignedClockReply`. This is a query, not a replayable command: workers
use a fresh challenge for each new sampling round. The SDK exposes `worker_clock`
in both clients; MCP exposes `research_worker_clock`.

One process-owned clock serves every worker under a database, workspace,
controller/control epoch, and signing key. It anchors UTC once, advances that
anchor with the native continuous counter, and freezes its policy for that
process. Runtime reopening retains the same instance and invalidation latch.
It checks both the original anchor and the previous sample for discontinuities.
Suspend, wall steps, counter reversal, profile changes, and read failures poison
that epoch. An inherited clock refuses use after fork. The initial continuity
tolerance is conservative: accumulated wall-clock drift can require an explicit
controller restart. There is no automatic re-anchor or policy-based reset.

A timed claim derives issue, launch-expiry, and hard-deadline timestamps from one
validated observation. It writes the original signed assignment, separate signed
time binding, budget reservation, and dispatch events in one transaction. A
rejected claim cannot leave a reservation. Exact accepted-command replay returns
the original acknowledgment even if the clock later becomes invalid.

Timed input acquisition verifies the sidecar against the original signed grant
and compares expiry in its issuing clock epoch. Authorization is repeated inside
the artifact-pin transaction. Clock failure or a missing/mismatched epoch denies
new input pins. A stream admitted before failure may finish; output uploads and
previously accepted command replay remain available for evidence recovery.

A fresh controller process owns a fresh clock epoch. It does not attach a new
time signature to an old grant or silently extend that grant. Persistent HTTP or
MCP servers and retained SDK runtimes can support sampling and admission in one
process. Independent direct-local CLI invocations cannot carry a calibration
across processes; a load-balanced controller clock is not qualified. Old grants
and evidence remain stored. Clock expiry alone never proves native closure,
frees a device, refunds an attempt, or permits a retry.

Tests cover scoped HTTP/async queries, same-process runtime reopening, native fork
rejection, independent-process epoch mismatch, immutable replay, input-pin races,
and output recovery. Explicit remote enrollment is available; the persistent
remote driver remains separate from this execution protocol.

## Durable worker timing

Timed journal admission retains the exact assignment-time signature separately
from the original signed assignment. Its admission digest includes the clock mode
and sidecar. Legacy local entries retain their original HMAC or signed-assignment
input digest; missing optional clock fields read as local mode.

`TimingEvidence` records the authenticated reply, original send/receive samples,
qualification sample, origin/profile, and conservative launch/hard caps. Native
nanosecond counters use canonical unsigned 64-bit decimal strings. They must not
use JSON numbers: nanosecond uptime exceeds the safe JCS integer range after
about 104 days. Parsing rejects signs, leading zeroes, exponent notation, and
overflow. Arithmetic remains integer-only.

The journal binds this evidence after required preparation, before a supervisor
claims the attempt. Exact attachment replay returns the existing entry. A refresh
must retain the same grant, clock epoch, origin, and native profile and may only
shorten caps. Another process cannot transplant its counters onto an attached
attempt. Historical verification rechecks the signatures and interval arithmetic
at the recorded qualification time; it does not prove that origin is alive now.

The legacy journal launch/release methods and a supervisor without its private
origin channel refuse timed entries. Raw UTC cannot expire a timed entry through the local settlement path;
explicit cancellation can settle an entry that is still durably unreleased.
Publication compares mode and timing sidecar with controller admission. When
timing evidence is present, it rechecks that evidence without demanding a current
clock sample. A timed not-started observation can therefore publish after clock
failure. Missing or altered timing authority cannot downgrade to local mode.

This durable layer supplies the evidence consumed by the live-origin gates below.

## Private origin transport

`OriginServer` and `SupervisorChannel` provide a bounded, authenticated local
transport for the native admission gates. They do not themselves commit
journal transitions or authorize a workload. A successful transport exchange must
still be paired with a live sampler and an atomic journal decision.

The server binds an ephemeral `127.0.0.1` socket with no address reuse. A random
256-bit capability and channel ID reach the intended supervisor through a private
stdin pipe. The launcher preloads at most 512 bytes and closes the writer before
spawn. The capability is absent from argv, environment, journals, and object repr.
The [subprocess stdin contract](https://docs.python.org/3.11/library/subprocess.html#popen-constructor)
supplies the bootstrap without a separate credential file. All channel descriptors
are marked non-inheritable.

Each frame has a bounded length and a direction-specific HMAC over its protocol
domain, channel ID, and canonical payload. Requests bind worker/session, attempt,
assignment, timing record, journal, origin, supervisor, phase, sequence, and fresh
nonce. There are exactly two phases: claim and release. A repeated or changed
phase cannot restart the sequence. Replies echo the exact request and cannot
extend the original caps or a stricter cap returned by the first exchange.

Partial reads and writes use [socket timeouts](https://docs.python.org/3.11/library/socket.html#socket.socket.settimeout)
of at most 50 milliseconds, checking one absolute suspend-inclusive deadline after
each operation. A fragment, stalled peer, reconnect, or callback delay cannot reset
that deadline. Up to eight unauthenticated peers receive at most 500 milliseconds
each, within the same total budget. Oversized frames and excessive JSON nesting
are rejected before authorization. Closing the server is synchronized with
accepted-connection registration.

An exchange proves that the channel owner responded at that point. It does not
prove uninterrupted liveness afterward. A native consumer must immediately
recheck its frozen launch cap before release and retain the separate hard-deadline
watchdog. Lost replies remain ambiguous: channel failure does not prove process
closure or make a reservation refundable.

Tests include a separate Python supervisor process, private bootstrap, partial
frames, malformed/unauthenticated peers, close/accept races, reused nonces, changed
reply bindings, cap extension, simulated suspend, and origin loss after claim.
This transport is implemented. Its native integration is described below;
platform qualification remains a separate claim.

## Calibrated native execution

After immutable preparation and machine-capacity reservation, `LocalWorker`
requests a fresh signed time sample, stores `TimingEvidence`, and retains its
actual `ClockSampler` in a `LiveTimingOrigin`. The sampler serializes readings and
permanently invalidates itself after profile changes, read failures, suspend,
counter reversal, or a wall step. Normal-looking later readings do not reset it.
An already attached grant can only reuse that exact live origin. A new worker
process cannot reconstruct launch authority from historical evidence.

The dedicated supervisor receives a one-use private bootstrap and requests a
claim. The origin checks the exact assignment and timing evidence inside the
journal transaction, after acquiring SQLite's write lock. Only a fresh,
uncancelled `RECEIVED` entry becomes `LAUNCHING`. The supervisor checks the
acknowledged owner, journal revision, and frozen caps before creating a native
child. A lost claim reply can be settled by that supervisor only while its own
control path has not attempted native creation. This settlement prevents a late
claim response from reopening the entry.

The child remains held while its native identity is journaled. A second request
checks the live origin, cancellation, ownership, and deadlines inside the release
transaction. `RELEASING` is durable release intent. Immediately before release,
the supervisor rechecks the absolute launch cap and cancellation. A reply delayed
past the launch cap cannot create a fresh timeout. The independent hard-deadline
watchdog starts before the held callback and remains effective after origin
death. Deadline enforcement depends on OS scheduling; it is not a real-time or
hostile-code isolation guarantee.

Startup failures close the private listener. An entry still `RECEIVED` can be
cancelled and settled as `NOT_STARTED`; a claimed entry needs its owning
supervisor's native absence or closure evidence. Otherwise it remains unknown
and its machine capacity is quarantined. The origin channel closes before result
publication, and sanitized channel failure codes remain in the journal. Losing a
release reply closes the held native scope without inferring that it ran.

Local verification includes real scoped HTTP CPU execution, immutable replay,
independent-connection lock delays at both gates, lost claim/release replies,
startup failure before and after claim, cancellation while held, and actual
origin-process termination before claim, while held, and after release. After
release, the original deadline still terminates the owned workload. These tests
qualify the tested native host and protocol; cross-host clock-rate assumptions,
physical suspend, hosted services, and accelerator behavior need separate proof.
