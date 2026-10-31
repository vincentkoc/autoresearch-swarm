# Hosted authority with native clock witnesses

Status: staged implementation and bounded local runtime qualification. No
production hosted authority is enabled. The existing
[hybrid profile](cloudflare.md) remains the supported Cloudflare path.
The original M5 requirement for authoritative Durable Object state and hosted
recovery remains open.

The witness wire records, domain-separated signatures, pinned source checks,
one-use response verification and epoch derivation are implemented. A frozen
public fixture is independently checked in Python and JavaScript. A separate
native source primitive observes the existing qualified clock implementation
and preserves its fault latch; independent child processes establish distinct
source origins locally. Internal provider generations, explicit requalification
and persisted retirement are locally verified. A one-use synchronous operation
context is implemented. Wire validity does not qualify a deployment. Local
workerd qualifies the awaited ordering bridge, bounded worker/artifact routing
and the actual native source lifecycle. Ordinary research admission has separate
tests and capability limits below. The complete hosted execution lifecycle and
authenticated production deployment remain open. The private HTTP source
transport does not establish Container routing.

## Decision and boundary

Keep research state, command deduplication, budgets, leases, events and outbox
effects in one SQLite Durable Object authority. Use a small native service to
observe time. On Cloudflare, that service can run in a Container. It must not
claim jobs, accept results, choose candidates or hold the research database.
Native compute workers remain external.

This separates an authoritative state decision from its clock implementation.
It does not move the research controller into a Container or make a native
controller a second state authority. Local and Temporal installations retain
their existing clock and storage implementations.

Cloudflare documents that deployed Workers timers advance after I/O and remain
frozen during computation. A serialized turn can establish an ordering point,
but timer monotonicity alone does not establish a bounded elapsed-rate relation
to a native worker. A forward clock discontinuity can make an already issued
native deadline too late. A clamped plateau can invalidate the lower bound of a
worker's estimated authority-time interval. Increasing a timeout does not fix
either case. [Workers timers](https://developers.cloudflare.com/workers/runtime-apis/performance/)

Containers support applications that need a Linux environment. That provides a
deployment candidate for the existing native clock observation mechanism; it
does not itself qualify a particular container image, host clock or deployment.
[Cloudflare Containers](https://developers.cloudflare.com/containers/)

## Acceptance linearization

The operation must hold exclusive authority ordering before requesting its
clock witness and until its state transaction completes. The witness observation
is the declared linearization point for time-dependent admission in that
operation. It is inside the caller's operation interval, after the complete
request and required preflight evidence are available.

```text
authenticate and bound the complete request
resolve and verify required immutable inputs
enter exclusive authority turn
  revalidate scope, ownership and current state prerequisites
  request one nonce-bound native clock witness
    native clock observes T
  verify witness identity, scope, epoch and request binding
  execute the synchronous domain transaction using T
  commit state, command outcome, events and outbox together
leave exclusive authority turn
release committed response and asynchronous effects
```

State must not change between the witness observation and the transaction's
decision. No other domain operation, credential revocation, ownership transfer,
alarm handler or recovery path may bypass the same ordering boundary. Any
preflight performed outside that boundary must be revalidated against the state
used by the transaction. Mutable reads obtained before entering the turn are not
an authority snapshot.

The SQL commit can finish after the acceptance deadline if this operation
linearized before it. This does not admit a result whose complete operation
arrived after expiry: its fresh witness will be at or after expiry and admission
will fail. An overlapping expiry or revocation operation orders before or after
the result; it cannot interleave between its witness and decision. This is an
explicit profile contract, not an assertion that a cached timestamp is fresh at
each subsequent Python instruction.

The existing native path also checks acceptance before later validation and
commit. Its implementation and regression expectations remain unchanged. The
hosted profile must prove its own ordering boundary rather than reuse a
synthetic provider that returns a fixed timestamp.

`blockConcurrencyWhile` can hold this ordering across the witness request.
Cloudflare documents that it blocks other delivered events and resets the
object when its callback exceeds the runtime's timeout. Synchronous SQLite
transactions provide the commit/rollback boundary. Neither mechanism permits
external side effects before durable acceptance. Runtime reset or reply loss
requires command replay; it is not evidence that the transaction failed.
[Durable Object state](https://developers.cloudflare.com/durable-objects/api/state/),
[SQLite transactions](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#transactionsync)

## Witness contract

The wire protocol must be versioned and domain-separated from assignments and
worker clock replies. Its exact schemas and canonical fixtures are a required
implementation deliverable. The minimum binding is:

| Field | Purpose |
|---|---|
| Witness protocol version | Prevent reinterpretation across protocol changes. |
| State authority identity | Identify the Durable Object namespace/object and logical authority activation. |
| Authority scope | Bind workspace, controller, control epoch and assignment-signing key. |
| Operation identity and request digest | Bind one prepared command/query and its authenticated caller without transmitting a credential. |
| Fresh nonce | Prevent reuse of an earlier observation for a new operation. |
| Source identity and verification key | Identify the admitted native clock service. |
| Source clock epoch | Identify one continuous, non-reanchored native clock origin. |
| Native clock profile and policy digest | Bind the admitted elapsed-rate and uncertainty assumptions. |
| Authority timestamp | Record the native observation, with exact integer encoding. |
| Authentication | Protect the complete binding and timestamp. |

The source must observe after it receives the exact request nonce. It must use a
qualified native clock, preserve its invalidation latch and refuse an unknown
scope or changed policy. It may not return a cached observation, silently choose
a new epoch after a continuity fault, or accept arbitrary caller-selected clock
profiles. Its scope registry, request body and response size must be bounded.

The Durable Object verifies the whole response against its one outstanding
request and pinned source configuration. A witness is consumed by one operation
context. Multiple time-dependent checks within that synchronous operation use
the same declared point. A different scope, nested replacement, use after the
turn, changed operation digest or reused nonce fails closed.

Only after signature and exact-request verification can a source-configuration
mismatch produce `AuthenticatedSourceMismatch` evidence through
`WitnessSourceChanged`. Its serialized error contains no proof or source details.
The internal evidence can support a later durable fence after checking the
original provider generation and revision; it cannot supply time to an operation.
A generic `STALE_FENCE`, wrong nonce, wrong request binding or bad signature is
not retirement evidence. An authenticated changed epoch with otherwise matching
source configuration remains a `VerifiedTimeWitness` for the registry to assess.

If the witness reply is uncertain before the domain transaction begins, abandon
that prepared context. A new attempt obtains a new nonce and observation. Do not
persist a timestamp and use it to admit the command on a later retry. Once the
domain command has committed, its ordinary idempotency record supplies the
original outcome; replay does not repeat the state decision or renew authority.

The service must be reachable only through its configured private route and
must authenticate its consumer. A public caller cannot provide a witness,
select a clock origin, replace a source key or invoke an unguarded synchronous
controller method. Production key provisioning and rotation need their own
qualified lifecycle. Public fixtures use public test keys only.

### Native source scope

`NativeWitnessSource` pins a `WitnessAuthorityScope`: state authority, logical
activation, workspace, controller, control epoch and assignment key. It validates
that binding before sampling its one `AuthorityClock`. The source descriptor
pins the native profile, policy, source identity and signing key. An incompatible
clock observation poisons that origin; another request or provider generation
cannot reset it.

The Durable Object owns `provider_generation`. The source signs the requested
generation, operation identity, digest and nonce as evidence; it does not admit
them as research authority. Requalifying a generation must not require restarting
the source and thereby producing yet another clock epoch. The consumer checks
the generation against its durable state under exclusive ordering. This also
means a fresh, valid witness for an old generation remains inadmissible there.

The source primitive is an in-process trusted interface. Its eventual network
transport must authenticate a configured consumer before invoking it; having a
matching request scope is not network authentication. Current tests cover native
sampling and process-origin separation plus synthetic continuity failures. They
do not establish Container routing or hosted clock qualification.

### Private source endpoint

`create_time_witness_app` exposes `POST /v1/time-witness` around one native source
and one explicitly provisioned consumer digest. It does not expose enrollment,
source configuration, research commands or an OpenAPI document. The application
factory starts no listener and deploys no service.

The operator provisions a high-entropy bearer token privately to the consumer
and its domain-separated digest to the source. The endpoint rejects missing,
duplicate, malformed or incorrect authorization before consuming the body.
It accepts one JSON content type and uncompressed input, limits the body to
16 KiB and defaults to a one-second body-read timeout. Duplicate JSON keys and
invalid schemas are refused before observing time. Credential rotation must
follow the deployment's source-origin and requalification lifecycle.

Successful replies are signed `SignedTimeWitness` records with `Cache-Control:
no-store`. Errors contain only the stable code and retryability flag. A scope
refusal is HTTP 403, an invalid clock origin is 409, and an unavailable source
is 503. A body timeout is 408. Responses do not include source exception text,
request bodies or credentials. The endpoint needs a private transport or an
explicitly configured authenticated TLS route; the token is not transport
encryption.

The local suite includes a real HTTP/native observation and bounded ASGI tests
for authentication ordering, partial input, disconnects and sanitized failures.
Deploying this application, provisioning secrets and qualifying the hosted route
remain separate work.

## Epochs, eviction and source replacement

Distinguish three identities:

1. **Logical authority activation:** durable ownership/configuration of the
   research state, including its control epoch.
2. **Native source epoch:** one continuous clock origin in the clock service.
3. **Operation context:** a volatile, one-use capability for the current exclusive
   turn. It never survives isolate loss.

A routine Durable Object eviction need not change the worker-facing clock epoch
when the durable authority activation and admitted native source epoch remain
the same. Reopening state does not revive an operation context or its witness.
Restoration, ownership transfer or source replacement must follow the declared
fencing protocol.

Derive the worker-facing epoch from a canonical, domain-separated record of the
state authority identity, durable provider generation, authority scope, source
identity/key, source epoch, native profile and policy digest. A UUID-shaped
encoding of a cryptographic digest can fit the existing identifier schema.
Golden fixtures must freeze the algorithm. Do not include the operation nonce
or worker session: a clock challenge and its assignment must share the same
time domain while their individual bindings remain independently authenticated.

The implemented v1 derivation uses `record_digest` with domain
`research.authority-time-epoch.v1`. Its payload contains the full versioned
`WitnessScope`, source ID/key ID/epoch, native profile and policy digest. Take
the first 16 SHA-256 bytes, set the UUID version nibble to `8` and variant bits
to `10`, then encode the lowercase UUID. Source keys use the separate
`research.time-witness-public-key` digest domain. The frozen fixture is
[`time-witness-v1.json`](../tests/conformance/time-witness-v1.json).

A deterministic hash alone is insufficient. Consider source epochs
`E1 -> E2 -> E1`: the last response must not resurrect an old grant. Persist the
current source binding and retired epochs with provider generation changes.
Reject retired-source responses. Never erase retirement evidence to make a
replacement fit. The initial implementation should refuse a changed source
epoch until explicit requalification advances the durable provider generation;
automatic replacement is a separate protocol to qualify.

An authenticated source change must durably mark the provider unqualified and
retire its previous source binding even when the triggering command rejects.
Use the command kernel's durable rejection/fencing boundary; rolling back the
invalidation with the rejected command would restore stale authority. A transport
timeout alone refuses that operation without retiring the source: absence of a
reply is not authenticated evidence of a different clock origin.
An unsigned HTTP error code alone is not epoch-change evidence either. It
refuses the current operation; durable retirement needs a verified source change
or an explicit authority action.
Clock challenges and other read-only entry points can discover the same fault.
They must persist an internal fencing transaction before returning refusal;
command rejection hooks alone do not cover those paths.

An ordinary isolate restart with the same admitted source needs no such
requalification. A native source restart is different: the service has lost its
continuous origin. Old attempts retain their original hard caps and resource
custody. Their late evidence follows the existing quarantine/reconciliation
path. A new clock origin cannot extend an old cap or prove old execution closed.

Backup and restore must preserve the active/retired bindings and produce a new
logical authority activation before accepting new commands. A restored copy
must not reproduce an old active authority merely because it copied its signing
key and clock configuration. Quiescent ownership transfer still fences the old
driver and state authority.

### Internal lifecycle state

`TimeProviders` implements the transactional source lifecycle against the shared
SQL port. It does not enable a hosted clock, acquire witnesses, expose management
commands or establish native rate qualification. Its `qualified` state means
that an explicit lifecycle admission recorded one source origin; the runtime
must separately enforce its complete hosted profile.

Each workspace stores one `time_provider` projection per logical activation UUID.
Loading it compares the complete authority scope, including the controller epoch,
assignment key and state authority. Registration starts unqualified. Explicit
requalification checks a fresh authenticated response against the previous
projection revision and commits a new generation UUID and immutable generation
record. That management response cannot supply time under the new generation.
The next operation needs its own witness.

Prepared operations bind the physical store identity, authenticated actor,
invocation ID, idempotency key, operation name, expected revision, provider
revision and normalized payload through `research.time-witness-operation.v1`.
Every state boundary receives the actual executing operation name and payload
and recomputes that digest. Existing command digest preimages remain unchanged.
Internal proof objects are not transport inputs.

The retirement key is the digest of source ID, source key ID and native epoch in
the `research.time-source-origin.v1` domain. It excludes activation, provider
generation, profile and policy. Retirement is workspace-wide. If several
activation projections name the same origin, they reuse the first immutable
retirement record and each obtains its own fence. A delayed proof cannot fence
a newer projection revision; replaying an already committed fence adds no event.

`check()` raises an internal `ProviderChanged` only for authenticated source
changes or an already retired origin. Command handlers pass that proof to
`fence()` in their rejection hook, after rejected handler effects roll back.
For a query, the internal write callback commits the fence and returns the
refusal; the caller raises it after commit. Register, requalify and fence also
check the workspace gate, so direct invocation cannot mutate sealed/restoring
state. The integration must preserve these semantics under the exclusive turn.

Local tests cover independent-connection qualification races, changed operations,
E1→E2→E1 across reopening, shared origins, stale proofs, immutable evidence,
rejection-hook rollback, query fencing and inactive workspace refusal. Native
workers and the existing local controller continue to use their current clock.

## One-use operation context

`WitnessAuthorityTimeProvider.prepare()` owns a fresh pending request and an
exclusive local guard until the caller leaves the turn. The hosting adapter must
also hold its Durable Object ordering gate from before the witness request
through the command commit. The Python guard does not establish that platform
property or stop code paths that bypass the adapter.

The turn exposes the exact request, accepts one response through `complete()`,
and runs one synchronous callback through `run()`. It checks the current provider
projection and actual transaction operation before installing an `AuthorityTime`.
Every observation in that callback returns the same declared linearization
instant. A changed source retains internal evidence for the transaction's
rejection hook; an invalid signature or uncertain transport does not.

Observation requires the exact authority scope, process, thread, async task and
current command execution. Each synchronous `Database.execute()` call has a
thread-local opaque marker covering its handler and rejection hooks. A nested
command cannot borrow the outer observation, including by restoring a copied
Python context. The marker grants no authority and is restored in `finally`.
Query callbacks can operate without a command marker, while nested commands still
receive a different marker.

`WorkerControl.clock()` has its own query operation boundary, so a nested clock
query cannot sign another nonce with the outer timestamp. Offer ranking and
input metadata use the same boundary. Hosted routing must prepare each query
independently after its time-free authorization preflight. Artifact pin commands
remain separate integration work.

Completion or callback failure consumes the turn. Awaitables and generators are
refused; no callback may suspend or perform external I/O. After the callback,
copied contexts and retained turn references cannot reactivate its timestamp.
The source verifier accepts the configured synchronous cryptography provider,
including the adapter's preloaded implementation.

The emitted profile is `hosted-native-witness-v1`, distinct from every native
profile. Current native workers reject it. Enabling it requires the complete
exclusive-ordering adapter, source lifecycle and deployment qualification; it
must not be disguised as a native profile to bypass that boundary.

### Command transaction boundary

Adapter preflight can call `validate_command_input()` to reuse the command
catalog's scopes and request schemas without executing the command. This is
validation, not a universal canonical-payload encoder: legacy local worker
enrollment omits its default clock mode from the kernel payload. A hosted
operation allowlist must audit its actual kernel encoding before binding replay
or witnesses, and require the qualified worker mode where applicable.

`CredentialStore.resolve(allow_expiring=False)` provides a conservative profile
for runtimes without qualified UTC expiry. It checks the current credential and
revocation state and refuses every expiring credential before consulting time.
Native resolution retains its existing default expiry behavior. The hosted
router must explicitly select this stricter mode inside its exclusive gate;
this helper does not enable a hosted route or claim expiring-token support.

Ordinary hosted construction must use `DurableObjectStore.open_existing()` and
`CredentialStore.open_existing()`. These paths issue only metadata reads: they
require the current version, service tables, columns, primary keys and required
indexes, plus credential token uniqueness and its workspace foreign key. They
refuse missing or incompatible state without creating tables, migrating versions,
changing foreign-key mode, or installing credentials. Explicit provisioning owns
the initializing constructors. Structural admission does not verify arbitrary
database tampering, credential activity, row integrity, or the separate immutable
authority configuration; the router must still check its configuration and
resolve current credentials for every request.

`adapters/cloudflare/research_router.py` supplies the initial shared-service
integration, `ExistingResearchRouter`. Opening and every operation use the same
exclusive gate. An explicit provisioner must already have persisted the immutable
configuration row, credentials, workspace and provider lifecycle. The configuration
binds physical store location, logical activation, workspace/controller epoch,
assignment public key, dispatch-key digest, witness source/policy and initial
credential-manifest digest. Reopening never reinstalls or requalifies them.

The profile admits calibrated `worker.enroll`, `worker.set_admission` and
`worker.cache_advertise` through the existing command catalog and `ResearchService`.
Current authentication and scopes precede exact acknowledgment replay; replay
precedes time-source access. Fresh commands repeat ownership checks in the shared
transaction after witness acquisition. `worker.clock` and offer-list queries have
their own witnessed scope. Worker record, pending, historical-pending and control
reads remain time-free. The adapter uses an unavailable byte port and opaque
reader ownership; it exposes no artifact, execution or restore route.

Native tests cover this wiring against real SQLite and synchronous native crypto,
including source fencing, canceled acquisition, credential revocation and restart
replay. Their async gate is a test fixture, not evidence of Durable Object event
ordering. The actual router then passed local workerd qualification at
`d548332f5cac0e2f5ca9fe96cbf3fe06d3bfb078` in
[run 41192503736](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41192503736)
on 2026-10-31: all three runtime tests passed in 69.88 seconds. The router probe
holds an external witness, observes a separately submitted read and verifies
committed enrollment before that read succeeds. A separate-object bypass removes
both router gates and must expose the missing worker before enrollment commits.
It also checks signed queries, timeout retry, source fencing, exact replay after
process restart, current credentials and absence of native SQLite/crypto/PID
imports. Timestamps and credentials remain synthetic; production provisioning,
authenticated transport, artifact custody and hosted worker admission remain
disabled. This evidence does not qualify the runtime's 30-second reset path.

The next artifact layer begins with `storage/async_r2.py`: bounded, awaited
generation operations without native staging directories or filesystem locks.
Its initial profile accepts complete bodies up to 1 MiB and shares the existing
native R2 key/metadata layout. Conditional payload publication requires complete
digest/size/metadata readback. Retirement writes and verifies a permanent empty
object at the same generation key, fencing delayed conditional payload writes.
Neither absence nor a failed response proves byte-operation closure. These byte
operations grant no catalog, quota, reader or scientific authority: the controller
must reserve occupancy before I/O and independently validate its commit afterward.
Unit tests use an in-memory transport. The actual Python/JavaScript R2 binding
passed local workerd qualification at `39c39c1` in
[run 41193599166](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41193599166).
That suite checks binary and 1 MiB bodies, corruption refusal, same-generation
recovery after a discarded response and retirement persistence across process
restart. Its late-write probe delays invocation until after retirement; it does
not prove completion of a previously issued request after timeout. The runtime
boundary converts the Python binding with the public `workers.python_to_rpc`
helper before JavaScript calls. Authoritative upload routing needs its own
runtime qualification.
The binding adapter must preserve the documented
[R2 conditional-write and read-after-write semantics](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/).

`worker-artifacts-witness-v1` is a separate configuration profile. It preserves
the worker-only configuration encoding and additionally pins R2 workspace,
bucket identity, prefix, object bound and logical payload quota. A caller must
supply the matching bounded object port; reopening cannot change those pins.
The profile currently exposes:

| Operation | Behavior |
| --- | --- |
| `artifact.begin_upload` | Reserve upload quota through the shared transaction. |
| `upload_part` | Verify one complete body at offset zero, up to the configured 1 MiB ceiling. |
| `artifact.upload_status` | Read transfer progress; absence differs from corruption, retirement or uncertainty. |
| `artifact.finalize_upload` | Verify bytes, obtain a fresh witness and publish through the shared catalog transaction. Empty files need no part call. |
| `artifact.abort_upload` | Record abort intent; keep occupancy until cleanup. |
| `artifact.cleanup_upload` | Confirm a permanent retirement fence, then obtain a fresh witness and finish the lifecycle. |

Commands require current `artifact.upload` and `read` scopes before replay.
Ownership permits the existing `artifact.admin` override. Byte access also checks
workspace admission and dedicated-driver authority. All operations share the DO
gate; upload and canonical artifact snapshots are checked after awaited I/O and
again in the commit transaction. No SQL transaction spans R2 or witness I/O.

Exact command replay needs neither R2 nor a witness. Finalize and abort never
perform implicit cleanup after replay; callers use the explicit cleanup command
and inspect current upload status separately from historical acknowledgments.
Duplicate cleanup verifies the actual selected canonical generation and retires
only the duplicate. Both duplicate and abort cleanup refuse canonical retirement.
If physical retirement succeeds but the final witness or transaction fails,
`cleanup` or `aborting` remains quota-accounted and can be resumed after reopening.
Logical payload quota excludes permanent empty retirement-marker overhead and
provider billing. This bounded profile supplies no streaming readers, restore,
artifact deletion or worker execution route and does not qualify those features.

The authoritative upload lifecycle passed local DO/R2 qualification at `315c548`
in [run 41194176308](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41194176308):
all five runtime tests passed in 115.94 seconds.
The fixture sends an actual source ZIP and empty object, discards a successful
PUT response, verifies shared publication, retires a duplicate before a deliberately
failed witness, restarts workerd and completes cleanup without releasing quota
early. A separate-object bypass proves that the test detects missing exclusion;
normal routing holds abort behind an externally delayed PUT invocation. Accepted
and rejected acknowledgments replay with byte access disabled and no additional
witness call. These remain local runtime results with synthetic clocks and keys.

`artifact.get` requires `read` and returns the shared metadata record. Bounded
content delivery requires `read`, an active workspace and a verified canonical
generation. The router reads and verifies the complete body inside its exclusive
gate, then rechecks credentials, configuration and the captured catalog row.
Only immutable `(ArtifactRef, bytes)` leaves the gate. Thus HTTP delivery cannot
keep an R2 reader, native session or catalog pin alive. Metadata queries and
downloads create no command acknowledgment, event, pin or witness. Provider
qualification of this download path is separate from the upload receipt above.

`adapters/cloudflare/hosted_http.py` maps this profile onto the existing SDK wire
contract. It accepts ordinary upload creation, status, parts and finalize routes,
explicit abort/cleanup commands, artifact metadata and content, plus the admitted
worker commands and queries. Capabilities list only implemented routes and limits;
they do not advertise research execution, proposal management or restore.
Authentication and known scopes precede body consumption. The runtime must supply
a body reader that enforces the selected bound during consumption; the mapper
checks the returned bytes again. Compressed request bodies and ranges are refused.
Downloads carry exact length, media type, digest ETag, identity encoding and
`no-store, no-transform` headers.

Unexpected failures after a mutating dispatch produce `DELIVERY_UNCERTAIN` with
the correct command-replay or upload-inspection recovery hint. They cannot become
input errors merely because serialization raised `ValueError`. Domain rejection
acknowledgments retain their original command identity and body. The HTTP module
does not provision an authority, bind a public endpoint or deploy credentials.
Unit tests drive the unchanged `ResearchClient` through its actual HTTP transport
with a mock network boundary. The actual Workers ingress subsequently passed at
`7d33b2f` in [run 41195858674](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41195858674):
six runtime tests passed in 137.23 seconds. The SDK sent binary and empty uploads,
downloaded verified bytes, recovered a lost finalize response, reopened durable
state and replayed cleanup with R2 disabled. Credential revocation waited behind
an admitted read and denied the next download. This local runtime receipt uses
synthetic authority time; it does not admit native workers or qualify deployment.

The Workers body bridge (`http_binding.py` and `http_io.mjs`) enforces that bound
while reading and refuses an unconfirmed read after five seconds. Ready chunk
batches yield to the event loop so an empty-chunk producer cannot starve its
deadline. Request cancellation is best effort; it is not closure evidence. R2
reads also end their consuming scope at timeout and dispose of late response
bodies. Native JavaScript tests use an independently bounded worker thread for
the starvation regression. These stream tests do not replace workerd HTTP proof.

`Database` accepts an optional internal `ExecutionBoundary`; native construction
leaves it absent. Exact acknowledgment replay, workspace admission and reserved
command-key checks run before the boundary. It is synchronous and runs inside the command
marker and outer write transaction.

`WitnessExecutionBoundary` wraps the complete decision callback: handler savepoint,
rejection hooks and acknowledgment persistence. This scope matters because an
acceptance-expiry hook can need the same witnessed time after the handler rolls
back. Closing the witness at handler exit would break that legitimate fence.

If provider validation fails before the decision starts, the boundary requests
one rejection. The kernel composes authenticated source retirement before the
existing domain rejection hook. A hook, fence, serialization or acknowledgment
failure aborts the entire outer transaction; it must not become a second rejection
attempt. The kernel permits exactly one decision or pre-handler rejection and
requires the returned acknowledgment to match the recorded canonical bytes.

The complete command outcome is recorded before the witness closes. The outer
SQL commit then follows while the hosting adapter still owns its ordering gate.
Replays need neither a new witness nor a prepared boundary. Public hosted routing,
query scopes and artifact preflight remain separate integration requirements.

### Ordinary research admission profile

`research-admission-witness-v1` is a separately provisioned configuration. It
inherits bounded artifact transfers and adds `project.register`,
`campaign.create/validate/seal/start`, `candidate.propose/validate`, `trial.submit`
and `worker.claim_job`. An existing worker-only or artifact-only configuration
does not acquire these operations. Their serialized configuration and capability
contracts remain unchanged.

Source commands authenticate current `read` and command scope before exact
acknowledgment replay. New commands read the complete bounded R2 generation under
the exclusive gate, verify the full ArtifactRef and every archive entry, and
resolve the project contract from those immutable bytes. They recheck credentials,
configuration and the catalog row before the witness and again before the shared
transaction. Source retention, records and events commit together. No temporary
filesystem, native reader pin, storage I/O or await enters that transaction.

The native path keeps its existing file preflight and calls the same project and
candidate transaction helpers. Prepared source objects are internal verification
products; no API accepts a caller-authored resolved project or source manifest.
Idempotency preimages remain the original typed command payloads.

Claims require explicit enrollment for the hosted clock profile and exact policy,
current worker/project authority, and the existing `python` CPU backend without
accelerators. The shared claim transaction owns capacity, budget reservations,
assignment signatures, timing sidecars and acceptance leases. No such records
are synthesized by the adapter. A claim is permission to execute, not proof that
execution occurred. This version does not expose worker input/output transfer,
result publication or evaluation authorization/completion; its capability answer
therefore says `cpu-claim-only`.

The unchanged SDK uses `GET /v1/records/{kind}/{id}` and `POST /v1/query` for a
bounded allowlist of project, campaign, candidate, trial, job, worker, attempt and
acceptance metadata. These views require `read`; restricted worker credentials
continue to use the existing scoped worker queries. The comparison, checkpoint,
report and other research surfaces are not implied by this allowlist.

Local tests exercise the complete ordinary SDK admission sequence, native CPU
profile discovery, independent signature/time binding verification, exact replay
with unavailable storage and witness, protected-file refusal, corrupted source,
catalog/credential changes, cancellation, competing claims and current read
permission on replay. They use SQLite with an asynchronous byte fixture and do
not establish actual workerd qualification by themselves. The separate real native
source/R2/DO admission test passed at `36f19cb` in
[qualification 41198210270](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41198210270):
eight workerd tests passed in 194.39 seconds. The unchanged SDK registered ordinary
source, submitted a trial and obtained a scoped signed claim; lost acknowledgments
replayed during outages and after a workerd restart. The worker independently
validated native deadline caps before restart. Replay does not renew those caps
or promise that the original grant remains launchable after a long restart.

### Buffered scoped worker inputs

`research-inputs-witness-v1` explicitly extends the admission profile with
`POST /v1/worker/input` and `POST /v1/worker/input/content`. Both retain the
existing SDK request schema and require only `worker.inputs`. Existing profiles
refuse these routes before consuming a body. Global artifact reads remain
unavailable to restricted workers.

The adapter checks current worker/session, project scope, unsettled assignment,
explicit hosted clock admission and membership in the signed assignment's source
or named inputs before storage access. Metadata requests take a fresh witness.
Content requests first capture the complete catalog record and verify its bounded
R2 generation while holding the exclusive gate. After storage completes, a fresh
witness precedes the shared `worker.input` query. That final query rechecks
ownership, settlement, signed time binding, issuance epoch/policy, expiry and the
expected catalog record. No client can supply the expected-record argument.

A grant that expires during storage I/O releases no bytes. A successful response
contains an immutable complete buffer; no storage reader remains active during
HTTP delivery. The path therefore creates no pin, reader session, command
acknowledgment or cleanup obligation. It is limited by the configured maximum
object size and supports neither ranges nor a streaming R2 reader. A future
streaming implementation requires a separate custody contract.

Local tests use credentials containing only `worker.inputs`, download ordinary
source and a named campaign input through the unchanged SDK, and refuse global
read access. Faults cover invalid ownership, settled grants, missing admission,
policy mismatch, expiry during a held read, corrupted/missing/retired/oversized
objects, catalog change after verification, revocation, source epoch change and
cancellation. A queued revocation orders after the completed read, then prevents
the next request. The actual native-source workerd/R2 fixture subsequently passed
at `dceafc3` in [CI 41198736103](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41198736103):
nine workerd tests passed in 219.17 seconds, including scoped source and named-input
downloads and restart replay. Targeted native tests passed on Linux (351), macOS
(351) and Windows (347, with four skips). This is local runtime qualification;
cloud deployment and subsequent execution/evaluation profiles require separate
qualification.

Worker-output preparation now shares the native begin and publication transaction
callbacks. Publication checks the exact upload reference and generation before
retaining evidence and writing its scoped provenance. Recovery publication also
requires a bounded parsed journal and verifies its original grant in the final
transaction. Byte verification and journal parsing remain internal preflight
responsibilities. The shared callbacks do not grant transport or recovery authority.

### Buffered scoped worker outputs

`research-outputs-witness-v1` explicitly extends the input profile with the existing
`worker.begin_upload`, `worker.finalize_upload`, `worker.abort_upload` and
`worker.cleanup_upload` commands, plus `POST /v1/worker/output` and
`PUT /v1/worker/output/parts`. These routes require only `worker.outputs` and retain
the SDK schemas. Older profiles refuse them before consuming a request body.

Every upload belongs to the original worker session, attempt, role and optional
declared output name. A fenced or replaced session can still preserve its evidence;
this grants no execution or input authority. Current-session revocation and current
credential revocation still prevent fresh operations. Exact accepted commands can
replay without storage or clock access, after current credential authentication.

Transfers contain the complete declared object at offset zero, bounded by the
provisioned R2 limit. The authority gate spans storage I/O. Finalization verifies
the generation and any existing canonical copy before acquiring a fresh witness.
The shared transaction rechecks provenance and the catalog selection, retains the
evidence, and records the receipt. No storage operation or native pin occurs after
that witness. Publication leaves the attempt and its budget settlement unchanged.

Cleanup is explicit. Abort records an intent and keeps storage quota occupied.
Cleanup retires the exact generation before atomically completing both upload
records. Duplicate cleanup verifies the canonical copy before retiring the surplus
generation. An uncertain storage operation or failed final witness leaves cleanup
retryable and quota reserved. Finalization never depends on implicit cleanup.

This profile refuses sealed or recovering workspaces before byte access; hosted
restoration has no qualified native recovery authority. Local adapter tests cover
the unchanged SDK, restricted scopes, historical ownership, verification faults,
revocation, cancellation, ordering, retry and quota retention.

The workerd qualification now includes a separately provisioned output-profile
case. It uploads through the unchanged SDK against real R2, loses committed
publication and cleanup responses, retires duplicate and abandoned generations,
then replays those commands after an owned workerd restart with storage and the
native source unavailable. The synthetic artifacts do not claim to be valid model
results. This extension passed at `10afb04` in
[CI 41199740476](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41199740476):
10 workerd tests passed in 167.21 seconds and 13 Node transport tests passed in
10.094 seconds. Targeted Linux and macOS suites each passed 375 tests; Windows
passed 371 tests with four skips in 664.87 seconds. This qualifies the local output
runtime, not result acceptance, evaluator completion or cloud deployment.

Result preparation now has shared current-owner and role-specific catalog selectors,
plus a byte decoder that verifies the immutable reference, 1 MiB bound, strict JSON
and object shape. Every role is authorized before identical reads are coalesced.
The native path keeps its original pin lifetime and acceptance transaction;
historical reconciliation remains a separately authorized operation. These seams
do not themselves expose hosted result or evaluator publication.

### Buffered execution-result publication

`research-publication-witness-v1` explicitly extends the output profile with
`worker.publish_attempt`. Its SDK command and `worker.publish` scope are unchanged.
Scoped workers need each artifact's original attempt/role receipt; legacy
unscoped workers retain the additional `read` requirement. Historical upload
permission cannot authorize fresh result acceptance from a replaced worker.

Publication selects current ownership and every evidence role before coalescing
identical objects. It permits at most 32 evidence roles and 8 MiB of unique
evidence; each object also obeys the configured R2 size limit. Every referenced
generation, including logs and outputs, is read and verified under the authority
gate. Journal/report observations additionally require exact reference checks,
strict JSON, the 1 MiB observation bound and their existing typed contracts.

Only after storage I/O and decoding finish does the adapter acquire a fresh
witness. The final transaction rechecks ownership, role receipts and catalog
selection, then calls the unchanged native acceptance and rejection-fencing
callbacks. There are no storage awaits or native reader pins after the witness.
The native accounting rules still distinguish proven closure, proven absence and
unknown execution. Acceptance expiry records a conservative fence rather than
claiming closure or settling reserved compute. Accepted acknowledgments replay
after current authentication even when storage and the time source are unavailable.

Local adapter tests execute a fixture-owned CPU process using a real admitted
native clock source. They cover successful closure, failed closure, proven
not-started execution, corrupt evidence, malformed observations, changed grants,
role bounds, revocation, cancellation, expiry and reopen replay. These tests use
an in-process HTTP/storage harness; actual workerd result qualification is a
separate step. The profile refuses historical acceptance and sealed/recovering
workspace operations. It creates no evaluator receipt or scientific promotion;
evaluator authorization and completion remain separate work.

The workerd fixture now provisions a separate publication-profile object. A real
native CPU worker downloads source, executes the ordinary project, uploads its
signed journal/report and loses the reply after the result commits. It then
replays with storage and witness unavailable, repeats that replay after restarting
only workerd, and checks that its native journal, result and single budget
settlement did not change. The source process stays alive across both controller
incarnations. This probe requires its own passing runtime receipt; the output
qualification above predates result acceptance.

The first publication run at `6833bdb`,
[CI 41200844689](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41200844689),
passed 410 targeted tests on Linux/macOS and 406 on Windows with four skips.
Workerd passed 10 tests but the new publication test stopped at a fixture
comparison between an integer and a precision-preserving decimal timing string.
The assertion now explicitly decodes both timing fields; this failed run is not
a passing result-publication receipt.

The follow-up at `ce192d0`,
[CI 41201395183](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41201395183),
passed the timing assertion and stopped at the replay fixture's use of `payload`
instead of the SDK's `request` argument name. Workerd again passed 10 other tests.
The fixture now preserves the actual SDK call signature; qualification still
requires a passing end-to-end receipt.

Evaluator preparation now exposes shared synchronous selection and final commit
callbacks. Authorization recomputes the job/trial/input selection after metadata
storage and verifies the generated manifest's exact digest and size before
retention. Completion selects the designated evaluator's report and rechecks its
catalog generation before the existing receipt transaction. Native authorization
still publishes metadata through the ordinary artifact lifecycle; native
completion holds its reader pin through commit. The common observation byte
decoder is independent of control services. These seams do not yet expose a
hosted evaluation command or qualify a cloud evaluator.

### Independent buffered evaluator

`research-evaluation-witness-v1` explicitly extends the publication profile with
`evaluation.authorize`, `evaluation.complete`, and ordinary get/list queries for
evaluation requests and receipts. The CLI/SDK request shapes remain unchanged.
Older profiles still refuse these commands before consuming their bodies.
Authorization requires `evaluation.request`, `read` and `artifact.upload`;
completion requires `evaluation.publish`, `read` and the exact designated
evaluator identity. Giving a worker publication permission does not make it an
evaluator.

Authorization derives the input manifest internally. Native and hosted byte paths
share its transfer digest, bounds and stable begin/finalize identities. Each
preparatory artifact command gets its own fresh witness. A duplicate generation
is retired through a separate stable cleanup command before proceeding; uncertain
retirement keeps quota occupied and the parent retryable. The final evaluation
witness is acquired only after this work. Its transaction rechecks the selected
job/trial/inputs and manifest catalog identity before retaining evidence and
issuing authority. A rejected parent can leave a verified manifest without
creating evaluator authority, as on the native path.

Completion selects the designated evaluator's exact report before byte access,
verifies and decodes the complete bounded generation, then obtains a fresh
evaluation witness. The final shared transaction rechecks the selected evidence
and requires the accepted successful evaluation-stage attempt, frozen request,
input identity and verified run result. It preserves incomplete and invalid
receipts as evidence. A valid receipt updates trial eligibility but creates no
promotion. No native pin or storage await occurs after the final witness.

Local adapter coverage executes both CPU stages and uses a separate evaluator
credential. It covers valid/incomplete/invalid receipts, foreign evaluator denial,
missing/corrupt reports, catalog changes, credential revocation, cancellation,
lost manifest writes, interrupted duplicate retirement and offline reopen replay.
These adapter tests use an in-process HTTP/storage harness; actual workerd
evaluation has separate evidence below, and cloud deployment remains unqualified. Hosted historical
acceptance, restoration, confirmation-plan creation and promotion remain outside
this profile.

The actual workerd fixture now provisions this profile with a third, separately
scoped evaluator credential. Its ordinary project executes training and scoring
as two native CPU stages. The probe loses the authorization and completion
responses after their commits, replays them with R2 and the witness unavailable,
and repeats replay after restarting only workerd. It checks two closed journals,
two run results and settlements, one evaluator receipt, unchanged trial evidence,
and no promotion or reader sessions. Older fixture profiles retain their original
two credentials and configuration digest inputs. This test requires its own
passing runtime receipt; source and collection checks alone are not qualification.

At `24c749e4d13a8bdf511d16d26802c8ba67874d80`,
[CI 41201873265](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41201873265)
passed the actual workerd suite with **12 tests in 339.40 seconds** and its Node
transport suite with **13 tests in 10.141 seconds**. Linux passed 431 tests in
108.12 seconds and macOS passed 431 tests in 114.68 seconds. Windows passed 427
tests with 4 skips in 564.33 seconds. This is local workerd/R2 qualification of native result
publication and independent evaluation, including restart replay. It is not a
deployed Cloudflare service or accelerator qualification.

### Witnessed lifecycle intent

`research-lifecycle-witness-v1` extends the evaluator profile with
`campaign.pause`, `worker.heartbeat` and `attempt.request_cancel`. Each uses one
fresh witness and its existing synchronous transaction. Resume uses the existing
`campaign.start` command. Worker admission still uses `worker.set_admission` and
the separate admission projection's revision. Older profile capabilities remain
unchanged.

Campaign pause blocks new trials while letting already-active trials finish their
remaining stages. Worker draining blocks every fresh claim, including the next
stage of an active trial. These controls are independent: coupling them can strand
a campaign drain until a worker becomes available again. Only drain-pause is
qualified. The shared kernel does not yet expose general campaign cancellation;
the hosted command records one dispatch-bound attempt cancellation request.

Heartbeat acknowledgments and presence do not establish process closure. Explicit
acceptance renewals use the existing calibrated authority and finite policy;
they cannot change signed execution deadlines. Failed acknowledgment validation
rolls back presence and renewals together. Cancellation intent, acknowledgment,
offline workers and elapsed leases do not refund execution obligations. Native
evidence and the independent acceptance path remain responsible for settlement.
Wall-clock presence/cancellation timestamps are metadata, not signed clock
authority.

Operator get/list queries now include `worker_admission`, `worker_presence` and
`attempt_control`. They retain the native generic-record semantics: an absent
record is absent. Scoped `worker.get` continues to expose the existing virtual
admission/presence state at revision zero; `worker.list(kind="controls")` retains
its bounded control stream. These record reads need no witness. Fresh offer and
clock queries retain their existing witnessed boundary.

Local adapter tests cover multistage campaign drain/resume, independent worker
draining, no new trials while pausing, sticky control acknowledgment, bounded
acceptance renewal, atomic invalid-ack refusal, post-witness revocation and
cancellation, and exact offline replay after reopen. Actual workerd lifecycle
qualification and provider deployment remain separate.

The workerd evaluator probe now includes a separately provisioned lifecycle
variant. It rejects new trials while pausing, rejects a fresh scoring claim while
the worker is draining, reopens worker admission, and finishes scoring under the
campaign drain. It checks `paused` before independent evaluator completion,
completes that evaluation while paused, then resumes through `campaign.start`.
A deliberately refused claim has its own idempotency key; reopening admission
does not overwrite its rejection. A lost heartbeat response and all successful
lifecycle commands replay after workerd restart with source and storage
unavailable. This extension needs its own passing runtime receipt and does not
claim running-process cancellation qualification.

At `544d54caeececfd65220b8c181f128b158607a2c`,
[CI 41202600727](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41202600727)
passed **13 actual workerd tests in 361.63 seconds**, including this lifecycle
case. Linux passed 439 tests in 177.56 seconds, macOS passed 439 in 181.18 seconds,
and Windows passed 435 with 4 skips in 696.39 seconds. Scientific closeout and
saved-turn orchestration require their later revisions' own receipts. The native
qualification job now allows 20 minutes as those additional real-process cases
join a Windows suite already measured at over 11 minutes.

### Scientific closeout

`research-scientific-witness-v1` extends the lifecycle profile with six existing
commands. It uses the same request models, scopes and synchronous transactions as
the local controller. Each fresh command gets one witness after validation; these
commands perform no artifact byte access, derived command or await inside the
transaction. Eligibility uses the authenticated evaluator receipt, accepted stage
results, comparison identity, catalog availability and append-only corrections.

| Command | Required scope | Revision authority |
| --- | --- | --- |
| `promotion.assess` | `promotion.assess` | Payload incumbent revision |
| `promotion.reinitialize` | `promotion.reinitialize` | Payload incumbent revision and fresh post-correction trial |
| `promotion.commit` | `promotion.commit` | Persisted assessment's incumbent revision |
| `evidence.invalidate` | `evidence.invalidate` | Append-only correction of named evidence |
| `report.create` | `report.create` | Immutable snapshot at current event watermark |
| `campaign.complete` | `campaign.manage` | Exact current campaign revision |

The first eligible baseline is observed evidence. Selecting it does not claim an
improvement or statistical confirmation. Assessments preserve the evidence and
expected incumbent revision; competing commits cannot both change that revision.
Corrections suspend affected incumbent eligibility and invalidate dependent trial
eligibility without rewriting measurements, receipts or prior reports. Explicit
reinitialization requires a new trial submitted after the correction.

Reports contain bounded JSON and Markdown with a content digest and event
watermark. `recent_limit` remains 1–50 and the shared serialized report limit is
1 MiB. Completion creates its final report in the same transaction as state,
events and any funding settlement. It requires closed resources, settled budgets,
completed evaluation of successful work and the declared stopping condition or
permission for manual completion. An unknown process, cancellation acknowledgment
or expired lease cannot satisfy closure. Failure to construct the report rolls
back the completion.

Operator get/list queries include promotions, corrections, reports and run results.
Their `read` permission is separate from mutation scopes. Exact accepted and
rejected command replay remains available after reopen without storage or witness
access, subject to current credential/profile checks. A replayed historical
promotion acknowledgment does not erase a later correction or restore eligibility.

Local adapter tests use two real native CPU stages and an independent evaluator.
They cover competing selection, completion, immutable report watermarks/digests,
late corrections, fresh baseline reinitialization, pending-evaluation refusal,
report transaction rollback, scope denial, post-witness revocation/cancellation
and offline replay. This profile needs separate actual workerd qualification.
Confirmation-plan creation, unresolved-resource scientific completion, orchestration
transfer and hosted restoration remain outside this profile.

The actual workerd fixture includes a third scientific profile case alongside the
evaluator and lifecycle cases. It selects the observed baseline, loses the final
completion reply after commit, applies a late correction and creates a current
report. After workerd restart it replays all successful acknowledgments and the
fresh-key invalidated promotion rejection with source and R2 unavailable. The
original report and evaluator receipt remain immutable; current trial eligibility
is invalid and the incumbent is suspended. Native journals, two run results and
two budget settlements remain unchanged. This case needs its own passing runtime
receipt and does not claim statistical confirmation or cloud deployment.

At `956b079e4fe0dcd74d458d3713f1b613c0622de5`,
[CI 41202953477](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41202953477)
passed **14 actual workerd tests in 243.20 seconds**, including scientific
closeout. Linux passed 447 tests in 210.76 seconds, macOS passed 447 in 199.15
seconds, and Windows passed 443 with four skips in 650.42 seconds. Saved-turn
orchestration was added afterward and needs its own runtime receipt.

### Saved-turn delivery boundary

`OrchestrationControl.prepare_execution()` selects the original saved action,
checks its route actor and current action scopes, preserves target/delivery key
separation, and returns either the previous acknowledgment or an internal guarded
target context. Preparation performs no target effect and creates no receipt.
The target transaction still verifies the saved turn, current binding and epoch;
fencing after preparation prevents a new effect.

The native executor uses this same preparation. A target acknowledgment with a
missing delivery receipt skips target dispatch and finishes only the receipt.
This seam lets an async adapter prepare bytes and obtain a target witness before
obtaining a different witness for receipt completion. It does not itself expose
hosted orchestration or permit one witness to cover two commands.

### Hosted saved actions and event waits

`research-orchestration-witness-v1` extends scientific closeout with the existing
bind, fence, prepare/execute/reconcile/close-turn, record/reconcile-wait and
acknowledge-wakeup commands. It exposes bounded get/list views of their records
and the ordinary `/v1/campaigns/{id}/context?limit=10` query, with a 1–50 limit.
`DurableTurnDriver` can use this API unchanged, including its authoritative event
watermark. The gateway's event watermark is never inferred from a record revision.

Saved control actions are currently limited to `evaluation.authorize`,
`promotion.assess`, `promotion.commit`, `campaign.pause`, `campaign.complete` and
`report.create`. The separate evaluator route permits `evaluation.complete`.
Unsupported actions fail before reserving an ordinal or pending turn. The shared
kernel still enforces route ownership, current action scopes, exact saved request,
target/delivery key separation, binding epoch and configuration identity.

Execution keeps the authority's exclusive gate while using an internal async
dispatcher that preserves the private `DriveGuard`. Public HTTP cannot supply
that guard. Evaluation artifact preparation retains its exact derived operations,
including duplicate-upload cleanup, and each command obtains its own witness.
The target effect commits before a different fresh witness can authorize the
delivery receipt. No SQL transaction spans asynchronous byte or witness I/O.

If the target commits but its receipt witness or response is lost, retry inspects
the original target acknowledgment and finishes only bookkeeping. Preflight
failure leaves the turn unresolved; an actual durable target rejection produces
a rejected receipt. Current target scopes are checked before replay. Fencing,
revoked credentials and task cancellation cannot manufacture a successful target
or a receipt for an unacknowledged effect. Explicit fenced closure preserves the
distinction between no acknowledged effect and a settled target result.

Waits are event-only in this profile. An event committed before registration is
found by catch-up; an event committed afterward creates readiness and a pending
wakeup in the event transaction. Notification acknowledgment does not consume
readiness or research evidence. The next saved action consumes the ready wait
using its planning watermark. Orchestration bookkeeping does not wake itself.
Timers remain refused because acquiring a native witness does not qualify the
kernel's UTC timer comparison. Successor bindings with startup checkpoints are
also refused until their transfer/recovery path is qualified.

Local adapter coverage includes SDK planning, missing-receipt recovery after
reopen, exact offline replay, revoked target scopes, two separately scoped saved
evaluator actions with real native CPU scoring, duplicate-manifest retirement
and interrupted cleanup, both event-registration orders, duplicate wakeup
acknowledgment, unsupported-action/timer refusal, fenced closure and post-witness
fencing/revocation/cancellation. These are SQLite/mock-R2 adapter tests. Actual
workerd and Workflow generation/restart qualification remain separate work;
this profile is not yet a deployed Workflow service.

The actual workerd orchestration probe provisions a fourth, separate control
driver and adds drive permission to the evaluator only in this explicit profile.
Older fixture credential manifests remain unchanged. A real two-stage CPU project
uses saved evaluator authorization and completion actions. The source deliberately
fails the receipt witness after authorization has committed; a restarted controller
recovers the original saved action without invoking its planner and finishes only
the receipt, with R2 unavailable and exactly one new witness. Native scoring and
an independent saved evaluator completion follow. A later event wait and wakeup
acknowledgment survive another workerd restart alongside all delivery receipts,
without consuming readiness or changing native journals or settlements. Source
and collection checks are not runtime qualification; this probe needs a passing
CI receipt. Naming the binding adapter `cloudflare-workflows` does not claim that
a Workflow has executed it.

At `c97d86652083288787f1203fa367747e59246042`,
[CI 41203847351](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41203847351)
passed **15 actual workerd tests in 462.39 seconds**, including saved evaluator
delivery and event-wait replay across authority restarts. Linux passed 463 tests
in 242.52 seconds and macOS passed 463 in 183.49 seconds. Windows finished with
458 passed, four skips and one setup error in 804.39 seconds: the native clock
refused a sample wider than its 50 ms admission limit during `candidate.propose`,
before worker enrollment or execution. The run is not a full matrix pass; the
clock guard remains unchanged. Scheduling contention was not established as the
cause. The two-process Workflow/facade probe was added afterward and requires its
own receipt.

### Async delivery contract

`AsyncDeliveryKernel` exposes the same bootstrap, inspect, execute and wait
operations over registered asynchronous `get` and `command` clients. Shared pure
helpers own binding identity, actor-route selection, saved-work verification,
deterministic delivery keys and outcome construction. The native kernel continues
using its existing startup recovery and wait driver; its API and command keys
remain unchanged.

The async runtime imports no synchronous research client, native custody or
startup machinery, web framework or vendor runtime. It acquires no clock witness
and makes no research decision. The authoritative command service still checks
the actor and saved turn and decides whether an effect or receipt can commit.
Missing target responses retry the same key. Pending event waits remain pending;
notification delivery cannot manufacture readiness. Timers and successor startup
remain explicit unavailable capabilities of this async adapter.

Local tests compare native and async outcomes on the same durable authority after
lost replies, duplicate execution, event readiness and fencing. They reject changed
bindings and route actors, refuse timers and real transferred successor startup,
and enforce the import boundary in a fresh interpreter. These tests do not qualify
a DO delivery facade or a running Cloudflare Workflow.

### Hosted delivery facade

`HostedDelivery` maps the same bounded `/v1/delivery/bootstrap`, `inspect`,
`execute` and `wait` HTTP protocol to the async kernel under the router's existing
exclusive gate. It requires the orchestration profile. Trusted application
configuration registers one exact binding and route-token map for each gateway
principal; the request body cannot register credentials or choose another actor.
The gateway requires `orchestration.deliver` and must differ from all route actors.
Registration inputs are copied, and both gateway and route credentials resolve
against current authority state on every request, including historical replay.
Expiring credentials remain unavailable in this runtime.

Request-local clients expose only the kernel's four record queries and two saved
delivery commands. They call the internal guarded dispatcher without acquiring a
second exclusive gate or inventing a witness. Each client checks its owning task
and request lifetime, so detached or retained clients cannot use the private
dispatch path. Unexpected failures and cancellation reach the gate before HTTP
translation. Unknown completion returns retryable uncertainty; it cannot become
a fabricated rejection or success. Requests and responses have a 16 KiB bound;
errors contain codes and retryability, never exception text or route credentials.

Native adapter tests cover copied registration, identity substitution, revocation,
pending and ready event waits, concurrent exact replay after a lost response,
cancellation, detached clients and malformed/oversized envelopes. The local
workerd fixture now routes delivery directly to one fixed orchestration DO;
request profile headers cannot redirect it. Its synthetic gateway is separate
from control and evaluator principals. Each reopen reconstructs registration from
the fixed driver's persisted owner row and verifies both route owners, workspace,
epoch, adapter and configuration. The request cannot select a campaign, and the
fixture never scans for a latest binding. Fenced bindings remain readable so the
delivery kernel can report retirement. This fixture is not production credential
provisioning or deployment routing.

`test_cloudflare_hosted_workflow.py` runs separate authority and Workflow workerd
processes with independent persistent directories. The authority keeps the same
origin across restart because the transport registry pins it. A report target
commits before its receipt witness fails; Workflow retry must recover one original
receipt and one report. Both processes then restart with a saved event wait carried
into a later generation. A false hint cannot satisfy it; an actual report event
without a hint must become ready through polling. Duplicate wakeup acknowledgment
leaves readiness unconsumed, a domain fence retires delivery, and a changed origin
cannot reuse the transport registration. Collection/source checks are not runtime
qualification; this probe needs a passing CI receipt.

At `c24779e74b36dfb7e9cde1d611ba718e93eb57cb`,
[CI 41205018578](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41205018578)
passed TypeScript checking but failed Prettier on `hybrid.ts`. The Node and workerd
integration steps were not reached. Commit `13930d0` formats that condition; it
was insufficient: CI 41206168329 rejected its continuation indentation too.
Commit `2573d8f` applies pinned Prettier 3.9.10 and passes the full local adapter
format check. The actual runtime probe remains pending.
The same run passed 478 native adapter tests on Linux in 265.82 seconds and on
macOS in 175.43 seconds; Windows subsequently passed too.

At `89fbc065cb353706af9dac6f06ac8b46e3982297`,
[CI 41206622520](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41206622520)
passed 17 workerd tests in 518.94 seconds, including the two-process Workflow
delivery and queued transfer probes. The HTTP/witness Node suite passed 13 tests;
the hybrid suites passed 13 and 4 tests, with TypeScript and formatting checks
passing. Native Linux passed 527 tests in 296.05 seconds and macOS passed 527 in
186.24 seconds; Windows was still running at this checkpoint. This receipt does
not cover successor startup, introduced in later commits.

### Transfer preparation boundary

`OrchestrationTransferControl._prepare_in()` selects an immutable transfer view
inside a read transaction. It retains the native requirement for a separate
operator with read, management and drive scopes; a retiring driver cannot transfer
its own authority. The shared request digest includes the exact header revision.
An existing transfer selects its original immutable result without inspecting the
now-stale campaign/binding input again.

Native transfer still pins all required artifact bytes before `_commit()`. The
commit recomputes the selected view and refuses changed evidence before creating
any checkpoint or successor. This extraction lets a future hosted adapter verify
R2 bytes asynchronously while retaining the same selection and transaction rules.
It does not expose hosted transfer, snapshot recovery or successor startup yet.

`TransferLimits` lets an adapter reduce record, manifest-entry, manifest-byte and
artifact-count ceilings and set an aggregate artifact-byte ceiling. Defaults keep
the native manifest ceilings; auxiliary custody and retention scans now also have
an explicit 100,000-row ceiling. SQL queries request at most the selected bound
plus one sentinel row before the bridge can materialize a result. Reference
traversal yields incrementally, and both preparation and commit enforce the
selected profile. Exceeding a bound refuses the transfer; it never truncates a
checkpoint or treats missing evidence as absent custody. Smaller valid profiles
produce the same view and checkpoint digests as the native profile.

The explicit `research-transfer-witness-v1` profile exposes
`orchestration.transfer` and bounded reads of its checkpoint, chunks, binding
history and startup records. The operator needs read, management and drive scopes
even for command replay. Before its one fresh witness, the adapter selects a
shared transfer view and exact artifact catalog records, verifies each required
R2 generation, then rechecks the entire selection. The final transaction rechecks
catalog identity and uses the shared `_commit()` with the same limits. Exact
command replay uses no R2 or witness; the same transfer identity under a new key
reuses its immutable receipt with one new command acknowledgment.

This profile permits at most 4,096 rows per bounded scan, 8,192 manifest entries,
4 MiB of manifest bytes, 32 required artifacts and 8 MiB of aggregate artifact
bytes, with the configured per-object storage bound still enforced. Required
retention and the existing exclusive gate protect verification; no native pin or
reader session is borrowed. Oversized, missing or changed evidence refuses the
operation before an ownership change.

Native adapter tests preserve queued jobs, trials, native authority and budgets;
recover absent, accepted and rejected saved targets without redelivery; test
competing transfers, rollback, cancellation, credential revocation and artifact
mutation; and keep an assigned retiring evaluator until actual native scoring and
its independent receipt complete. The successor receives pending startup records.
Automatic snapshot recovery and `acknowledge_start` remain unavailable in this
profile, so transfer acceptance does not claim executable successor recovery.
`test_cloudflare_transfers.py` adds a separate transfer-profile workerd fixture
with fresh successor credentials. A saved report commits before its delivery
receipt witness fails. Transfer first refuses unavailable R2 without a fence,
then recovers that original target and commits despite a lost HTTP reply. After
authority restart, the exact command replays with both R2 and the witness source
unavailable; a new key for the same transfer needs one witness and no R2 reads.
Bounded fixture inspection compares native controller identity, assignment key,
budgets, reservations and settlements. Queued trial/job records remain unchanged,
there are no workers or attempts, and both successor starts remain pending.
This scenario does not qualify active or unknown native custody. The actual
workerd run and deployed qualification still need separate passing receipts.

### Signed clock query boundary

`SnapshotLimits` provides shared recovery projection ceilings. Effective item
and byte limits are the smaller of the request and adapter profile; aggregate
retained bytes include other active snapshots in the workspace. Record and budget
scans include a SQL limit with one overflow sentinel before a hosted bridge can
materialize rows. A bound failure rolls back every item and retention pin instead
of returning a partial view. Default limits and accepted snapshot scope, ordering,
watermark and content remain unchanged. These limits alone do not qualify hosted
snapshot expiry or successor startup; that path still needs epoch-bound trusted
time and a complete verified snapshot reader.

The shared recovery service now accepts a trusted `SnapshotClock` that returns
one timestamp and a required clock epoch. Creation persists that epoch in a
private immutable record in the same transaction as snapshot contents. Public
snapshot bytes and native UTC defaults remain unchanged. Active reads, bounded
collection and event-floor pins compare only snapshots in the current clock
domain; a new epoch invalidates old snapshots even if its timestamp regresses.
The native UTC reader also refuses an epoch-bound snapshot instead of silently
interpreting its deadline as wall time. Release and collection retain tombstones,
and exact create/release replay does not renew the lifetime or require a clock.

Page and item reads now use `Database.query`, which activates the operation's
witness and persists a source-change fence before returning refusal. These reads
produce no command acknowledgment. Witness tests cover same-epoch reads, source
requalification, regressed timestamps, expiry boundaries, pinned event floors,
atomic clock-record rollback and offline command replay. This shared foundation
does not expose a hosted snapshot endpoint by itself.

The same generic execution boundary supports `Database.query()`. Every query
gets a fresh synchronous execution marker. Native queries use the existing read
transaction. A configured witnessed query uses an outer write transaction only
to persist conservative fencing; its domain reader remains read-only and runs
inside a savepoint.

The reader's result must be synchronous before it is wrapped in an internal
`QueryOutcome`. The boundary must return that exact outcome object and call the
decision or rejection path once. A reader refusal rolls back its savepoint,
applies any source fence and returns the failure object for outer commit. The
database raises the refusal after commit. A fencing or boundary failure rolls
back the whole transaction. Queries create no command acknowledgments or replay
cache, and a legitimate `None` result remains a successful read.

`WorkerControl.validate_clock()` checks query scope, current worker/session
ownership, calibrated mode and assignment-signing authority without obtaining
time. Hosted routing must call it before witness acquisition. `clock()` repeats
those checks inside the query callback before signing the nonce-bound reply.
The existing transport DTO is unchanged. The earlier internal names
`CommandBoundary` and `WitnessCommandBoundary` remain import aliases.

Local tests use actual worker enrollment and signed clock replies. They cover
preflight refusal without a witness, changed ownership, persisted source
retirement across reopening, nested copied-context queries, asynchronous-reader
refusal and rollback when fencing fails. Hosted transport ordering and deployment
qualification are still required.

### Hosted successor startup

`research-startup-witness-v1` extends the transfer profile with
`stream.create_snapshot`, `stream.release_snapshot`, bounded operator
`stream.collect_expired`, and `orchestration.acknowledge_start`. The ordinary SDK
uses POST `/v1/snapshot/page` and `/v1/snapshot/item` for actor-scoped recovery.
Each operation checks current credentials and profile, and each fresh timed
operation obtains its own witness under the authority gate. Ownership, cursor
identity, item existence and offset bounds are checked before time acquisition.
Queries recheck ownership and lifetime inside the shared query transaction.
Exact committed commands replay without source access or renewed lifetimes.

Snapshots permit at most 4,096 items, 4 MiB each and 32 MiB retained across the
workspace. The normal per-actor/workspace snapshot-count limits still apply.
Operator expiry collection requires `stream.manage`, uses the same clock epoch,
and reclaims at most the requested 64-snapshot ceiling. It prevents snapshots
abandoned by revoked or retired readers from permanently consuming shared quota.
Event-floor administration remains unavailable in this profile.

`recover_driver_start()` verifies the immutable checkpoint digest, ordered chunk
identities, manifest count and aggregate digest. It then exhausts `SnapshotReader`
at or beyond the checkpoint watermark, verifying every chunk, item digest,
ordinal and byte/item total. Only then does it acknowledge that route's start
revision and release disposable snapshot contents. The domain still requires a
separate acknowledgment for each driver route. An acknowledgment is the driver's
attestation; the API does not independently prove that an arbitrary client ran
the SDK verifier. The provided SDK path supplies that full verification.

Native adapter tests run this unchanged SDK over the hosted HTTP adapter through
authority reopen, then execute one successor report. They cover lost create,
acknowledge and release responses; corrupt checkpoint/content refusal; wrong
actors and scopes; source replacement; cancellation and revocation after witness
acquisition; and abandoned-reader quota recovery. Native worker authority and
budgets stay unchanged. Older transfer/orchestration profiles retain their
startup refusal. Automatic snapshot recovery by async delivery remains unavailable:
the external SDK performs separate bounded HTTP calls rather than holding one DO
gate across the entire recovery. After every registered route has acknowledged
startup, async delivery bootstrap reads each start and its immutable acknowledgment
through that route's credential. It verifies the exact record, campaign, epoch,
adapter, configuration, route, actor and observed watermark, then rereads current
binding ownership and the same start IDs. There are at most two routes. Ordinary
binding revision advances do not invalidate unchanged ownership. Pending or
inconsistent starts refuse; a concurrent ownership change retires startup.

This path creates no snapshots, acknowledgments or commands. Capability discovery
keeps `automatic_delivery_bootstrap=false` and adds
`delivery_after_acknowledgment=true`. Native tests cover partial recovery,
inconsistent references/receipts, ownership loss during a read and legitimate
turn preparation advancing the binding revision during bootstrap. The hosted
delivery facade reads the same transfer records and executes one saved successor
report with exact retry. Actual Workflow qualification must start the Workflow
after recovery; a previously failed bootstrap is not silently restarted.
The workerd startup probe now starts the separate real Workflow process only
after both SDK acknowledgments. Its fixed local successor registration refuses
partial startup, then duplicate starts deliver one saved turn and produce the
accepted receipt before the SDK replays it. That replay must consume no witness.
The fixture's authority selection is a launcher binding, never a caller header,
and remains excluded from the production bundle. This extension passed in
19 actual workerd tests in 580.04 seconds at
`2f3548fb37866539b73379eef96863b9a0ab414e` in
[qualification 41210738134](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41210738134).
That revision passed 660 native tests on Linux in 315.66 seconds and on macOS
in 291.00 seconds. Windows finished with 653 passed, six skipped and one fixture
setup error in 1035.02 seconds: the native clock sample exceeded its existing
interval bound during candidate admission. The interval guard remains unchanged;
later diagnostics retain the measured span. This receipt
predates the production post-bootstrap registration extension.

`test_cloudflare_startup.py` now exercises this SDK against the actual startup
profile in workerd. It transfers queued work, restarts authority storage, loses a
committed snapshot-creation response, recovers the same snapshot identity, and
acknowledges control and evaluator starts independently. It then executes one
successor report and checks old-driver fencing, preserved jobs/budgets/native
authority, two released snapshot tombstones and zero retained items. After a
second restart, acknowledged starts and the saved action replay with the witness
source and R2 unavailable. The complete local workerd suite passed 18 tests in
574.70 seconds at `ea30d781c037daa1c407ba61b0812734ca17f363` in
[qualification 41207429552](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41207429552).
That same head passed 570 native tests on Linux and macOS, and 566 with four skips
on Windows. These results qualify the tested local startup path, not deployment.

### Offer and input query boundaries

`WorkerQueries.validate_list()` checks the existing scope rule, current ownership
and cursor binding without reading time. `list(kind="offers")` repeats those
checks within `Database.query("worker.list")` before evaluating locality. Native
locality still ignores an unavailable cache hint. Pending, historical-pending,
control and record views remain time-free so clock outages do not hide recovery
evidence. Cursor identity, scan bounds and eligibility semantics are unchanged.

`WorkerArtifacts.validate_input()` checks assignment ownership, pending status,
exact input identity and catalog availability without obtaining time. The
`worker.input` query repeats them and checks the existing assignment-clock binding
and expiry before returning metadata. It creates neither a read pin nor an
acknowledgment. A changed authenticated source can still commit a conservative
fence before the query refuses.

Content streaming is a later operation. Its derived `artifact.pin` command
requires its own replay check and prepared witness for timed input authorization.
It cannot borrow the metadata query's observation. Physical verification,
streaming and `artifact.unpin` cleanup must occur outside the metadata/pin witness
lifetime. Hosted content streaming remains unavailable until its adapter wires
those separate boundaries; native readers retain their existing behavior.

### Async ordering bridge

The adapter's `exclusive()` bridge passes an async callable to
`blockConcurrencyWhile`. Python results remain local to that call. Expected
`ResearchError` refusals leave the runtime callback normally and are raised only
after its gate releases; unexpected exceptions still reach the runtime's reset
boundary. Closure rejects a late callback, and caller cancellation settles a
separately scheduled callback before returning. Python bridge tests alone do not
prove provider event ordering.

The actual local workerd probe passed at `595af9f`: A holds an independent HTTP
witness response while B is submitted to the same object; persisted event sequence
and prior-effect counts require A before B. A fixture-only bypass must produce
B before A. A separate async task cannot complete its parent's witness. A fetch
timeout produces no acknowledgment or retirement, the expired response is observed
while a retry is held, and the retry uses a new nonce and creates one effect.
Accepted and rejected commands replay after process restart without another
witness request. Both runtime tests passed in 41.82 seconds in
[qualification 41190864271](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41190864271).

This establishes the tested local runtime ordering and operation ownership. The
timestamps and keys are public fixtures. The 30-second runtime reset boundary,
production source authentication, service routing, artifact custody and hosted
restore remain unqualified; the complete hosted execution profile remains unavailable.
The separately qualified native-source chain and explicit worker clock admission
do not by themselves supply hosted job execution, input custody or publication.

## Production deployment inputs

`deployment.py` defines a public `DeploymentManifest` and a separate protected
secret bundle. The manifest pins one namespace, authority name and object ID,
the `RESEARCH_AUTHORITY` and `ARTIFACTS` binding names, the startup-profile
configuration, witness origin/timeout, credential roster and optional exact
delivery registrations. Its canonical digest covers this entire configuration,
including routing fields outside `HostedStartupConfiguration.digest`. A production
provisioner persists that complete seal and every normal reopen verifies it.

The loader checks physical store identity, assignment/dispatch key agreement,
credential digests and workspace/scope agreement. The provisioning actor must
be able to install every roster principal. Gateway credentials differ from route
actors and reference the exact declared control/evaluator credentials. Nested
maps are immutable, and registrations receive copied public bindings. A separate
bootstrap secret cannot double as a normal API, witness or key secret. Secret
values have no JSON model and their representation is redacted; parse failures
return an opaque error with the original exception suppressed.

Production origin admission requires HTTPS with no userinfo, path, query or
fragment. Local workerd tests may opt into loopback through an explicit loader
argument, which is not an API request field. Input and roster sizes are bounded.
Golden digests and native tests cover mismatched identity, credentials, scopes,
registrations, malformed input and mutation resistance. These checks establish
configuration consistency. The production entrypoint verifies both the receiving
DO's actual ID and named-object resolution. Deployment must still qualify the
physical bucket binding. No resources, secrets or routes are deployed by this module.

`CredentialStore.initialize_in()` and `install_in()` can participate in one
owner-controlled SQL write alongside provisioning intent and source registration.
They do not open or commit another transaction. The ordinary installation API
retains its authorization and conflict behavior; an existing revoked or expiring
credential is not repaired. Atomic roster tests preserve pre-existing credentials
and roll back new schema/members when initialization or a later member fails.
This is the shared preparation boundary, not a deployed bootstrap endpoint.

`provisioning.py` uses two atomic phases. A separate bootstrap credential and the
actual target identity are checked before calling the store factory. The factory
must initialize only an empty store, or open existing current-schema state without
migration. The first transaction refuses foreign data, seals the complete manifest,
creates the workspace and credential roster, and registers an unqualified source.
A fresh witness is acquired outside SQL while the existing operation gate remains
held. The second transaction rechecks the exact intent, roster and source before
admitting time, publishing the ordinary router configuration and storing the
immutable provisioning receipt together.

An interrupted preparation has no partial roster. An interrupted witness or
completion leaves prepared state unavailable to normal opens. Resume accepts only
the same manifest and intact roster; deleted, revoked, expiring or changed
credentials are not recreated. Completed bootstrap replay returns the original
admission receipt without contacting the source or reinstalling credentials.
That historical receipt is not a statement of current source availability.
`require_deployment()` provides a read-only full-manifest/configuration seal check
for the production entrypoint. Its bootstrap store factory initializes only empty
storage and uses current-schema read-only admission for existing state; it never
migrates an existing authority.

Native SQLite tests cover interrupted phases, source outage, concurrent resume,
changed roster/manifest, revocation after the witness, foreign-state refusal,
cancellation and completed offline replay. These tests do not qualify workerd
provisioning, an operator bootstrap route, or a deployed controller.

`production.py` separates `Default.fetch`, `ResearchAuthority` and the named
`Bootstrap.provision(credential)` entrypoint. Public requests resolve only the
manifest's authority name, require the namespace-derived ID to match, and forward
to that object. The receiver checks its actual ID again. Unknown HTTP paths,
including fixture probes and deployment provisioning, refuse before opening state.
The full immutable deployment seal is checked inside the router's existing gate.
Normal opens do not contact or requalify the source, initialize tables, migrate
schemas, install credentials or repair missing state. Existing credential
revocation remains effective after reopen.

An operator service binding selects `entrypoint: "Bootstrap"` and invokes
`await env.RESEARCH_BOOTSTRAP.provision(credential)`. Both this entrypoint and the
DO authenticate the separate bootstrap secret; neither accepts a replacement
manifest, target, roster or witness origin from the caller. Returned JSON contains
only an admission receipt or a sanitized error. `Bootstrap.fetch` refuses HTTP.
The production loader and witness transport require HTTPS; local fixture headers
cannot alter them. Named entrypoints share the Worker's environment and are a
callable capability boundary, not separate secret environments. A deployment that
requires secret isolation must place a public facade in another Worker and give
it only the research service binding.
[Cloudflare named Python entrypoints](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/rpc/#named-entrypoints),
[RPC capability visibility](https://developers.cloudflare.com/workers/runtime-apis/rpc/visibility/).

Native SQLite runtime tests cover missing/prepared state, exact manifest sealing,
offline reopen, retained revocation, fixed object resolution, ineffective fixture
headers, excluded routes and sanitized recovery responses. The actual workerd
named-RPC receipt appears below. Neither test surface establishes deployed service
routing, bucket identity or clock behavior.

### Building the production entrypoint

From the adapter's separately qualified dependency environment, run
`npm run build:production`. It writes `.generated-production/`, copies the actual
shared package and production adapter modules, and bundles the pinned Ed25519
implementation. It excludes all qualification modules, fixture transport and
synthetic keys. The existing `npm run build` retains the independent `.generated/`
qualification target. A build may replace only its own marked output directory;
linked paths, unrelated directories and the other target's marker refuse.

`wrangler.production.jsonc` is an operator template. `workers_dev` and
`preview_urls` are disabled and it defines no public routes. Resource names are
placeholders to be matched to an authorized account and explicit bucket binding.
There is no manifest or secret default: supply the validated `RESEARCH_MANIFEST`
and protected `RESEARCH_SECRETS` bindings. Local qualification uses an owned
secret file, never secret values on command arguments. Preparing this bundle or
template does not create a resource, install a secret or deploy a service.

The operator's separate service configuration selects the named entrypoint:

```json
{
  "services": [{
    "binding": "RESEARCH_BOOTSTRAP",
    "service": "research-swarm-authority",
    "entrypoint": "Bootstrap"
  }]
}
```

No operator HTTP proxy is included in the production bundle. Bootstrap callers
already need the private service binding and the independent bootstrap credential.
Deployment admission must verify the real DO name/ID, bucket binding, protected
secret placement, HTTPS witness and absence of unintended ingress.

`test_cloudflare_production.py` adds an opt-in local workerd probe of this exact
production bundle. A separate loopback-only JavaScript operator fixture discovers
the namespace-derived object ID without invoking a DO method, forwards ordinary
HTTP through a default service binding, and invokes the named bootstrap binding.
That operator fixture is excluded from production. The test's auxiliary Python
configuration has its own private `.dev.vars` file and a link to the already
qualified adapter's frozen `python_modules`; it never installs through that link.
The operator's config directory has none of those secret bindings.

An actual native witness serves HTTPS with a task-owned certificate. The launcher
adds that trust anchor through `NODE_EXTRA_CA_CERTS`; neither certificate checking
nor the production HTTPS requirement is disabled. The probe covers failed bootstrap
authentication, default-entrypoint RPC refusal, missing/prepared state, source
outage during provisioning, lost completed receipts, unchanged SDK source upload
and queued campaign admission through R2, one report effect, offline exact replay,
process restart and a coherently changed but physically wrong object identity.
Secret bundles and individual tokens are removed from failure diagnostics.
The probe does not deploy any resources; its runtime qualification is limited to
the exact revision and receipt below.

This production-entrypoint probe passed as part of 19 actual workerd tests in
610.97 seconds at `42a245e343a410eab7a63eaf4581831ea0a31c6a` in
[qualification 41209431793](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41209431793/job/124561128648).
The production build, formatter, TypeScript and supporting Node suites also passed.
That receipt covers the fixed production entrypoint and bootstrap; it predates
the native launcher and acknowledgment-aware async startup changes.

### Register delivery for new production campaigns

New deployments can use `deliveries: []` and preprovision separate operator,
gateway, control and evaluator credentials. Create and seal a campaign through
the ordinary API, bind its dedicated driver actors, then call
[`orchestration.register_delivery`](orchestration.md#register-gateway-delivery-after-campaign-creation).
The campaign ID no longer needs to exist when the deployment manifest is sealed.

The production router applies two boundaries. The shared command verifies the
active epoch, exact configuration, live credential scopes, issuer scope ceiling
and driver ownership. Its production preflight additionally requires the selected
credential IDs, token digests and complete principals to match the unchanged
deployment roster. This runs before witness acquisition and again before commit.
It never installs credentials, changes the deployment seal or accepts tokens in
the command payload.

Delivery resolves one immutable record by the authenticated gateway actor under
the existing authority gate. An alias token for that actor cannot use the registered
credential's authority. The resolver checks live credentials against the roster
before body consumption and at the request's ownership checks. The existing kernel
then checks binding ownership and saved work. Fencing retires the old registration;
it cannot silently select a successor, newer campaign or different credential.

Existing static deployment registrations remain supported. A static gateway cannot
be registered through the API, and any static/durable collision refuses instead
of choosing precedence. An exact command replay returns historical evidence,
including after target revocation, but grants no live delivery authority.

SQLite/HTTP harness tests cover a real post-bootstrap source upload and campaign,
registration races, one saved report across a lost reply and reopen, offline
replay, credential aliases, revoked/expiring/changed credentials, revocation while
awaiting a witness or reading a body, retirement and static collisions.

The production workerd probe now includes that route: ordinary campaign creation,
dedicated binding and API registration, followed by a separate actual Workflow.
It requires the accepted saved-turn receipt while the Workflow is running, before
any manual delivery replay. After stopping it and restarting the authority, both
registration and delivery replay without a witness. This extension passed with
19 actual workerd tests in 610.48 seconds at
`da25f340de6cd53d4d55f4159ceea5d31bb1f9b5` in
[qualification 41211517897](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41211517897).
This is a local workerd receipt, not a deployed-account qualification.
At the same revision, Linux passed 690 tests in 321.30 seconds and macOS passed
690 in 296.35 seconds. Windows did not complete: GitHub's check annotation says
the job exceeded its 20-minute maximum. Its interrupted pytest summary printed
668 passed and two skipped in 1176.96 seconds; it is not a completed receipt.
The workflow now permits 30 minutes for Windows while retaining 20 minutes for
the other native jobs. Runtime clock guards and experiment deadlines are unchanged.

This release retains a fixed credential roster. A new campaign or transfer needs
unused preprovisioned driver and gateway actors. Adding actors or rotating tokens
requires a separate credential lifecycle implementation; changing or resealing
the bootstrap manifest is not an admitted workaround.

### Native witness launcher

The installed CLI can serve the native time source independently:

```sh
research witness serve \
  --config /run/research/witness.json \
  --signing-key-file /run/secrets/witness.key
```

`WitnessServerConfiguration` contains the exact `WitnessAuthorityScope`, complete
expected `WitnessSource`, and `consumer_digest` from `witness_consumer_digest()`.
The source includes its public key, key ID, native clock profile and complete
clock policy. These must match the controller's pinned descriptor. The launcher
does not discover a controller, infer scope or replace a declared profile with the
current machine's profile. Its only signing input is a separate 32-byte seed file;
the server needs the consumer digest rather than its cleartext bearer credential.

Configuration is strict JSON bounded to 16 KiB. Additional listener fields are
`host` (numeric IP, default `127.0.0.1`), `port` (default `8788`), `transport`
(default `loopback-http`) and `body_timeout_seconds` (positive, at most ten,
default one). Loopback mode refuses a non-loopback address. `tls` requires both
`--tls-certificate-file` and `--tls-key-file`. `private-http` is an explicit choice
for a separately protected private network or TLS-terminating ingress; the CLI
does not infer this from headers. Research API/controller/state options refuse
for this command.

Signing and TLS key files use the native bounded protected-file reader: restrictive
POSIX permissions or admitted Windows ACLs, with no symlink secret files. Use
direct protected mounts rather than weakening this requirement for symlink-based
secret layouts. Signing-key/public-descriptor mismatch refuses before the
listener opens. Invalid configuration errors omit Pydantic input representations.
TLS snapshots the admitted key and bounded certificate bytes once, refuses
encrypted keys noninteractively, and constructs a fixed `SSLContext` before
listening. Python's certificate loader receives fresh protected temporary copies;
they are removed before serving. Uvicorn receives the context factory rather than
the original mutable file paths. This requires Uvicorn 0.47 or later; the frozen
environment retains its existing 0.54 version.
[Uvicorn context-factory introduction](https://uvicorn.dev/release-notes/#0470-may-14-2026).

The launcher creates one `NativeWitnessSource`, then runs one Uvicorn process
without reload, access logging or trusted proxy headers. It explicitly sets
`workers=1`, overriding environment-driven worker counts.
[Uvicorn server settings](https://uvicorn.dev/settings/).
The same source handles every request. A continuity failure remains latched;
changed provider generations cannot reset it. A fresh process has a new source
epoch and requires the consumer's explicit requalification lifecycle. The launcher
does not create a database, initialize research state, contact a DO, install
credentials or requalify an authority. Process supervisors may invoke this same
command with read-only mounted inputs; image/host clock qualification remains
separate from the launcher implementation.

Tests cover malformed/oversized configuration, key/profile/listener refusal,
secret permissions, TLS option agreement, encrypted-key refusal, path replacement
after admission, opaque CLI errors, controller-option
refusal and an HTTP-observed sticky clock fault. Pairs of actual launcher subprocesses
over HTTP and verified TLS sign authenticated nonce-bound requests with stable
epochs within each process and distinct epochs after restart, even when
`WEB_CONCURRENCY` requests seven workers. This is native launcher evidence, not a
deployed Container receipt.

## Native worker calibration

The worker measures the entire challenge round trip:

```text
worker send -> Durable Object -> native clock witness
            -> Durable Object -> worker receive
```

The native observation must occur after authentication of the exact worker
challenge and before the corresponding response. Existing conservative interval
arithmetic includes the complete round trip. Do not subtract the Container hop,
estimate a midpoint or restart the receive anchor after an uncertain response.
The extra hop can reduce availability by exceeding the admitted round-trip or
uncertainty bound; it cannot justify increasing execution authority.

Use a distinct admitted authority profile. A worker must not mistake a hosted
observation for an in-process clock read. The profile identifies the qualified
native source and serialized operation semantics. Worker clock origins remain
native. Signature, nonce, assignment, policy and composed-epoch checks remain
mandatory.

Native launch and hard caps remain monotonic and non-extending. An accepted
renewal or later sample can only intersect a cap already attached to an
attempt. Witness unavailability prevents new time-dependent authority; it does
not release capacity, erase consumption or imply cancellation. A running
supervisor follows its existing bounded offline policy and proves closure
independently.

## Logical checkpoint wire format

`sealed-do-logical-v1` is separate from the existing `sealed-native-v1` bundle.
The signed manifest binds the source physical/logical authority, full deployment
digest, configuration and storage digests, custody pin, operator, seal ID/revision/
digest, and snapshot watermark. The seal's evidence watermark can precede the
snapshot watermark because sealing and custody registration append events.
Signatures cover the manifest digest under `research.logical-checkpoint.v1`.
Verification requires expected manifest, source authority and custody identities
from outside the received envelope, using an explicitly supplied crypto provider.

The semantic layout digest fixes column order, cell types, nullability, primary
keys and UTF-8 binary text ordering for exactly these tables:

| Tables | Preserved state |
|---|---|
| `workspaces`, `records` | Authority counters and every domain record, including fencing history |
| `commands`, `events`, `outbox` | Exact acknowledgments, evidence and pending event delivery |
| `budgets`, `budget_reservations`, `budget_settlements`, `budget_entries` | Original liabilities, consumption and settlement responses |
| `recovery_snapshots`, `recovery_items` | Snapshot identities, retention state and exact binary contents |
| `credentials` | Credential digests, principals, expiry and revocation |

Every table appears in the manifest, including empty tables. Chunks have contiguous
global ordinals and table row positions. Signed descriptors bind their encoded
length, row count and digest. Primary keys must increase strictly within a table,
including across chunk boundaries. Text ordering uses UTF-8 bytes; JavaScript's
default UTF-16 ordering cannot substitute for it.

Cells contain only raw TEXT, strict exact integers within the interoperable JSON
range, SQL NULL, or `{ "blob_base64": "..." }` with canonical padded base64.
Boolean/float coercion, invalid UTF-8, changed column counts, noncanonical encoding
and duplicate row keys refuse. JSON stored inside SQL TEXT stays opaque: whitespace,
Unicode composition and original response bytes remain unchanged. Empty binary,
empty text and NULL remain distinct. Input contains no executable SQL or caller-
selected columns. Layout identity does not depend on SQLite DDL text.

Initial limits intersect: one MiB per encoded chunk, one MiB per manifest,
64 MiB across chunks, 100,000 rows, 4,096 chunks and 10,000 artifact references.
An inventory can hit its byte ceiling before its item count. A row that does not
fit a chunk refuses; this profile has no implicit continuation or truncation.

Wire verification checks provenance, canonical encoding and complete ordered
coverage. It does not prove that the source SQL cut was sealed, that its relational
contents are valid, that artifact bytes exist, that native execution ended, or that
a target may activate. The exporter must reject unsupported columns,
foreign-workspace rows and unaccounted application state. `research_schema` is
verified as version metadata; `hosted_authority_config` and `hosted_provisioning`
are bound through source identity digests and must never be copied as active target
configuration. Provider internals require explicit exclusion. Export, inactive
import, original-source election and provider recovery remain separate work.

Golden layout/manifest/chunk digests and tests cover raw value preservation,
coverage/order mutations, forged signatures, wrong trusted identities and bounds.
This format has no deployed-provider qualification yet.

Logical schema admission is separate from ordinary service reopening. It requires
exact portable column order, declared types, nullability, defaults and primary keys;
it also checks uniqueness, key collation/direction and foreign-key actions.
Additional uniqueness constraints refuse unless they duplicate an admitted key.
Nonunique indexes may be omitted because they contain no independent research
state. These checks do not claim arbitrary DDL equivalence for every possible
CHECK, conflict policy or deferrability clause.

The Cloudflare adapter admits exactly the portable tables plus the three metadata
tables above. Unknown application tables, views, triggers and generated columns
refuse. Reserved `sqlite_`, `__cf_` and `_cf_` names are excluded case-insensitively
before table inspection, including the protected KV and alarm state described in
[Cloudflare's storage documentation](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
and [workerd's metadata implementation](https://github.com/cloudflare/workerd/blob/main/src/workerd/util/sqlite-metadata.h).
It checks metadata layouts, schema version and the complete provisioning seal in
the caller's SQL snapshot. Native tests cover drift and retained revocations; the
workerd storage probe exercises the exact bounded PRAGMA queries. That extension
passed 19 actual workerd tests in 368.69 seconds at
`27895cdaa275063ff78d77a586a258b2adc6afe0` in
[qualification 41213290280](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41213290280).
Linux passed 772 native tests in 322.55 seconds and macOS passed 772 in 378.79
seconds. This receipt predates capture, fingerprint and import runtime probes.

The internal `checkpoint_capture.capture_checkpoint()` source port captures a
complete image in one consistent `StateStore.read()` callback. It authenticates
the deployment's current, nonexpiring root provisioning credential, checks the
physical store and full deployment seal, and requires an externally supplied
custody pin. It checks the persisted provider's complete authority and configured
source without requesting a new witness. Qualified and unqualified clock history
remain evidence; capture grants neither state execution nor requalification.

Before reading portable payloads, scalar SQL queries check bounded row counts,
cell types and raw UTF-8 byte lengths, including embedded NUL. Only after raw
admission passes does a separate query measure escaped TEXT with SQLite's
[`json_quote`](https://www.sqlite.org/json1.html#the_json_quote_function), without
parsing embedded JSON or returning payloads. Integer/BLOB framing remains
conservative. Metadata and operator projections are bounded separately
before parsing. The initial source profile permits 4 MiB of encoded chunks, 4,096
rows, 128 KiB chunks, a 64 KiB conservative row bound, 1,024 chunks and a 128 KiB
manifest. It reads at most eight rows per query. These source limits are narrower
than the wire format. Final canonical row/chunk/image checks retain every byte cap.
They bound capture allocations, not total runtime RSS. The complete source RPC
passed before the subsequent election-anchor extension described below.

Explicit columns and binary primary-key pagination preserve raw TEXT/BLOB values.
Chunk boundaries depend on encoded rows, not page size. No cursor survives the
synchronous callback, and the result retains encoded chunks instead of a second
complete decoded database. Capture writes no records, uses no R2 access and
exports no deployment secrets. The caller may sign its manifest with the separately
held source custody key. Publication must still verify artifacts and revalidate
the source after external I/O. This internal port is not yet a public backup API,
an inactive importer, a source election service or an activation path.

Native tests verify deterministic byte preservation, independent-connection
snapshot isolation, changed authority/custody refusals, revoked operators, clock
outages and every source bound. The workerd storage probe additionally exercises
the shared preflight and pagination code with Unicode ordering, embedded NUL and
binary cells. The shared helper passed in the
[native-import qualification](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41215181158).
The subsequent [full source-RPC probe](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41216865957)
exposed a sixfold TEXT estimate rejecting ordinary saved reports. A native SDK
regression now preserves both reports through seal/capture; the updated workerd
probe also carries large ASCII and every JSON control character. The
[corrected run](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41218485151)
passed 20 workerd tests in 469.14 seconds at
`8342b6829b9bcd4c2979befddd43a53351bcc83e`. That revision predates election anchors.

`checkpoints.cut.inspect_sealed_cut()` provides read-only relational inspection
through the shared SQL port. It checks the sole workspace, authority and seal,
the exact worker and unresolved-attempt inventories, closed uploads and readers,
artifact generations, retained references, storage bounds and custody identity.
Foreign-workspace rows refuse. The logical exporter must require both custody and
immutable seal history. Native v1 verification permits absent history from older
bundles, but rejects a conflicting history row when present. Native schema,
integrity, private-file and operator-token checks remain in the native wrapper.
The inspector does not read artifact bytes or authorize restoration.

## Bounded private checkpoint transport

The private RPC transport in `checkpoint_wire.py` carries the signed envelope
and ordered chunks as canonical base64 fields in one ASCII JSON string. It caps
the complete string at 6 MiB before parsing, rejects duplicate/unknown fields and
noncanonical base64, and enforces the source profile's envelope, chunk, count and
aggregate byte limits before decoded allocations. The codec preserves opaque
bytes; the importer remains responsible for signature, identity and SQL validation.

Cloudflare documents a 32 MiB serialized RPC limit and structured-cloneable values
for Python/JavaScript service calls. This profile uses a string to avoid transferring
Python models, tuples or proxies. The maximum source byte profile fits below the
6 MiB cap in native tests. The full private RPC passed an exactly 6 MiB padded
string in the corrected workerd run; maximum decoded-image memory remains unqualified.
Sources: [RPC limits](https://developers.cloudflare.com/workers/runtime-apis/rpc/#limitations),
[Python RPC](https://developers.cloudflare.com/changelog/post/2026-08-03-python-javascript-rpc/).

## Private source checkpoint API

Production `Bootstrap` and its fixed `ResearchAuthority` expose binding-only
`checkpoint_seal(credential, request_json)` and
`checkpoint_capture(credential, request_json)`. Both authenticate bootstrap and
the fixed physical target; the DO repeats those checks. `Default` exposes neither
method, and HTTP checkpoint routes remain absent. The ordinary agent API is
unchanged. This source API has native transaction tests. The actual production
workerd probe now exercises the private RPC with existing project/artifact/report
evidence, validates signed bytes, loses replies and restarts before exact replay
during a witness outage. It checks default/public denial and no additional witness
calls. The corrected workerd run above passed that source RPC and restart flow.
Full decoded-image memory and deployed service qualification remain separate.

The optional protected `RESEARCH_CHECKPOINT_SOURCE` binding contains:

```json
{
  "schema_version": "1.0",
  "deployment_digest": "<exact deployment digest>",
  "authority": "<complete WitnessAuthorityScope object>",
  "pin": "<BackupCustodyPin object>",
  "custody_seed_hex": "<separately generated 32-byte custody seed>"
}
```

This is a shape illustration, not a deployable credential file. Keep the binding
in protected operator custody across restarts; neither RPC generates a replacement
key. The loader checks the complete deployment and authority, pin epoch, public
key possession and separation from assignment/dispatch keys before opening state.
It loads only after bootstrap authentication. Secret representations and errors
are redacted. Existing `RESEARCH_SECRETS` configuration stays unchanged.

The seal request contains `schema_version`, a stable UUID `request_id`, the
`expected_revision` of the workspace gate (zero when absent), and a `seal` object
using the shared `SealWorkspace` contract. It must name the deployment's current
controller and epoch. The source executes the existing seal and custody commands
under one outer SQL write and the DO exclusive gate, with separate stable
idempotency keys and command identities. Each command checks current root credential validity, full schema
and deployment, source bounds, provider authority and existing custody before
consulting saved command outcomes. Revocation therefore also refuses a retry.

The existing commands execute through nested SQL savepoints. Before the outer
commit, a complete capture validates the generated cut, records, acknowledgments
and manifest against the source profile. An interruption or capacity refusal rolls
back both commands, including their events and acknowledgments; no partly sealed,
unexportable authority remains. Retry the exact request after a transport failure;
do not change its request ID, reason, revision or backup ID. A lost final reply
returns the original two receipts without new events. A
different preexisting custody pin refuses without replacing it. Neither phase
claims that a running experiment has stopped or releases its reserved resources.

Capture accepts `schema_version` and `backup_id`. It returns the bounded transport
string above, signing only the manifest constructed by verified source capture.
There is no arbitrary-message signing method. An unchanged source produces the
same bytes; a changed cut produces a new image identity. The operator must retain
the exact exported bundle for a later target import. These operations need no
fresh clock witness or R2 access. Artifact rows remain catalog evidence, and
original-source election, byte restoration and target activation remain separate.

## Shared inactive restore transaction

`checkpoints.inactive.restore_inactive_in()` extracts the existing native restore
transition behind `SqlSession`. It consumes a verified `RestorationCut` and a
caller-owned transaction. It archives worker sessions, records missing worker
fences, advances the control epoch once, changes the workspace gate to `restoring`,
and appends an inactive receipt with its events and outbox entries. Unresolved
attempts and budget reservations remain unchanged. It neither opens a database
nor commits independently, so a future logical import can include row insertion
and this transition in one atomic write.

Native restoration retains its existing bundle verification, private files,
source custody, target intent and preservation checks. The extracted module has
no imports of the native runtime, SQLite driver, file locks or native crypto
implementation. Its optional timestamp is metadata only; it grants no timing or
execution authority. Tests retain the existing native restore scenarios and prove
that an interrupted outer transaction rolls back both staged rows and restoration.

This internal primitive does not authorize source bytes or a target. Hosted logical
image import, original-source election, R2 recovery and fenced activation have
separate ownership and provider qualification boundaries.

`checkpoints.fingerprint.fingerprint_state()` supplies a separate inactive replay
primitive. It validates portable schemas, preflights counts/types/lengths, then
streams the complete twelve-table state through the same bounded binary paginator.
Its `research.logical-inactive-state.v1` hash binds schema and layout identities,
all table counts (including empty tables), table names and canonical typed rows.
Each byte string has an ASCII byte-length prefix, so concatenation cannot conflate
rows, binary values, NULL or text. SQL page size does not affect the result.

The initial inactive profile permits 32 MiB of conservative row-size estimates and
32,768 rows, providing headroom beyond the source image for worker archives, fences,
events and outbox entries. It retains pages and a running hash, not another encoded
image. Overflow refuses instead of truncating evidence. A future import transaction
must save this fingerprint with its trusted target intent atomically and compare it
before returning an exact inactive retry. It must separately check physical target,
deployment identity, complete owner-table roster, gate and restoration phase.
The fingerprint alone is not an authority check, nor protection against someone
rewriting both the trusted fingerprint and its database. Later legitimate recovery
changes must not silently replace it to make old import verification succeed.

Native tests pin a golden digest, modify evidence in every portable table, test
addition/deletion and page independence, and cover bounded recovery headroom. The
workerd capture probe also checks unchanged and changed fingerprints; its extended
runtime receipt remains pending.

## Private logical inactive import

`checkpoint_import.import_checkpoint()` is an internal source-byte ingestion port.
It requires the protected target bootstrap credential and checks its digest against
the fixed target manifest before SQL access. The immutable `LogicalImportIntent`
binds source manifest digest, restore UUID, actual target backend/location, target
deployment digest, restoring operator and target custody public key. The caller
holds the target private key separately; its possession is checked before import.
The source custody key cannot be reused as the target key. The owning integration
must generate and protect a fresh key for each target.

The target must have a different physical authority and activation, the same
workspace/controller, and the next control epoch. This initial profile requires
the target provisioning operator to match the image operator. Source authority
and custody expectations arrive from the trusted caller, separately from the
signed envelope. The entrypoint verifies the envelope and every chunk itself;
passing a caller-constructed verification object is insufficient.

Before copying, the serialized write checks that every portable table is empty
and that the complete application-table roster contains only the research schema
and the initialized portable tables. Credential schema creation, when needed,
is part of the transaction. Existing deployment/provisioning metadata, foreign
tables and application triggers refuse. Explicit fixed-column inserts preserve
raw values; there is no replacement, credential reinstallation or normalization.
After insertion, schema/FK checks, scalar profile preflight, exact row counts and
strict source-cut inspection must agree with the signed manifest. The imported
provider must retain its source authority. No target witness is acquired here.

The same write runs the shared inactive transformation, checks its receipt and
phase, and saves the original intent, receipt and full portable-state fingerprint
in target-owned `hosted_checkpoint_import` metadata. Source rows, worker copies,
epoch change, events, outbox and this metadata commit together. An interrupted
insert, transform or fingerprint leaves the target unchanged. The source's
credential revocations, historical acknowledgments, budgets and binary recovery
items remain exact; source workers and native process custody are untouched.

An exact retry reauthenticates target bootstrap, reverifies source bytes and pins,
then checks the target roster/schema, exact intent, complete fingerprint, receipt,
epoch and inactive gate in one serialized snapshot. It returns the original
receipt without rerunning the transformation. Changed, added or missing rows,
extra deployment metadata and different target identities refuse. Later legitimate
recovery transitions require a separate historical receipt lookup; they must not
refresh this fingerprint to claim the original import still matches.

The original import, capture and fingerprint helpers passed the native matrix at
`253dc09ee51431fc7ace2e84ebd591ca330e73d9`: Linux 854 tests in 361.58 seconds,
macOS 854 in 396.16 seconds, and Windows 848 with six skips in 1114.70 seconds.
The same [qualification run](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41215181158)
passed 19 actual workerd tests in 484.51 seconds, including the shared capture and
fingerprint probe. That runtime receipt predates source and target RPC and does
not qualify actual workerd import.

The import port's native SQLite tests cover rollback
at all three mutation phases, independent-connection races, invalid signed cuts,
changes in all twelve tables, changed bootstrap pairings, exact worker archival
and source preservation. Actual workerd import subsequently passed at `8342b68`
through the fixed source/A/B production RPC probe described below.
No target R2 bytes are fetched or declared available.
Imported artifact rows are historical catalog evidence; byte restoration and
verification must precede activation. The target has no completed provisioning or
authority configuration and cannot open through the ordinary production router.
Original-source election, recovery reconciliation, credential compatibility and
activation remain mandatory. Re-exporting a subsequently recovered authority will
also require explicit admission of its recovery metadata and custody history.

Production now exposes `Bootstrap.checkpoint_import(credential, bundle_json)` and
the matching fixed DO method. `Default` and HTTP expose neither operation. The
private target binding `RESEARCH_CHECKPOINT_TARGET` has exactly these fields:

```json
{
  "schema_version": "1.0",
  "intent": "<LogicalImportIntent object>",
  "expected_source": "<WitnessAuthorityScope object>",
  "expected_custody": "<BackupCustodyPin object>",
  "custody_seed_hex": "<separately generated 32-byte target custody seed>"
}
```

This is a shape illustration. Keep the key and intent in protected operator
configuration across retries/restarts. Requests carry only the credential and the
opaque bundle; they cannot replace the source pins, target identity or import
intent. Both private entrypoints authenticate and resolve the fixed target. The
Bootstrap entrypoint checks the 6 MiB ASCII string bound before forwarding. The
DO loads the bounded target binding after authentication and rejects reused
assignment/dispatch key material or inconsistent source/custody scope.

`verify_import_inputs()` checks target identity, key possession, signature and
the complete pinned source image before the target factory can initialize SQL.
The importer repeats that bounded verification inside the exclusive operation;
the precheck result is not accepted as mutation authority. On a validly signed but
relationally invalid image, a fresh factory may retain initialized empty schema,
but the importer rolls back all imported domain state. Existing completed
deployments refuse import. Imported state refuses ordinary provisioning and
router opening. No target credential installation, witness or R2 operation runs.

The wrapper's native tests cover pre-factory refusal, concurrent exact delivery,
fresh runtime-owner replay, changed protected inputs, invalid signed cuts and
mutual exclusion with provisioning. The actual multi-target workerd probe uses
three separately named production Workers and fixed private source/A/B bindings.
It discovers physical identities before installing protected configuration,
captures ordinary project/artifact evidence, refuses A's intent at B, and imports
at A with lost-reply and restart replay. B then receives its own protected intent
and key and imports a separate inactive copy. Neither target becomes active.

The local-only operator pads the real captured JSON with trailing ASCII spaces.
It sends 6 MiB plus one byte first and requires refusal, then sends exactly 6 MiB
and requires successful import. This exercises the inclusive RPC string limit
through Bootstrap and the DO while preserving every signed envelope/chunk byte.
Public HTTP carries only small fixture selectors and credentials. The probe also
checks public/default denial, refusal of ordinary provisioning/router access,
unchanged source capture, and no witness calls. The corrected workerd run at
`8342b68` passed this complete probe before the election-anchor extension.
This transport-size probe does not establish maximum decoded-image memory usage,
artifact transfer, source election or activation.

The logical election wire contract binds the complete `LogicalImportIntent`,
including physical target and deployment, to the full original source authority
and source deployment digest. `LogicalElectionRequest` embeds the unchanged native
`SignedRestoreRequest`; its outer signature comes from the target custody key.
`LogicalElection` embeds that signed request and the unchanged `SignedRestoreClaim`;
the source custody key signs its outer envelope. Separate
`research.logical-election-request.v1` and `research.logical-election.v1` digest
domains preserve existing native preimages.

`checkpoints.elections` verifies all four signatures against caller-supplied target
intent, backup ID, source scope, source deployment and custody pins. The backup ID
must come from the independently verified image or restoration receipt, never
from the incoming request. It checks cross-layer
restore, image, workspace, controller and next-epoch identity. A stripped native
claim, changed physical destination, shared custody key or exhausted epoch refuses.
These bounded records and pure verifiers supply no storage, election or activation
authority. The private admission path below owns the recovery transition.

The shared database accepts an owner-supplied `RecoveryAdmissionVerifier` through
`Database.from_store(..., recovery_verifier=...)`. It receives the current SQL
snapshot, physical store identity, caller context, restoration receipt, admission
and crypto provider. The synchronous hook may read that snapshot and verify
signatures; it must not perform I/O, mutate state, sample clocks or start nested
commands. Returning an awaitable or a value refuses capability creation.
`Database(path)` installs the native SQLite verifier. A generic adapter defaults
to no recovery verifier, even if its records contain a valid native admission.
Runtime construction owns this capability; API callers and research project
manifests cannot enable it. This port alone does not enable hosted recovery.

Logical admissions carry a target-signed `logical_election_digest` over the full
`SignedLogicalElection`, including its outer signature, in the distinct
`research.logical-election-envelope.v1` domain. Their companion record uses kind
`logical_recovery_election`, the restoration ID, immutable state and revision 1,
with no campaign or parent. Recovery guards recheck its bounded payload and digest
against the already verified admission at the transaction boundary. The companion
cannot replace the native claim inside the admission. Native admission omits the
optional field entirely, retaining existing canonical bytes and signatures.
Native-only verification, worker routes and activation refuse the logical profile
until a deployment-aware route explicitly verifies its proof and destination.

Private `Bootstrap.checkpoint_admit(bootstrap_credential, election_json)` opens
only the already imported fixed target. Protected configuration supplies the
operator and custody; copied source credentials cannot authorize this RPC. First
admission checks the complete inactive fingerprint and source seal, verifies all
four election signatures, and signs the admission with the protected target key.
One outer SQL write retains the election companion, fences orchestration, drains
running campaigns, changes the workspace gate to `recovering`, and stores the
exact acknowledgment. Any failure rolls back the whole transition, including
the companion and acknowledgment. The receipt does not close native processes,
release reservations, settle budgets, accept scientific results or activate jobs.

Exact retries validate current protected deployment, immutable import history,
signed admission, retained election, research fences and original acknowledgment.
They do not require the pre-admission fingerprint after legitimate recovery writes.
Missing or reopened fences refuse both replay and recovery capability use; the
transaction guard rechecks them after capability minting. Native tests cover
independent SQLite connection races, lost replies, rollback after actual SDK
campaign fencing, retained funding and pending turns. Production workerd admission
qualification remains pending. Ordinary hosted routers still refuse this target;
evidence delivery, artifact restoration and activation require separate support.

The production qualification probe now uses a sixth runtime startup to replay
admission after a deliberately lost accepted reply. It verifies the target's
signature and complete election digest, refuses the losing target and public
entrypoint, and checks that provisioning, import, request generation and ordinary
router access cannot reopen the recovering target. Its fixture drops only a
recognized accepted admission; unexpected failures remain visible. This probe
requires a new passing workerd receipt before hosted runtime qualification.

`Bootstrap.checkpoint_turn(bootstrap_credential, request_json)` admits only
`orchestration.reconcile_turn` and `orchestration.close_turn` on the recovering
fixed target. The request supplies a stable UUID `request_id`, optional
`expected_revision`, operation and its ordinary typed payload. Close requires the
original turn revision, epoch and finding; reconcile uses the turn ID. The target
operator comes from protected configuration. Each call verifies admitted recovery
and the original fenced turn/owner before reading even an existing acknowledgment.
The shared `OrchestrationControl` performs the transition under the same SQL write;
its action dispatcher always refuses. A retained target acknowledgment produces
the existing receipt semantics; absence permits explicit unacknowledged closure.
Neither operation resumes a provider, launches an experiment or releases funding.
Native adapter tests cover both branches, independent connection races, a lost
first committed reply, restart replay and changed-owner refusal. Actual workerd
turn-maintenance qualification remains pending.

The six-startup probe additionally binds a real source driver and saves a report
turn through the SDK before sealing. After admission, the winner closes that turn
without an action acknowledgment, loses its accepted reply, and replays both
admission and closure byte-for-byte after restart. The loser, public entrypoint and
attempted execute-turn remain refused. The saved action remains unexecuted.

Private `Bootstrap.checkpoint_request(credential)` and its fixed target DO method
generate a request from protected target configuration, with no caller-supplied
destination or source digest. They repeat bootstrap and physical-target checks and
use `open_existing`; they never initialize an empty target. The request operation
holds the local exclusive gate through one read snapshot and signing.

That read verifies the complete target schema, saved intent, all portable rows via
the retained fingerprint, restoration receipt, next epoch and inactive gate. It
also compares the original gate seal with immutable seal history and the receipt's
worker/attempt inventories. The imported marker supplies source deployment identity
after its source scope and custody agree with protected pins. The marker remains
historical evidence here; the source-only election table is neither required nor
recreated at the target.

The target signs the existing native request and its hosted wrapper, verifies the
result, and returns canonical bytes. It writes no acknowledgment or record and
generates no new ID or timestamp. Unchanged imported state and retained key yield
identical retries. Native tests cover the full import/request/source-election path,
concurrent read-only retries, empty and legacy targets, changed configuration,
corrupt receipts/seals/anchors and signer failure. Workerd qualification of this
request RPC passed in the workerd receipt below; requesting election cannot activate the target.

A fresh source seal also registers `LogicalElectionAnchor` in the portable record
ledger and its matching row in the original-only `hosted_checkpoint_elections`
table. Registration, its event/outbox, seal/custody acknowledgments and final
capture-capacity check share one transaction. The marker binds the complete source
scope, physical store, deployment and custody; it contains no future image digest.
The captured image therefore includes the marker without a circular digest.

Source admission checks the complete pair before replaying commands. A missing
table, missing row, orphan metadata or changed binding refuses without recreation.
The mutable election table is explicitly admitted by the source schema inspector
and excluded from every portable image. Inactive imports preserve the marker as
historical evidence and cannot acquire the source's election authority. Legacy
images without markers still verify, capture and import; replaying their old seal
through the new source RPC refuses instead of initializing unused election state.
No automatic legacy upgrade is defined.

Private `Bootstrap.checkpoint_elect` and its fixed DO method implement the
`open → elected` transition through one serialized source write. Current bootstrap,
root operator, physical deployment, provider history, schema and anchor admission
precede both new elections and cached replies. A new request must pass both target
signatures and match the complete freshly captured sealed image. Source and target
locations/deployments must differ, and this initial profile retains one operator.
The source signs a native claim plus its hosted envelope and persists them in the
same transaction. No witness or R2 call runs.

On replay, the operation verifies the persisted proof and incoming target proof
before comparing the complete signed requests. A competing valid destination
refuses; an exact retry returns the canonical persisted bytes without a new claim
ID, timestamp, signature or capture. Neither transition appends a portable event
or command acknowledgment, so the selected image stays unchanged. Native tests
exercise independent-connection races, lost commit replies, current-credential
revocation, missing anchors, signature corruption and source-cut drift. The private
RPC passed in the workerd receipt below. Election alone cannot activate a target.

The extended three-Worker probe now generates a request at each completed target,
checks denial at an empty target and at default/public entrypoints, and sends both
valid requests concurrently to the original source. Its fixture drops both replies,
including the committed winner's. Retries must reveal one signed winner and one
conflict. A fifth workerd startup then verifies identical target requests and exact
winner bytes, continued loser refusal, inactive targets and unchanged source capture
during the same witness outage. At
`aeed14e090d6c0f25475583ce6bdd84cea1a0816`, the
[workerd qualification](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41219997884)
passed 20 tests in 758.34 seconds, including this five-startup extension. That
revision predates hosted admission and saved-turn maintenance; it does not qualify
their later six-startup probe.

Whole-store rollback/PITR of the original authority is not qualified:
restoring both marker and election row to an earlier state requires an independent
non-rollbackable authority before this protocol could safely elect again.

## Required implementation slices

1. Freeze witness schemas, signatures, epoch derivation and one-use operation
   context. Prove rejection and scope behavior with deterministic fixtures.
2. Add the private native witness service with exact clock/profile admission,
   bounded transport and independent-process restart tests.
3. Add the serialized Durable Object authority wrapper. Use the shared command
   kernel, explicit source requalification and persisted retirement fencing.
   Keep unsupported operations disabled until they have provider-safe inputs
   and artifact custody.
4. Supply provider-safe artifact staging, R2 verification and key custody. Inputs
   must be immutable and complete before the operation's witness point. Storage
   pins and cleanup retain their existing authorization and uncertainty rules.
5. Implement inactive export/import and fenced activation for Durable Object
   state and R2 artifacts. Local filesystem backup does not qualify this slice.
6. Run the shared transaction/race suite and native worker cycle against the
   local provider runtime. Then perform separately authorized hosted deployment
   and recovery qualification on the exact published revision.

Each slice is a cohesive commit boundary. None changes the original spec's
completion criteria or turns a local emulator result into hosted evidence.

## Counterexamples and acceptance tests

The conformance suite must cover at least these cases:

| Case | Required outcome |
|---|---|
| Complete result arrives after expiry | Fresh witness refuses ordinary acceptance. |
| Result witnesses before expiry; commit finishes later | Accept only under the declared exclusive linearization rule. |
| Revocation or expiry races with result | One serial order; no witness/state interleaving. |
| CPU stalls between witness and commit | No clock refresh fiction or authority extension; reset/replay preserves atomic outcome. |
| Witness transport stalls | No new admission; no capacity release. |
| Lost witness response | New operation context uses a fresh nonce. |
| Lost committed result response | Original idempotent outcome; no repeated accounting. |
| Different caller, scope, key or request digest | Reject before time-dependent authority. |
| Source clock reverses, suspends incompatibly or changes profile | Source epoch invalidates and remains invalid. |
| Source restarts | Changed epoch refuses until explicit provider requalification. |
| Retired source returns after replacement | Reject; old clock authority cannot reappear. |
| DO evicts with unchanged logical/source epoch | Reopen state; retain epoch; discard every old operation capability. |
| DO restore copies old keys and configuration | Remain inactive until fenced activation creates a new authority identity. |
| Asymmetric worker/DO/Container delays | Original complete-RTT interval remains conservative. |
| Challenge or assignment replay crosses epochs | Reject; never recompute an extended native cap. |
| R2 input is incomplete, changed or revoked | No result admission from a stale preflight snapshot. |
| Native worker continues during control outage | Preserve UNKNOWN custody and original hard deadline. |
| Provider reset occurs around transaction commit | Replay distinguishes committed outcome from absent command without assuming either. |

Local workerd tests establish adapter and protocol behavior. Native clock tests
establish the tested source implementation. A hosted run must separately record
the deployment revision, image/environment, service routing, clock metadata,
key lifecycle, restore behavior and actual native worker closure. M5 remains
unqualified until those required boundaries are demonstrated.
