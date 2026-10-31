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
not establish actual workerd qualification for this new profile.

### Signed clock query boundary

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
