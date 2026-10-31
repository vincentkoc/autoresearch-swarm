# Hosted authority with native clock witnesses

Status: implementation design. This profile is not enabled or qualified. The
existing [hybrid profile](cloudflare.md) remains the supported Cloudflare path.
The original M5 requirement for authoritative Durable Object state and hosted
recovery remains open.

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
