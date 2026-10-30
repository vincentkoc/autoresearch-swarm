# Controller commands

The Python `ResearchService` owns project registration, campaign activation,
candidate admission, and trial graph creation. Transports call these handlers.
They do not write records or interpret success independently.

## State authority

The command kernel depends on a synchronous `StateStore` and `SqlSession` contract.
The default `SQLiteStore` owns native connections, WAL, migration setup, busy
timeouts, filesystem backup and SQL savepoints. `Database(path)` keeps the local
construction path. `Database.from_store(store)` binds an already initialized store
to the same command implementation; it does not create a second set of domain rules.

`read(callback)` provides a consistent snapshot. `write(callback)` serializes the
outer mutation. `session.atomic(callback)` creates a nested rollback boundary.
Callbacks run once and must finish synchronously, materialize their returned data,
and avoid external I/O. A callback must not return an awaitable or a live cursor.
Rows support named and positional access; `one()` requires exactly one row.
Constraint violations use the portable `IntegrityViolation` exception. CAS uses
`UPDATE ... RETURNING`, without interpreting provider billing counters as row counts.

The command kernel checks exact acknowledgment replay before the nested operation.
If that operation raises `ResearchError`, its writes roll back first. An optional
conservative rejection hook then runs in the outer transaction, followed by the
rejected acknowledgment. Both commit together. Hook failures, unexpected exceptions
and invalid response serialization roll back the outer transaction. A durable
rejection is raised to the caller only after its acknowledgment commits.

`StoreIdentity` identifies the actual transaction authority. Services sharing a
ledger compare that identity rather than inventing filesystem paths for hosted
stores. Native connection escape hatches and filesystem backup explicitly refuse
stores that cannot provide those capabilities.

The command, replay, bootstrap, record, snapshot, event and notification paths use
these callbacks. Credentials, campaign queries, scoped worker/proposer reads,
orchestration turn inspection and recovery page/chunk reads use the same boundary.
Credential installation keeps its lookup and conditional insert in one write;
queries retain their ownership checks and pagination bounds in one snapshot.
Worker query authority does not grant global research read access.

Artifact and execution-evidence preflights also use callback reads. Upload lifecycle
guards, byte transfer, verification, source extraction and journal parsing remain
outside those reads. Original evidence stays pinned through the final command;
commit-time checks revalidate its ownership, generation, revision and policy.
Finalize/cleanup preserve acknowledgment replay both before and inside the lifecycle
guard. Successful publication remains acknowledged if subsequent cleanup fails.

The complete hosted service remains unqualified until native clock initialization,
key custody and artifact operations have provider implementations. Real local
training/evaluation and late-result reconciliation tests forbid native connection
access and check that blob I/O stays outside SQL callbacks. The
[Cloudflare runtime fixture](../adapters/cloudflare/README.md) runs the shared command
kernel and schema migrations through its actual SQL adapter.

## Worker admission

`worker.enroll` binds an operator-approved worker session and principal to explicit
engine capabilities. The controller does not infer hardware from a name.
`worker.claim_job` requires the current job revision and the enrolled principal.
It checks campaign admission, dependency success, input availability, engine and
isolation requirements, worker concurrency, and campaign trial concurrency.

One transaction creates the Attempt, reserves its attempt-count and execution-time
budgets, updates the Job and Trial, and records a dispatch event. Retries with the
same key return the original grant. Competing workers cannot claim one job.
The native machine registry independently reserves physical scheduling capacity
before launch. Controller admission alone does not prove that a process started.

Native cleanup allowance is part of the execution reservation. Budget ownership
belongs to the Attempt, not the Trial. A later execution retry needs a new attempt;
a delivery retry preserves its original attempt and reservation.

Enrollment replacement requires explicit fencing. The current implementation
resumes an existing session and rejects implicit replacement. Protected evaluator
admission remains unavailable until its isolation profile is qualified.

## Native observation publication

`worker.publish_attempt` accepts verified artifact references from the enrolled
worker principal. It checks the original dispatch, immutable experiment, current
job attempt, profile, seed, input identities, timing contract, native closure, and
log identities. A training process cannot use proposal authority to publish results.

An accepted closed result settles the original reservation exactly once, records a
descriptive RunResult, updates its Job and Trial, and resolves dependencies in the
same transaction. A failed predecessor prevents its dependent stages from becoming
ready. Uncertainty preserves both cost custody and trial concurrency. A valid late
closure settles that same attempt; a changed accepted publication is a conflict.

The original CPU project now completes train → evaluate through this worker path.
Its native evaluation report does not grant scientific acceptance: the authenticated
evaluation-request/receipt service and promotion policy validate separate evidence.

## Immutable project revisions

Register a verified source artifact with `RegisterProject`. The controller checks
every source entry against its manifest, then resolves the project contract from
that bundle. An artifact pin protects the source through the acceptance transaction.

Registration creates a logical Project and an immutable ProjectRevision. A later
registration can update the Project's current revision with an expected revision.
An existing Campaign retains its original ProjectRevision.

Source reads occur before the write transaction. A committed retry returns its
original response before it tries to read source again. A concurrent retry repeats
the idempotency check inside the write transaction.

## Campaign activation

Create produces a draft Campaign and budget accounts atomically. Validate checks
the contract without activating it. Seal freezes the contract and moves it to
ready. Start moves a sealed campaign to running.

The initial native execution contract requires two cumulative budget dimensions:

| Dimension | Unit | Meaning |
|---|---|---|
| `attempts` | `count` | Each admitted physical execution attempt |
| `execution_ms` | `ms` | Native execution occupancy, including cleanup |

Additional dimensions remain explicit. Capacity and evaluation exposure have
different accounting semantics. Starting a campaign does not reserve all of its
future experiments.

Constant input artifacts form part of the campaign protocol identity. A stage's
required input name must have exactly one provider: a declared direct predecessor
or a campaign input. Missing and ambiguous providers prevent sealing.

## Candidate and trial admission

Propose preserves a candidate's source, hypothesis, and parent identities.
Parents must belong to the same project and campaign. Static validation compares
the candidate against the campaign's frozen baseline and mutation rules. It does
not trust a manifest supplied by the candidate as new authority.

Rejected mutations remain visible as candidate validation records. Static source
validation does not claim syntax, shape, runtime, or scientific correctness.
Compute checks belong to accounted stages.

Submit requires a running campaign, its current revision, and a matching valid
candidate validation. One transaction creates the sealed ExperimentSpec, Trial,
all stage Jobs, and their events. Root jobs become ready. Dependent jobs remain
blocked. The transaction also enforces the declared trial-count limit.

A queued Trial has no scientific score and consumes no execution reservation.
Worker claims, execution result acceptance, evaluation, and promotion are later
operations. They must preserve these distinct boundaries.

## Verification

The controller tests cover immutable revision pinning, protected evaluator edits,
allowed training edits, concurrent duplicate delivery, cross-campaign references,
atomic trial limits, transaction rollback, transient source failure, and corrupted
source content. The native runner and transport integration have separate proof.
