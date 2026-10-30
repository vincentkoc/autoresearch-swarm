# Controller commands

The Python `ResearchService` owns project registration, campaign activation,
candidate admission, and trial graph creation. Transports call these handlers.
They do not write records or interpret success independently.

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
evaluation-request/receipt service and promotion policy are separate remaining work.

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
