# Controller commands

The Python `ResearchService` owns project registration, campaign activation,
candidate admission, and trial graph creation. Transports call these handlers.
They do not write records or interpret success independently.

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
