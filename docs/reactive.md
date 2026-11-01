# Training-time reactive decisions

A reactive session records bounded decisions inside one training attempt. It
does not create research trials, choose an incumbent, or authorize evaluation.
The immutable contract declares numeric controls, initial values, bounds,
training/development diagnostics, a policy fingerprint and decision limits.

## Local session API

`DecisionJournal.create(path, binding, contract)` creates a new SQLite session.
`open(path)` opens an existing session without changing its state. A journal
offers `inspect`, `get`, `pending`, `respond` and paginated `events`. A response
must identify its original request and control revision. Repeated identical
submissions return the recorded result; conflicting submissions fail.

The training process holds `journal.trainer()` as a context manager. That
exclusive native owner can `request`, `claim_application`, `record_application`,
`settle` and `close`. Only one request can be outstanding. Claiming application
persists the intent before the workload changes controls; the receipt must match
the request, proposed values, actual application step and control revision.
The journal does not mutate a model or verify workload readback itself.

Each transition appends an event and updates the current projection in one
transaction. Failed, expired and cancelled requests remain visible. A response
that arrives after settlement is retained as late and cannot reactivate the
request. Closing a session cancels pending decisions and prevents new requests.

## Recovery and trust

Readers never recover a training session. Another training owner must acquire
the native lock before recording interruption. An interrupted application
becomes uncertain; no completion or new control revision is inferred. Its
controls field retains only the last confirmed values. The session cannot
resume or repeat the application. A new training attempt needs a new session.

These APIs assume trusted local workloads, agents and filesystem access. The
diagnostic allowlist is a declaration, not proof that workload code has not
mislabelled a metric. It is not a Python sandbox or evaluator boundary. No cloud
adapter or hosted authorization is implied by the local journal API.
