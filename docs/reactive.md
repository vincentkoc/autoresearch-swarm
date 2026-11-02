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

## Cooperative training hook

`ReactiveHook(journal, target)` owns a fresh session with the declared initial
controls. The workload provides synchronous `target.read()` and `target.apply()`
methods. They must be short operations on the declared controls. They must not
perform inference, block on network calls or replace optimizer implementations.

Call `await hook.boundary(observation)` between completed updates. The hook
yields once to cooperative tasks, polls for a response and returns without
waiting for a decision. A due observation creates a request with bounded recent
history. Delayed decisions expire at the declared step limit. An explicit
`step` argument records a later request/application boundary than the observation.

Only the boundary can apply a response. The hook checks current controls,
applies a changed target and reads back actual values before recording success.
A keep response or a set response with identical values performs no mutation.
Exceptions, unexpected control drift and readback mismatches record uncertainty
and stop later interventions. They do not restore model or optimizer state.

Use the hook as a context manager or call `close()` explicitly. Closing cancels
pending requests without waiting for inference. The workload still owns and
enforces its training deadline; the hook does not extend that deadline or bound
the execution time of arbitrary workload code.
