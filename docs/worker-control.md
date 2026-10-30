# Worker operational control

The controller records three different facts: admission policy, worker presence,
and cancellation progress. A heartbeat is an observation. It is not a lease
extension, proof of native process closure, or authority to refund a reservation.

The shared command catalog exposes these operations through the Python SDK,
HTTP command endpoint, JSON CLI, and MCP. The scoped worker query endpoints also
accept the new read and list kinds. No new worker credential scope is required.

| Operation | Authority | Revision / ordering | Result |
| --- | --- | --- | --- |
| `worker.set_admission` | `worker.manage` | CAS on `worker_admission.revision` | Active or draining admission record |
| `worker.heartbeat` | `worker.publish`, exact enrollment owner | Strictly increasing heartbeat sequence | Presence and admission snapshots |
| `attempt.request_cancel` | `campaign.manage` | CAS on the attempt revision | Disposition and sticky control record, or `already_terminal` |
| Worker get, `worker_admission` / `worker_presence` | `worker.query`, exact enrollment owner | Current read | Own operational record |
| Worker list, `controls` | `worker.query`, exact enrollment owner | Insertion-watermark cursor | Requested cancellations, at most 100 per page |

## Independent records

Immutable enrollment continues to describe the worker's principal, binding,
profiles, and environment contracts. It does not change when the worker reports
presence or an operator drains it.

`worker_admission` has the full binding, operator identity, reason, and receipt
time. Its state is `active` or `draining`. An enrollment with no admission record
is active at revision zero. Its first state change inserts revision one. Setting
the same state is an accepted no-op and preserves its original reason and
revision. Operators can list admission records; absence means revision zero.

`worker_presence` records the full binding, last heartbeat sequence, driver
instance ID, and controller receipt time. Before the first heartbeat, the scoped
read returns `unobserved` at revision zero. Reads do not create either record.
Every accepted heartbeat advances presence only. An operator's admission CAS
therefore remains valid during unrelated heartbeat traffic.

The sequence is monotone across driver restarts within an enrollment. Gaps are
allowed. New command keys carrying old or equal sequences are rejected. Exact
idempotency replay returns the original acknowledgment without refreshing time.
A replayed admission snapshot can be old; read current admission and controls
before relying on them. A new driver instance does not reset the sequence.

Receipt timestamps are informational. Missing heartbeats do not establish that
a worker, supervisor, child, accelerator context, or reservation is gone.

## Drain linearization

Fresh claim admission reads worker policy inside the same transaction that
reserves attempt budget and issues its assignment. A committed drain blocks
fresh claims and removes the worker's offers. A claim committed first remains
valid and can finish. Replaying that accepted command still returns its original
grant after a drain.

Draining does not revoke credentials, mutate signed assignments, extend clocks,
or prevent input access, pending-work recovery, output upload, or publication.
Returning admission to active is an explicit operator command with the current
admission revision. It does not alter unresolved native work.

## Cancellation lifecycle

`attempt.request_cancel` creates one immutable intent identity for one admitted
attempt. The record contains its full original worker binding, dispatch digest,
sequence one, requester, reason, and receipt time. Repeated requests preserve
that original intent. There is no resume/uncancel operation in this version.

The request leaves the attempt, job, trial, signed grant, native capacity,
execution deadline, and budget reservation unchanged. A terminal attempt without
a prior control returns `already_terminal` and creates no pending work.

Workers fetch requested records through `WorkerQuery(kind="controls")`. Each
heartbeat may acknowledge up to 100 distinct controls using attempt ID, dispatch
digest, and sequence. Every acknowledgment must match the current worker's full
binding. The transaction validates the entire batch and commits acknowledgment,
presence, events, and outbox together. A failed item rolls back the batch.

The acknowledgment means the worker has persisted the cancellation locally.
The controller cannot observe that write directly; authenticated worker code is
responsible for honoring this contract. Acknowledgment does not claim closure.
It changes the record to `acknowledged` and removes it from requested delivery.

Only accepted native completion resolves the control, in the same transaction
that settles the attempt. Resolution records the accepted `attempt_outcome` and one of
`cancelled`, `not_started`, or `completed_without_cancellation`. Success racing a
cancel remains success, including its measurements and normal stage dependency
progression. An invalid workload report can make an attempt fail even when its native process
exits zero; the attempt outcome does not reinterpret the native receipt. A late acknowledgment
updates its timestamp without reopening completed control. UNKNOWN publication
keeps the control and resource uncertainty unresolved.

This command cancels one attempt. It does not stop a trial, campaign, descendants,
or another worker. A broader research stop requires its own explicit policy.

## Bounded delivery and recovery

Control pages are bound to actor, workspace, complete worker binding, and query
kind. A fixed row-ID watermark makes the scan finite. Acknowledgment during a
scan may remove pending rows; the cursor tracks scanned rows. Newly created
controls beyond the watermark appear in a fresh scan. A cursor is not an
acknowledgment. The requested-control index excludes completed history.

The controller can retain requests while a worker is offline. Drivers must
persist a received control before acknowledging it, synchronize before starting
new work, and poll independently while supervisors execute. They must replay
uncertain heartbeat delivery before advancing their sequence. Native journal
and supervisor gates remain the authority for preventing launch and stopping
owned process scopes.

This slice implements and tests the controller API and publication resolution.
Automatic polling and application in the remote driver is the next slice; this
API alone does not establish that an existing worker has applied a request.
