# Native worker execution

The core includes a native supervisor and worker journal. Controller scheduling,
resource admission, and the remote worker service are separate integration work.
These primitives do not authorize enrollment or execution on another machine.

## Authority and durable launch

`DispatchVerifier` authenticates a dispatch with an enrolled worker key. The v1
permit is an HMAC-SHA256 value over the canonical dispatch payload, with a named
algorithm and domain. The payload excludes only `launch_permit_digest`.

Verification binds the controller, workspace, worker, session, epoch, sealed
experiment, stage, reservations, and expiration. A plain digest match is insufficient.
Keep keys outside scientific records and command arguments. A remote transport must
also authenticate its peer, qualify clock handling, and enforce resource admission.

Only an internal `VerifiedDispatch` enters `WorkerJournal.receive`. The journal
checks the invocation against its sealed stage. The journal uses SQLite WAL, FULL
synchronous writes, an atomic event outbox, and one launch claim per attempt.
Two independent supervisor processes can race. Only one can claim the execution.

The launch sequence is:

~~~text
authenticated dispatch
  -> received
  -> durable launch claim
  -> native process held behind a start barrier
  -> native identity committed
  -> cancellation and deadline checked again
  -> release intent committed
  -> barrier released
  -> native outcome saved
  -> observation committed and queued for publication
~~~

A launch gap remains unresolved. Recovery does not spawn the command again or
overwrite a live supervisor's launch phase merely because its receipt is absent.
`reconcile_outcome` can recover a saved outcome after a journal publication failure.
It never signals a process using a persisted PID.

Cancellation is a durable request, separate from native closure. A prelaunch
cancellation has no fabricated process exit code or `RunResult`. Original dispatch
authority stays attached to observations after a restart. Acknowledgment replays do
not change the observed execution or repeat the workload.

## Supervisor lifetime and timing

The supervisor runs in a separate process from controller communication. Losing the
transport process does not remove the deadline watchdog. The supervisor translates
the granted remaining time to a monotonic local deadline at startup.

An independent watchdog enforces the native deadline even if a journal callback
blocks. The watchdog retains only the live owned native capability. The supervisor
joins it before reaping the leader or closing native handles. A supervisor crash can still leave
unknown execution on platforms without qualified lifetime containment.

`timeout_ms` bounds the supervised execution, including native startup.
`grace_ms` and `cleanup_ms` govern cancellation and closure observation. Cleanup
must leave time after graceful termination for forced termination. A signal request
does not prove termination. Missing closure evidence keeps the outcome unknown.

These timings do not replace a project's active-training clock. A project that
requires a trusted training timer still needs a qualified harness.

## Native boundaries

On POSIX systems, a small isolated Python bootstrap waits on a private pipe before
executing project code. The workload gets its own process group. The supervisor
retains the unreaped leader while inspecting or signaling the group, so that group
identity cannot be recycled underneath cleanup.

Group supervision is for trusted code. A process can escape a POSIX group.
Observed live escapees prevent closure. This profile does not contain unobserved
escapees. Stronger isolation needs a separate qualified backend. A process group
or source directory is not a security sandbox.

On Windows, `CreateProcessW` creates a suspended process inside a Job Object through
`PROC_THREAD_ATTRIBUTE_JOB_LIST`. There is no separate post-creation assignment
window. The job disallows breakaway and enables kill-on-last-handle-close. An explicit
handle list limits inheritance to stdin, stdout, and stderr. The initial thread is
resumed only after the durable release gate.

Windows closure requires the job's active process count to reach zero and the
primary process exit to be observed. A dead primary process alone is insufficient.
Headless Windows cancellation terminates the job. A qualified application protocol
can provide graceful cancellation before job termination.

## Output and environments

Each stream has a declared retention limit. The supervisor continues draining and
counting discarded bytes after the limit, so a full log cannot deadlock the workload.
The outcome records retained bytes, observed bytes, content digest, and EOF status.
Draining is nonblocking and bounded per turn. Windows uses `PeekNamedPipe` before
reading anonymous pipes. No unbounded in-memory log queue is created.

The default native environment copies only operating-system necessities. Controller
and provider credentials are excluded. Execution profiles must explicitly add their
required environment and secret references. Relative executables and relative PATH
entries resolve from the project working directory. Commands never run through an
implicit shell.

## Evidence and current limits

Real subprocess tests cover the start barrier, denied release, supervisor loss before
release, duplicate launch claims, descendant cancellation, surviving children,
deadline enforcement during a blocked callback, stream flooding, and durable outcome
recovery. A separate-process test removes the transport owner while the supervisor
continues to its deadline.

These tests qualify only the operating systems where they actually run. Passing a
type checker for Windows is not native Windows qualification. Physical device
exclusivity, remote enrollment, authenticated evaluator receipts, and scientific
promotion remain separate contracts.
