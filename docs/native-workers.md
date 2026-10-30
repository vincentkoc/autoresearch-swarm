# Native worker execution

The core includes native supervision, a worker journal, `LocalWorker`, and a local
async campaign driver. [Remote CPU enrollment](remote-workers.md) uses explicit
operator bundles and a separate scoped HTTP driver.

## Run a local campaign

After submitting trials through the API, SDK, CLI, or MCP, run:

~~~sh
research worker run CAMPAIGN_ID
~~~

This command automatically enrolls a local CPU worker and evaluator, then drains
the campaign's executable stage graphs. Use `--cpu-slots 2` to admit two CPU slots.
Keep the same value on resume. Changing an established profile requires recovery.
`--state-dir` selects the controller before the `worker` subcommand.

The driver uses the same revision checks, budgets, and command service as agents.
It authorizes evaluation before native execution and completes receipts with the
separate evaluator identity. It preserves failed stages and invalid receipts.
It does not propose candidates, select an incumbent, or complete the campaign.

The JSON result reports `idle`, `paused`, `blocked`, `terminal`, or `stopped`.
It includes counts and up to 50 attempt IDs and findings. `blocked` exits with
code 3. A command error exits with code 2. An idle worker does not imply campaign
completion. External agents can submit more work and invoke the driver again.

The driver persists command envelopes before delivery, including route, payload,
revision, and idempotency key. Lost acknowledgments replay the same command.
A known capacity rejection permits a new scheduling decision. A failure after
attempt admission resumes that attempt instead of creating another one.

One native lock owns the worker driver. Restart reconciles its admitted attempts
before claiming new work and completes pending receipts independently of the
ready queue. Uncertain execution retains capacity and blocks further admission.
Work admitted for another campaign requires that campaign's driver to resume.

The foreground process exits when no more work can progress. It is not a background
daemon. On the first interrupt, its async task drains already launched native
execution before releasing driver ownership. A forced process termination still
requires journal reconciliation on restart. Terminal loss does not prove closure.

Python callers can use `await LocalDriver(client, enrollment).drain(campaign_id)`.
This driver accepts local transports. `ScopedWorkerDriver` supplies outbound HTTP
execution with restricted worker authority. Hosted workflow adapters remain separate.

## Local enrollment

`ensure_local_enrollment(runtime)` prepares the Python CPU worker and a separate
authenticated evaluator. It persists both identities and private credentials before
provisioning them. Repeated calls resume the same worker session. A crash between
credential creation and enrollment does not create another identity.

The worker can claim and publish native attempts. It cannot publish evaluator
receipts. The evaluator can publish receipts but cannot claim jobs. Both clients
resolve their credentials on each request, so revocation takes effect immediately.
These roles separate authority. They do not create a protected evaluation sandbox.

The default machine registry is shared across local controller directories at
`~/.local/state/autoresearch-swarm-machine/resources.sqlite`. Its native pools
have one controller owner. A second controller cannot silently claim the same
capacity. An explicit registry path is useful for isolated tests and deployments
whose operator manages separate native resource admission.

Enrollment records a CPU execution fingerprint from the OS, architecture, CPU
model, Python runtime, and admitted CPU slots. An unavailable model probe uses a
worker-specific identity to prevent accidental cross-worker equivalence. This
fingerprint does not prove thermal equivalence or kernel resource isolation.

Changing the profile, registry, or controller epoch requires explicit recovery.
Initialized enrollment also pins the journal and machine registry instance IDs.
A missing or replaced database cannot silently reset execution history or capacity.
Missing state and revoked credentials are not silently recreated. This helper
does not install frameworks, rotate credentials, or enroll remote machines.

## Machine resource admission

`MachineResources` stores native scheduling reservations in one operator-controlled
SQLite database per host. All participating workers and controllers must use that
registry. Each resource pool has one controller/workspace owner and a declared
capacity. Repeating the same enrollment is safe. A conflicting owner or capacity
requires an explicit transfer or reconfiguration, which this primitive does not infer.

A claim reserves all requested pools in one transaction. Concurrent independent
processes cannot oversubscribe the enrolled capacity. Claims bind the original
dispatch, attempt, worker session, and fence. A duplicate claim returns the same
reservation. It cannot change its resources or reopen a closed execution.

Lease expiry, process loss, and quarantine do not free capacity. Release requires
the trusted local journal's native closure or proof that the attempt never started.
A repeated closure has one effect. A receipt from another dispatch cannot release
the claim.

These pools implement scheduling admission. They do not enforce kernel CPU or memory
quotas, discover unrelated device owners, or police workers outside the registry.
Physical device qualification and operator allowances remain necessary. The worker
service must connect this admission step to dispatch before launching research code.

`LocalWorker` performs that connection for enrolled Python CPU profiles. It accepts
an authenticated client, the registered worker identity, its dispatch verification
key, a private work directory, and the host's shared resource registry. The worker
resolves `python` and `python3` to its explicitly configured interpreter.
Other engine adapters must qualify their own execution profiles.

`PreparedExecution` connects a sealed workload context, its exact file contents,
the profile's absolute executable, and a machine claim to a journal dispatch.
The journal includes this preparation in its immutable retry identity. The
supervisor checks the claim before launch and again while the native process is
held. It injects `RESEARCH_CONTEXT` and preserves the declared stage arguments.
The controller environment does not supply additional credentials.

Cancellation or expiry can settle an unstarted grant even if its context file has
been lost. Other preparation failures do not cross the native start barrier.
The lower-level native test primitive can still run without a prepared workload.
The integrated project worker must supply one. A real supervisor subprocess test
trains the CPU example through this binding and releases its recorded capacity.

Preparation ownership enters the journal before source materialization or native
reservation. Such an entry requires an immutable preparation binding before launch.
If preparation fails, the worker records an authenticated unstarted outcome and
releases an existing native claim. It never infers no-launch from a missing PID.
An interrupted extraction requires a new attempt. The worker retains its partial
files as task-owned working material until cleanup.

Each stage receives fresh source extracted from the immutable candidate bundle.
Inputs are downloaded by their recorded digest and size. The worker uploads bounded
stdout/stderr, its native observation, the workload report, and verified named outputs.
It releases native capacity from the journal before publishing completion.
The controller independently checks publication identity and artifact availability,
then settles budgets and resolves dependencies in one transaction.

Preparation failure and cancellation before release are unstarted work, including
a held native child that never passed the barrier. They consume no launched-attempt
count. A held child's observed supervision time still consumes execution time.
Unknown execution keeps its reservation and campaign slot. A later original closure
can settle it without repeating execution. Storage or transport errors during upload
leave publication retryable. They do not convert successful execution into a scientific
failure. Reports remain descriptive until accepted evaluator receipts support them.

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
