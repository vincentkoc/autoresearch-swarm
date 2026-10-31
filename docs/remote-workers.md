# Remote worker enrollment

Remote enrollment has three explicit steps: describe the worker machine, provision
its authority on the controller, and connect the worker with that private bundle.
The worker uses the same typed HTTP operations as other clients. It never opens a
local controller database or receives an evaluator credential.

This release supplies enrollment, scoped discovery, calibrated native execution,
and publication. `worker connect` verifies and persists the connection, then exits.
`worker start` runs the scoped worker in the foreground.

## Describe the worker

The default description admits native controller clocks. For a qualified hosted
witness controller, explicitly supply an `AuthorityClockAdmission` JSON file
with `schema_version: "1.0"`, `profile: "hosted-native-witness-v1"` and the exact
`policy_digest` selected by the operator. Pass it with
`research worker describe --worker-dir ./worker-state --clock-admission ./clock-admission.json`.
The Python equivalent is `describe_worker(..., clock_admission=admission)`.
The installation persists this choice before provisioning; reopening with a
different choice fails. The controller echoes it into the worker and archived
session, and bundle import requires exact equality. Existing replacement retains
that choice; a different admission requires a new explicit enrollment.

An admission is a permission and policy pin, not a clock-rate measurement or
proof of a hosted execution deployment. It accepts only the named authority
profile and policy, with no fallback to native authority profiles. The worker's
own clock must still use a supported native implementation. Descriptions without
this optional field retain their original serialization and behavior.

Run on the worker machine:

~~~sh
research worker describe --worker-dir ./worker-state --cpu-slots 2 > worker-description.json
~~~

The description is public metadata: a stable worker UUID, CPU profile, and native
clock profile. The CPU fingerprint covers OS, architecture, CPU model, Python
runtime, and admitted capacity. The private installation record is written before
the description is returned. Repeating the command preserves the UUID and profile.
It cannot silently change CPU slots or replace missing native execution history.

Pass `--registration path/to/private-engine.json` to pin a
[registered execution environment](engines.md). The description then includes its
public execution profile and environment-contract allowlist. Interpreter and probe
paths remain in the private installation. Match `--cpu-slots` to the registration.
Registered accelerator workers admit one exclusive device and have concurrency one;
the same provision/connect/start commands apply. Physical accelerator qualification
remains separate from the tested CPU transport path.

All workers on a physical host must use the same machine registry. The default is
`~/.local/state/autoresearch-swarm-machine/resources.sqlite`. An explicit
`--machine-registry` is available for deployments with operator-managed admission.
Use the same path and capacity on resume. Creating another registry is not a way
to bypass occupied or quarantined capacity.

## Provision on the controller

Transfer the public description to the controller. Use an operator with
`worker.manage`, `auth.manage`, `read`, and authority to grant the worker scopes:

~~~sh
research --state-dir ./controller-state worker provision \
  --description worker-description.json \
  --url https://research.example \
  --project PROJECT_UUID \
  --bundle ./private-bundles/worker.json
~~~

Repeat `--project` to admit several projects. Provisioning checks those projects,
persists worker session/principal/credential IDs and a random token, then installs
the credential and issues `worker.enroll` with a stable command key. The enrollment
requires `bounded-continuous-v1` and pins the complete assignment public authority.
The bundle contains the controller URL, description, credential, and exact worker
record. No signing private key is exported.

The worker credential has exactly `worker.query`, `worker.claim`, `worker.inputs`,
`worker.outputs`, and `worker.publish`. It has no workspace-wide read/upload,
operator, promotion, or evaluator authority. Evaluator authorization and receipt
completion remain controller-side responsibilities.

The destination directory and bundle require private native permissions, including
Windows ACL checks. An existing different bundle is not overwritten. Provisioning
prints a public worker record, never the token. Missing or revoked credentials
after completed provisioning require explicit recovery; repeating the command
cannot restore them. Interrupted provisioning retains its original identities
and can resume the same enrollment or export.

Bundle delivery is an operator action. Provisioning does not SSH to machines,
install services, deploy cloud resources, or send credentials elsewhere.

## Connect and resume

Transfer the private bundle to the described worker through an appropriate private
channel, then run:

~~~sh
research worker connect --worker-dir ./worker-state --bundle ./private-bundles/worker.json
research worker connect --worker-dir ./worker-state
~~~

The first command freezes the bundle before network or native initialization.
It verifies the machine profile and obtains the worker's own record through the
scoped API. That record must match the complete pinned bundle. Once initialized,
the installation also pins journal and machine-registry instance IDs. A changed
bundle, missing/replaced native database, revoked credential, stale session, or
changed controller authority fails closed. It does not erase historical attempts
or create another controller.

URLs require HTTPS outside loopback; redirects and environment proxy credentials
are disabled by the existing HTTP client contract. Connection verification is not
a clock-rate or accelerator benchmark. Each actual timed attempt obtains fresh
clock evidence and passes the [native admission gates](worker-assignments.md).

Python callers can retain a connected worker:

~~~python
from pathlib import Path
from research_swarm.workers.remote_enrollment import connect_worker

with connect_worker(Path("worker-state")) as enrollment:
    worker = enrollment.worker
    # Scoped discovery/claims use worker.client; execute an admitted attempt with:
    # result = worker.execute(attempt_id)
~~~

The context closes the HTTP client. Native closure and machine-claim release remain
explicit execution outcomes.

## Run the scoped worker

~~~sh
research worker start --worker-dir ./worker-state
research worker start --worker-dir ./worker-state --once
~~~

The persistent mode polls for admitted project work. `--once` drains currently
available work and returns a bounded JSON result. `idle` means no eligible work is
available to this worker. It does not complete a campaign. Evaluation jobs still
need a controller-side authorization; the worker never issues evaluator receipts
or changes scientific selection.

Polling defaults to 500 milliseconds and backs off to 15 seconds when idle or
disconnected. `--poll-ms` and `--maximum-backoff-ms` configure bounded integer
durations. One native lock owns scheduling, command delivery, and drain. Driver
identity pins the endpoint, credential ID, full enrollment, journal, and registry.
The command-intent store accepts a remote worker route only; local controller and
evaluator routing retain their existing authority requirements.

Before new admission, the driver replays unresolved command envelopes with their
original keys, queries its own pending assignments, and reconciles its journal.
An unavailable acknowledgment remains uncertain. An explicit rejected command can
support a later scheduling decision. A lost publication acknowledgment recovers
the already closed attempt even when the controller no longer lists it as pending.

HTTP operations run outside the event loop. A slow claim does not postpone an
already admitted execution task. Free slots are refilled while other experiments
continue; there is no generation barrier. Recovery retries have per-attempt timers,
so a slow sibling cannot delay reconciliation of another result.

Observed native uncertainty adds a worker-local admission fence. The driver can
reconcile the uncertain attempt, but it holds new native execution until terminal
accepted publication agrees with local closure. It rechecks completed tasks and
journals before each claim and after awaited claim replies. A claim accepted while
an in-flight failure becomes visible remains reserved for recovery. Other workers
retain their independent authority and resource accounting. Clock or lease expiry
never substitutes for native closure.

`driver.stop.set()` stops new scheduling and drains already launched native work.
Cancellation of the run task also retains its owner lock until outstanding control
calls and execution threads finish. A control call accepted during cancellation
can remain durably pending for the next resume. Forced driver termination leaves
the independent supervisor's original deadline in force; restart observes the
original process instead of launching its attempt again.

The driver also [synchronizes worker controls](worker-control.md): heartbeats,
admission drain, and attempt cancellation. Its independent control pump remains
active through native draining. Durable heartbeat envelopes and journal receipts
survive lost replies; control uncertainty blocks fresh launch without refunding
accepted work. Keep the worker's private `driver` directory on resume.

Python callers can use:

~~~python
from research_swarm.orchestration.scoped import ScopedWorkerDriver

with connect_worker(Path("worker-state")) as enrollment:
    result = await ScopedWorkerDriver(enrollment).run(once=True)
~~~

Results report `idle`, `blocked`, `disconnected`, or `stopped`, observation counts,
and at most 50 recent attempt IDs/findings. Observation counts include successful
reconciliation deliveries and are not counts of distinct scientific experiments.
The CLI returns exit code 3 for blocked/disconnected drain results and code 2 for
command errors. Persistent mode requires explicit stop; it does not install a
background service.

## Recover original evidence without execution authority

An old worker can submit its original native journal after its controller epoch
changes. Recovery uses the initialized installation, original credential, signing
key, journal identity, and machine registry identity:

~~~sh
research worker recover --worker-dir ./worker-state --attempt ATTEMPT_UUID
~~~

The command takes one explicit attempt. It holds the same driver and attempt
locks as normal execution. It does not import a replacement bundle, initialize
missing databases, verify an interpreter, sample a new clock, claim work, launch
a process, or send cancellation. Existing driver command intents are not replayed.
A missing original installation or changed custody identity requires repair.

Recovery first authenticates the original assignment and frozen timing, then
observes a durable native outcome. Proven closure releases the original machine
reservation before contacting the controller. A missing receipt or a running
supervisor retains capacity. Controller acceptance cannot stand in for local
native closure. Missing project files, logs, reports, and registered interpreters
do not prevent journal-only delivery.

The command freezes its bounded journal and delivery keys in a separate private
recovery intent. Lost acknowledgments replay those exact bytes. If the supervisor
closes while an older observation is awaiting delivery, recovery finishes that
original submission first. Run the same command again to submit the newer closure.
Submission is always quarantined; an operator chooses explicit reconciliation.
It never creates scientific success, evaluator receipts, or a promoted candidate.

The JSON result separates `native_state`, `custody_state`, `delivery_state`,
`submitted_native_state`, and `submission_id`. A completed delivery recommends
`operator_reconcile` when it contains the latest closure, or `observe_native`
when more observation is needed. Transport uncertainty recommends `retry_delivery`;
revoked or unavailable historical authority recommends `repair_authority`.
A durably rejected command returns `review_rejection` and its command ID. Its
original key remains rejected even after conditions change; this command does not
automatically rotate it. An operator must resolve the cause and arrange a separate
explicit evidence submission. Do not delete custody or pending intents to retry.

`controller_terminal` reports a concurrent accepted or `closed_unaccepted`
disposition separately and does not borrow its native proof. Exit code 3 means
delivery is pending or blocked; successful quarantine or a recognized terminal
controller disposition returns 0.

The Python equivalent uses the same path and returns the same typed report:

~~~python
from pathlib import Path
from research_swarm.workers.recovery import recover_worker_attempt

report = recover_worker_attempt(Path("worker-state"), attempt_id)
~~~

## Provision and install a replacement session

First fence the exact original session through [`worker.fence`](worker-control.md#explicit-worker-fencing-and-session-replacement).
Keep its fence ID and the returned worker revision. Replacement provisioning
requires local operator `worker.manage`, `auth.manage`, and `read` authority.
Reuse the original public description; this flow preserves capabilities and
project scope.

~~~sh
research --state-dir ./controller-state worker provision-replacement \
  --description ./worker-description.json \
  --fence FENCE_UUID --revision FENCED_WORKER_REVISION \
  --reason "Replace the original session" \
  --url https://controller.example \
  --bundle ./private-bundles/replacement.json
~~~

The operator stores one private provisioning record per worker and fence. Fresh
principal, session, credential, token, command key, exact replacement request,
and export destination are persisted before delivery. Retrying the same command
replays that intent after lost acknowledgments. Changed inputs or destinations
are conflicts. Revoked credentials, later fencing, and changed authority block
export; they never trigger silent credential rotation.

Transfer the private bundle to the original machine through an authorized
channel. The platform does not send it to another host. Install into a separate,
non-nested directory:

~~~sh
research worker install-replacement \
  --original-worker-dir ./worker-state \
  --worker-dir ./worker-state-next \
  --bundle ./replacement.json
~~~

Installation verifies the original installation, journal and registry identities,
then authenticates the new session and its own `worker_replacement` receipt.
That receipt must identify the original binding. The worker receives only its
own replacement receipt through the scoped query API.

The new installation copies the original public description and exact private
environment registration. It preserves the machine registry path and identity,
creates a separate journal, and does not copy heartbeat, driver-command, or
recovery state. A pending installation already pins the original registry, so a
missing or swapped registry cannot be recreated empty during first connection.
Interrupted initialization resumes its existing journal. Installation does not
launch a workload or environment probe. The original installation remains the
source for late evidence recovery.

The replacement remains draining. Activate it explicitly with
`worker.set_admission`, using its new binding and current admission revision,
then start it with `research worker start --worker-dir ./worker-state-next`.
Outstanding original attempts continue to occupy controller concurrency and
native capacity until their respective closure requirements are met. A different
controller's native registry ownership requires separate explicit migration;
this flow does not transfer that ownership.

The Python entry points are `provision_replacement` and `install_replacement`
in `research_swarm.workers.remote_replacement`. Provisioning returns a private bundle;
installation returns public worker identity; CLI
output contains public identity only. Protect returned bundle objects as credentials.

## Evidence and limits

Tests run real CLI subprocesses, scoped HTTP connections, and CPU execution. They
cover concurrent setup, interrupted credential/enrollment/export, interrupted native
initialization, immutable resume, secret-safe validation errors, changed profiles
and bundles, and missing or revoked authority. Driver tests cover asynchronous
slot refill, delayed HTTP replies, exact uncertain-command recovery, worker-lock
ownership during cancellation, and uncertainty discovered during offer/claim RPCs.
An independent HTTP controller, worker driver, and native supervisor test kills the
driver while training runs and verifies recovery of the same native identity.
Physical remote hosts, service installation, and non-CPU engine deployment remain
separately qualified features. Remote heartbeats and cancellation are qualified
locally through HTTP and independent process recovery. No cloud service is required
for the local default.
