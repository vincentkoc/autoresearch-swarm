# Remote CPU enrollment

Remote enrollment has three explicit steps: describe the worker machine, provision
its authority on the controller, and connect the worker with that private bundle.
The worker uses the same typed HTTP operations as other clients. It never opens a
local controller database or receives an evaluator credential.

This release supplies enrollment, scoped discovery, calibrated native execution,
and publication. `worker connect` verifies and persists the connection, then exits.
A persistent polling/scheduling driver is still separate integration work.

## Describe the worker

Run on the worker machine:

~~~sh
research worker describe --worker-dir ./worker-state --cpu-slots 2 > worker-description.json
~~~

The description is public metadata: a stable worker UUID, CPU profile, and native
clock profile. The CPU fingerprint covers OS, architecture, CPU model, Python
runtime, and admitted capacity. The private installation record is written before
the description is returned. Repeating the command preserves the UUID and profile.
It cannot silently change CPU slots or replace missing native execution history.

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

The context closes the HTTP client. It does not terminate native work or release
machine claims. A future driver must reconcile journals and pending commands
before new admission, preserve uncertain outcomes, and drain work on graceful stop.

## Evidence and limits

Tests run real CLI subprocesses, scoped HTTP connections, and CPU execution. They
cover concurrent setup, interrupted credential/enrollment/export, interrupted native
initialization, immutable resume, secret-safe validation errors, changed profiles
and bundles, and missing or revoked authority. Physical remote hosts, service
installation, persistent remote scheduling, and non-CPU engines are separately
qualified features. No cloud service is required for the local default.
