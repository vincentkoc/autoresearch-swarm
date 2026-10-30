# Temporal delivery adapter

Temporal delivers the same prepared research turns and waits as the local API.
One Workflow owns delivery for one workspace, campaign, and orchestration epoch.
The domain ledger owns authority, command outcomes, experiment identities,
budgets, resource custody, and scientific decisions.

An external agent or policy service prepares a turn through the research API.
The Workflow notices it and schedules a short Activity with that exact turn ID.
Activity retries recover the original acknowledgment. They cannot select another
ordinal or create another training realization. Native CPU, CUDA, ROCm, MPS, and
MLX workers still use the ordinary worker protocol; they receive no Temporal
task tokens.

The adapter does not propose research. If a planner disconnects, saved work can
finish and the Workflow waits for another prepared action. Keep proposal calls
and policy decisions in the separately recoverable agent service.

## Install and connect

```sh
uv sync --extra temporal --group dev
```

The optional extra requires Temporal Python 1.35 or newer within major version 1.
The lockfile selects the exact tested SDK. Core installations do not require it;
the development group includes it for adapter contract and typing checks.

Use private, scoped research clients for the binding's control and evaluator
actors. Register those clients in the Activity worker. Credentials and research
URLs do not belong in Workflow input. The following example assumes the domain
binding and clients already exist:

```python
from temporalio.client import Client

from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.orchestration.delivery import (
    DriverConnection,
    binding_reference,
)
from research_swarm.orchestration.temporal.activities import TemporalActivities
from research_swarm.orchestration.temporal.bridge import TemporalBridge
from research_swarm.orchestration.temporal.models import WorkflowInput, workflow_id
from research_swarm.orchestration.temporal.runtime import DATA_CONVERTER, create_worker

binding = OrchestrationBinding.model_validate(
    control_client.get("orchestration_binding", campaign_id)["data"]
)
reference = binding_reference(binding)
connection = DriverConnection(reference, {"control": control_client, "evaluator": evaluator_client})
activities = TemporalActivities({workflow_id(reference): connection})

temporal = await Client.connect(
    "127.0.0.1:7233", data_converter=DATA_CONVERTER, identity="research-delivery"
)
bridge = TemporalBridge(temporal, task_queue="research-delivery")
async with create_worker(temporal, task_queue="research-delivery", activities=activities):
    handle = await bridge.start(WorkflowInput(reference))
    await handle.result()
```

Supply only `control` for a control-only binding. Production authentication,
namespace selection, TLS, and task-queue deployment use the Temporal SDK's normal
configuration. The example starts no native worker and changes no campaign state.

The worker factory requires the research data converter. It preserves safe error
codes and retry fields while stripping arbitrary exception messages, details,
causes' payloads, and stack paths from recorded failures. Activity implementations
also sanitize transport and domain failures. Keep operational diagnostic logging
within the private deployment rather than putting private exceptions into history.

## Prepare work through the same API

`DurableTurnDriver.prepare_turn()` persists one action without executing it.
It recovers the saved ordinal before calling a planner. The existing
`advance_turn()` remains the local prepare-and-execute composition.

```python
from research_swarm.orchestration.turns import DurableTurnDriver

driver = DurableTurnDriver(
    control_client,
    campaign_id,
    epoch=reference.epoch,
    configuration_digest=reference.configuration_digest,
)
saved = driver.prepare_turn(
    ordinal,
    lambda: planned_action,
    planning_watermark=observed_watermark,
)
```

The planning function returns a `DriveAction`; an external LLM can instead call
`orchestration.prepare_turn` directly through HTTP, MCP, CLI, or the Python SDK.
Use the binding's authorized route principal. Proposal-agent credentials alone
do not grant driver authority. Inspect `drive_turn`, `drive_receipt`, and
`drive_closure` for the authoritative outcome.

The Workflow input and Activity results contain bounded identities and compact
dispositions. Source, action payloads, reports, logs, checkpoint manifests, and
credentials stay in the research service or its artifact store. Workflow code
uses the default Temporal sandbox and performs no network, filesystem, database,
LLM, or subprocess operations.

## Waits, signals, and recovery

Register a durable domain wait before waiting. The adapter reconciles its exact
ID, then uses a Temporal timer or signal as a prompt to recheck readiness.
Signals do not establish readiness and do not consume evidence. After readiness,
the Workflow parks until the agent prepares the next action or wait with an
appropriate observed watermark.

For lower latency, deliver the domain outbox through the bridge:

```python
from research_swarm.orchestration.waits import DurableWaitDriver

waits = DurableWaitDriver(
    control_client, campaign_id, epoch=reference.epoch, actor_id=reference.actors["control"]
)
await bridge.deliver(reference, waits)
```

Delivery acknowledges the domain notification only after Temporal accepts the
signal. A lost acknowledgment can produce duplicate signals. The signal targets
the exact epoch and actor; it never starts or resurrects a Workflow. Bounded
polling also handles missed signals and prepared actions without notifications.

The Workflow continues as new after a bounded number of iterations or a server
history recommendation. It carries an unresolved wait reference across the
boundary. A pending turn is always settled before voluntary history rotation.
Each retry receives its original Activity input, even if a timed-out Activity
continues its already-admitted HTTP call.

Startup checks the registered binding. After a domain ownership transfer, it
verifies the immutable checkpoint and chunks, reads a fresh verified snapshot,
and acknowledges each route separately. Snapshot IDs derive from a frozen
recovery generation in the bootstrap Activity input, so a lost creation reply
does not allocate another snapshot. An expired or released snapshot causes an
explicit Workflow continuation with a new recovery generation. The successor
campaign remains paused until an authorized `campaign.start`.

Starting an existing epoch returns its existing Workflow after checking its
original binding and settings. Changed input is an idempotency conflict.
Completed or failed epoch histories are not silently replaced. Investigate and
use an explicit Temporal recovery or domain ownership transfer.

Canceling or terminating a Workflow stops orchestration delivery only. It does
not cancel a native process, release a budget reservation, close an uncertain
attempt, or mark a scientific failure. Use the research cancellation and
reconciliation APIs for those operations.

## Qualification

```sh
RESEARCH_TEMPORAL_TESTS=1 uv run --no-sync pytest -q tests/test_temporal.py
```

The opt-in suite starts disposable real Temporal CLI dev servers and the real
loopback research HTTP service. It exercises lost command replies, overlapping
Activity timeout retries, duplicate startup and signals, Activity-worker restart,
SQLite-backed server restart, Continue-As-New, snapshot creation uncertainty,
invalidated recovery snapshots, transferred startup, and cancellation during a
real native CPU trial. Each generated Workflow history is replayed with the
default sandbox. The normal suite also checks cross-Workflow reference rejection
and failure-data sanitation.

Local evidence uses Temporal Python 1.35.0, CLI 1.9.1, and server 1.32.0 on macOS.
This qualifies the exercised local provider behavior. It does not establish
Temporal Cloud deployment, high availability, physical accelerator execution, or
research-quality improvements. A dedicated Linux CI job runs the same real-server
suite; its results qualify the specific tested commit.

The design follows Temporal's [Activity retry semantics](https://docs.temporal.io/develop/python/best-practices/error-handling),
[Signals](https://docs.temporal.io/develop/python/workflows/message-passing),
[Continue-As-New](https://docs.temporal.io/develop/python/workflows/continue-as-new),
and [history replay testing](https://docs.temporal.io/develop/python/best-practices/testing-suite).
