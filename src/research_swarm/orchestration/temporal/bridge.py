"""Idempotent epoch startup and outbox delivery; signals never create a Workflow."""

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration_waits import WakeupDelivery
from research_swarm.orchestration.temporal.models import (
    BindingReference,
    WakeSignal,
    WorkflowInput,
    workflow_id,
)
from research_swarm.orchestration.temporal.workflow import CampaignWorkflow
from research_swarm.orchestration.waits import DurableWaitDriver


class TemporalBridge:
    def __init__(self, client: Client, *, task_queue: str) -> None:
        if not 1 <= len(task_queue) <= 128:
            raise ValueError("Temporal task queue must contain 1 to 128 characters")
        self.client, self.task_queue = client, task_queue

    async def start(self, request: WorkflowInput) -> WorkflowHandle[CampaignWorkflow, str]:
        if request.pending is not None:
            raise ValueError("Pending work is reserved for Workflow continuation")
        identifier = workflow_id(request.binding)
        try:
            handle = await self.client.start_workflow(
                CampaignWorkflow.run,
                request,
                id=identifier,
                task_queue=self.task_queue,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            )
        except WorkflowAlreadyStartedError:
            handle = self.client.get_workflow_handle_for(CampaignWorkflow.run, identifier)
        async for event in handle.fetch_history_events(page_size=1):
            decoded = await self.client.data_converter.decode(
                event.workflow_execution_started_event_attributes.input.payloads,
                [WorkflowInput],
            )
            original = decoded[0]
            if (
                original.binding != request.binding
                or original.poll_seconds != request.poll_seconds
                or original.rotate_after != request.rotate_after
                or original.activity_timeout_seconds != request.activity_timeout_seconds
            ):
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Workflow epoch already has another input"
                )
            return handle
        raise ResearchError(
            ErrorCode.TRANSPORT_UNAVAILABLE, "Workflow start history is unavailable"
        )

    async def notify(self, binding: BindingReference, delivery: WakeupDelivery) -> None:
        if (
            delivery.campaign_id != binding.campaign_id
            or delivery.epoch != binding.epoch
            or binding.actors.get(delivery.route) != delivery.actor_id
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Wakeup belongs to another Workflow")
        handle = self.client.get_workflow_handle_for(CampaignWorkflow.run, workflow_id(binding))
        await handle.signal(
            CampaignWorkflow.wake,
            WakeSignal(delivery.campaign_id, delivery.epoch, delivery.actor_id, delivery.wait_id),
        )

    async def deliver(
        self, binding: BindingReference, driver: DurableWaitDriver
    ) -> tuple[str, ...]:
        async def notify(delivery: WakeupDelivery) -> None:
            await self.notify(binding, delivery)

        return await driver.deliver_once(notify)
