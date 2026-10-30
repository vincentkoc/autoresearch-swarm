"""Deterministic delivery only. Research decisions and native execution stay outside."""

from contextlib import suppress
from dataclasses import replace
from datetime import timedelta
from typing import TypedDict

from temporalio import workflow
from temporalio.common import RetryPolicy

from research_swarm.orchestration.temporal.models import (
    BindingReference,
    PendingWork,
    StartupRequest,
    WakeSignal,
    WorkflowInput,
    WorkOutcome,
)


class ActivityOptions(TypedDict):
    start_to_close_timeout: timedelta
    retry_policy: RetryPolicy


@workflow.defn(name="research.campaign-delivery-v1")
class CampaignWorkflow:
    def __init__(self) -> None:
        self.binding: BindingReference | None = None
        self.woken = False

    @workflow.signal(name="research.wakeup-v1")
    def wake(self, signal: WakeSignal) -> None:
        binding = self.binding
        if binding is not None and (
            signal.campaign_id == binding.campaign_id
            and signal.epoch == binding.epoch
            and signal.actor_id in binding.actors.values()
        ):
            self.woken = True

    @workflow.run
    async def run(self, request: WorkflowInput) -> str:
        self.binding = request.binding
        options: ActivityOptions = {
            "start_to_close_timeout": timedelta(seconds=request.activity_timeout_seconds),
            "retry_policy": RetryPolicy(
                initial_interval=timedelta(seconds=0.1),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=8,
            ),
        }
        startup: str = await workflow.execute_activity(
            "research.bootstrap-v1",
            StartupRequest(request.binding, str(workflow.uuid4())),
            result_type=str,
            **options,
        )
        if startup == "expired":
            workflow.continue_as_new(request)
        if startup == "retired":
            return "retired"
        pending = request.pending
        iterations = 0
        while True:
            self.woken = False
            if pending is None:
                observed: PendingWork = await workflow.execute_activity(
                    "research.inspect-v1",
                    request.binding,
                    result_type=PendingWork,
                    **options,
                )
                if observed.state in {"terminal", "retired"}:
                    return observed.state
                pending = observed.work
            if pending is not None:
                result: WorkOutcome = await workflow.execute_activity(
                    "research.execute-v1" if pending.kind == "turn" else "research.wait-v1",
                    pending,
                    result_type=WorkOutcome,
                    **options,
                )
                if result.state == "settled":
                    pending = None
                else:
                    await self._sleep(request.poll_seconds)
            else:
                await self._sleep(request.poll_seconds)
            iterations += 1
            if iterations >= request.rotate_after or workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(replace(request, pending=pending))

    async def _sleep(self, seconds: float) -> None:
        with suppress(TimeoutError):
            await workflow.wait_condition(lambda: self.woken, timeout=seconds)
