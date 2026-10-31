"""Replay negative control with only deterministic Workflow imports."""

from datetime import timedelta

from temporalio import workflow

from research_swarm.orchestration.temporal.models import WorkflowInput


@workflow.defn(name="research.campaign-delivery-v1")
class IncompatibleCampaignWorkflow:
    @workflow.run
    async def run(self, request: WorkflowInput) -> str:
        # Recorded v1 starts a bootstrap Activity, so this must fail replay.
        await workflow.sleep(timedelta(seconds=request.poll_seconds))
        return "retired"
