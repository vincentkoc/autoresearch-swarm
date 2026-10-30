"""The one-command local path leaves further research under explicit agent control."""

from pathlib import Path
from typing import Literal

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.engines.registration import EnvironmentRegistration
from research_swarm.orchestration.bootstrap import RunOptions, prepare_run
from research_swarm.orchestration.local import DriverResult, LocalDriver
from research_swarm.state.database import JsonObject
from research_swarm.workers.local_enrollment import ensure_local_enrollment


class LocalRunResult(VersionedRecord):
    run_id: ID
    campaign_id: ID
    candidate_id: ID
    trial_id: ID
    source_digest: Digest
    state: Literal[
        "awaiting_proposals",
        "awaiting_completion",
        "needs_review",
        "blocked",
        "paused",
        "terminal",
        "stopped",
    ]
    driver: DriverResult
    context: JsonObject
    next_action: JsonObject


async def run_project(
    client: ResearchClient,
    source: Path,
    *,
    key: str | None = None,
    options: RunOptions | None = None,
    cpu_slots: int = 1,
    machine_registry: Path | None = None,
    registration: EnvironmentRegistration | None = None,
) -> LocalRunResult:
    if not isinstance(client.transport, LocalTransport):
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Local runs require local state")
    enrollment = ensure_local_enrollment(
        client.transport.runtime,
        cpu_slots=cpu_slots,
        machine_registry=machine_registry,
        principal=client.transport.resolve_principal(),
        registration=registration,
    )
    prepared = prepare_run(
        client, source, key=key, options=options, profile=enrollment.worker.profile
    )
    plan = prepared.plan
    assert plan.campaign_id is not None and plan.candidate_id is not None
    assert plan.trial_id is not None and plan.source_digest is not None
    try:
        driven = await LocalDriver(client, enrollment).drain(plan.campaign_id)
    except ResearchError as error:
        if error.code != ErrorCode.RESOURCE_UNAVAILABLE:
            raise
        driven = DriverResult(
            campaign_id=plan.campaign_id,
            state="blocked",
            finding_count=1,
            findings=(error.code.value,),
        )
    context = client.context(plan.campaign_id)
    trial = client.get("trial", plan.trial_id)
    state = driven.state
    if state == "idle":
        count = sum(item["count"] for item in context["counts"] if item["kind"] == "trial")
        outcome = (
            "needs_review"
            if trial["state"] != "succeeded" or trial["data"]["evaluation_status"] != "valid"
            else "awaiting_completion"
            if count >= plan.options.max_trials
            else "awaiting_proposals"
        )
    else:
        outcome = state
    return LocalRunResult(
        run_id=plan.id,
        campaign_id=plan.campaign_id,
        candidate_id=plan.candidate_id,
        trial_id=plan.trial_id,
        source_digest=plan.source_digest,
        state=outcome,
        driver=driven,
        context=context,
        next_action={
            "operation": "context",
            "campaign_id": plan.campaign_id,
        },
    )
