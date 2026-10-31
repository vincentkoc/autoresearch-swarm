"""Pure reference checks and outcomes shared by synchronous and async delivery."""

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Literal

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriverRoute, OrchestrationBinding
from research_swarm.domain.orchestration_transfer import DriverStart, DriverStartAcknowledgment
from research_swarm.orchestration.delivery_models import (
    BindingReference,
    WorkOutcome,
    WorkReference,
)

if TYPE_CHECKING:
    from research_swarm.state.database import JsonObject


def require_routes(binding: BindingReference, routes: Collection[DriverRoute]) -> None:
    if set(routes) != set(binding.actors) or "control" not in routes:
        raise ValueError("Register exactly the binding's control and evaluator clients")


def binding_reference(binding: OrchestrationBinding) -> BindingReference:
    return BindingReference(
        workspace_id=binding.workspace_id,
        campaign_id=binding.campaign_id,
        epoch=binding.epoch,
        configuration_digest=binding.configuration_digest,
        actors={route: actor for route, actor in binding.routes.items()},
    )


def current_binding(
    reference: BindingReference, row: JsonObject
) -> tuple[OrchestrationBinding, bool]:
    binding = OrchestrationBinding.model_validate(row["data"])
    return binding, row["state"] == "active" and binding_reference(binding) == reference


def acknowledged_start(
    binding: OrchestrationBinding, route: DriverRoute, start_id: str, row: JsonObject
) -> DriverStart:
    start = DriverStart.model_validate(row["data"])
    if (
        row["id"] != start_id
        or row["kind"] != "orchestration_start"
        or row["campaign_id"] != binding.campaign_id
        or start.campaign_id != binding.campaign_id
        or start.epoch != binding.epoch
        or start.adapter != binding.adapter
        or start.configuration_digest != binding.configuration_digest
        or start.route != route
        or start.actor_id != binding.routes[route]
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Successor startup identity differs")
    if row["state"] != "acknowledged":
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "Successor requires completed external SDK recovery",
        )
    return start


def require_start_acknowledgment(
    binding: OrchestrationBinding, start_id: str, start: DriverStart, row: JsonObject
) -> None:
    acknowledgment = DriverStartAcknowledgment.model_validate(row["data"])
    if (
        row["id"] != start_id
        or row["kind"] != "orchestration_start_ack"
        or row["campaign_id"] != binding.campaign_id
        or row["state"] != "immutable"
        or acknowledgment.start_id != start_id
        or acknowledgment.campaign_id != binding.campaign_id
        or acknowledgment.epoch != binding.epoch
        or acknowledgment.acknowledged_by != start.actor_id
        or acknowledgment.observed_watermark < start.watermark
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Successor acknowledgment differs")


def work_route(work: WorkReference, routes: Collection[DriverRoute]) -> DriverRoute:
    matches = [route for route in routes if work.binding.actors[route] == work.actor_id]
    if len(matches) != 1:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Work route is not registered")
    return matches[0]


def verify_work(work: WorkReference, value: JsonObject) -> None:
    if (
        value["campaign_id"] != work.binding.campaign_id
        or value["epoch"] != work.binding.epoch
        or value["configuration_digest"] != work.binding.configuration_digest
        or value["actor_id"] != work.actor_id
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Work reference differs")


def delivery_key(kind: Literal["turn", "wait"], record_id: str) -> str:
    namespace, field = {
        "turn": ("research.drive-execute-v1", "turn_id"),
        "wait": ("research.reconcile-wait-v1", "wait_id"),
    }[kind]
    return record_digest(namespace, {field: record_id})


def turn_outcome(work: WorkReference, result: JsonObject) -> WorkOutcome:
    evidence = result.get("closure") or result["receipt"]
    return WorkOutcome(
        "settled",
        work.record_id,
        evidence["data"].get("disposition", "closed_unacknowledged"),
        evidence["id"],
    )


def wait_outcome(work: WorkReference, result: JsonObject) -> WorkOutcome:
    readiness = result["readiness"]
    return WorkOutcome(
        "settled",
        work.record_id,
        readiness["data"]["cause"],
        readiness["id"],
        readiness["data"]["watermark"],
    )
