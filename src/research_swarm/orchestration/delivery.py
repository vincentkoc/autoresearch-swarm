"""One API-only delivery implementation shared by local and provider adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from research_swarm.client import ResearchClient
from research_swarm.control.orchestration_models import DriveTurnTarget
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriverRoute, DriveTurn, OrchestrationBinding
from research_swarm.domain.orchestration_waits import OrchestrationWait
from research_swarm.orchestration.custody import run_owned_step as run_owned_step
from research_swarm.orchestration.delivery_models import (
    BindingReference,
    PendingWork,
    StartupRequest,
    WorkOutcome,
    WorkReference,
    delivery_id,
)
from research_swarm.orchestration.startup import recover_driver_start
from research_swarm.orchestration.waits import DurableWaitDriver
from research_swarm.state.database import JsonObject


@dataclass(frozen=True)
class DriverConnection:
    binding: BindingReference
    clients: dict[DriverRoute, ResearchClient]

    def __post_init__(self) -> None:
        if set(self.clients) != set(self.binding.actors) or "control" not in self.clients:
            raise ValueError("Register exactly the binding's control and evaluator clients")


def binding_reference(binding: OrchestrationBinding) -> BindingReference:
    return BindingReference(
        workspace_id=binding.workspace_id,
        campaign_id=binding.campaign_id,
        epoch=binding.epoch,
        configuration_digest=binding.configuration_digest,
        actors={route: actor for route, actor in binding.routes.items()},
    )


class DeliveryKernel:
    def __init__(self, connections: Mapping[str, DriverConnection]) -> None:
        self.connections = dict(connections)

    def _connection(self, reference: BindingReference) -> DriverConnection:
        connection = self.connections.get(delivery_id(reference))
        if connection is None or connection.binding != reference:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Driver registration differs")
        return connection

    def _current(
        self, reference: BindingReference
    ) -> tuple[DriverConnection, OrchestrationBinding, bool]:
        connection = self._connection(reference)
        row = connection.clients["control"].get("orchestration_binding", reference.campaign_id)
        binding = OrchestrationBinding.model_validate(row["data"])
        return (
            connection,
            binding,
            row["state"] == "active" and binding_reference(binding) == reference,
        )

    def bootstrap(self, request: StartupRequest) -> str:
        connection, binding, active = self._current(request.binding)
        if not active:
            return "retired"
        try:
            for route, start_id in (binding.start_ids or {}).items():
                recover_driver_start(
                    connection.clients[route], start_id, recovery_id=request.recovery_id
                )
        except ResearchError as error:
            if error.code == ErrorCode.SNAPSHOT_EXPIRED:
                return "expired"
            raise
        return "active" if self._current(request.binding)[2] else "retired"

    def inspect(self, reference: BindingReference) -> PendingWork:
        connection, binding, active = self._current(reference)
        if not active:
            return PendingWork("retired")
        client = connection.clients["control"]
        if binding.pending_turn_id is not None:
            turn = DriveTurn.model_validate(
                client.get("drive_turn", binding.pending_turn_id)["data"]
            )
            return PendingWork(
                "work", WorkReference(reference, "turn", binding.pending_turn_id, turn.actor_id)
            )
        if binding.active_wait_id is not None:
            row = client.get("orchestration_wait", binding.active_wait_id)
            if row["state"] == "waiting":
                return PendingWork(
                    "work", WorkReference(reference, "wait", row["id"], row["data"]["actor_id"])
                )
        campaign = client.get("campaign", reference.campaign_id)
        if campaign["state"] in {"completed", "cancelled", "failed"}:
            return PendingWork("terminal")
        return PendingWork("idle")

    def _client(self, work: WorkReference) -> ResearchClient:
        connection = self._connection(work.binding)
        matches = [
            role for role in connection.clients if work.binding.actors[role] == work.actor_id
        ]
        if len(matches) != 1:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Work route is not registered")
        return connection.clients[matches[0]]

    def execute(self, work: WorkReference) -> WorkOutcome:
        if work.kind != "turn":
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Expected a saved turn")
        client = self._client(work)
        turn = DriveTurn.model_validate(client.get("drive_turn", work.record_id)["data"])
        self._verify(work, turn.model_dump(mode="json"))
        result = client.command(
            "orchestration.execute_turn",
            DriveTurnTarget(turn_id=work.record_id),
            idempotency_key=record_digest("research.drive-execute-v1", {"turn_id": work.record_id}),
        )["result"]
        evidence = result.get("closure") or result["receipt"]
        return WorkOutcome(
            "settled",
            work.record_id,
            evidence["data"].get("disposition", "closed_unacknowledged"),
            evidence["id"],
        )

    def wait(self, work: WorkReference) -> WorkOutcome:
        if work.kind != "wait":
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Expected a saved wait")
        client = self._client(work)
        waited = OrchestrationWait.model_validate(
            client.get("orchestration_wait", work.record_id)["data"]
        )
        self._verify(work, waited.model_dump(mode="json"))
        try:
            result = DurableWaitDriver(
                client,
                work.binding.campaign_id,
                epoch=work.binding.epoch,
                actor_id=work.actor_id,
            ).reconcile(work.record_id)
        except ResearchError as error:
            if error.code == ErrorCode.RESOURCE_UNAVAILABLE:
                return WorkOutcome("pending", work.record_id)
            raise
        readiness = result["readiness"]
        return WorkOutcome(
            "settled",
            work.record_id,
            readiness["data"]["cause"],
            readiness["id"],
            readiness["data"]["watermark"],
        )

    @staticmethod
    def _verify(work: WorkReference, value: JsonObject) -> None:
        if (
            value["campaign_id"] != work.binding.campaign_id
            or value["epoch"] != work.binding.epoch
            or value["configuration_digest"] != work.binding.configuration_digest
            or value["actor_id"] != work.actor_id
        ):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Work reference differs")
