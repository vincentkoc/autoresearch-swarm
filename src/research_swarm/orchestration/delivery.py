"""Synchronous API-only delivery for native local and provider adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from research_swarm.client import ResearchClient
from research_swarm.control.orchestration_models import DriveTurnTarget
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriverRoute, DriveTurn, OrchestrationBinding
from research_swarm.domain.orchestration_waits import OrchestrationWait
from research_swarm.orchestration.custody import run_owned_step as run_owned_step
from research_swarm.orchestration.delivery_contract import binding_reference as binding_reference
from research_swarm.orchestration.delivery_contract import (
    current_binding,
    delivery_key,
    require_routes,
    turn_outcome,
    verify_work,
    wait_outcome,
    work_route,
)
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


@dataclass(frozen=True)
class DriverConnection:
    binding: BindingReference
    clients: dict[DriverRoute, ResearchClient]

    def __post_init__(self) -> None:
        require_routes(self.binding, self.clients)


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
        binding, active = current_binding(reference, row)
        return connection, binding, active

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
        return connection.clients[work_route(work, connection.clients)]

    def execute(self, work: WorkReference) -> WorkOutcome:
        if work.kind != "turn":
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Expected a saved turn")
        client = self._client(work)
        turn = DriveTurn.model_validate(client.get("drive_turn", work.record_id)["data"])
        self._verify(work, turn.model_dump(mode="json"))
        result = client.command(
            "orchestration.execute_turn",
            DriveTurnTarget(turn_id=work.record_id),
            idempotency_key=delivery_key("turn", work.record_id),
        )["result"]
        return turn_outcome(work, result)

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
        return wait_outcome(work, result)

    _verify = staticmethod(verify_work)
