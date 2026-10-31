"""Async delivery over registered clients, without native startup or thread custody."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from research_swarm.control.orchestration_models import DriveTurnTarget
from research_swarm.control.wait_models import OrchestrationWaitTarget
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriverRoute, DriveTurn, OrchestrationBinding
from research_swarm.domain.orchestration_waits import OrchestrationWait
from research_swarm.orchestration.delivery_contract import (
    acknowledged_start,
    current_binding,
    delivery_key,
    require_routes,
    require_start_acknowledgment,
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

if TYPE_CHECKING:
    from research_swarm.state.database import JsonObject


class DeliveryClient(Protocol):
    async def get(self, kind: str, identifier: str) -> JsonObject: ...

    async def command(
        self, operation: str, request: JsonObject, *, idempotency_key: str
    ) -> JsonObject: ...


@dataclass(frozen=True)
class AsyncDriverConnection:
    binding: BindingReference
    clients: dict[DriverRoute, DeliveryClient]

    def __post_init__(self) -> None:
        require_routes(self.binding, self.clients)


class AsyncDeliveryKernel:
    def __init__(self, connections: Mapping[str, AsyncDriverConnection]) -> None:
        self.connections = dict(connections)

    def _connection(self, reference: BindingReference) -> AsyncDriverConnection:
        connection = self.connections.get(delivery_id(reference))
        if connection is None or connection.binding != reference:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Driver registration differs")
        return connection

    async def _current(
        self, reference: BindingReference
    ) -> tuple[AsyncDriverConnection, OrchestrationBinding, bool]:
        connection = self._connection(reference)
        row = await connection.clients["control"].get(
            "orchestration_binding", reference.campaign_id
        )
        binding, active = current_binding(reference, row)
        return connection, binding, active

    async def bootstrap(self, request: StartupRequest) -> str:
        connection, binding, active = await self._current(request.binding)
        if not active:
            return "retired"
        if binding.start_ids is not None:
            starts = dict(binding.start_ids)
            if set(starts) != set(binding.routes) or len(set(starts.values())) != len(starts):
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Successor startup routes differ"
                )
            for route, identifier in starts.items():
                client = connection.clients[route]
                start = acknowledged_start(
                    binding, route, identifier, await client.get("orchestration_start", identifier)
                )
                require_start_acknowledgment(
                    binding,
                    identifier,
                    start,
                    await client.get("orchestration_start_ack", identifier),
                )
            _, current, active = await self._current(request.binding)
            if not active or current.start_ids != starts:
                return "retired"
        return "active"

    async def inspect(self, reference: BindingReference) -> PendingWork:
        connection, binding, active = await self._current(reference)
        if not active:
            return PendingWork("retired")
        client = connection.clients["control"]
        if binding.pending_turn_id is not None:
            turn = DriveTurn.model_validate(
                (await client.get("drive_turn", binding.pending_turn_id))["data"]
            )
            return PendingWork(
                "work", WorkReference(reference, "turn", binding.pending_turn_id, turn.actor_id)
            )
        if binding.active_wait_id is not None:
            row = await client.get("orchestration_wait", binding.active_wait_id)
            if row["state"] == "waiting":
                return PendingWork(
                    "work", WorkReference(reference, "wait", row["id"], row["data"]["actor_id"])
                )
        campaign = await client.get("campaign", reference.campaign_id)
        if campaign["state"] in {"completed", "cancelled", "failed"}:
            return PendingWork("terminal")
        return PendingWork("idle")

    def _client(self, work: WorkReference) -> DeliveryClient:
        connection = self._connection(work.binding)
        return connection.clients[work_route(work, connection.clients)]

    async def execute(self, work: WorkReference) -> WorkOutcome:
        if work.kind != "turn":
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Expected a saved turn")
        client = self._client(work)
        turn = DriveTurn.model_validate((await client.get("drive_turn", work.record_id))["data"])
        verify_work(work, turn.model_dump(mode="json"))
        result = await client.command(
            "orchestration.execute_turn",
            DriveTurnTarget(turn_id=work.record_id).model_dump(mode="json"),
            idempotency_key=delivery_key("turn", work.record_id),
        )
        return turn_outcome(work, result["result"])

    async def wait(self, work: WorkReference) -> WorkOutcome:
        if work.kind != "wait":
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Expected a saved wait")
        client = self._client(work)
        waited = OrchestrationWait.model_validate(
            (await client.get("orchestration_wait", work.record_id))["data"]
        )
        verify_work(work, waited.model_dump(mode="json"))
        try:
            result = await client.command(
                "orchestration.reconcile_wait",
                OrchestrationWaitTarget(wait_id=work.record_id).model_dump(mode="json"),
                idempotency_key=delivery_key("wait", work.record_id),
            )
        except ResearchError as error:
            if error.code == ErrorCode.RESOURCE_UNAVAILABLE:
                return WorkOutcome("pending", work.record_id)
            raise
        return wait_outcome(work, result["result"])
