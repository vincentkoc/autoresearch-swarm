"""Temporal's retry and privacy boundary around the shared API delivery kernel."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TypeVar

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.orchestration.delivery import DeliveryKernel, run_owned_step
from research_swarm.orchestration.delivery import DriverConnection as DriverConnection
from research_swarm.orchestration.delivery import binding_reference as binding_reference
from research_swarm.orchestration.temporal.models import (
    BindingReference,
    PendingWork,
    StartupRequest,
    WorkOutcome,
    WorkReference,
    workflow_id,
)

T = TypeVar("T")


class TemporalActivities:
    def __init__(self, connections: Mapping[str, DriverConnection]) -> None:
        self.delivery = DeliveryKernel(connections)

    def _bootstrap(self, request: StartupRequest) -> str:
        return self.delivery.bootstrap(request)

    def _inspect(self, reference: BindingReference) -> PendingWork:
        return self.delivery.inspect(reference)

    def _execute(self, work: WorkReference) -> WorkOutcome:
        return self.delivery.execute(work)

    def _wait(self, work: WorkReference) -> WorkOutcome:
        return self.delivery.wait(work)

    @staticmethod
    async def _call(reference: BindingReference, function: Callable[[], T]) -> T:
        try:
            if activity.info().workflow_id != workflow_id(reference):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Activity belongs to another Workflow"
                )
            return await run_owned_step(function)
        except ResearchError as error:
            raise ApplicationError(
                "Research command failed: " + error.code.value,
                type=error.code.value,
                non_retryable=not error.retryable
                and error.code
                not in {
                    ErrorCode.TRANSPORT_UNAVAILABLE,
                    ErrorCode.DELIVERY_UNCERTAIN,
                },
            ) from None
        except (httpx.TransportError, TimeoutError, ConnectionError):
            raise ApplicationError(
                "Research transport unavailable", type="ResearchTransportError"
            ) from None
        except Exception:
            raise ApplicationError(
                "Research adapter failed", type="ResearchAdapterError", non_retryable=True
            ) from None

    @activity.defn(name="research.bootstrap-v1")
    async def bootstrap(self, request: StartupRequest) -> str:
        return await self._call(request.binding, lambda: self._bootstrap(request))

    @activity.defn(name="research.inspect-v1")
    async def inspect(self, reference: BindingReference) -> PendingWork:
        return await self._call(reference, lambda: self._inspect(reference))

    @activity.defn(name="research.execute-v1")
    async def execute(self, work: WorkReference) -> WorkOutcome:
        return await self._call(work.binding, lambda: self._execute(work))

    @activity.defn(name="research.wait-v1")
    async def wait(self, work: WorkReference) -> WorkOutcome:
        return await self._call(work.binding, lambda: self._wait(work))
