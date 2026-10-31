"""Cloudflare delivery control; research state remains in the native authority."""

from __future__ import annotations

import hashlib
import json
from functools import partial
from typing import Literal, Self
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field

from research_swarm.client import controller_url
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.domain.orchestration_transfer import DriverStart, TransferCheckpoint
from research_swarm.domain.orchestration_waits import WakeupDelivery
from research_swarm.domain.types import ID
from research_swarm.orchestration.custody import run_owned_step
from research_swarm.orchestration.delivery import binding_reference
from research_swarm.orchestration.delivery_models import BindingReference
from research_swarm.orchestration.gateway_models import GatewayBinding, GatewayWork
from research_swarm.orchestration.waits import DurableWaitDriver
from research_swarm.state.database import JsonObject

MAX_BYTES = 16 * 1024
MAX_HISTORY_HOPS = 128


def validate_outbox(binding: BindingReference, driver: DurableWaitDriver) -> None:
    """Resolve a prior binding through immutable checkpoints, without scanning history."""
    saved = OrchestrationBinding.model_validate(
        driver.client.get("orchestration_binding", driver.campaign_id)["data"]
    )
    for hops in range(MAX_HISTORY_HOPS + 1):
        if saved.epoch <= binding.epoch:
            if binding_reference(saved) != binding:
                raise ResearchError(ErrorCode.STALE_FENCE, "Outbox binding differs")
            return
        if hops == MAX_HISTORY_HOPS:
            break
        start_id = (saved.start_ids or {}).get("control")
        if start_id is None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Outbox history is unavailable")
        start = DriverStart.model_validate(
            driver.client.get("orchestration_start", start_id)["data"]
        )
        checkpoint = TransferCheckpoint.model_validate(
            driver.client.get("orchestration_checkpoint", start.checkpoint_id)["data"]
        )
        previous = checkpoint.binding_before
        if (
            start.campaign_id != saved.campaign_id
            or start.epoch != saved.epoch
            or start.configuration_digest != saved.configuration_digest
            or start.actor_id != saved.routes["control"]
            or start.route != "control"
            or checkpoint.workspace_id != saved.workspace_id
            or checkpoint.campaign_id != saved.campaign_id
            or previous.workspace_id != saved.workspace_id
            or previous.campaign_id != saved.campaign_id
            or previous.epoch != saved.epoch - 1
            or record_digest("research.transfer-checkpoint-v1", checkpoint.model_dump(mode="json"))
            != start.checkpoint_digest
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Outbox checkpoint differs")
        saved = previous
    raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Outbox history exceeds the bounded lookup")


class WorkflowOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    poll_seconds: int = Field(default=5, strict=True, ge=1, le=60)
    rotate_after: int = Field(default=128, strict=True, ge=2, le=128)
    request_timeout_seconds: int = Field(default=120, strict=True, ge=1, le=120)


class WorkflowStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(pattern=r"^research-[0-9a-f]{64}-[0-9]{1,10}$", max_length=84)
    generation: int = Field(strict=True, ge=0, le=1_000_000_000)
    status: Literal[
        "queued",
        "running",
        "paused",
        "errored",
        "terminated",
        "complete",
        "waiting",
        "waitingForPause",
        "unknown",
        "creating",
        "unavailable",
    ]
    initial_pending: GatewayWork | None


class WorkflowRecoveryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    binding: GatewayBinding
    request_id: ID
    expected_generation: int = Field(strict=True, ge=0, le=999_999_999)
    expected_instance_id: str = Field(pattern=r"^research-[0-9a-f]{64}-[0-9]{1,10}$", max_length=84)
    reason: str = Field(min_length=1, max_length=1024)


class WorkflowRecoveryReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: ID
    predecessor_id: str
    predecessor_generation: int = Field(strict=True, ge=0, le=999_999_999)
    id: str
    generation: int = Field(strict=True, ge=1, le=1_000_000_000)


def workflow_id(reference: BindingReference, generation: int = 0) -> str:
    """Match the adapter's normalized JSON identity, independent of map insertion order."""
    if type(generation) is not int or not 0 <= generation <= 1_000_000_000:
        raise ValueError("Invalid Cloudflare generation")
    binding = GatewayBinding.from_reference(reference)
    normalized = {
        "schema_version": "1.0",
        "workspace_id": binding.workspace_id,
        "campaign_id": binding.campaign_id,
        "epoch": binding.epoch,
        "configuration_digest": binding.configuration_digest,
        "actors": {
            route: binding.actors[route]
            for route in ("control", "evaluator")
            if route in binding.actors
        },
    }
    encoded = json.dumps(normalized, separators=(",", ":")).encode()
    return f"research-{hashlib.sha256(encoded).hexdigest()}-{generation}"


class CloudflareBridge:
    """A private adapter credential never becomes a research or worker credential."""

    def __init__(self, url: str, token: str, *, timeout: float = 30) -> None:
        url = controller_url(url)
        if urlsplit(url).path not in {"", "/"}:
            raise ValueError("Cloudflare adapter URL must be an origin")
        if not 32 <= len(token) <= 4096 or any(not 33 <= ord(c) <= 126 for c in token):
            raise ValueError("Adapter token must contain 32 to 4096 printable non-space characters")
        if isinstance(timeout, bool) or not 0 < timeout <= 120:
            raise ValueError("Adapter timeout must be between 0 and 120 seconds")
        self.http = httpx.AsyncClient(
            base_url=url,
            headers={"Authorization": "Bearer " + token},
            timeout=timeout,
            trust_env=False,
            follow_redirects=False,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self.http.aclose()

    async def _request(self, path: str, payload: JsonObject) -> JsonObject:
        encoded = canonical_bytes(payload)
        if len(encoded) > MAX_BYTES:
            raise ValueError("Adapter request exceeds its bound")
        try:
            async with self.http.stream(
                "POST",
                "/adapter/" + path,
                content=encoded,
                headers={"Content-Type": "application/json"},
            ) as response:
                content = bytearray()
                async for block in response.aiter_bytes(MAX_BYTES):
                    if len(content) + len(block) > MAX_BYTES:
                        raise ValueError("Adapter response exceeds its bound")
                    content.extend(block)
                result = strict_json_loads(bytes(content))
                if not isinstance(result, dict):
                    raise ValueError("Invalid adapter response")
                if response.status_code in {401, 403}:
                    raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Adapter access denied")
                if response.status_code == 409 and result == {
                    "error": "ADAPTER_CAPABILITY_UNSUPPORTED"
                }:
                    raise ResearchError(
                        ErrorCode.ADAPTER_CAPABILITY_UNSUPPORTED, "Adapter profile unavailable"
                    )
                if response.status_code != 200 or "error" in result:
                    raise ValueError("Adapter did not confirm acceptance")
                return dict(result)
        except (httpx.HTTPError, ValueError, RecursionError):
            # A failed response does not establish whether the provider accepted the request.
            raise ResearchError(
                ErrorCode.DELIVERY_UNCERTAIN,
                "Cloudflare delivery was not confirmed",
                retryable=True,
            ) from None

    async def start(self, binding: BindingReference, options: WorkflowOptions | None = None) -> str:
        options = options or WorkflowOptions()
        result = await self._request(
            "start",
            {
                "binding": GatewayBinding.from_reference(binding).model_dump(mode="json"),
                "options": options.model_dump(mode="json"),
            },
        )
        expected = workflow_id(binding)
        if result != {"id": expected}:
            raise ResearchError(
                ErrorCode.DELIVERY_UNCERTAIN,
                "Adapter returned another start identity",
                retryable=True,
            )
        return expected

    async def status(self, binding: BindingReference) -> WorkflowStatus:
        result = await self._request(
            "status",
            {
                "binding": GatewayBinding.from_reference(binding).model_dump(mode="json"),
            },
        )
        try:
            status = WorkflowStatus.model_validate(result)
            if status.id != workflow_id(binding, status.generation) or (
                status.initial_pending is not None
                and status.initial_pending.binding != GatewayBinding.from_reference(binding)
            ):
                raise ValueError("Foreign Workflow status")
            return status
        except ValueError:
            raise ResearchError(
                ErrorCode.DELIVERY_UNCERTAIN, "Invalid adapter status", retryable=True
            ) from None

    async def notify(self, binding: BindingReference, delivery: WakeupDelivery) -> None:
        if (
            delivery.campaign_id != binding.campaign_id
            or delivery.epoch != binding.epoch
            or binding.actors.get(delivery.route) != delivery.actor_id
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Wakeup belongs to another Workflow")
        result = await self._request(
            "wake",
            {
                "binding": GatewayBinding.from_reference(binding).model_dump(mode="json"),
                "actor_id": delivery.actor_id,
                "wait_id": delivery.wait_id,
            },
        )
        if result != {"accepted": True} or type(result.get("accepted")) is not bool:
            raise ResearchError(
                ErrorCode.DELIVERY_UNCERTAIN, "Wakeup acceptance was not confirmed", retryable=True
            )

    async def recover(
        self,
        binding: BindingReference,
        *,
        request_id: str,
        expected: WorkflowStatus,
        reason: str,
    ) -> WorkflowRecoveryReceipt:
        if expected.status != "errored" or expected.id != workflow_id(binding, expected.generation):
            raise ValueError("Recovery requires the selected binding's errored Workflow")
        request = WorkflowRecoveryRequest(
            binding=GatewayBinding.from_reference(binding),
            request_id=request_id,
            expected_generation=expected.generation,
            expected_instance_id=expected.id,
            reason=reason,
        )
        if not request.reason.strip():
            raise ValueError("Recovery requires a reason")
        result = await self._request("recover", request.model_dump(mode="json"))
        try:
            receipt = WorkflowRecoveryReceipt.model_validate(result)
            if receipt.model_dump() != {
                "request_id": request_id,
                "predecessor_id": expected.id,
                "predecessor_generation": expected.generation,
                "id": workflow_id(binding, expected.generation + 1),
                "generation": expected.generation + 1,
            }:
                raise ValueError("Recovery receipt identifies another successor")
            return receipt
        except ValueError:
            raise ResearchError(
                ErrorCode.DELIVERY_UNCERTAIN, "Invalid Workflow recovery receipt", retryable=True
            ) from None

    async def deliver(
        self, binding: BindingReference, driver: DurableWaitDriver
    ) -> tuple[str, ...]:
        if (driver.campaign_id, driver.epoch) != (
            binding.campaign_id,
            binding.epoch,
        ) or driver.actor_id not in binding.actors.values():
            raise ResearchError(ErrorCode.STALE_FENCE, "Outbox belongs to another Workflow")

        await run_owned_step(partial(validate_outbox, binding, driver))

        async def notify(delivery: WakeupDelivery) -> None:
            await self.notify(binding, delivery)

        return await driver.deliver_once(notify)
