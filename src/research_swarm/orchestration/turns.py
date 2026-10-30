"""API-only single-action delivery boundary shared by local and hosted drivers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from research_swarm.client import ResearchClient
from research_swarm.control.orchestration_models import DriveTurnTarget, PrepareDriveTurn
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import (
    DriveAction,
    DriveTurn,
    OrchestrationBinding,
    drive_turn_id,
)
from research_swarm.state.database import JsonObject


class DurableTurnDriver:
    """Retry the same ordinal; never wrap a multi-action advance loop in provider retries."""

    def __init__(
        self, client: ResearchClient, campaign_id: str, *, epoch: int, configuration_digest: str
    ) -> None:
        self.client, self.campaign_id = client, campaign_id
        self.epoch, self.configuration_digest = epoch, configuration_digest

    def advance_turn(
        self, ordinal: int, plan: Callable[[], DriveAction], *, planning_watermark: int = 0
    ) -> JsonObject:
        row = self.client.get("orchestration_binding", self.campaign_id)
        binding = OrchestrationBinding.model_validate(row["data"])
        identifier = drive_turn_id(binding.workspace_id, self.campaign_id, self.epoch, ordinal)
        try:
            saved = self.client.get("drive_turn", identifier)
        except ResearchError as error:
            if error.code != ErrorCode.NOT_FOUND:
                raise
            if row["state"] != "active" or binding.epoch != self.epoch:
                raise ResearchError(ErrorCode.STALE_FENCE, "Driver binding is fenced") from None
            if binding.configuration_digest != self.configuration_digest:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Driver configuration changed"
                ) from None
            if binding.pending_turn_id is not None or ordinal != binding.next_ordinal:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Recover the existing drive ordinal first"
                ) from None
            request = PrepareDriveTurn(
                campaign_id=self.campaign_id,
                epoch=self.epoch,
                ordinal=ordinal,
                configuration_digest=self.configuration_digest,
                planning_watermark=planning_watermark,
                action=plan(),
            )
            saved = self.client.command(
                "orchestration.prepare_turn",
                request,
                idempotency_key=record_digest("research.drive-prepare-v1", {"turn_id": identifier}),
                expected_revision=row["revision"],
            )["result"]
        turn = DriveTurn.model_validate(saved["data"])
        if (turn.campaign_id, turn.epoch, turn.ordinal, turn.configuration_digest) != (
            self.campaign_id,
            self.epoch,
            ordinal,
            self.configuration_digest,
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Saved turn differs from driver configuration"
            )
        return dict(
            self.client.command(
                "orchestration.execute_turn",
                DriveTurnTarget(turn_id=identifier),
                idempotency_key=record_digest("research.drive-execute-v1", {"turn_id": identifier}),
            )["result"]
        )

    async def advance_turn_async(
        self, ordinal: int, plan: Callable[[], DriveAction], *, planning_watermark: int = 0
    ) -> JsonObject:
        return await asyncio.to_thread(
            self.advance_turn, ordinal, plan, planning_watermark=planning_watermark
        )
