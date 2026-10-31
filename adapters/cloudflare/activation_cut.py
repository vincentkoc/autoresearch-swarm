"""Bound a hosted activation snapshot before the shared kernel projects any payloads."""

from dataclasses import dataclass

from pydantic import TypeAdapter

from research_swarm.checkpoints.capture import CaptureLimits, preflight_rows
from research_swarm.checkpoints.image_schema import require_portable_image_schema
from research_swarm.control.activation_view import ActivationView, inspect_activation
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID
from research_swarm.state.database import Transaction

if __package__:
    from .provisioning import require_roster
else:
    from provisioning import require_roster

LIMITS = CaptureLimits(image_bytes=8 * 1024**2, rows=8192)
MAX_ARTIFACTS = 1024
MAX_ARTIFACT_BYTES = 32 * 1024**2


@dataclass(frozen=True)
class HostedActivationCut:
    view: ActivationView
    retired_credential_ids: tuple[str, ...]


def require_activation_bounds(sql):
    """Also call in the final outer transaction, before the command activation guard."""
    require_portable_image_schema(sql)
    preflight_rows(sql, LIMITS)


def inspect_activation_cut(sql, context, receipt, runtime, storage):
    require_activation_bounds(sql)
    if (
        context.principal.workspace_id != receipt.workspace_id
        or runtime.plan.restore_id != receipt.restore_id
        or runtime.plan.authority.workspace_id != receipt.workspace_id
        or storage.workspace_id != receipt.workspace_id
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Hosted activation cut identity differs")
    require_roster(sql, runtime.plan.credentials)
    view = inspect_activation(Transaction(sql, context, observe_waits=False), receipt)
    if (
        len(view.artifacts) > MAX_ARTIFACTS
        or any(item.reference.size_bytes > storage.max_object_bytes for item in view.artifacts)
        or sum(item.reference.size_bytes for item in view.artifacts) > MAX_ARTIFACT_BYTES
    ):
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Activation artifact bound exceeded")
    target_ids = {member.id for member in runtime.plan.credentials}
    retired = tuple(
        row[0]
        for row in sql.execute(
            "SELECT id FROM credentials WHERE workspace_id=? ORDER BY id", (receipt.workspace_id,)
        )
        if row[0] not in target_ids
    )
    retired = TypeAdapter(tuple[ID, ...]).validate_python(retired)
    return HostedActivationCut(view, retired)
