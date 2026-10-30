"""Activation requires a native capability for the same complete transaction cut."""

from research_swarm.control.activation_view import inspect_activation
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.state.database import Transaction
from research_swarm.state.recovery_guard import require_recovery_authority
from research_swarm.state.sql import SqlSession, StoreIdentity


def require_recovery_activation(
    connection: SqlSession, context: CommandContext, authority: StoreIdentity, request_digest: str
) -> None:
    proof = context._recovery_activation
    if proof is None or proof.request_digest != request_digest:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Native activation proof is required")
    receipt = require_recovery_authority(connection, context, authority, proof)
    if context.expected_revision != proof.gate_revision:
        raise ResearchError(ErrorCode.REVISION_CONFLICT, "Activation gate revision differs")
    view = inspect_activation(Transaction(connection, context, observe_waits=False), receipt)
    if view.digest != proof.view_digest or view.artifact_digest != proof.artifact_digest:
        raise ResearchError(ErrorCode.STALE_FENCE, "Verified recovery activation cut changed")
