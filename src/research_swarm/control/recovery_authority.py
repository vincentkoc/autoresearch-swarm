"""Capture the elected target independently of any particular recovery operation."""

from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryAuthority
from research_swarm.state.database import Database, Transaction
from research_swarm.state.sql import SqlSession


def recovery_authority(
    database: Database, context: CommandContext, *, crypto_provider: Ed25519Provider | None = None
) -> RecoveryAuthority | None:
    def read(connection: SqlSession) -> tuple[int, RestoreReceipt, RecoveryAdmission] | None:
        workspace = context.principal.workspace_id
        gate = connection.execute(
            "SELECT state,revision FROM records WHERE workspace_id=? "
            "AND kind='workspace_gate' AND id=?",
            (workspace, workspace),
        ).fetchone()
        if gate is None or gate["state"] == "active":
            return None
        if gate["state"] != "recovering":
            raise ResearchError(ErrorCode.STALE_FENCE, "Workspace does not admit recovery evidence")
        current = connection.execute(
            "SELECT control_epoch FROM workspaces WHERE id=?", (workspace,)
        ).one()
        restored = connection.execute(
            "SELECT id FROM records WHERE workspace_id=? AND kind='restoration' "
            "AND json_extract(payload,'$.target_control_epoch')=?",
            (workspace, current["control_epoch"]),
        ).one()
        transaction = Transaction(connection, context)
        receipt = RestoreReceipt.model_validate(
            transaction.get("restoration", restored["id"])["data"]
        )
        admitted = RecoveryAdmission.model_validate(
            transaction.get("recovery_admission", receipt.restore_id)["data"]
        )
        database.verify_recovery_admission(
            connection, context, receipt, admitted, crypto_provider=crypto_provider
        )
        return gate["revision"], receipt, admitted

    snapshot = database.store.read(read)
    if snapshot is None:
        return None
    revision, receipt, admitted = snapshot
    return RecoveryAuthority(
        authority=(database.authority_identity.backend, database.authority_identity.location),
        restore_id=receipt.restore_id,
        restoration_digest=receipt.digest,
        admission_digest=record_digest(
            "research.recovery-admission-envelope.v1", admitted.model_dump(mode="json")
        ),
        gate_revision=revision,
        control_epoch=receipt.target_control_epoch,
        actor_id=context.principal.id,
    )
