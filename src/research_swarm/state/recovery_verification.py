"""Owner-supplied synchronous admission verification at the authority snapshot."""

from typing import Protocol

from research_swarm.control.recovery_proofs import require_native_admission, verify_admission
from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.state.sql import SqlSession, StoreIdentity


class RecoveryAdmissionVerifier(Protocol):
    def verify(
        self,
        sql: SqlSession,
        *,
        authority: StoreIdentity,
        context: CommandContext,
        receipt: RestoreReceipt,
        admission: RecoveryAdmission,
        crypto: Ed25519Provider,
    ) -> None:
        """Read this snapshot and verify proof; no I/O, writes, clocks or nested commands."""
        ...


class NativeRecoveryVerifier:
    def verify(
        self,
        sql: SqlSession,
        *,
        authority: StoreIdentity,
        context: CommandContext,
        receipt: RestoreReceipt,
        admission: RecoveryAdmission,
        crypto: Ed25519Provider,
    ) -> None:
        if authority.backend != "sqlite":
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Native recovery requires its SQLite authority"
            )
        require_native_admission(admission)
        verify_admission(admission, receipt, crypto_provider=crypto)


def require_research_fences(sql: SqlSession, workspace: str, admission: RecoveryAdmission) -> None:
    for kind, identifiers, states in (
        ("orchestration_binding", admission.orchestration_campaign_ids, {"fenced"}),
        ("campaign", admission.paused_campaign_ids, {"pausing", "paused"}),
    ):
        for identifier in identifiers:
            current = sql.execute(
                "SELECT state FROM records WHERE workspace_id=? AND kind=? AND id=?",
                (workspace, kind, identifier),
            ).fetchone()
            if current is None or current["state"] not in states:
                raise ResearchError(ErrorCode.STALE_FENCE, "Recovery research fence differs")
