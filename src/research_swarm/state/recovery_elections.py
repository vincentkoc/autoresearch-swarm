"""Recheck retained logical proof identity; signature verification belongs to the owner."""

from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.checkpoint_elections import SignedLogicalElection
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.sql import SqlSession

LOGICAL_ELECTION_KIND = "logical_recovery_election"


def retained_election(
    sql: SqlSession, receipt: RestoreReceipt, admission: RecoveryAdmission
) -> SignedLogicalElection | None:
    if admission.logical_election_digest is None:
        return None
    row = sql.execute(
        "SELECT payload FROM records WHERE workspace_id=? AND kind=? AND id=? "
        "AND state='immutable' AND revision=1 AND typeof(payload)='text' "
        "AND campaign_id IS NULL AND parent_id IS NULL "
        "AND length(CAST(payload AS BLOB))<=16384",
        (receipt.workspace_id, LOGICAL_ELECTION_KIND, receipt.restore_id),
    ).fetchone()
    if row is None:
        raise ResearchError(ErrorCode.STALE_FENCE, "Logical recovery election is absent")
    elected = SignedLogicalElection.model_validate(strict_json_loads(row[0]))
    if (
        elected.digest != admission.logical_election_digest
        or elected.election.signed_claim != admission.signed_claim
        or elected.election.signed_claim.claim.request != receipt.claim_request()
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Logical recovery election changed")
    return elected
