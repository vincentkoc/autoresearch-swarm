"""Shared authority admission, including callers that do not use an HTTP server."""

from research_swarm.domain.backup import WorkspaceSeal
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.sql import SqlSession


def require_workspace_mutation(connection: SqlSession, workspace_id: str, operation: str) -> None:
    row = connection.execute(
        "SELECT state,payload FROM records WHERE workspace_id=? AND kind='workspace_gate' AND id=?",
        (workspace_id, workspace_id),
    ).fetchone()
    if row is None:
        return
    seal = WorkspaceSeal.model_validate(strict_json_loads(row["payload"]))
    if seal.workspace_id != workspace_id:
        raise ResearchError(ErrorCode.STALE_FENCE, "Workspace recovery identity differs")
    if row["state"] == "sealed" and operation == "workspace.seal":
        return  # The command still verifies the exact original seal and current revision.
    raise ResearchError(
        ErrorCode.STALE_FENCE,
        "Workspace authority is inactive",
        details={"phase": row["state"], "backup_id": seal.backup_id},
    )
