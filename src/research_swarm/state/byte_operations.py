"""Byte operations must close before a workspace can seal or activate."""

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction
from research_swarm.state.sql import SqlSession


def require_closed_byte_operations(transaction: Transaction) -> None:
    require_closed_byte_operations_in(transaction.connection, transaction.workspace_id)


def require_closed_byte_operations_in(connection: SqlSession, workspace_id: str) -> None:
    # Mutators admit a pending upload/deletion before I/O; readers retain a pin.
    transient = connection.execute(
        "SELECT kind,id,state FROM records WHERE workspace_id=? AND ("
        "(kind='upload' AND state NOT IN ('finalized','aborted')) OR "
        "(kind='worker_upload' AND state NOT IN ('finalized','aborted')) OR "
        "(kind='artifact' AND state='deleting') OR "
        "(kind='artifact_reference' AND state='retained' "
        "AND (json_extract(payload,'$.retention.owner_kind')='artifact_read' "
        "OR json_extract(payload,'$.reader_session_id') IS NOT NULL))) LIMIT 1",
        (workspace_id,),
    ).fetchone()
    if transient is not None:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE,
            "Artifact operations must close before changing workspace authority",
            details={"kind": transient["kind"], "id": transient["id"]},
        )
    inconsistent = connection.execute(
        "SELECT w.id FROM records w LEFT JOIN records u ON u.workspace_id=w.workspace_id "
        "AND u.kind='upload' AND u.id=w.id WHERE w.workspace_id=? AND w.kind='worker_upload' "
        "AND (u.id IS NULL OR w.state!=u.state "
        "OR json_extract(w.payload,'$.upload_id') IS NOT w.id) LIMIT 1",
        (workspace_id,),
    ).fetchone()
    if inconsistent is not None:
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Worker upload closure differs")
