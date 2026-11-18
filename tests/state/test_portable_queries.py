"""Service reads preserve their authority through a callback-only state store."""

import pytest

from research_swarm.control.auth import CredentialStore
from research_swarm.control.queries import RecordQuery, ResearchQueries
from research_swarm.control.worker_queries import WorkerQueries
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.state.test_state_store import CallbacksOnly
from tests.workers.test_worker_queries import claim, query, read, scoped_worker


def context(principal):
    return CommandContext(principal=principal, idempotency_key=new_id())


def test_query_snapshots_and_scoped_worker_access_without_native_connection(pipeline):
    client, worker, token = scoped_worker(pipeline)
    attempt = claim(client, worker, pipeline.ready())
    runtime = pipeline.runtime
    portable = Database.from_store(CallbacksOnly(runtime.database.store))
    credentials = CredentialStore(portable)
    principal = credentials.resolve(token)
    assert "read" not in principal.scopes
    worker_queries = WorkerQueries(portable)
    assert worker_queries.get(context(principal), read(worker, "attempt", attempt["id"])) == attempt
    assert worker_queries.list(context(principal), query(worker, "pending")) == client.worker_list(
        query(worker, "pending")
    )
    stranger = principal.model_copy(update={"id": new_id()})
    with pytest.raises(ResearchError) as denied:
        worker_queries.get(context(stranger), read(worker, "attempt", attempt["id"]))
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    queries = ResearchQueries(portable)
    with pytest.raises(ResearchError) as global_read:
        queries.list(context(principal), RecordQuery(kind="campaign"))
    assert global_read.value.code == ErrorCode.AUTHORIZATION_DENIED
    operator = context(runtime.principal())
    campaign_id = pipeline.campaign["id"]
    assert queries.list(operator, RecordQuery(kind="campaign", limit=1)) == pipeline.client.list(
        RecordQuery(kind="campaign", limit=1)
    )
    assert queries.budget(operator, campaign_id) == pipeline.client.budget(campaign_id)
    assert queries.context(operator, campaign_id) == pipeline.client.context(campaign_id)
    issued = credentials.issue(runtime.principal(), stranger)
    assert credentials.resolve(issued.token) == stranger
    credentials.revoke(runtime.principal(), issued.id)
    with pytest.raises(ResearchError) as revoked:
        credentials.resolve(issued.token)
    assert revoked.value.code == ErrorCode.AUTHORIZATION_DENIED
