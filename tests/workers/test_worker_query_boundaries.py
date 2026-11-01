from __future__ import annotations

import contextvars

import pytest

from research_swarm.control.witness_commands import WitnessExecutionBoundary
from research_swarm.control.witness_time import WitnessAuthorityTimeProvider
from research_swarm.control.worker_artifacts import WorkerArtifacts
from research_swarm.control.worker_locality import AdvertiseWorkerCache, WorkerLocality
from research_swarm.control.worker_queries import WorkerQueries, WorkerQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database
from tests.api.test_query_boundaries import clock as clock
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.workers.test_time_providers import Registry
from tests.workers.test_time_witnesses import identifier
from tests.workers.test_witness_time import response
from tests.workers.test_worker_inputs import active_read_pins, assigned_input, revoke_worker


def acknowledgments(database):
    return database.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM commands").one()[0])


def offer_query(challenge, **changes):
    return WorkerQuery(
        worker_id=challenge.worker_id, session_id=challenge.session_id, kind="offers", **changes
    )


def observed_locality(clock):
    registry, _, boundary, database, _, workers, context, _, worker = clock
    locality = WorkerLocality(database, workers)
    publication = context.model_copy(
        update={
            "principal": context.principal.model_copy(
                update={"scopes": context.principal.scopes | {"worker.publish"}}
            ),
            "expected_revision": 0,
        }
    )
    advertisement = AdvertiseWorkerCache(binding=worker.binding, snapshot_id=new_id())
    with boundary.prepare(
        publication,
        registry.current(),
        "worker.cache_advertise",
        advertisement.model_dump(mode="json"),
    ) as turn:
        turn.complete(response(turn))
        locality.advertise(publication, advertisement)
    return locality


def test_offer_ranking_uses_its_own_time_and_preflight_uses_none(clock, monkeypatch):
    registry, provider, boundary, database, _, _, context, challenge, _ = clock
    queries = WorkerQueries(database, observed_locality(clock))
    observed = []
    observe = provider.observe

    def record(scope):
        value = observe(scope)
        observed.append(value)
        return value

    monkeypatch.setattr(provider, "observe", record)
    request = offer_query(challenge)
    before = acknowledgments(database)
    queries.validate_list(context, request)
    assert observed == []
    with boundary.prepare(
        context, registry.current(), "worker.list", request.model_dump(mode="json")
    ) as turn:
        turn.complete(response(turn))
        assert queries.list(context, request) == {"items": [], "next_cursor": None, "scanned": 0}
    assert len(observed) == 1 and observed[0].profile == "hosted-native-witness-v1"
    assert acknowledgments(database) == before
    # Recovery views do not depend on witness availability.
    for kind in ("pending", "controls", "historical_pending"):
        assert queries.list(context, request.model_copy(update={"kind": kind}))["items"] == []


@pytest.mark.parametrize("changed", ["principal", "scope", "session", "cursor"])
def test_offer_preflight_refuses_bad_ownership_or_cursor_before_a_witness(clock, changed):
    registry, _, _, database, _, workers, context, challenge, _ = clock
    queries = WorkerQueries(database, WorkerLocality(database, workers))
    request = offer_query(challenge)
    if changed in {"principal", "scope"}:
        context = context.model_copy(
            update={
                "principal": context.principal.model_copy(
                    update={"id": new_id()} if changed == "principal" else {"scopes": frozenset()}
                )
            }
        )
    else:
        request = request.model_copy(
            update={"session_id": new_id()} if changed == "session" else {"cursor": "!bad"}
        )
    before = registry.database.snapshot(registry.context())
    with pytest.raises(ResearchError):
        queries.validate_list(context, request)
    assert registry.database.snapshot(registry.context()) == before


def test_offer_query_persists_changed_source_and_rejects_nested_timestamp_borrowing(
    clock, monkeypatch
):
    registry, provider, boundary, database, _, _, context, challenge, _ = clock
    locality = observed_locality(clock)
    queries = WorkerQueries(database, locality)
    nested = WorkerQueries(registry.database, locality)
    request = offer_query(challenge)
    copied = contextvars.copy_context()
    refused_observations = []
    observe = provider.observe

    def record(scope):
        try:
            return observe(scope)
        except ResearchError as error:
            refused_observations.append(error.code)
            raise

    monkeypatch.setattr(provider, "observe", record)
    with boundary.prepare(context, registry.current(), "outer", {}) as turn:
        turn.complete(response(turn))
        result = database.query(
            context,
            "outer",
            {},
            "worker.query",
            lambda tx: copied.run(nested.list, context, request),
        )
    # Native locality deliberately ignores an unavailable hint. Its nested query
    # must still refuse the outer timestamp instead of treating it as its own.
    assert result["items"] == [] and refused_observations == [ErrorCode.STALE_FENCE]
    assert registry.current().state == "qualified"
    with boundary.prepare(
        context, registry.current(), "worker.list", request.model_dump(mode="json")
    ) as turn:
        turn.complete(response(turn, source_epoch=identifier(8)))
        with pytest.raises(ResearchError) as refused:
            queries.list(context, request)
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert registry.current().state == "unqualified"


@pytest.fixture
def input_query(pipeline):
    _, worker, credential, request = assigned_input(pipeline)

    def management(revision=None):
        return CommandContext(
            principal=pipeline.runtime.principal(),
            idempotency_key=new_id(),
            expected_revision=revision,
        )

    registry = Registry(pipeline.runtime.database, management)
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(pipeline.runtime.database.authority_identity)
    boundary = WitnessExecutionBoundary(registry.providers, provider)
    database = Database.from_store(pipeline.runtime.database.store, execution_boundary=boundary)
    artifacts = WorkerArtifacts(database, pipeline.runtime.artifacts)
    context = CommandContext(principal=credential.principal, idempotency_key=new_id())
    return pipeline, registry, boundary, database, artifacts, context, request, worker, credential


def test_input_preflight_and_metadata_create_no_read_pin_or_acknowledgment(
    input_query, monkeypatch
):
    pipeline, registry, boundary, database, artifacts, context, request, _, credential = input_query
    before = acknowledgments(database)
    with monkeypatch.context() as patch:
        patch.setattr(
            "research_swarm.control.worker_artifacts.utc_now",
            lambda: pytest.fail("Preflight observed time"),
        )
        reference = artifacts.validate_input(context, request)
        with pytest.raises(ResearchError, match="not an assigned input"):
            artifacts.validate_input(context, request.model_copy(update={"artifact_id": new_id()}))
    with boundary.prepare(
        context, registry.current(), "worker.input", request.model_dump(mode="json")
    ) as turn:
        turn.complete(response(turn))
        assert artifacts.input_metadata(context, request) == reference
    assert acknowledgments(database) == before
    assert active_read_pins(pipeline, credential) == 0


@pytest.mark.parametrize("change", ["worker", "source"])
def test_input_query_rechecks_after_preflight_and_commits_only_source_fences(input_query, change):
    pipeline, registry, boundary, database, artifacts, context, request, worker, credential = (
        input_query
    )
    artifacts.validate_input(context, request)
    before = acknowledgments(database)
    with boundary.prepare(
        context, registry.current(), "worker.input", request.model_dump(mode="json")
    ) as turn:
        turn.complete(
            response(turn, **({"source_epoch": identifier(8)} if change == "source" else {}))
        )
        if change == "worker":
            revoke_worker(pipeline, worker)
        with pytest.raises(ResearchError) as refused:
            artifacts.input_metadata(context, request)
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert registry.current().state == ("unqualified" if change == "source" else "qualified")
    assert acknowledgments(database) == before
    assert active_read_pins(pipeline, credential) == 0
