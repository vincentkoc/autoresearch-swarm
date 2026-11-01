from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from research_swarm.control.time_witnesses import WitnessSigner, WitnessSourceChanged
from research_swarm.domain.backup import WorkspaceSeal
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.time_providers import source_origin_id
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import Database, Transaction
from research_swarm.state.time_providers import ProviderChanged, TimeProviders
from tests.workers.test_time_witnesses import (
    SEED,
    authenticate_arbitrary,
    encoded,
    identifier,
    payload,
    scope,
    source,
)


class Registry:
    def __init__(self, database, context, *, assignment_key_id=None):
        self.database, self.context = database, context
        self.providers = TimeProviders(database.authority_identity)
        controller = database.store.read(
            lambda sql: sql.execute("SELECT controller_id FROM workspaces").one()[0]
        )
        self.authority = scope().authority.model_copy(
            update={"workspace_id": context().principal.workspace_id, "controller_id": controller}
        )
        if assignment_key_id is not None:
            self.authority = self.authority.model_copy(
                update={"assignment_key_id": assignment_key_id}
            )
        self.write(
            context(revision=0), lambda tx: self.providers.register(tx, self.authority, source())
        )

    def write(self, context, callback):
        return self.database.store.write(lambda sql: callback(Transaction(sql, context)))

    def current(self):
        return self.database.store.read(
            lambda sql: self.providers.load(Transaction(sql, self.context()), self.authority)
        )

    def prepare(self, context, *, expected=None, epoch=None, proposed=None):
        operation, waiting = self.providers.prepare(
            context, expected or self.current(), "fixture.operation", {}, source=proposed
        )
        value = payload(waiting.request).model_copy(
            update={
                "source_epoch": epoch or identifier(7),
                "source_id": waiting.source.id,
                "native_profile": waiting.source.native_profile,
                "policy": waiting.source.policy,
            }
        )
        verified = waiting.complete(encoded(WitnessSigner(waiting.source, SEED).sign(value)))
        return operation, verified

    def qualify(self, *, epoch=None, proposed=None):
        context = self.context(revision=self.current().revision)
        operation, verified = self.prepare(context, epoch=epoch, proposed=proposed)
        return self.write(
            context,
            lambda tx: self.providers.requalify(
                tx, operation, verified, name="fixture.operation", payload={}
            ),
        )

    def discontinuity(self, *, epoch=None):
        context = self.context()
        operation, verified = self.prepare(context, epoch=epoch or identifier(8))
        with pytest.raises(ProviderChanged) as changed:
            self.write(
                context,
                lambda tx: self.providers.check(
                    tx, operation, verified, name="fixture.operation", payload={}
                ),
            )
        return context, changed.value


@pytest.fixture
def registry(database, context):
    return Registry(database, context)


def test_registration_and_requalification_require_a_new_operational_witness(registry):
    initial = registry.current()
    assert initial.state == "unqualified" and initial.data.source_epoch is None
    context = registry.context(revision=initial.revision)
    operation, verified = registry.prepare(context)
    with pytest.raises(ResearchError) as unqualified:
        registry.write(
            context,
            lambda tx: registry.providers.check(
                tx, operation, verified, name="fixture.operation", payload={}
            ),
        )
    assert unqualified.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    qualified = registry.write(
        context,
        lambda tx: registry.providers.requalify(
            tx, operation, verified, name="fixture.operation", payload={}
        ),
    )
    assert qualified.data.scope.provider_generation != initial.data.scope.provider_generation
    with pytest.raises(ResearchError) as stale:
        registry.write(
            context,
            lambda tx: registry.providers.check(
                tx, operation, verified, name="fixture.operation", payload={}
            ),
        )
    assert stale.value.code == ErrorCode.REVISION_CONFLICT
    current_context = registry.context()
    operation, verified = registry.prepare(current_context)
    registry.write(
        current_context,
        lambda tx: registry.providers.check(
            tx, operation, verified, name="fixture.operation", payload={}
        ),
    )
    assert len(registry.database.events(registry.context())) == 2


def test_retired_source_cannot_return_after_reopen_or_policy_change(registry):
    first = registry.qualify()
    context, changed = registry.discontinuity()
    fenced = registry.write(
        context,
        lambda tx: registry.providers.fence(
            tx, changed.evidence, name="fixture.operation", payload={}
        ),
    )
    assert fenced.state == "unqualified" and fenced.data == first.data
    before = registry.database.snapshot(registry.context())
    assert (
        registry.write(
            context,
            lambda tx: registry.providers.fence(
                tx, changed.evidence, name="fixture.operation", payload={}
            ),
        )
        == fenced
    )
    assert registry.database.snapshot(registry.context()) == before
    registry.database = Database(registry.database.path)
    registry.providers = TimeProviders(registry.database.authority_identity)
    changed_policy = source().model_copy(
        update={"policy": source().policy.model_copy(update={"maximum_roundtrip_ms": 250})}
    )
    for proposed in (source(), changed_policy):
        with pytest.raises(ResearchError) as retired:
            registry.qualify(proposed=proposed)
        assert retired.value.code == ErrorCode.STALE_FENCE
    second = registry.qualify(epoch=identifier(8))
    assert second.data.scope.provider_generation != first.data.scope.provider_generation
    with pytest.raises(ResearchError) as late:
        registry.write(
            context,
            lambda tx: registry.providers.fence(
                tx, changed.evidence, name="fixture.operation", payload={}
            ),
        )
    assert late.value.code == ErrorCode.REVISION_CONFLICT
    assert registry.current() == second
    context, returned = registry.discontinuity(epoch=identifier(7))
    registry.write(
        context,
        lambda tx: registry.providers.fence(
            tx, returned.evidence, name="fixture.operation", payload={}
        ),
    )
    with pytest.raises(ResearchError) as resurrected:
        registry.qualify(epoch=identifier(7))
    assert resurrected.value.code == ErrorCode.STALE_FENCE


def test_one_requalification_wins_across_independent_connections(registry):
    expected = registry.current()
    barrier = threading.Barrier(2)

    def qualify(_):
        database = Database(registry.database.path)
        providers = TimeProviders(database.authority_identity)
        context = registry.context(revision=expected.revision)
        operation, verified = registry.prepare(context, expected=expected)
        barrier.wait(timeout=5)
        try:
            database.store.write(
                lambda sql: providers.requalify(
                    Transaction(sql, context),
                    operation,
                    verified,
                    name="fixture.operation",
                    payload={},
                )
            )
            return "qualified"
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(qualify, range(2)))
    assert results.count("qualified") == 1
    assert results.count(ErrorCode.REVISION_CONFLICT) == 1
    assert len(registry.database.events(registry.context())) == 2


@pytest.mark.parametrize("fail_hook", [False, True])
def test_rejection_fence_and_acknowledgment_share_the_outer_commit(registry, fail_hook):
    registry.qualify()
    context = registry.context()
    operation, verified = registry.prepare(context, epoch=identifier(8))
    before = registry.database.snapshot(context)
    marker = new_id()

    def handler(tx):
        tx.insert("fixture", marker, {}, state="temporary")
        registry.providers.check(tx, operation, verified, name="fixture.operation", payload={})
        pytest.fail("Changed source was admitted")

    def reject(tx, error):
        assert isinstance(error, ProviderChanged)
        registry.providers.fence(tx, error.evidence, name="fixture.operation", payload={})
        if fail_hook:
            raise RuntimeError("Injected fence commit failure")

    with pytest.raises(RuntimeError if fail_hook else ResearchError):
        registry.database.execute(
            context, "fixture.operation", {}, "write", handler, on_rejection=reject
        )
    after = registry.database.snapshot(context)
    assert not any(item["kind"] == "fixture" for item in after["records"])
    if fail_hook:
        assert after == before
        assert registry.database.replay(context, "fixture.operation", {}, "write") is None
    else:
        assert registry.current().state == "unqualified"
        assert after["watermark"] == before["watermark"] + 1
        assert len(registry.database.pending_notifications()) == after["watermark"]
        with pytest.raises(ResearchError) as replay:
            registry.database.with_preflight(
                context,
                "fixture.operation",
                {},
                "write",
                lambda: pytest.fail("Replay reached an unavailable source"),
            )
        assert replay.value.code == ErrorCode.STALE_FENCE
        assert registry.database.snapshot(context) == after


def test_query_discontinuity_commits_before_refusal(registry):
    registry.qualify()
    context = registry.context()
    operation, verified = registry.prepare(context, epoch=identifier(8))

    def query(tx):
        try:
            registry.providers.check(tx, operation, verified, name="fixture.operation", payload={})
        except ProviderChanged as error:
            registry.providers.fence(tx, error.evidence, name="fixture.operation", payload={})
            return error
        pytest.fail("Query admitted changed source")

    error = registry.write(context, query)
    assert isinstance(error, ProviderChanged)
    assert registry.current().state == "unqualified"
    assert (
        registry.database.store.read(
            lambda sql: sql.execute("SELECT COUNT(*) FROM commands").one()[0]
        )
        == 0
    )


@pytest.mark.parametrize("change", ["actor", "command", "key", "store", "request"])
def test_wrong_operation_cannot_fence_the_current_provider(registry, change):
    registry.qualify()
    context, changed = registry.discontinuity()
    operation = changed.evidence.operation
    if change == "actor":
        context = context.model_copy(
            update={
                "principal": Principal(
                    id=new_id(),
                    workspace_id=context.principal.workspace_id,
                    scopes=frozenset({"*"}),
                )
            }
        )
    elif change == "command":
        context = context.model_copy(update={"command_id": new_id()})
    elif change == "key":
        context = context.model_copy(update={"idempotency_key": new_id()})
    elif change == "store":
        operation = replace(operation, store=replace(operation.store, location="another-authority"))
    else:
        operation = replace(
            operation, request=operation.request.model_copy(update={"nonce": new_id()})
        )
    evidence = replace(changed.evidence, operation=operation)
    before = registry.database.snapshot(registry.context())
    with pytest.raises(ResearchError) as refused:
        registry.write(
            context,
            lambda tx: registry.providers.fence(tx, evidence, name="fixture.operation", payload={}),
        )
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert registry.database.snapshot(registry.context()) == before


def test_authenticated_configuration_change_uses_the_same_retirement_boundary(registry):
    registry.qualify()
    context = registry.context()
    operation, pending = registry.providers.prepare(
        context, registry.current(), "fixture.operation", {}
    )
    value = payload(pending.request).model_copy(update={"native_profile": "changed-profile"})
    with pytest.raises(WitnessSourceChanged) as mismatch:
        pending.complete(encoded(authenticate_arbitrary(value)))
    with pytest.raises(ProviderChanged) as changed:
        registry.write(
            context,
            lambda tx: registry.providers.check(
                tx, operation, mismatch.value.evidence, name="fixture.operation", payload={}
            ),
        )
    registry.write(
        context,
        lambda tx: registry.providers.fence(
            tx, changed.value.evidence, name="fixture.operation", payload={}
        ),
    )
    assert registry.current().state == "unqualified"
    assert changed.value.as_dict()["error"]["details"] == {}


@pytest.mark.parametrize("gate", ["sealed", "restoring"])
def test_direct_query_fence_cannot_mutate_an_inactive_workspace(registry, gate):
    registry.qualify()
    context, changed = registry.discontinuity()
    seal = WorkspaceSeal(
        backup_id=new_id(),
        workspace_id=registry.authority.workspace_id,
        controller_id=registry.authority.controller_id,
        control_epoch=registry.authority.control_epoch,
        operator_id=context.principal.id,
        reason="Inactive fixture",
        sealed_at=utc_now(),
        evidence_through=0,
        unresolved_attempt_ids=(),
        worker_ids=(),
    )
    registry.write(
        context,
        lambda tx: tx.insert(
            "workspace_gate", tx.workspace_id, seal.model_dump(mode="json"), state=gate
        ),
    )
    before = registry.database.snapshot(context)
    with pytest.raises(ResearchError):
        registry.write(
            context,
            lambda tx: registry.providers.fence(
                tx, changed.evidence, name="fixture.operation", payload={}
            ),
        )
    assert registry.database.snapshot(context) == before


def test_retirement_identity_ignores_requalification_policy():
    original = source()
    assert source_origin_id(original, identifier(7)) == (
        "sha256:ee8f9ab39f0b73240db533029e0b414ec8d2152418ca68bbd78d9b62de998e02"
    )
    changed = original.model_copy(update={"native_profile": "another-profile"})
    assert source_origin_id(original, identifier(7)) == source_origin_id(changed, identifier(7))
    assert source_origin_id(original, identifier(7)) != source_origin_id(original, identifier(8))


@pytest.mark.parametrize("change", ["operation", "payload", "expected_revision"])
@pytest.mark.parametrize("boundary", ["check", "fence", "requalify"])
def test_actual_execution_must_match_the_prepared_operation(registry, change, boundary):
    if boundary != "requalify":
        registry.qualify()
    context = registry.context(revision=registry.current().revision)
    operation, verified = registry.prepare(context, epoch=identifier(8))
    evidence = None
    if boundary == "fence":
        with pytest.raises(ProviderChanged) as changed:
            registry.write(
                context,
                lambda tx: registry.providers.check(
                    tx, operation, verified, name="fixture.operation", payload={}
                ),
            )
        evidence = changed.value.evidence
    name, data = "fixture.operation", {}
    if change == "operation":
        name = "another.operation"
    elif change == "payload":
        data = {"changed": True}
    else:
        context = context.model_copy(update={"expected_revision": context.expected_revision + 1})
    before = registry.database.snapshot(context)
    with pytest.raises(ResearchError) as refused:
        if boundary == "fence":
            registry.write(
                context, lambda tx: registry.providers.fence(tx, evidence, name=name, payload=data)
            )
        else:
            method = getattr(registry.providers, boundary)
            registry.write(
                context, lambda tx: method(tx, operation, verified, name=name, payload=data)
            )
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert registry.database.snapshot(context) == before


def test_shared_origin_retirement_fences_each_activation_without_rewriting_first_evidence(registry):
    registry.qualify()
    first_authority = registry.authority
    registry.authority = first_authority.model_copy(update={"activation_id": new_id()})
    registry.write(
        registry.context(revision=0),
        lambda tx: registry.providers.register(tx, registry.authority, source()),
    )
    registry.qualify()
    second_authority = registry.authority
    registry.authority = first_authority
    context, first_change = registry.discontinuity()
    registry.write(
        context,
        lambda tx: registry.providers.fence(
            tx, first_change.evidence, name="fixture.operation", payload={}
        ),
    )
    origin = source_origin_id(source(), identifier(7))
    first_retirement = registry.database.read_record(context, "time_source_retirement", origin)
    registry.authority = second_authority
    # Its sample still names E1, but another activation already retired that origin.
    context, second_change = registry.discontinuity(epoch=identifier(7))
    registry.write(
        context,
        lambda tx: registry.providers.fence(
            tx, second_change.evidence, name="fixture.operation", payload={}
        ),
    )
    assert registry.current().state == "unqualified"
    assert (
        registry.database.read_record(context, "time_source_retirement", origin) == first_retirement
    )
    records = registry.database.snapshot(context)["records"]
    assert sum(item["kind"] == "time_source_retirement" for item in records) == 1
    assert sum(item["kind"] == "time_provider_fence" for item in records) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("state_authority_id", "sha256:" + "f" * 64),
        ("assignment_key_id", "sha256:" + "f" * 64),
        ("control_epoch", 2),
        ("controller_id", identifier(99)),
    ],
)
def test_activation_lookup_requires_the_complete_current_authority(registry, field, value):
    authority = registry.authority.model_copy(update={field: value})
    with pytest.raises(ResearchError) as refused:
        registry.write(registry.context(), lambda tx: registry.providers.load(tx, authority))
    assert refused.value.code == ErrorCode.STALE_FENCE
