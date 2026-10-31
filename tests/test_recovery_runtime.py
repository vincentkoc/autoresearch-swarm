"""Preparation capabilities cannot become execution or credential repair authority."""

from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from research_swarm.checkpoints.admission import admit_recovery
from research_swarm.control.auth import CredentialStore
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryRuntimeGuard
from research_swarm.domain.recovery_runtime import (
    RecoveryRuntimeCredential,
    RecoveryRuntimePlan,
    RequalifyRecoveryRuntime,
)
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction, _request_digest
from research_swarm.state.recovery_runtime import PLAN_KIND
from research_swarm.state.sql import StoreIdentity
from research_swarm.state.time_providers import (
    ProviderChanged,
    TimeProviders,
    fencing_payload,
    qualification_payload,
    registration_payload,
)
from research_swarm.state.workspace_gate import require_workspace_mutation
from tests.test_checkpoint_import import snapshot
from tests.test_recovery_admission import election as election
from tests.test_recovery_admission import prepared
from tests.test_recovery_admission import sealed_runtime as sealed_runtime
from tests.test_time_witnesses import SEED, encoded, payload, scope, source


@pytest.fixture
def runtime(election):
    native, context, bundle, claim = prepared(election)
    admit_recovery(native, context, bundle, claim)
    database = native.database
    authority = recovery_authority(database, context)
    assert authority is not None
    token = "rsw_" + "R" * 43
    member = RecoveryRuntimeCredential(
        id=new_id(), principal=context.principal, token_digest=CredentialStore._digest(token)
    )
    plan = RecoveryRuntimePlan(
        restore_id=authority.restore_id,
        deployment_digest="sha256:" + "a" * 64,
        configuration_digest="sha256:" + "b" * 64,
        authority=scope().authority.model_copy(
            update={
                "workspace_id": context.principal.workspace_id,
                "controller_id": native.identity.controller_id,
                "control_epoch": authority.control_epoch,
                "activation_id": new_id(),
            }
        ),
        source=source(),
        credentials=(member,),
    )
    database.store.write(
        lambda sql: Transaction(sql, context).insert(
            PLAN_KIND, plan.restore_id, plan.model_dump(mode="json"), state="immutable"
        )
    )
    return SimpleNamespace(
        native=native,
        database=database,
        context=context,
        authority=authority,
        plan=plan,
        member=member,
        token=token,
        registry=TimeProviders(database.authority_identity),
    )


def guarded(r, operation, data, *, context=None):
    context = (context or r.context).model_copy()
    context._recovery_runtime = RecoveryRuntimeGuard(
        **asdict(r.authority),
        plan_digest=r.plan.digest,
        operation=operation,
        request_digest=_request_digest(context, operation, data),
    )
    return context


def credential_payload(member):
    return {
        "id": member.id,
        "principal": member.principal.model_dump(mode="json"),
        "token_digest": member.token_digest,
    }


def install(r, *, context=None, member=None, token=None, sql=None, authority=None):
    member = member or r.member
    context = context or guarded(r, "credential.install", credential_payload(member))

    def write(connection):
        return CredentialStore.install_in(
            connection,
            r.context.principal,
            member.principal,
            identifier=member.id,
            token=token or r.token,
            context=context,
            authority=authority or r.database.authority_identity,
        )

    return write(sql) if sql is not None else r.database.store.write(write)


def register(r, *, authority=None, configured=None):
    authority, configured = authority or r.plan.authority, configured or r.plan.source
    context = guarded(
        r,
        "time_provider.register",
        registration_payload(authority, configured),
        context=r.context.model_copy(update={"expected_revision": 0}),
    )
    return r.database.store.write(
        lambda sql: r.registry.register(Transaction(sql, context), authority, configured)
    )


def qualification(r, current, *, proposed=None):
    context = r.context.model_copy(
        update={"command_id": new_id(), "expected_revision": current.revision}
    )
    data = {"plan_digest": r.plan.digest}
    operation, pending = r.registry.prepare(
        context, current, "runtime.prepare", data, source=proposed
    )
    context = guarded(
        r,
        "time_provider.requalify",
        qualification_payload(operation, "runtime.prepare", data),
        context=context,
    )
    value = payload(pending.request).model_copy(update={"source_id": pending.source.id})
    proof = pending.complete(encoded(WitnessSigner(pending.source, SEED).sign(value)))
    return context, operation, proof, data


def qualify(r, context, operation, proof, data):
    return r.database.store.write(
        lambda sql: r.registry.requalify(
            Transaction(sql, context), operation, proof, name="runtime.prepare", payload=data
        )
    )


def test_fresh_credentials_and_witness_prepare_without_activating(runtime):
    r = runtime
    before = snapshot(r.database.store)
    issued = install(r)
    current = register(r)
    assert current.state == "unqualified"
    assert r.native.credentials.resolve(issued.token) == r.member.principal
    context, operation, proof, data = qualification(r, current)
    result = qualify(r, context, operation, proof, data)
    assert result.state == "qualified" and result.revision == current.revision + 1
    assert result.data.scope.provider_generation != current.data.scope.provider_generation
    after = snapshot(r.database.store)
    for table in ("budgets", "budget_reservations", "budget_settlements", "budget_entries"):
        assert after[table] == before[table]
    assert (
        r.database.read_record(r.context, "workspace_gate", r.member.principal.workspace_id)[
            "state"
        ]
        == "recovering"
    )
    with pytest.raises(ResearchError, match="inactive"):
        r.native.credentials.issue(r.context.principal, r.context.principal)
    assert "_recovery_runtime" not in context.model_dump(mode="json")
    assert "_recovery_runtime" not in CommandContext.model_json_schema()["properties"]


@pytest.mark.parametrize("collision", ["same", "id", "token", "revoked", "expiring"])
def test_existing_credentials_are_never_reinstalled_or_repaired(runtime, collision):
    r = runtime
    install(r)
    if collision in {"revoked", "expiring"}:
        r.database.store.write(
            lambda sql: sql.execute(
                "UPDATE credentials SET revoked=?,expires_at=? WHERE id=?",
                (
                    int(collision == "revoked"),
                    "2099-01-01T00:00:00Z" if collision == "expiring" else None,
                    r.member.id,
                ),
            )
        )
    member = r.member
    token = r.token
    if collision == "id":
        token = "rsw_" + "S" * 43
        member = member.model_copy(update={"token_digest": CredentialStore._digest(token)})
    elif collision == "token":
        member = member.model_copy(update={"id": new_id()})
    if collision in {"id", "token"}:
        r.plan = r.plan.model_copy(update={"credentials": (member,)})
        r.database.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET payload=? WHERE kind=?",
                (r.plan.model_dump_json(), PLAN_KIND),
            )
        )
    before = snapshot(r.database.store)
    with pytest.raises(ResearchError, match="must be fresh"):
        install(r, member=member, token=token)
    assert snapshot(r.database.store) == before


@pytest.mark.parametrize("change", ["id", "principal", "token", "store", "issuer", "digest"])
def test_credential_effect_is_bound_to_actual_arguments(runtime, change):
    r = runtime
    member, token, authority = r.member, r.token, r.database.authority_identity
    if change == "id":
        member = member.model_copy(update={"id": new_id()})
    elif change == "principal":
        member = member.model_copy(
            update={"principal": member.principal.model_copy(update={"id": new_id()})}
        )
    elif change == "token":
        token = "rsw_" + "S" * 43
    elif change == "store":
        authority = StoreIdentity("sqlite", "other-synthetic-store")
    context = guarded(r, "credential.install", credential_payload(member))
    if change == "issuer":
        context = context.model_copy(
            update={"principal": context.principal.model_copy(update={"id": new_id()})}
        )
    elif change == "digest":
        context._recovery_runtime = replace(
            context._recovery_runtime, plan_digest="sha256:" + "c" * 64
        )
    before = snapshot(r.database.store)
    with pytest.raises(ResearchError):
        install(r, context=context, member=member, token=token, authority=authority)
    assert snapshot(r.database.store) == before


@pytest.mark.parametrize("phase", ["active", "restoring", "sealed", "absent"])
def test_retained_preparation_guard_never_works_in_another_phase(runtime, phase):
    r = runtime
    context = guarded(r, "credential.install", credential_payload(r.member))
    r.database.store.write(
        lambda sql: sql.execute(
            "DELETE FROM records WHERE kind='workspace_gate'"
            if phase == "absent"
            else "UPDATE records SET state=? WHERE kind='workspace_gate'",
            () if phase == "absent" else (phase,),
        )
    )
    before = snapshot(r.database.store)
    with pytest.raises(ResearchError):
        install(r, context=context)
    assert snapshot(r.database.store) == before


@pytest.mark.parametrize(
    "other",
    [
        "_drive_guard",
        "_recovery_guard",
        "_recovery_operation",
        "_recovery_reader",
        "_recovery_turn",
        "_recovery_route",
        "_recovery_activation",
    ],
)
def test_preparation_cannot_mix_with_other_capabilities(runtime, other):
    r = runtime
    context = guarded(r, "credential.install", credential_payload(r.member))
    setattr(context, other, object())
    before = snapshot(r.database.store)
    with pytest.raises(ResearchError):
        install(r, context=context)
    assert snapshot(r.database.store) == before


@pytest.mark.parametrize(
    "operation",
    ["worker.claim", "workspace.activate_recovery", "artifact.begin_upload"],
)
def test_preparation_guard_cannot_authorize_unrelated_mutation(runtime, operation):
    r = runtime
    context = guarded(r, operation, {})
    with pytest.raises(ResearchError, match="preparation proof"):
        r.database.store.write(
            lambda sql: require_workspace_mutation(
                sql,
                context.principal.workspace_id,
                operation,
                context=context,
                authority=r.database.authority_identity,
                payload={},
                request_digest=_request_digest(context, operation, {}),
            )
        )


@pytest.mark.parametrize("damage", [None, "revision", "leaf", "mixed", "active"])
def test_requalification_root_requires_its_exact_plan_and_command(runtime, damage):
    r = runtime
    name = "workspace.requalify_runtime"
    data = RequalifyRecoveryRuntime(
        request_id=new_id(),
        expected_provider_revision=3,
        previous_qualification_digest="sha256:" + "a" * 64,
        reason="Explicit target runtime requalification",
    ).model_dump(mode="json")
    context = guarded(r, name, data, context=r.context.model_copy(update={"expected_revision": 3}))
    if damage == "revision":
        context = guarded(
            r, name, data, context=context.model_copy(update={"expected_revision": 4})
        )
    elif damage == "leaf":
        context = guarded(r, "time_provider.requalify", data, context=context)
    elif damage == "mixed":
        context._recovery_activation = object()
    elif damage == "active":
        r.database.store.write(
            lambda sql: sql.execute("UPDATE records SET state='active' WHERE kind='workspace_gate'")
        )
    before = snapshot(r.database.store)

    def check():
        return r.database.store.read(
            lambda sql: require_workspace_mutation(
                sql,
                context.principal.workspace_id,
                name,
                context=context,
                authority=r.database.authority_identity,
                payload=data,
                request_digest=_request_digest(context, name, data),
            )
        )

    if damage is None:
        check()
    else:
        with pytest.raises(ResearchError):
            check()
    assert snapshot(r.database.store) == before


@pytest.mark.parametrize("damage", [None, "leaf", "plan", "admission", "active"])
def test_runtime_retirement_is_bound_and_cannot_reactivate_old_source(runtime, damage):
    r = runtime
    current = register(r)
    current = qualify(r, *qualification(r, current))
    context = r.context.model_copy(
        update={"command_id": new_id(), "expected_revision": current.revision}
    )
    data, name = {"plan_digest": r.plan.digest}, "runtime.activate"
    operation, pending = r.registry.prepare(context, current, name, data)
    value = payload(pending.request).model_copy(update={"source_epoch": new_id()})
    proof = pending.complete(encoded(WitnessSigner(pending.source, SEED).sign(value)))
    with pytest.raises(ProviderChanged) as changed:
        r.database.store.read(
            lambda sql: r.registry.check(
                Transaction(sql, context), operation, proof, name=name, payload=data
            )
        )
    leaf = fencing_payload(changed.value.evidence, name, data)
    owned = guarded(r, "time_provider.fence", leaf, context=context)
    if damage == "leaf":
        owned._recovery_runtime = replace(
            owned._recovery_runtime, request_digest="sha256:" + "0" * 64
        )
    elif damage == "plan":
        owned._recovery_runtime = replace(owned._recovery_runtime, plan_digest="sha256:" + "0" * 64)
    elif damage == "admission":
        r.database.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET state='mutable' WHERE kind='recovery_admission'"
            )
        )
    elif damage == "active":
        r.database.store.write(
            lambda sql: sql.execute("UPDATE records SET state='active' WHERE kind='workspace_gate'")
        )
    before = snapshot(r.database.store)

    def fence():
        return r.database.store.write(
            lambda sql: r.registry.fence(
                Transaction(sql, owned), changed.value.evidence, name=name, payload=data
            )
        )

    if damage is not None:
        with pytest.raises(ResearchError):
            fence()
        assert snapshot(r.database.store) == before
        return
    retired = fence()
    assert retired.state == "unqualified" and retired.revision == current.revision + 1
    after = snapshot(r.database.store)
    assert len([row for row in after["records"] if row["kind"] == "time_source_retirement"]) == 1
    assert after["commands"] == before["commands"]
    # qualification() emits the original epoch. Its durable retirement is final.
    with pytest.raises(ResearchError):
        qualify(r, *qualification(r, retired))
    assert snapshot(r.database.store) == after


@pytest.mark.parametrize("change", ["activation", "source", "epoch"])
def test_provider_registration_cannot_change_the_plan(runtime, change):
    r = runtime
    authority, configured = r.plan.authority, r.plan.source
    if change == "activation":
        authority = authority.model_copy(update={"activation_id": new_id()})
    elif change == "epoch":
        authority = authority.model_copy(update={"control_epoch": authority.control_epoch - 1})
    else:
        configured = configured.model_copy(update={"id": new_id()})
    before = snapshot(r.database.store)
    with pytest.raises(ResearchError):
        register(r, authority=authority, configured=configured)
    assert snapshot(r.database.store) == before


@pytest.mark.parametrize("change", ["source", "request", "admission"])
def test_qualification_rechecks_current_custody_and_complete_witness(runtime, change):
    r = runtime
    current = register(r)
    proposed = r.plan.source.model_copy(update={"id": new_id()}) if change == "source" else None
    context, operation, proof, data = qualification(r, current, proposed=proposed)
    if change == "request":
        operation = replace(
            operation, request=operation.request.model_copy(update={"nonce": new_id()})
        )
    elif change == "admission":
        r.database.store.write(
            lambda sql: sql.execute(
                "UPDATE records SET state='mutable' WHERE kind='recovery_admission'"
            )
        )
    before = snapshot(r.database.store)
    with pytest.raises(ResearchError):
        qualify(r, context, operation, proof, data)
    assert snapshot(r.database.store) == before


def test_partial_preparation_rolls_back_inserted_credentials(runtime):
    r = runtime
    before = snapshot(r.database.store)

    def partial(sql):
        install(r, sql=sql)
        raise RuntimeError("Synthetic interruption before provider registration")

    with pytest.raises(RuntimeError, match="before provider"):
        r.database.store.write(partial)
    assert snapshot(r.database.store) == before


@pytest.mark.parametrize("change", ["state", "revision", "missing", "epoch"])
def test_plan_record_is_immutable_and_bound_to_the_recovered_epoch(runtime, change):
    r = runtime
    context = guarded(r, "credential.install", credential_payload(r.member))

    def change_plan(sql):
        if change == "missing":
            sql.execute("DELETE FROM records WHERE kind=?", (PLAN_KIND,))
        elif change == "epoch":
            sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.authority.control_epoch',0) "
                "WHERE kind=?",
                (PLAN_KIND,),
            )
        elif change == "state":
            sql.execute("UPDATE records SET state='mutable' WHERE kind=?", (PLAN_KIND,))
        else:
            sql.execute("UPDATE records SET revision=2 WHERE kind=?", (PLAN_KIND,))

    r.database.store.write(change_plan)
    before = snapshot(r.database.store)
    with pytest.raises(ResearchError):
        install(r, context=context)
    assert snapshot(r.database.store) == before


def test_another_root_actor_cannot_prepare_the_elected_operators_runtime(runtime):
    r = runtime
    context = r.context.model_copy(
        update={"principal": r.context.principal.model_copy(update={"id": new_id()})}
    )
    context = guarded(r, "credential.install", credential_payload(r.member), context=context)
    context._recovery_runtime = replace(context._recovery_runtime, actor_id=context.principal.id)
    with pytest.raises(ResearchError, match="owner or plan"):
        r.database.store.write(
            lambda sql: CredentialStore.install_in(
                sql,
                context.principal,
                r.member.principal,
                identifier=r.member.id,
                token=r.token,
                context=context,
                authority=r.database.authority_identity,
            )
        )


@pytest.mark.parametrize("missing", ["context", "authority"])
def test_credential_context_and_store_must_be_supplied_together(runtime, missing):
    r = runtime
    context = guarded(r, "credential.install", credential_payload(r.member))
    with pytest.raises(ResearchError, match="owner context"):
        r.database.store.write(
            lambda sql: CredentialStore.install_in(
                sql,
                r.context.principal,
                r.member.principal,
                identifier=r.member.id,
                token=r.token,
                context=None if missing == "context" else context,
                authority=None if missing == "authority" else r.database.authority_identity,
            )
        )
