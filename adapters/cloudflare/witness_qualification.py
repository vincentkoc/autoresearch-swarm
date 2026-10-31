"""Synthetic witness/lifecycle probe. No native time or hosted clock qualification."""

import sys
from uuid import NAMESPACE_URL, uuid5

from durable_sql import DurableObjectStore
from ed25519 import NobleEd25519

from research_swarm.control.time_authority import ClockAuthorityScope
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.control.witness_commands import WitnessCommandBoundary
from research_swarm.control.witness_time import WitnessAuthorityTimeProvider
from research_swarm.crypto import public_key
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.clocks import ClockPolicy
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.time_witnesses import (
    TimeWitnessPayload,
    WitnessAuthorityScope,
    WitnessSource,
    witness_key_id,
)
from research_swarm.state.database import SCHEMA, Database, Transaction
from research_swarm.state.time_providers import TimeProviders

# Public RFC 8032 fixture only. This endpoint must never be deployed.
SEED = bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")


def identifier(name):
    return str(uuid5(NAMESPACE_URL, "research-witness-qualification/" + name))


class BootstrapFailure(Exception):
    pass


def exercise(storage, object_id, case, module, *, fail_bootstrap=False):
    if case not in {"sample", "changed-source", "domain-reject"}:
        raise ValueError("Unknown witness qualification case")
    store = DurableObjectStore(
        storage, namespace="witness-fixture", object_id=object_id, schema=SCHEMA
    )
    crypto = NobleEd25519(module)
    registry = TimeProviders(store.identity, crypto_provider=crypto)
    provider = WitnessAuthorityTimeProvider(store.identity)
    boundary = WitnessCommandBoundary(registry, provider)
    database = Database.from_store(store, execution_boundary=boundary)
    workspace, controller = identifier("workspace"), identifier("controller")
    database.bootstrap(workspace, controller)
    principal = Principal(id=identifier("operator"), workspace_id=workspace, scopes={"*"})
    context = CommandContext(principal=principal, idempotency_key=case)
    authority = WitnessAuthorityScope(
        state_authority_id=record_digest("fixture.state", {"object_id": object_id}),
        activation_id=identifier("activation"),
        workspace_id=workspace,
        controller_id=controller,
        control_epoch=1,
        assignment_key_id="sha256:" + "a" * 64,
    )
    public = public_key(crypto, SEED).hex()
    source = WitnessSource(
        id=identifier("source"),
        public_key=public,
        key_id=witness_key_id(public),
        native_profile="linux-boottime-v1",
        policy=ClockPolicy(),
    )
    signer = WitnessSigner(source, SEED, crypto_provider=crypto)
    scope = ClockAuthorityScope(workspace, controller, 1, authority.assignment_key_id)

    def response(request, epoch):
        return canonical_bytes(
            signer.sign(
                TimeWitnessPayload(
                    request=request,
                    source_id=source.id,
                    source_epoch=identifier(epoch),
                    native_profile=source.native_profile,
                    policy=source.policy,
                    authority_time_ms=1_793_000_000_000,
                )
            ).model_dump(mode="json")
        )

    def current():
        return store.read(lambda sql: registry.load(Transaction(sql, context), authority))

    def preflight():
        try:
            snapshot = current()
        except ResearchError as error:
            if error.code != ErrorCode.NOT_FOUND:
                raise
            setup = CommandContext(
                principal=principal, idempotency_key="register", expected_revision=0
            )

            def initialize(sql):
                initial = registry.register(Transaction(sql, setup), authority, source)
                if fail_bootstrap:
                    raise BootstrapFailure("Injected synthetic bootstrap failure")
                admission = CommandContext(
                    principal=principal,
                    idempotency_key="qualify",
                    expected_revision=initial.revision,
                )
                binding, waiting = registry.prepare(admission, initial, "fixture.admission", {})
                # This fixture signs a synthetic timestamp locally. Real source
                # acquisition must happen outside the synchronous SQL callback.
                observation = waiting.complete(response(waiting.request, "epoch-1"))
                return registry.requalify(
                    Transaction(sql, admission),
                    binding,
                    observation,
                    name="fixture.admission",
                    payload={},
                )

            snapshot = store.write(initialize)

        with boundary.prepare(context, snapshot, "fixture.witness", {"case": case}) as turn:
            turn.complete(
                response(turn.request, "epoch-2" if case == "changed-source" else "epoch-1")
            )

            def callback():
                first = provider.observe(scope)
                assert provider.observe(scope) is first
                return {"epoch": first.epoch, "time": first.milliseconds, "profile": first.profile}

            def command(transaction):
                transaction.insert("fixture_effect", identifier(case), {}, state="temporary")
                result = callback()
                if case == "domain-reject":
                    raise ResearchError(ErrorCode.ACCEPTANCE_EXPIRED, "Synthetic domain rejection")
                return result

            def reject(transaction, error):
                if error.code == ErrorCode.ACCEPTANCE_EXPIRED:
                    assert provider.observe(scope).milliseconds == 1_793_000_000_000
                    transaction.insert("fixture_domain_fence", identifier(case), {}, state="fenced")

            return database.execute(
                context, "fixture.witness", {"case": case}, "write", command, on_rejection=reject
            )

    try:
        result = database.with_preflight(
            context, "fixture.witness", {"case": case}, "write", preflight
        )
    except BootstrapFailure:
        return store.read(
            lambda sql: {
                "bootstrap_failed": True,
                "records": sql.execute("SELECT COUNT(*) FROM records").one()[0],
                "events": sql.execute("SELECT COUNT(*) FROM events").one()[0],
                "commands": sql.execute("SELECT COUNT(*) FROM commands").one()[0],
            }
        )
    except ResearchError as error:
        result = {
            "disposition": "rejected",
            "code": error.code.value,
            "command_id": error.command_id,
        }
    try:
        provider.observe(scope)
    except ResearchError as error:
        assert error.code == ErrorCode.CAPABILITY_UNAVAILABLE
    else:
        raise AssertionError("Witness survived its command")
    result["closed"] = True
    result["provider_state"] = current().state
    result["counts"] = store.read(
        lambda sql: {
            "effects": sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='fixture_effect'"
            ).one()[0],
            "retirements": sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='time_source_retirement'"
            ).one()[0],
            "domain_fences": sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='fixture_domain_fence'"
            ).one()[0],
            "acknowledgments": sql.execute("SELECT COUNT(*) FROM commands").one()[0],
        }
    )
    assert "research_swarm.crypto_native" not in sys.modules and "cryptography" not in sys.modules
    assert "research_swarm.state.sqlite" not in sys.modules and "sqlite3" not in sys.modules
    return result
