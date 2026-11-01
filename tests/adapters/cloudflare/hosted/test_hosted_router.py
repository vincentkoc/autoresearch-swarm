"""Shared service wiring on real SQLite; DO exclusion needs the workerd suite."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from adapters.cloudflare.research.research_router import (
    ExistingResearchRouter,
    HostedConfiguration,
    dispatch_key_digest,
)
from research_swarm.control.auth import CredentialStore
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.control.workers import EnrollWorker, WorkerData
from research_swarm.crypto import resolve_crypto
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.clocks import ClockChallenge, SignedClockReply
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database, encode
from research_swarm.state.sql import StoreIdentity
from research_swarm.workers.clock_signatures import ClockVerifier
from tests.workers.test_time_providers import Registry
from tests.workers.test_time_witnesses import SEED, encoded, identifier, payload, scope, source


def test_worker_only_configuration_digest_remains_stable():
    configuration = HostedConfiguration(
        store_location="synthetic/unit-fixture",
        authority=scope().authority.model_copy(
            update={"assignment_key_id": signing_key_id(source().public_key)}
        ),
        source=source(),
        assignment_public_key=source().public_key,
        dispatch_key_digest=dispatch_key_digest(bytes(32)),
        credential_manifest_digest="sha256:" + "b" * 64,
    )
    assert configuration.digest == (
        "sha256:79dd95106dc1b2ec6e7fea79bad5f433520dcb7db8fcaf687f521875edf2850e"
    )


class FixtureStore:
    def __init__(self, delegate):
        self.delegate = delegate
        self.identity = StoreIdentity("cloudflare-do", "synthetic/unit-fixture")

    def read(self, callback):
        return self.delegate.read(callback)

    def write(self, callback):
        return self.delegate.write(callback)


class Gate:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.waiting = asyncio.Event()

    async def blockConcurrencyWhile(self, callback):
        if self.lock.locked():
            self.waiting.set()
        async with self.lock:
            await callback()


class Witness:
    def __init__(self):
        self.requests = []
        self.epoch = identifier(7)
        self.failure = None
        self.release = None
        self.entered = asyncio.Event()
        self.suppress_cancel = False

    async def __call__(self, request):
        self.requests.append(request)
        self.entered.set()
        if self.release is not None:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                if not self.suppress_cancel:
                    raise
        if self.failure is not None:
            raise self.failure
        value = payload(request).model_copy(update={"source_epoch": self.epoch})
        return encoded(WitnessSigner(source(), SEED).sign(value))


@pytest.fixture
async def hosted(database, context, principal):
    store = FixtureStore(database.store)
    portable = Database.from_store(store)
    registry = Registry(portable, context, assignment_key_id=signing_key_id(source().public_key))
    registry.qualify()
    credentials = CredentialStore(portable)
    operator = credentials.issue(principal, principal)
    worker_principal = principal.model_copy(
        update={"id": new_id(), "scopes": {"worker.query", "worker.publish"}}
    )
    worker_credential = credentials.issue(principal, worker_principal)
    dispatch_key = b"\x00" * 32
    config = HostedConfiguration(
        store_location=store.identity.location,
        authority=registry.authority,
        source=source(),
        assignment_public_key=source().public_key,
        dispatch_key_digest=dispatch_key_digest(dispatch_key),
        credential_manifest_digest=record_digest(
            "fixture.initial-credentials",
            {"operator": operator.id, "worker": worker_credential.id},
        ),
    )
    store.write(
        lambda sql: sql.execute(
            "CREATE TABLE hosted_authority_config ("
            "singleton INTEGER PRIMARY KEY CHECK(singleton=1),digest TEXT,payload TEXT)"
        )
    )
    store.write(
        lambda sql: sql.execute(
            "INSERT INTO hosted_authority_config VALUES (1,?,?)",
            (config.digest, encode(config.model_dump(mode="json"))),
        )
    )
    gate, witness = Gate(), Witness()

    async def reopen(configuration=config, **changes):
        arguments = {
            "dispatch_key": dispatch_key,
            "assignment_key": SEED,
            "crypto": resolve_crypto(None),
            "acquire": witness,
        }
        arguments.update(changes)
        return await ExistingResearchRouter.open(gate, lambda: store, configuration, **arguments)

    request = EnrollWorker(
        worker_id=new_id(),
        session_id=new_id(),
        principal_id=worker_principal.id,
        name="Synthetic hosted worker",
        clock_mode="bounded-continuous-v1",
        profiles=(
            {
                "name": "cpu",
                "backend": "process",
                "cpu_slots": 1,
                "execution_fingerprint": "sha256:" + "b" * 64,
            },
        ),
    ).model_dump(mode="json")
    return SimpleNamespace(
        router=await reopen(),
        reopen=reopen,
        registry=registry,
        store=store,
        database=portable,
        config=config,
        credentials=credentials,
        operator=operator,
        worker_credential=worker_credential,
        request=request,
        witness=witness,
        gate=gate,
        principal=principal,
    )


async def enroll(hosted):
    acknowledgment = await hosted.router.command(
        hosted.operator.token, "worker.enroll", hosted.request, idempotency_key="enroll"
    )
    return acknowledgment, WorkerData.model_validate(acknowledgment["result"]["data"])


def query(hosted, kind="pending"):
    return {
        "worker_id": hosted.request["worker_id"],
        "session_id": hosted.request["session_id"],
        "kind": kind,
    }


def counts(hosted):
    return hosted.store.read(
        lambda sql: tuple(
            sql.execute("SELECT COUNT(*) FROM " + table).one()[0]
            for table in ("commands", "records", "events", "credentials")
        )
    )


async def test_real_shared_worker_commands_queries_and_scoped_clock(hosted):
    acknowledgment, worker = await enroll(hosted)
    assert acknowledgment["command_id"]
    advertisement = {
        "binding": worker.binding.model_dump(mode="json"),
        "snapshot_id": new_id(),
    }
    cached = await hosted.router.command(
        hosted.worker_credential.token,
        "worker.cache_advertise",
        advertisement,
        idempotency_key="cache",
        expected_revision=0,
    )
    assert cached["result"]["revision"] == 1
    challenge = ClockChallenge(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        nonce=new_id(),
    )
    reply = await hosted.router.query(
        hosted.worker_credential.token, "worker.clock", challenge.model_dump(mode="json")
    )
    verified = ClockVerifier(worker.assignment_authority).reply(
        SignedClockReply.model_validate(reply), challenge
    )
    assert verified.reply.payload.clock_profile == "hosted-native-witness-v1"
    assert (
        await hosted.router.query(
            hosted.worker_credential.token, "worker.list", query(hosted, "offers")
        )
    )["items"] == []
    drained = await hosted.router.command(
        hosted.operator.token,
        "worker.set_admission",
        {
            "binding": worker.binding.model_dump(mode="json"),
            "admission": "draining",
            "reason": "fixture",
        },
        idempotency_key="drain",
        expected_revision=0,
    )
    assert drained["result"]["state"] == "draining"
    assert len(hosted.witness.requests) == 5 and counts(hosted)[0] == 3
    before = len(hosted.witness.requests)
    admission = await hosted.router.query(
        hosted.worker_credential.token,
        "worker.get",
        {
            **query(hosted),
            "kind": "worker_admission",
            "id": worker.binding.worker_id,
        },
    )
    assert admission["state"] == "draining"
    for kind in ("pending", "historical_pending", "controls"):
        assert (
            await hosted.router.query(
                hosted.worker_credential.token, "worker.list", query(hosted, kind)
            )
        )["items"] == []
    assert len(hosted.witness.requests) == before


async def test_authenticated_exact_replay_before_unavailable_source_and_reopen(hosted):
    accepted, worker = await enroll(hosted)
    request = {
        "binding": worker.binding.model_dump(mode="json"),
        "admission": "draining",
        "reason": "fixture",
    }
    with pytest.raises(ResearchError) as rejected:
        await hosted.router.command(
            hosted.operator.token,
            "worker.set_admission",
            request,
            idempotency_key="rejected",
            expected_revision=9,
        )
    assert rejected.value.code == ErrorCode.REVISION_CONFLICT
    before, calls = counts(hosted), len(hosted.witness.requests)
    hosted.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture unavailable")
    reopened = await hosted.reopen()
    assert (
        await reopened.command(
            hosted.operator.token, "worker.enroll", hosted.request, idempotency_key="enroll"
        )
        == accepted
    )
    with pytest.raises(ResearchError) as replayed:
        await reopened.command(
            hosted.operator.token,
            "worker.set_admission",
            request,
            idempotency_key="rejected",
            expected_revision=9,
        )
    assert replayed.value.code == rejected.value.code
    with pytest.raises(ResearchError) as conflict:
        await reopened.command(
            hosted.operator.token,
            "worker.enroll",
            {**hosted.request, "name": "Changed"},
            idempotency_key="enroll",
        )
    assert conflict.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    with pytest.raises(ResearchError) as changed_revision:
        await reopened.command(
            hosted.operator.token,
            "worker.set_admission",
            request,
            idempotency_key="rejected",
            expected_revision=0,
        )
    assert changed_revision.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert counts(hosted) == before and len(hosted.witness.requests) == calls


@pytest.mark.parametrize("mode", ["revoked", "expiring"])
async def test_current_credentials_gate_even_exact_acknowledgment_replay(hosted, mode):
    await enroll(hosted)
    if mode == "revoked":
        hosted.credentials.revoke(hosted.principal, hosted.operator.id)
    else:
        hosted.store.write(
            lambda sql: sql.execute(
                "UPDATE credentials SET expires_at='2999-01-01T00:00:00.000Z' WHERE id=?",
                (hosted.operator.id,),
            )
        )
    before, calls = counts(hosted), len(hosted.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await (await hosted.reopen()).command(
            hosted.operator.token, "worker.enroll", hosted.request, idempotency_key="enroll"
        )
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert counts(hosted) == before and len(hosted.witness.requests) == calls


@pytest.mark.parametrize("mode", ["scope", "session", "cursor", "command", "query", "clock_mode"])
async def test_unsupported_or_unauthorized_preflight_has_no_witness_or_effect(hosted, mode):
    await enroll(hosted)
    before, calls = counts(hosted), len(hosted.witness.requests)
    with pytest.raises(ResearchError):
        if mode in {"scope", "command", "clock_mode"}:
            await hosted.router.command(
                hosted.worker_credential.token if mode == "scope" else hosted.operator.token,
                "artifact.begin" if mode == "command" else "worker.enroll",
                {
                    **hosted.request,
                    **({"clock_mode": "local-utc-v1"} if mode == "clock_mode" else {}),
                },
                idempotency_key="denied",
            )
        else:
            request = query(hosted, "offers")
            if mode == "session":
                request["session_id"] = new_id()
            elif mode == "cursor":
                request["cursor"] = "invalid"
            await hosted.router.query(
                hosted.worker_credential.token,
                "artifact.metadata" if mode == "query" else "worker.list",
                request,
            )
    assert counts(hosted) == before and len(hosted.witness.requests) == calls


@pytest.mark.parametrize(
    "mode", ["store", "activation", "source", "credentials", "dispatch", "assignment", "missing"]
)
async def test_configuration_mismatch_refuses_without_reprovisioning(hosted, mode):
    before = counts(hosted)
    config, changes = hosted.config, {}
    if mode == "store":
        hosted.store.identity = replace(hosted.store.identity, location="synthetic/copied-state")
    elif mode == "activation":
        config = config.model_copy(
            update={"authority": config.authority.model_copy(update={"activation_id": new_id()})}
        )
    elif mode == "source":
        config = config.model_copy(
            update={"source": config.source.model_copy(update={"id": new_id()})}
        )
    elif mode == "credentials":
        config = config.model_copy(update={"credential_manifest_digest": "sha256:" + "f" * 64})
    elif mode == "dispatch":
        changes["dispatch_key"] = b"x" * 32
    elif mode == "assignment":
        changes["assignment_key"] = b"x" * 32
    else:
        hosted.store.write(lambda sql: sql.execute("DELETE FROM hosted_authority_config"))
    with pytest.raises(ResearchError) as refused:
        await hosted.reopen(config, **changes)
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert counts(hosted) == before and hosted.witness.requests == []


async def test_source_discontinuity_fences_once_while_recovery_and_replay_survive(hosted):
    accepted, worker = await enroll(hosted)
    hosted.witness.epoch = identifier(8)
    request = {
        "binding": worker.binding.model_dump(mode="json"),
        "snapshot_id": new_id(),
    }
    with pytest.raises(ResearchError) as changed:
        await hosted.router.command(
            hosted.worker_credential.token,
            "worker.cache_advertise",
            request,
            idempotency_key="fenced",
            expected_revision=0,
        )
    assert changed.value.code == ErrorCode.STALE_FENCE
    assert hosted.registry.current().state == "unqualified"
    before, calls = counts(hosted), len(hosted.witness.requests)
    hosted.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture unavailable")
    reopened = await hosted.reopen()
    with pytest.raises(ResearchError) as replayed:
        await reopened.command(
            hosted.worker_credential.token,
            "worker.cache_advertise",
            request,
            idempotency_key="fenced",
            expected_revision=0,
        )
    assert replayed.value.code == ErrorCode.STALE_FENCE
    assert (
        await reopened.command(
            hosted.operator.token, "worker.enroll", hosted.request, idempotency_key="enroll"
        )
        == accepted
    )
    assert (await reopened.query(hosted.worker_credential.token, "worker.list", query(hosted)))[
        "items"
    ] == []
    assert counts(hosted) == before and len(hosted.witness.requests) == calls


async def test_witness_suspension_excludes_recovery_read_until_commit(hosted):
    hosted.witness.release = asyncio.Event()
    command = asyncio.create_task(enroll(hosted))
    await asyncio.wait_for(hosted.witness.entered.wait(), 2)
    read = asyncio.create_task(
        hosted.router.query(hosted.worker_credential.token, "worker.list", query(hosted))
    )
    try:
        await asyncio.wait_for(hosted.gate.waiting.wait(), 2)
        assert not read.done() and counts(hosted)[0] == 0
        hosted.witness.release.set()
        await asyncio.wait_for(command, 2)
        assert (await asyncio.wait_for(read, 2))["items"] == []
        assert counts(hosted)[0] == 1
    finally:
        for task in (command, read):
            if not task.done():
                task.cancel()
        await asyncio.gather(command, read, return_exceptions=True)


async def test_transport_refusal_leaves_no_ack_and_retry_uses_fresh_nonce(hosted):
    hosted.witness.failure = TimeoutError("Synthetic transport timeout")
    before = counts(hosted)
    with pytest.raises(ResearchError) as refused:
        await enroll(hosted)
    assert refused.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert counts(hosted) == before
    first = hosted.witness.requests[-1]
    hosted.witness.failure = None
    await enroll(hosted)
    assert hosted.witness.requests[-1].nonce != first.nonce
    assert counts(hosted)[0] == before[0] + 1


async def test_cancelled_witness_cannot_later_commit_or_hold_the_gate(hosted):
    hosted.witness.release = asyncio.Event()
    before = counts(hosted)
    command = asyncio.create_task(enroll(hosted))
    try:
        await asyncio.wait_for(hosted.witness.entered.wait(), 2)
        command.cancel()
        with pytest.raises(asyncio.CancelledError):
            await command
        first = hosted.witness.requests[-1]
        assert counts(hosted) == before and not hosted.gate.lock.locked()
        hosted.witness.release.set()
        await enroll(hosted)
        assert hosted.witness.requests[-1].nonce != first.nonce
        assert counts(hosted)[0] == before[0] + 1
    finally:
        if not command.done():
            command.cancel()
        await asyncio.gather(command, return_exceptions=True)


async def test_live_controller_change_is_checked_again_after_open(hosted):
    hosted.store.write(lambda sql: sql.execute("UPDATE workspaces SET control_epoch=2"))
    before = counts(hosted)
    with pytest.raises(ResearchError) as changed:
        await enroll(hosted)
    assert changed.value.code == ErrorCode.STALE_FENCE
    assert counts(hosted) == before and hosted.witness.requests == []


@pytest.mark.parametrize("mode", ["timeout", "caller"])
async def test_transport_cannot_commit_late_bytes_by_suppressing_cancellation(
    hosted, monkeypatch, mode
):
    if mode == "timeout":
        monkeypatch.setattr(
            "adapters.cloudflare.research.research_router.ACQUISITION_TIMEOUT_SECONDS", 0.01
        )
    hosted.witness.release = asyncio.Event()
    hosted.witness.suppress_cancel = True
    before = counts(hosted)
    command = asyncio.create_task(enroll(hosted))
    try:
        await asyncio.wait_for(hosted.witness.entered.wait(), 2)
        if mode == "caller":
            command.cancel()
        if mode == "timeout":
            with pytest.raises(ResearchError) as late:
                await asyncio.wait_for(command, 2)
            assert late.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
        else:
            with pytest.raises(asyncio.CancelledError):
                await command
        assert counts(hosted) == before and hosted.router.boundary._turn is None
        first = hosted.witness.requests[-1]
        hosted.witness.release.set()
        await enroll(hosted)
        assert hosted.witness.requests[-1].nonce != first.nonce
        assert counts(hosted)[0] == before[0] + 1
    finally:
        if not command.done():
            command.cancel()
        await asyncio.gather(command, return_exceptions=True)
