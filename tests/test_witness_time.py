from __future__ import annotations

import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor

import pytest

from research_swarm.control.time_authority import ClockAuthorityScope
from research_swarm.control.witness_time import HOSTED_WITNESS_PROFILE, WitnessAuthorityTimeProvider
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.time_providers import ProviderChanged
from tests.test_time_providers import registry as registry
from tests.test_time_witnesses import authenticate_arbitrary, encoded, identifier, payload, signed


def clock_scope(turn):
    value = turn.request.scope
    return ClockAuthorityScope(
        value.workspace_id, value.controller_id, value.control_epoch, value.assignment_key_id
    )


def response(turn, **changes):
    return encoded(signed(payload(turn.request).model_copy(update=changes)))


def run(registry, context, turn, callback, **changes):
    return registry.write(
        context,
        lambda tx: turn.run(
            tx,
            callback,
            name=changes.get("name", "fixture.operation"),
            payload=changes.get("payload", {}),
        ),
    )


def test_one_operation_uses_one_instant_and_cannot_reopen_its_witness(registry):
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as turn:
        scope = clock_scope(turn)
        with pytest.raises(ResearchError):
            provider.observe(scope)
        turn.complete(response(turn))
        with pytest.raises(ResearchError):
            provider.observe(scope)
        copied = contextvars.copy_context()

        def operation():
            first = provider.observe(scope)
            assert provider.observe(scope) is first
            assert copied.run(provider.observe, scope) is first
            assert first.profile == HOSTED_WITNESS_PROFILE
            assert first.milliseconds == payload(turn.request).authority_time_ms
            return first

        observed = run(registry, context, turn, operation)
        for observe in (
            lambda: provider.observe(scope),
            lambda: copied.run(provider.observe, scope),
        ):
            with pytest.raises(ResearchError):
                observe()
        with pytest.raises(ResearchError):
            run(registry, context, turn, lambda: pytest.fail("Witness reopened"))
    with pytest.raises(ResearchError):
        turn.complete(response(turn))
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as fresh:
        assert fresh.request.nonce != turn.request.nonce
        with pytest.raises(ResearchError) as replayed:
            fresh.complete(response(turn))
        assert replayed.value.code == ErrorCode.STALE_FENCE
    assert observed.epoch != payload(turn.request).source_epoch


@pytest.mark.parametrize("change", ["scope", "transaction", "operation", "payload"])
def test_other_authority_or_operation_cannot_observe_time(registry, change):
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as turn:
        turn.complete(response(turn))
        if change == "scope":
            scope = clock_scope(turn)
            other = ClockAuthorityScope(
                scope.workspace_id, new_id(), scope.control_epoch, scope.signing_key_id
            )

            def callback():
                return provider.observe(other)
        else:

            def callback():
                pytest.fail("Different operation entered callback")

        with pytest.raises(ResearchError) as refused:
            run(
                registry,
                registry.context() if change == "transaction" else context,
                turn,
                callback,
                **(
                    {"name": "different.operation"}
                    if change == "operation"
                    else {"payload": {"changed": True}}
                    if change == "payload"
                    else {}
                ),
            )
        assert refused.value.code == ErrorCode.STALE_FENCE


def test_nested_turn_and_foreign_thread_cannot_borrow_time(registry):
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as turn:
        turn.complete(response(turn))

        def operation():
            scope = clock_scope(turn)
            with (
                pytest.raises(ResearchError) as nested,
                provider.prepare(
                    registry.providers, context, registry.current(), "fixture.operation", {}
                ),
            ):
                pytest.fail("Nested turn entered")
            assert nested.value.code == ErrorCode.RESOURCE_UNAVAILABLE
            copied = contextvars.copy_context()
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(copied.run, provider.observe, scope)
                with pytest.raises(ResearchError) as foreign:
                    future.result(timeout=5)
            assert foreign.value.code == ErrorCode.STALE_FENCE
            return provider.observe(scope)

        run(registry, context, turn, operation)


def test_child_async_task_cannot_complete_the_owners_pending_witness(registry):
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()

    async def exercise():
        with provider.prepare(
            registry.providers, context, registry.current(), "fixture.operation", {}
        ) as turn:

            async def child():
                with pytest.raises(ResearchError) as foreign:
                    turn.complete(response(turn))
                assert foreign.value.code == ErrorCode.STALE_FENCE

            await asyncio.create_task(child())
            turn.complete(response(turn))
            run(registry, context, turn, lambda: provider.observe(clock_scope(turn)))

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "failure", ["bad_response", "callback", "coroutine", "generator", "async_generator"]
)
def test_failure_consumes_and_clears_the_operation(registry, failure):
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as turn:

        async def coroutine():
            pytest.fail("Async body ran")

        def generator():
            pytest.fail("Generator body ran")
            yield

        async def async_generator():
            pytest.fail("Async generator body ran")
            yield

        def callback():
            raise RuntimeError("Injected callback failure")

        if failure == "bad_response":
            with pytest.raises(ValueError):
                turn.complete(b"{}")
        else:
            turn.complete(response(turn))
            with pytest.raises(RuntimeError if failure == "callback" else ResearchError):
                run(
                    registry,
                    context,
                    turn,
                    {
                        "callback": callback,
                        "coroutine": coroutine,
                        "generator": generator,
                        "async_generator": async_generator,
                    }[failure],
                )
        with pytest.raises(ResearchError):
            provider.observe(clock_scope(turn))
        with pytest.raises(ResearchError):
            run(registry, context, turn, lambda: pytest.fail("Failed turn reopened"))


@pytest.mark.parametrize("boundary", ["handler", "rejection_hook"])
@pytest.mark.parametrize("outer", ["command", "query"])
@pytest.mark.parametrize("copied_context", [False, True])
def test_nested_command_cannot_borrow_the_outer_execution_instant(
    registry, boundary, outer, copied_context
):
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as turn:
        turn.complete(response(turn))

        def callback():
            scope = clock_scope(turn)
            before = provider.observe(scope)
            copied = contextvars.copy_context()

            def observe():
                return (
                    copied.run(provider.observe, scope)
                    if copied_context
                    else provider.observe(scope)
                )

            # A separate database keeps the fixture from recursively waiting on its own writer.
            from research_swarm.state.database import Database

            nested = Database(registry.database.path.with_name("nested.sqlite"))
            nested.bootstrap(context.principal.workspace_id, new_id())

            def rejected(tx):
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Injected nested rejection")

            with pytest.raises(ResearchError) as refused:
                nested.execute(
                    registry.context(),
                    "nested.operation",
                    {},
                    "write",
                    (lambda tx: {"time": observe().milliseconds})
                    if boundary == "handler"
                    else rejected,
                    on_rejection=(lambda tx, error: observe())
                    if boundary == "rejection_hook"
                    else None,
                )
            assert refused.value.code == ErrorCode.STALE_FENCE
            assert provider.observe(scope) is before
            return {"time": before.milliseconds}

        if outer == "command":
            result = registry.database.execute(
                context,
                "fixture.operation",
                {},
                "write",
                lambda tx: turn.run(tx, callback, name="fixture.operation", payload={}),
            )
            assert result["disposition"] == "accepted"
        else:
            assert (
                run(registry, context, turn, callback)["time"]
                == payload(turn.request).authority_time_ms
            )
    from research_swarm.state.execution import current_execution

    assert current_execution() is None


@pytest.mark.parametrize("changed", ["epoch", "configuration"])
def test_authenticated_discontinuity_fences_after_command_rollback(registry, changed):
    registry.qualify()
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as turn:
        if changed == "epoch":
            turn.complete(response(turn, source_epoch=identifier(8)))
        else:
            turn.complete(
                encoded(
                    authenticate_arbitrary(
                        payload(turn.request).model_copy(
                            update={"native_profile": "changed-profile"}
                        )
                    )
                )
            )

        def handler(tx):
            tx.insert("fixture", new_id(), {}, state="temporary")
            return turn.run(
                tx,
                lambda: pytest.fail("Changed time was admitted"),
                name="fixture.operation",
                payload={},
            )

        def fence(tx, error):
            assert isinstance(error, ProviderChanged)
            registry.providers.fence(tx, error.evidence, name="fixture.operation", payload={})

        with pytest.raises(ResearchError) as refused:
            registry.database.execute(
                context, "fixture.operation", {}, "write", handler, on_rejection=fence
            )
        assert refused.value.code == ErrorCode.STALE_FENCE
    assert registry.current().state == "unqualified"
    assert not any(
        item["kind"] == "fixture" for item in registry.database.snapshot(context)["records"]
    )
    with pytest.raises(ResearchError):
        registry.database.with_preflight(
            context,
            "fixture.operation",
            {},
            "write",
            lambda: pytest.fail("Replay requested another witness"),
        )


def test_injected_crypto_verifies_the_prepared_witness(registry):
    from research_swarm.state.time_providers import TimeProviders
    from tests.test_crypto_providers import ObservedProvider

    registry.qualify()
    crypto = ObservedProvider()
    registry.providers = TimeProviders(registry.database.authority_identity, crypto_provider=crypto)
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as turn:
        turn.complete(response(turn))
        run(registry, context, turn, lambda: provider.observe(clock_scope(turn)))
    assert crypto.calls == ["verify"]


def test_hosted_profile_does_not_silently_become_native_worker_authority():
    from dataclasses import replace

    from research_swarm.workers.clock_signatures import ClockVerifier
    from tests.test_clock_estimates import fixture

    estimate, _, signer, challenge = fixture()
    payload = estimate.verified.reply.payload.model_copy(
        update={"clock_profile": HOSTED_WITNESS_PROFILE}
    )
    verified = ClockVerifier(signer.authority).reply(signer.reply(payload), challenge)
    with pytest.raises(ResearchError) as unqualified:
        replace(estimate, verified=verified)
    assert unqualified.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


def test_unknown_source_profile_cannot_supply_operation_time(registry):
    from tests.test_time_witnesses import source

    registry.qualify(proposed=source().model_copy(update={"native_profile": "unknown-profile"}))
    provider = WitnessAuthorityTimeProvider(registry.database.authority_identity)
    context = registry.context()
    with provider.prepare(
        registry.providers, context, registry.current(), "fixture.operation", {}
    ) as turn:
        turn.complete(
            encoded(
                authenticate_arbitrary(
                    payload(turn.request).model_copy(update={"native_profile": "unknown-profile"})
                )
            )
        )
        with pytest.raises(ResearchError) as unqualified:
            run(registry, context, turn, lambda: pytest.fail("Unknown clock entered operation"))
        assert unqualified.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
