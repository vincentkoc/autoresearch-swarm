"""Local-only native source qualification. Public fixture credentials only."""

from typing import Literal

from durable_sql import DurableObjectStore
from exclusive import exclusive
from pydantic import Field
from research_router import ExistingResearchRouter, HostedConfiguration
from router_qualification import DISPATCH_KEY, NAMESPACE, configuration, prepare_provision
from witness_binding import WitnessHTTPTransport
from witness_qualification import SEED

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_witnesses import WitnessSource
from research_swarm.domain.types import VersionedRecord
from research_swarm.state.database import Transaction

CONSUMER = "public_fixture_consumer_" + "x" * 32


class NativeProbe(VersionedRecord):
    action: Literal["describe", "provision", "inspect", "requalify"]
    source: WitnessSource | None = None
    origin: str | None = Field(default=None, max_length=256)


async def open_native_router(state, object_id, crypto, transport):
    def open_store():
        return DurableObjectStore.open_existing(
            state.storage, namespace=NAMESPACE, object_id=object_id
        )

    async def load():
        def read(sql):
            config = HostedConfiguration.model_validate_json(
                sql.execute("SELECT payload FROM hosted_authority_config WHERE singleton=1").one()[
                    0
                ]
            )
            origin = sql.execute(
                "SELECT origin FROM native_witness_transport WHERE singleton=1"
            ).one()[0]
            return config, origin

        return open_store().read(read)

    config, origin = await exclusive(state, load)
    acquire = WitnessHTTPTransport(transport, origin, CONSUMER, allow_loopback=True)
    return await ExistingResearchRouter.open(
        state,
        open_store,
        config,
        dispatch_key=DISPATCH_KEY,
        assignment_key=SEED,
        crypto=crypto,
        acquire=acquire,
    )


async def exercise(state, object_id, probe, crypto, transport):
    try:
        if probe.action in {"describe", "provision"}:
            if probe.source is None:
                raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Native source is required")
            config, principals = configuration(object_id, crypto, source=probe.source)
            if probe.action == "describe":
                return config.model_dump(mode="json")
            acquire = WitnessHTTPTransport(transport, probe.origin, CONSUMER, allow_loopback=True)

            async def setup():
                request, complete = prepare_provision(
                    state, object_id, crypto, config, principals, native_origin=probe.origin
                )
                return complete(await acquire(request))

            return await exclusive(state, setup)
        router = await open_native_router(state, object_id, crypto, transport)
        _, principals = configuration(object_id, crypto, source=router.configuration.source)

        async def control():
            if probe.action == "requalify":
                context = CommandContext(
                    principal=principals[0], idempotency_key="native-requalify"
                )
                registry = router.registry
                snapshot = router.database.store.read(
                    lambda sql: registry.load(
                        Transaction(sql, context), router.configuration.authority
                    )
                )
                context = context.model_copy(update={"expected_revision": snapshot.revision})
                binding, pending = registry.prepare(
                    context, snapshot, "fixture.requalification", {}
                )
                proof = pending.complete(await router.acquire(pending.request))
                router.database.store.write(
                    lambda sql: registry.requalify(
                        Transaction(sql, context),
                        binding,
                        proof,
                        name="fixture.requalification",
                        payload={},
                    )
                )

            def inspect(sql):
                return {
                    "commands": sql.execute("SELECT COUNT(*) FROM commands").one()[0],
                    "events": sql.execute("SELECT COUNT(*) FROM events").one()[0],
                    "provider_state": sql.execute(
                        "SELECT state FROM records WHERE kind='time_provider'"
                    ).one()[0],
                    "retirements": sql.execute(
                        "SELECT COUNT(*) FROM records WHERE kind='time_source_retirement'"
                    ).one()[0],
                }

            return router.database.store.read(inspect)

        return await exclusive(state, control)
    except ResearchError as error:
        return error.as_dict()
