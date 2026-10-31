"""Local-only native source qualification. Public fixture credentials only."""

from typing import Literal

from durable_sql import DurableObjectStore
from exclusive import exclusive
from pydantic import Field, TypeAdapter
from r2_binding import R2BindingTransport
from research_router import (
    ExistingResearchRouter,
    HostedConfiguration,
    HostedEvaluationConfiguration,
    HostedInputConfiguration,
    HostedLifecycleConfiguration,
    HostedOrchestrationConfiguration,
    HostedOutputConfiguration,
    HostedPublicationConfiguration,
    HostedResearchConfiguration,
    HostedScientificConfiguration,
    HostedTransferConfiguration,
)
from router_qualification import DISPATCH_KEY, NAMESPACE, configuration, prepare_provision
from witness_binding import WitnessHTTPTransport
from witness_qualification import SEED

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_witnesses import WitnessSource
from research_swarm.domain.types import VersionedRecord
from research_swarm.state.database import Transaction
from research_swarm.storage.async_r2 import BoundedR2Objects

CONSUMER = "public_fixture_consumer_" + "x" * 32


class NativeProbe(VersionedRecord):
    action: Literal["describe", "provision", "inspect", "requalify"]
    source: WitnessSource | None = None
    origin: str | None = Field(default=None, max_length=256)
    research_profile: bool = Field(default=False, strict=True)
    input_profile: bool = Field(default=False, strict=True)
    output_profile: bool = Field(default=False, strict=True)
    publication_profile: bool = Field(default=False, strict=True)
    evaluation_profile: bool = Field(default=False, strict=True)
    lifecycle_profile: bool = Field(default=False, strict=True)
    scientific_profile: bool = Field(default=False, strict=True)
    orchestration_profile: bool = Field(default=False, strict=True)
    transfer_profile: bool = Field(default=False, strict=True)


class NativeFixtureR2(R2BindingTransport):
    def __init__(self, *args, unavailable=False):
        super().__init__(*args)
        self.unavailable = unavailable

    async def get(self, *args, **kwargs):
        if self.unavailable:
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture R2 unavailable")
        return await super().get(*args, **kwargs)

    async def put(self, *args, **kwargs):
        if self.unavailable:
            raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture R2 unavailable")
        return await super().put(*args, **kwargs)


async def open_native_router(
    state, object_id, crypto, transport, *, bucket=None, r2_module=None, r2_unavailable=False
):
    def open_store():
        return DurableObjectStore.open_existing(
            state.storage, namespace=NAMESPACE, object_id=object_id
        )

    async def load():
        def read(sql):
            config = TypeAdapter(
                HostedConfiguration
                | HostedResearchConfiguration
                | HostedInputConfiguration
                | HostedOutputConfiguration
                | HostedPublicationConfiguration
                | HostedEvaluationConfiguration
                | HostedLifecycleConfiguration
                | HostedScientificConfiguration
                | HostedOrchestrationConfiguration
                | HostedTransferConfiguration
            ).validate_json(
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
    objects = (
        BoundedR2Objects(
            config.storage,
            NativeFixtureR2(
                bucket,
                r2_module,
                config.storage,
                unavailable=r2_unavailable,
            ),
        )
        if isinstance(config, HostedResearchConfiguration)
        else None
    )
    return await ExistingResearchRouter.open(
        state,
        open_store,
        config,
        dispatch_key=DISPATCH_KEY,
        assignment_key=SEED,
        crypto=crypto,
        acquire=acquire,
        objects=objects,
    )


async def exercise(state, object_id, probe, crypto, transport, *, bucket=None, r2_module=None):
    try:
        if probe.action in {"describe", "provision"}:
            if probe.source is None:
                raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Native source is required")
            config, principals = configuration(
                object_id,
                crypto,
                source=probe.source,
                research_profile=probe.research_profile,
                input_profile=probe.input_profile,
                output_profile=probe.output_profile,
                publication_profile=probe.publication_profile,
                evaluation_profile=probe.evaluation_profile,
                lifecycle_profile=probe.lifecycle_profile,
                scientific_profile=probe.scientific_profile,
                orchestration_profile=probe.orchestration_profile,
                transfer_profile=probe.transfer_profile,
            )
            if probe.action == "describe":
                return config.model_dump(mode="json")
            acquire = WitnessHTTPTransport(transport, probe.origin, CONSUMER, allow_loopback=True)

            async def setup():
                request, complete = prepare_provision(
                    state, object_id, crypto, config, principals, native_origin=probe.origin
                )
                return complete(await acquire(request))

            return await exclusive(state, setup)
        router = await open_native_router(
            state, object_id, crypto, transport, bucket=bucket, r2_module=r2_module
        )
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
                result = {
                    "commands": sql.execute("SELECT COUNT(*) FROM commands").one()[0],
                    "events": sql.execute("SELECT COUNT(*) FROM events").one()[0],
                    "provider_state": sql.execute(
                        "SELECT state FROM records WHERE kind='time_provider'"
                    ).one()[0],
                    "retirements": sql.execute(
                        "SELECT COUNT(*) FROM records WHERE kind='time_source_retirement'"
                    ).one()[0],
                    "evidence_counts": {
                        kind: sql.execute(
                            "SELECT COUNT(*) FROM records WHERE kind=?", (kind,)
                        ).one()[0]
                        for kind in (
                            "run_result",
                            "evaluation_receipt",
                            "promotion",
                            "artifact_read_session",
                        )
                    },
                    "settlements": [
                        dict(row)
                        for row in sql.execute(
                            "SELECT * FROM budget_settlements ORDER BY settlement_id"
                        ).fetchall()
                    ],
                }
                if isinstance(router.configuration, HostedTransferConfiguration):

                    def bounded(statement):
                        rows = sql.execute(statement + " LIMIT 65").fetchall()
                        if len(rows) > 64:
                            raise ResearchError(
                                ErrorCode.RESOURCE_UNAVAILABLE,
                                "Fixture invariant projection is too large",
                            )
                        return [dict(row) for row in rows]

                    result["transfer_invariants"] = {
                        "authority": dict(
                            sql.execute(
                                "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
                                (router.configuration.authority.workspace_id,),
                            ).one()
                        ),
                        "assignment_key_id": router.configuration.authority.assignment_key_id,
                        "budgets": bounded("SELECT * FROM budgets ORDER BY dimension"),
                        "reservations": bounded("SELECT * FROM budget_reservations ORDER BY id"),
                        "native_counts": {
                            kind: sql.execute(
                                "SELECT COUNT(*) FROM records WHERE kind=?", (kind,)
                            ).one()[0]
                            for kind in ("worker", "attempt")
                        },
                    }
                return result

            return router.database.store.read(inspect)

        return await exclusive(state, control)
    except ResearchError as error:
        return error.as_dict()
