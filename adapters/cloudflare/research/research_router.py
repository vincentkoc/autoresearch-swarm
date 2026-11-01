"""Bounded shared-service router for an already provisioned hosted authority.

This adapter is not a deployed controller. Its separate worker-only and bounded
artifact profiles exclude worker execution and expiring credentials. The caller
supplies the gate, existing-store opener, crypto and bounded I/O transports.
"""

import asyncio
import hashlib
from typing import Literal

from pydantic import TypeAdapter, ValidationError, model_validator

from research_swarm.control.auth import CredentialStore
from research_swarm.control.dispatch import dispatch_command, validate_command_input
from research_swarm.control.queries import ResearchQueries
from research_swarm.control.service import ResearchService
from research_swarm.control.witness_commands import WitnessExecutionBoundary
from research_swarm.control.witness_time import WitnessAuthorityTimeProvider
from research_swarm.control.worker_lifecycle import operator_worker
from research_swarm.control.worker_queries import WorkerQueries, WorkerQuery, WorkerRead
from research_swarm.control.workers import worker_authority
from research_swarm.crypto import public_key
from research_swarm.domain.assignments import PublicKeyHex, signing_key_id
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.clocks import ClockChallenge
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_witnesses import WitnessAuthorityScope, WitnessSource
from research_swarm.domain.types import ByteCount, Digest, Name, VersionedRecord, new_id
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, Transaction
from research_swarm.state.reader_sessions import OpaqueReaderOwnership
from research_swarm.state.schema_contracts import require_table
from research_swarm.state.time_providers import TimeProviders
from research_swarm.storage.async_r2 import BoundedR2Configuration

if __package__:
    from ..runtime.exclusive import exclusive
else:
    from exclusive import exclusive


COMMANDS = frozenset({"worker.enroll", "worker.set_admission", "worker.cache_advertise"})
QUERIES = frozenset({"worker.clock", "worker.list", "worker.get"})
ACQUISITION_TIMEOUT_SECONDS = 10


class HostedConfiguration(VersionedRecord):
    profile: Literal["worker-control-witness-v1"] = "worker-control-witness-v1"
    store_backend: Literal["cloudflare-do"] = "cloudflare-do"
    store_location: Name
    authority: WitnessAuthorityScope
    source: WitnessSource
    assignment_public_key: PublicKeyHex
    dispatch_key_digest: Digest
    credential_manifest_digest: Digest

    @property
    def digest(self):
        return record_digest("research.hosted-configuration", self.model_dump(mode="json"))


class HostedArtifactConfiguration(HostedConfiguration):
    profile: Literal["worker-artifacts-witness-v1"] = "worker-artifacts-witness-v1"
    storage: BoundedR2Configuration
    storage_limit_bytes: ByteCount

    @model_validator(mode="after")
    def storage_scope(self):
        if self.storage.workspace_id != self.authority.workspace_id:
            raise ValueError("Artifact storage belongs to another workspace")
        return self


class HostedResearchConfiguration(HostedArtifactConfiguration):
    profile: Literal["research-admission-witness-v1"] = "research-admission-witness-v1"


class HostedInputConfiguration(HostedResearchConfiguration):
    profile: Literal["research-inputs-witness-v1"] = "research-inputs-witness-v1"


class HostedOutputConfiguration(HostedInputConfiguration):
    profile: Literal["research-outputs-witness-v1"] = "research-outputs-witness-v1"


class HostedPublicationConfiguration(HostedOutputConfiguration):
    profile: Literal["research-publication-witness-v1"] = "research-publication-witness-v1"


class HostedEvaluationConfiguration(HostedPublicationConfiguration):
    profile: Literal["research-evaluation-witness-v1"] = "research-evaluation-witness-v1"


class HostedLifecycleConfiguration(HostedEvaluationConfiguration):
    profile: Literal["research-lifecycle-witness-v1"] = "research-lifecycle-witness-v1"


class HostedScientificConfiguration(HostedLifecycleConfiguration):
    profile: Literal["research-scientific-witness-v1"] = "research-scientific-witness-v1"


class HostedOrchestrationConfiguration(HostedScientificConfiguration):
    profile: Literal["research-orchestration-witness-v1"] = "research-orchestration-witness-v1"


class HostedTransferConfiguration(HostedOrchestrationConfiguration):
    profile: Literal["research-transfer-witness-v1"] = "research-transfer-witness-v1"


class HostedStartupConfiguration(HostedTransferConfiguration):
    profile: Literal["research-startup-witness-v1"] = "research-startup-witness-v1"


class HostedReconciliationConfiguration(HostedStartupConfiguration):
    profile: Literal["research-reconciliation-witness-v1"] = "research-reconciliation-witness-v1"


class HostedTimerConfiguration(HostedReconciliationConfiguration):
    profile: Literal["research-timers-witness-v1"] = "research-timers-witness-v1"


def supports_artifacts(configuration):
    return isinstance(configuration, HostedArtifactConfiguration)


def supports_research(configuration):
    return isinstance(configuration, HostedResearchConfiguration)


def supports_inputs(configuration):
    return isinstance(configuration, HostedInputConfiguration)


def supports_outputs(configuration):
    return isinstance(configuration, HostedOutputConfiguration)


def supports_publication(configuration):
    return isinstance(configuration, HostedPublicationConfiguration)


def supports_evaluation(configuration):
    return isinstance(configuration, HostedEvaluationConfiguration)


def supports_lifecycle(configuration):
    return isinstance(configuration, HostedLifecycleConfiguration)


def supports_scientific(configuration):
    return isinstance(configuration, HostedScientificConfiguration)


def supports_orchestration(configuration):
    return isinstance(configuration, HostedOrchestrationConfiguration)


def supports_transfer(configuration):
    return isinstance(configuration, HostedTransferConfiguration)


def supports_startup(configuration):
    return isinstance(configuration, HostedStartupConfiguration)


def supports_reconciliation(configuration):
    return isinstance(configuration, HostedReconciliationConfiguration)


def supports_timers(configuration):
    return isinstance(configuration, HostedTimerConfiguration)


def allowed_commands(configuration):
    if __package__:
        from ..delivery.orchestration import COMMANDS as orchestration_commands
        from ..delivery.startup import COMMANDS as startup_commands
        from ..delivery.transfers import COMMANDS as transfer_commands
        from .artifact_router import COMMANDS as artifact_commands
        from .evaluations import COMMANDS as evaluation_commands
        from .lifecycle import COMMANDS as lifecycle_commands
        from .reconciliation import COMMANDS as reconciliation_commands
        from .research_admission import COMMANDS as research_commands
        from .result_publication import COMMANDS as publication_commands
        from .scientific import COMMANDS as scientific_commands
        from .worker_outputs import COMMANDS as output_commands
    else:
        from artifact_router import COMMANDS as artifact_commands
        from evaluations import COMMANDS as evaluation_commands
        from lifecycle import COMMANDS as lifecycle_commands
        from orchestration import COMMANDS as orchestration_commands
        from reconciliation import COMMANDS as reconciliation_commands
        from research_admission import COMMANDS as research_commands
        from result_publication import COMMANDS as publication_commands
        from scientific import COMMANDS as scientific_commands
        from startup import COMMANDS as startup_commands
        from transfers import COMMANDS as transfer_commands
        from worker_outputs import COMMANDS as output_commands
    return (
        COMMANDS
        | (artifact_commands if supports_artifacts(configuration) else frozenset())
        | (research_commands if supports_research(configuration) else frozenset())
        | (output_commands if supports_outputs(configuration) else frozenset())
        | (publication_commands if supports_publication(configuration) else frozenset())
        | (evaluation_commands if supports_evaluation(configuration) else frozenset())
        | (lifecycle_commands if supports_lifecycle(configuration) else frozenset())
        | (scientific_commands if supports_scientific(configuration) else frozenset())
        | (orchestration_commands if supports_orchestration(configuration) else frozenset())
        | (transfer_commands if supports_transfer(configuration) else frozenset())
        | (startup_commands if supports_startup(configuration) else frozenset())
        | (reconciliation_commands if supports_reconciliation(configuration) else frozenset())
    )


def dispatch_key_digest(key):
    if not isinstance(key, bytes) or len(key) < 32:
        raise ValueError("Hosted dispatch key must contain at least 32 bytes")
    return "sha256:" + hashlib.sha256(b"research.hosted-dispatch-key-v1\0" + key).hexdigest()


def unavailable(message="Operation is outside the hosted worker-control profile"):
    return ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, message)


def validated(model, payload):
    try:
        return model.model_validate(payload)
    except ValidationError as error:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid hosted request") from error


class UnavailableBytes:
    """A programming error is visible before this profile can acquire byte custody."""

    def refuse(self, *args, **kwargs):
        raise unavailable("Artifact bytes are outside the hosted worker-control profile")

    upload_guard = begin = progress = append = finalize = verify = reader = fetch = refuse
    discard_upload = delete = refuse


class ExistingResearchRouter:
    @classmethod
    async def open(
        cls,
        state,
        open_store,
        configuration,
        *,
        dispatch_key,
        assignment_key,
        crypto,
        acquire,
        objects=None,
    ):
        """Open only: provisioning, credentials and source qualification must already exist."""

        async def construct():
            config = TypeAdapter(
                HostedConfiguration
                | HostedArtifactConfiguration
                | HostedResearchConfiguration
                | HostedInputConfiguration
                | HostedOutputConfiguration
                | HostedPublicationConfiguration
                | HostedEvaluationConfiguration
                | HostedLifecycleConfiguration
                | HostedScientificConfiguration
                | HostedOrchestrationConfiguration
                | HostedTransferConfiguration
                | HostedStartupConfiguration
                | HostedReconciliationConfiguration
                | HostedTimerConfiguration
            ).validate_python(configuration)
            store = open_store()
            if (
                (store.identity.backend, store.identity.location)
                != (config.store_backend, config.store_location)
                or public_key(crypto, assignment_key).hex() != config.assignment_public_key
                or signing_key_id(config.assignment_public_key)
                != config.authority.assignment_key_id
                or dispatch_key_digest(dispatch_key) != config.dispatch_key_digest
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Hosted authority configuration differs")
            instance = cls()
            instance.state, instance.configuration = state, config
            instance.objects = objects
            instance.acquire = acquire
            instance.registry = TimeProviders(store.identity, crypto_provider=crypto)
            provider = WitnessAuthorityTimeProvider(store.identity)
            instance.boundary = WitnessExecutionBoundary(instance.registry, provider)
            instance.database = Database.from_store(store, execution_boundary=instance.boundary)
            instance._check_configuration()
            instance.credentials = CredentialStore.open_existing(instance.database)
            artifacts = ArtifactService(
                instance.database,
                UnavailableBytes(),
                reader_ownership=OpaqueReaderOwnership(),
                storage_limit_bytes=(
                    config.storage_limit_bytes
                    if isinstance(config, HostedArtifactConfiguration)
                    else 0
                ),
                max_object_bytes=(
                    config.storage.max_object_bytes
                    if isinstance(config, HostedArtifactConfiguration)
                    else 0
                ),
            )
            instance.service = ResearchService(
                instance.database,
                artifacts,
                dispatch_key=dispatch_key,
                assignment_key=assignment_key,
                authority_time_provider=provider,
                crypto_provider=crypto,
            )
            instance.queries = WorkerQueries(instance.database, instance.service.worker_locality)
            instance.records = ResearchQueries(instance.database)
            if __package__:
                from ..delivery.orchestration import HostedOrchestration
                from ..delivery.startup import HostedStartup
                from ..delivery.transfers import HostedTransfers
                from .artifact_router import HostedArtifacts
                from .evaluations import HostedEvaluations
                from .lifecycle import HostedLifecycle
                from .reconciliation import HostedReconciliation
                from .research_admission import HostedResearchAdmission
                from .result_publication import HostedResultPublication
                from .scientific import HostedScientific
                from .worker_inputs import HostedWorkerInputs
                from .worker_outputs import HostedWorkerOutputs
            else:
                from artifact_router import HostedArtifacts
                from evaluations import HostedEvaluations
                from lifecycle import HostedLifecycle
                from orchestration import HostedOrchestration
                from reconciliation import HostedReconciliation
                from research_admission import HostedResearchAdmission
                from result_publication import HostedResultPublication
                from scientific import HostedScientific
                from startup import HostedStartup
                from transfers import HostedTransfers
                from worker_inputs import HostedWorkerInputs
                from worker_outputs import HostedWorkerOutputs
            instance.artifacts = HostedArtifacts(instance)
            instance.research = HostedResearchAdmission(instance)
            instance.inputs = HostedWorkerInputs(instance)
            instance.outputs = HostedWorkerOutputs(instance)
            instance.publication = HostedResultPublication(instance)
            instance.reconciliation = HostedReconciliation(instance)
            instance.evaluations = HostedEvaluations(instance)
            instance.lifecycle = HostedLifecycle(instance)
            instance.scientific = HostedScientific(instance)
            instance.orchestration = HostedOrchestration(instance)
            instance.transfers = HostedTransfers(instance)
            instance.startup = HostedStartup(instance)
            return instance

        return await exclusive(state, construct)

    def _check_configuration(self):
        config, objects = self.configuration, self.objects
        if isinstance(config, HostedArtifactConfiguration):
            if (
                objects is None
                or objects.config != config.storage
                or objects.transport.configuration_digest != config.storage.digest
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Hosted artifact destination differs")
        elif objects is not None:
            raise unavailable("Worker-only profile cannot acquire artifact bytes")

        def check(sql):
            require_table(
                sql, "hosted_authority_config", "singleton digest payload", ("singleton",)
            )
            rows = sql.execute(
                "SELECT singleton,digest,payload FROM hosted_authority_config"
            ).fetchall()
            config = self.configuration
            if (
                len(rows) != 1
                or rows[0]["singleton"] != 1
                or rows[0]["digest"] != config.digest
                or strict_json_loads(rows[0]["payload"]) != config.model_dump(mode="json")
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Hosted authority configuration differs")
            current = sql.execute(
                "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
                (config.authority.workspace_id,),
            ).fetchone()
            if current is None or (current["controller_id"], current["control_epoch"]) != (
                config.authority.controller_id,
                config.authority.control_epoch,
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Hosted controller authority changed")

        self.database.store.read(check)

    def _context(self, token, *, idempotency_key, expected_revision=None):
        self._check_configuration()
        principal = self.credentials.resolve(token, allow_expiring=False)
        if principal.workspace_id != self.configuration.authority.workspace_id:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Credential workspace differs")
        return validated(
            CommandContext,
            {
                "principal": principal,
                "idempotency_key": idempotency_key,
                "expected_revision": expected_revision,
            },
        )

    async def authenticate(self, token):
        async def resolve():
            return self._context(token, idempotency_key=new_id()).principal

        return await exclusive(self.state, resolve)

    async def _witnessed(self, context, operation, payload, perform):
        snapshot = self.database.store.read(
            lambda sql: self.registry.load(Transaction(sql, context), self.configuration.authority)
        )
        if snapshot.data.source != self.configuration.source:
            raise ResearchError(ErrorCode.STALE_FENCE, "Hosted witness source differs")
        with self.boundary.prepare(context, snapshot, operation, payload) as turn:
            owner = asyncio.current_task()
            timeout = asyncio.timeout(ACQUISITION_TIMEOUT_SECONDS)
            try:
                async with timeout:
                    response = await self.acquire(turn.request)
                if timeout.expired():
                    # A transport may suppress cancellation and still return bytes.
                    raise TimeoutError("Witness response arrived after its deadline")
            except TimeoutError as error:
                raise ResearchError(
                    ErrorCode.TRANSPORT_UNAVAILABLE, "Witness acquisition timed out", retryable=True
                ) from error
            if owner is None or owner.cancelling():
                raise asyncio.CancelledError("Witness owner was cancelled")
            turn.complete(response)
            if owner.cancelling():
                raise asyncio.CancelledError("Witness owner was cancelled")
            # No I/O or await is permitted from completion through the shared commit.
            return perform()

    def _preflight_command(self, context, operation, request):
        def check(sql):
            transaction = Transaction(sql, context)
            if operation == "worker.enroll":
                if request.clock_mode != "bounded-continuous-v1":
                    raise unavailable("Hosted enrollment requires calibrated authority time")
                for identifier in request.project_ids or ():
                    transaction.get("project", identifier)
            elif operation == "worker.set_admission":
                operator_worker(transaction, request.binding)
            else:
                worker = worker_authority(
                    transaction, request.binding.worker_id, request.binding.worker_session_id
                )
                if worker.binding != request.binding:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Cache hint binding differs")
                if worker.clock_mode != "bounded-continuous-v1":
                    raise unavailable("Hosted cache hints require calibrated authority time")
                for reference in request.references:
                    ArtifactCatalog.available(transaction, reference)

        self.database.store.read(check)

    async def command(self, token, operation, payload, *, idempotency_key, expected_revision=None):
        async def execute():
            context = self._context(
                token, idempotency_key=idempotency_key, expected_revision=expected_revision
            )
            return await self._command_in_context(token, context, operation, payload)

        return await exclusive(self.state, execute)

    async def _command_in_context(self, token, context, operation, payload):
        """The gate owner may preserve a private saved-turn or derived context."""
        current = self._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Command credential changed")
        if operation.startswith("artifact."):
            return await self.artifacts.command(token, context, operation, payload)
        if operation in self.research.commands:
            return await self.research.command(token, context, operation, payload)
        if operation in self.outputs.commands:
            return await self.outputs.command(token, context, operation, payload)
        if operation in self.publication.commands:
            return await self.publication.command(token, context, operation, payload)
        if operation in self.reconciliation.commands:
            return await self.reconciliation.command(token, context, operation, payload)
        if operation in self.evaluations.commands:
            return await self.evaluations.command(token, context, operation, payload)
        if operation in self.lifecycle.commands:
            return await self.lifecycle.command(token, context, operation, payload)
        if operation in self.scientific.commands:
            return await self.scientific.command(token, context, operation, payload)
        if operation in self.orchestration.commands:
            return await self.orchestration.command(token, context, operation, payload)
        if operation in self.transfers.commands:
            return await self.transfers.command(token, context, operation, payload)
        if operation in self.startup.commands:
            return await self.startup.command(token, context, operation, payload)
        if operation not in COMMANDS:
            raise unavailable()
        definition, request = validate_command_input(context, operation, payload)
        # These three encodings match their kernel payloads. Local enrollment
        # has a legacy omission and is intentionally outside this profile.
        if operation == "worker.enroll" and request.clock_mode != "bounded-continuous-v1":
            raise unavailable("Hosted enrollment requires calibrated authority time")
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, definition.scope)
        if previous is not None:
            return previous
        self._preflight_command(context, operation, request)
        return await self._witnessed(
            context,
            operation,
            normalized,
            lambda: dispatch_command(self.service, context, operation, normalized),
        )

    async def upload_part(self, token, upload_id, offset, content):
        async def write():
            context = self._context(token, idempotency_key=new_id())
            return await self.artifacts.upload_part(token, context, upload_id, offset, content)

        return await exclusive(self.state, write)

    async def artifact_content(self, token, artifact_id):
        async def read():
            context = self._context(token, idempotency_key=new_id())
            return await self.artifacts.content(token, context, {"id": artifact_id})

        return await exclusive(self.state, read)

    async def worker_content(self, token, payload):
        async def read():
            context = self._context(token, idempotency_key=new_id())
            return await self.inputs.read(token, context, payload, content=True)

        return await exclusive(self.state, read)

    async def worker_upload_part(self, token, payload, offset, content):
        async def write():
            context = self._context(token, idempotency_key=new_id())
            return await self.outputs.upload_part(token, context, payload, offset, content)

        return await exclusive(self.state, write)

    async def query(self, token, operation, payload):
        async def read():
            context = self._context(token, idempotency_key=new_id())
            if operation in {"snapshot.page", "snapshot.item"}:
                return await self.startup.query(token, context, operation, payload)
            if operation in {"get", "list"}:
                if supports_reconciliation(self.configuration):
                    return self.reconciliation.query(token, context, operation, payload)
                if supports_transfer(self.configuration):
                    return self.transfers.query(token, context, operation, payload)
                if supports_orchestration(self.configuration):
                    return self.orchestration.query(token, context, operation, payload)
                if supports_scientific(self.configuration):
                    return self.scientific.query(token, context, operation, payload)
                if supports_lifecycle(self.configuration):
                    return self.lifecycle.query(token, context, operation, payload)
                if supports_evaluation(self.configuration):
                    return self.evaluations.query(token, context, operation, payload)
                return self.research.query(context, operation, payload)
            if operation == "context":
                return self.orchestration.query(token, context, operation, payload)
            if operation == "worker.input":
                reference, _ = await self.inputs.read(token, context, payload, content=False)
                return reference.model_dump(mode="json")
            if operation == "worker.output":
                return await self.outputs.status(token, context, payload)
            if operation == "artifact.upload_status":
                return await self.artifacts.status(token, context, payload)
            if operation == "artifact.get":
                return self.artifacts.metadata(token, context, payload)
            if operation == "worker.get":
                return self.queries.get(context, validated(WorkerRead, payload))
            if operation == "worker.list":
                request = validated(WorkerQuery, payload)
                if request.kind != "offers":
                    return self.queries.list(context, request)
                self.queries.validate_list(context, request)
                worker = self.database.store.read(
                    lambda sql: worker_authority(
                        Transaction(sql, context), request.worker_id, request.session_id
                    )
                )
                if worker.clock_mode != "bounded-continuous-v1":
                    raise unavailable("Hosted offers require calibrated authority time")
                return await self._witnessed(
                    context,
                    operation,
                    request.model_dump(mode="json"),
                    lambda: self.queries.list(context, request),
                )
            if operation == "worker.clock":
                request = validated(ClockChallenge, payload)
                self.service.workers.validate_clock(context, request)
                result = await self._witnessed(
                    context,
                    operation,
                    request.model_dump(mode="json"),
                    lambda: self.service.worker_clock(context, request),
                )
                return result.model_dump(mode="json")
            raise unavailable()

        return await exclusive(self.state, read)
