"""Original-worker journal delivery at an admitted target, with no execution clock."""

import asyncio
from dataclasses import dataclass
from typing import Literal

from research_swarm.control.auth import CredentialStore
from research_swarm.control.observations import PublishAttempt
from research_swarm.control.recovery_operations import MAX_JOURNAL_BYTES, recovery_context
from research_swarm.control.service import ResearchService
from research_swarm.control.worker_queries import WorkerQueries, WorkerRead
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.state.artifacts import ArtifactService
from research_swarm.state.database import Database, Transaction
from research_swarm.state.reader_sessions import OpaqueReaderOwnership

if __package__:
    from .artifact_router import HostedArtifacts
    from .checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from .checkpoint_import import verify_target_configuration
    from .checkpoint_target import load_target
    from .exclusive import exclusive
    from .hosted_http import MAX_JSON_BYTES, command_capabilities
    from .research_router import UnavailableBytes, supports_outputs, validated
    from .worker_outputs import COMMANDS as OUTPUT_COMMANDS
    from .worker_outputs import HostedWorkerOutputs
else:
    from artifact_router import HostedArtifacts
    from checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from checkpoint_import import verify_target_configuration
    from checkpoint_target import load_target
    from exclusive import exclusive
    from hosted_http import MAX_JSON_BYTES, command_capabilities
    from research_router import UnavailableBytes, supports_outputs, validated
    from worker_outputs import COMMANDS as OUTPUT_COMMANDS
    from worker_outputs import HostedWorkerOutputs


COMMANDS = OUTPUT_COMMANDS | {"worker.submit_evidence"}


class RecoveryWorkerRead(WorkerRead):
    kind: Literal["worker_session", "historical_attempt"]


@dataclass(frozen=True)
class RecoveryHTTPPolicy:
    max_part_bytes: int

    def admit(self, kind, identifier):
        if (
            kind in {"capabilities", "output", "worker-upload"}
            or (kind == "query" and identifier == "worker.get")
            or (kind == "command" and identifier in COMMANDS)
        ):
            return
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Recovery route is unavailable")

    def scopes(self, kind, identifier):
        if kind == "command" and identifier == "worker.submit_evidence":
            return ("worker.publish",)
        return (
            ("worker.outputs",)
            if kind in {"command", "output", "worker-upload"}
            else ("worker.query",)
        )

    def body_limit(self, kind):
        return (
            self.max_part_bytes
            if kind == "worker-upload"
            else 0
            if kind == "capabilities"
            else MAX_JSON_BYTES
        )

    def describe(self):
        return {
            "schema_version": "1.0",
            "profile": "original-worker-recovery-v1",
            "execution": "unavailable",
            "clock": "unavailable",
            "commands": command_capabilities(COMMANDS),
            "worker_queries": {
                "scope": "worker.query",
                "operations": {"worker.get": RecoveryWorkerRead.model_json_schema()},
            },
            "outputs": {
                "roles": ["journal"],
                "max_object_bytes": self.max_part_bytes,
                "max_part_bytes": self.max_part_bytes,
                "transfer": "complete-body-offset-zero",
            },
            "expiring_credentials": False,
            "scientific_acceptance": False,
        }


class ExistingRecoveryRouter:
    @classmethod
    async def open(cls, state, open_store, manifest, secrets, *, target_json, crypto, objects):
        custody = load_target(target_json, manifest, secrets, credential=secrets.bootstrap)

        async def construct():
            store = open_store()
            operator = verify_target_configuration(
                manifest,
                secrets,
                custody.binding.intent,
                identity=store.identity,
                credential=secrets.bootstrap,
                target_seed=custody.seed,
                expected_source=custody.binding.expected_source,
                expected_custody=custody.binding.expected_custody,
                crypto=crypto,
            )
            instance = cls()
            instance.state, instance.configuration = state, manifest.configuration
            instance.crypto, instance.objects = crypto, objects
            instance.verifier = HostedRecoveryVerifier(manifest, custody.binding)
            instance.database = Database.from_store(store, recovery_verifier=instance.verifier)
            instance._verify(CommandContext(principal=operator, idempotency_key=new_id()))
            instance.credentials = CredentialStore.open_existing(instance.database)
            artifacts = ArtifactService(
                instance.database,
                UnavailableBytes(),
                reader_ownership=OpaqueReaderOwnership(),
                storage_limit_bytes=instance.configuration.storage_limit_bytes,
                max_object_bytes=min(
                    MAX_JOURNAL_BYTES, instance.configuration.storage.max_object_bytes
                ),
            )
            # No dispatch/signing key or time provider is installed in this service.
            instance.service = ResearchService(instance.database, artifacts, crypto_provider=crypto)
            instance.artifacts = HostedArtifacts(instance)
            instance.outputs = HostedWorkerOutputs(instance, recovery=True)
            instance.policy = RecoveryHTTPPolicy(artifacts.max_object_bytes)
            return instance

        return await exclusive(state, construct)

    def _verify(self, context, sql=None):
        if (
            not supports_outputs(self.configuration)
            or self.objects is None
            or self.objects.config != self.configuration.storage
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery storage configuration differs")

        def check(connection):
            return self.verifier.read_admission(
                connection,
                authority=self.database.authority_identity,
                context=context,
                crypto=self.crypto,
            )

        return self.database.store.read(check) if sql is None else check(sql)

    def _context(self, token, *, idempotency_key, expected_revision=None):
        context = validated(
            CommandContext,
            {
                "principal": self.credentials.resolve(token, allow_expiring=False),
                "idempotency_key": idempotency_key,
                "expected_revision": expected_revision,
            },
        )
        self._verify(context)
        return context

    async def authenticate(self, token):
        async def resolve():
            return self._context(token, idempotency_key=new_id()).principal

        return await exclusive(self.state, resolve)

    def _identity(self, token, context, scope):
        current = self._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery credential changed")
        current.principal.require(scope)
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Recovery request owner was cancelled")

    async def query(self, token, operation, payload):
        async def read():
            context = self._context(token, idempotency_key=new_id())
            if operation == "worker.output":
                return await self.outputs.status(token, context, payload)
            if operation != "worker.get":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Recovery query is unavailable"
                )
            request = validated(RecoveryWorkerRead, payload)

            def inspect(sql):
                receipt, _ = self._verify(context, sql)
                if request.worker_id not in receipt.worker_ids or (
                    request.kind == "historical_attempt"
                    and request.id not in receipt.unresolved_attempt_ids
                ):
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Read is outside recovery inventory"
                    )
                database = Database.from_store(
                    AdmissionStore(self.database.authority_identity, sql)
                )
                return WorkerQueries(database).get(context, request)

            return self.database.store.read(inspect)

        return await exclusive(self.state, read)

    async def worker_upload_part(self, token, payload, offset, content):
        async def write():
            context = self._context(token, idempotency_key=new_id())
            return await self.outputs.upload_part(token, context, payload, offset, content)

        return await exclusive(self.state, write)

    async def command(self, token, operation, payload, *, idempotency_key, expected_revision=None):
        async def perform():
            context = self._context(
                token, idempotency_key=idempotency_key, expected_revision=expected_revision
            )
            if operation in OUTPUT_COMMANDS:
                return await self.outputs.command(token, context, operation, payload)
            if operation != "worker.submit_evidence":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Recovery command is unavailable"
                )
            request = validated(PublishAttempt, payload)
            normalized = request.model_dump(mode="json")
            if (
                request.report_artifact is not None
                or request.outputs
                or request.logs
                or request.journal_artifact.size_bytes > MAX_JOURNAL_BYTES
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Recovery accepts bounded journal evidence only"
                )
            context = recovery_context(
                self.database,
                context,
                operation,
                normalized,
                attempt_id=request.attempt_id,
                artifact_id=request.journal_artifact.id,
                crypto_provider=self.crypto,
            )
            self._identity(token, context, "worker.publish")
            previous = self.database.replay(context, operation, normalized, "worker.publish")
            if previous is not None:
                return previous

            def select(sql):
                transaction = Transaction(sql, context)
                worker, attempt = self.service.reconciliation._owner(transaction, request)
                records = self.service.results._evidence_records_in(
                    transaction, request, attempt, worker, scope="worker.publish"
                )
                return worker, records

            selected = self.database.store.read(select)
            record = selected[1][request.journal_artifact.id]
            body = await self.objects.read(
                record.workspace_id, self.service.artifacts._blob(record)
            )
            journal = self.service.worker_outputs._parse_journal(body)
            self._identity(token, context, "worker.publish")

            def commit(transaction):
                if select(transaction.connection) != selected:
                    raise ResearchError(
                        ErrorCode.STALE_FENCE, "Recovery evidence changed during I/O"
                    )
                return self.service.reconciliation._submit_verified(
                    transaction, request, selected[0], journal
                )

            return self.database.execute(context, operation, normalized, "worker.publish", commit)

        return await exclusive(self.state, perform)
