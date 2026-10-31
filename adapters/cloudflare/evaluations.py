"""Independent evaluator authority using buffered bytes and shared transactions."""

import asyncio
from typing import Literal

from research_swarm.control.dispatch import validate_command_input
from research_swarm.control.recovery_operations import require_raw_operation
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.observation_bytes import decode_observation_bytes
from research_swarm.projects.workload import WorkloadReport
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Transaction

if __package__:
    from .research_admission import AdmissionQuery, AdmissionRead, RecordKind
    from .research_router import supports_evaluation, validated
else:
    from research_admission import AdmissionQuery, AdmissionRead, RecordKind
    from research_router import supports_evaluation, validated

COMMANDS = frozenset({"evaluation.authorize", "evaluation.complete"})
EvaluationRecordKind = Literal[RecordKind, "evaluation_request", "evaluation_receipt"]


class EvaluationQuery(AdmissionQuery):
    kind: EvaluationRecordKind


class EvaluationRead(AdmissionRead):
    kind: EvaluationRecordKind


class HostedEvaluations:
    commands = COMMANDS

    def __init__(self, router):
        self.router = router
        self.database, self.service = router.database, router.service.evaluations

    def _identity(self, token, context):
        if not supports_evaluation(self.router.configuration):
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Evaluation is unavailable")
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Evaluation credential changed")
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Evaluation owner was cancelled")

    def query(self, token, context, operation, payload):
        self._identity(token, context)
        context.principal.require("read")
        if operation == "get":
            request = validated(EvaluationRead, payload)
            return self.router.records.get(context, request.kind, request.id)
        return self.router.records.list(context, validated(EvaluationQuery, payload))

    def _select(self, context, operation, request):
        require_raw_operation(self.database, context, operation, request.model_dump(mode="json"))
        select = (
            self.service._authorization_in
            if operation == "evaluation.authorize"
            else self.service._completion_in
        )
        return self.database.store.read(lambda sql: select(Transaction(sql, context), request))

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        if operation not in COMMANDS:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Evaluation is unavailable")
        definition, request = validate_command_input(context, operation, payload)
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, definition.scope)
        if previous is not None:
            return previous
        selected = self._select(context, operation, request)
        if operation == "evaluation.authorize":
            manifest = await self.router.artifacts.publish_bytes(
                token,
                context,
                self.service.input_manifest_bytes(request, selected),
                purpose="evaluation-input-manifest",
            )
            _, manifest_record = self.router.artifacts._readable(token, context, manifest.id)

            def accept(transaction):
                if ArtifactCatalog.available(transaction, manifest) != manifest_record:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Evaluation manifest changed")
                return self.service._authorize_in(transaction, request, selected, manifest)
        else:
            record = selected[2]
            content = await self.router.objects.read(
                record.workspace_id, self.router.service.artifacts._blob(record)
            )
            try:
                report = WorkloadReport.model_validate(
                    decode_observation_bytes(record.reference, content)
                )
            except ValueError as error:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid evaluation report"
                ) from error

            def accept(transaction):
                return self.service._complete_prepared(transaction, request, selected, report)

        self._identity(token, context)
        # The gate prevents ordinary commands from interleaving during byte I/O.
        # Recheck before the witness and repeat inside the final transaction.
        if self._select(context, operation, request) != selected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Evaluation preparation changed during I/O")

        def commit():
            self._identity(token, context)
            return self.database.execute(context, operation, normalized, definition.scope, accept)

        return await self.router._witnessed(context, operation, normalized, commit)
