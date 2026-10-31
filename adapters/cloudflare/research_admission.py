"""Ordinary research admission using shared transactions and bounded source bytes.

Every entry runs under the router gate. Prepared source objects are internal
verification products, never request fields. No awaited I/O follows the witness.
"""

import asyncio
from typing import Literal
from zipfile import BadZipFile

from pydantic import ValidationError

from research_swarm.control.dispatch import dispatch_command, validate_command_input
from research_swarm.control.models import JobData, TrialData
from research_swarm.control.queries import RecordQuery
from research_swarm.control.service import check_revision
from research_swarm.control.workers import worker_authority
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.projects.manifest import resolve_project
from research_swarm.projects.source import verify_bundle
from research_swarm.state.database import Transaction
from research_swarm.workers.clocks import require_authority_clock

COMMANDS = frozenset(
    {
        "project.register",
        "campaign.create",
        "campaign.validate",
        "campaign.seal",
        "campaign.start",
        "candidate.propose",
        "candidate.validate",
        "trial.submit",
        "worker.claim_job",
    }
)
SOURCE_COMMANDS = frozenset({"project.register", "candidate.propose"})
RecordKind = Literal[
    "project",
    "project_revision",
    "campaign",
    "candidate",
    "candidate_validation",
    "trial",
    "job",
    "worker",
    "worker_session",
    "attempt",
    "acceptance_lease",
]


class AdmissionQuery(RecordQuery):
    kind: RecordKind


class AdmissionRead(VersionedRecord):
    kind: RecordKind
    id: ID


class HostedResearchAdmission:
    commands = COMMANDS

    def __init__(self, router):
        self.router = router
        self.database, self.service = router.database, router.service

    def _profile(self):
        # Kept independent of the router module to avoid an import cycle.
        if self.router.configuration.profile != "research-admission-witness-v1":
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Research admission is unavailable"
            )

    def _identity(self, token, context):
        self._profile()
        current = self.router._context(
            token,
            idempotency_key=context.idempotency_key,
            expected_revision=context.expected_revision,
        )
        if current.principal != context.principal:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Research credential changed")
        owner = asyncio.current_task()
        if owner is None or owner.cancelling():
            raise asyncio.CancelledError("Research admission owner was cancelled")

    def _preflight(self, context, operation, request):
        def check(sql):
            transaction = Transaction(sql, context)
            if operation == "project.register" and request.project_id is not None:
                check_revision(context, transaction.get("project", request.project_id))
            elif operation == "candidate.propose":
                check_revision(context, transaction.get("campaign", request.campaign_id))
            elif operation == "worker.claim_job":
                worker = worker_authority(transaction, request.worker_id, request.session_id)
                if worker.clock_mode != "bounded-continuous-v1":
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "Hosted claims require calibrated time"
                    )
                require_authority_clock(
                    "hosted-native-witness-v1",
                    self.router.configuration.source.policy,
                    worker.clock_admission,
                )
                row = transaction.get("job", request.job_id)
                check_revision(context, row)
                job = JobData.model_validate(row["data"])
                trial = TrialData.model_validate(transaction.get("trial", job.trial_id)["data"])
                if (
                    worker.project_ids is not None
                    and trial.spec.project_id not in worker.project_ids
                ):
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Project is outside worker scope"
                    )
                profile = next(
                    (
                        item
                        for item in worker.profiles
                        if item.name == job.stage.resources.engine_profile
                    ),
                    None,
                )
                if (
                    profile is None
                    or profile.backend != "python"
                    or profile.accelerator_count
                    or job.stage.resources.accelerator_count
                ):
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "This profile admits CPU claims only"
                    )

        self.database.store.read(check)

    async def command(self, token, context, operation, payload):
        self._identity(token, context)
        definition, request = validate_command_input(context, operation, payload)
        if operation in SOURCE_COMMANDS:
            context.principal.require("read")
        normalized = request.model_dump(mode="json")
        previous = self.database.replay(context, operation, normalized, definition.scope)
        if previous is not None:
            return previous
        self._preflight(context, operation, request)
        prepared, source_row = None, None
        if operation in SOURCE_COMMANDS:
            source_row, record = self.router.artifacts._readable(
                token, context, request.source_artifact.id
            )
            if record.reference != request.source_artifact:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Source reference differs")
            reference, content = await self.router.artifacts.content(
                token, context, {"id": request.source_artifact.id}
            )
            if reference != request.source_artifact:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Source reference differs")
            try:
                prepared = verify_bundle(content)
            except (ValueError, KeyError, BadZipFile) as error:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Invalid source bundle"
                ) from error
            if operation == "project.register":
                try:
                    prepared = resolve_project(content, manifest_path=request.manifest_path)
                except (ValueError, KeyError, BadZipFile) as error:
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid project contract"
                    ) from error

        def recheck():
            self._identity(token, context)
            if source_row is not None:
                current, _ = self.router.artifacts._readable(
                    token, context, request.source_artifact.id
                )
                if current != source_row:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Source changed during admission")

        def commit():
            recheck()
            if operation in SOURCE_COMMANDS:
                handler = (
                    self.service._register_project
                    if operation == "project.register"
                    else self.service._propose_candidate
                )
                return self.service._execute(
                    context,
                    operation,
                    request,
                    definition.scope,
                    lambda transaction: handler(transaction, request, prepared),
                )
            return dispatch_command(self.service, context, operation, normalized)

        recheck()
        return await self.router._witnessed(context, operation, normalized, commit)

    def query(self, context, operation, payload):
        self._profile()
        context.principal.require("read")
        try:
            if operation == "get":
                request = AdmissionRead.model_validate(payload)
                return self.router.records.get(context, request.kind, request.id)
            return self.router.records.list(context, AdmissionQuery.model_validate(payload))
        except ValidationError as error:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid research query"
            ) from error
