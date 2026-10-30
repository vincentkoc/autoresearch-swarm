"""Immutable metadata-only import; project-local normalizers own source semantics."""

from uuid import UUID, uuid5

from research_swarm.control.import_models import ImportedEvidence, ImportHistoricalEvidence
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction


class HistoricalImportControl:
    def __init__(self, database: Database, artifacts: ArtifactService) -> None:
        self.database, self.artifacts = database, artifacts

    def record(self, context: CommandContext, intent: ImportHistoricalEvidence) -> JsonObject:
        context.principal.require("read")
        payload = intent.model_dump(mode="json")
        return self.database.with_preflight(
            context,
            "evidence.import_historical",
            payload,
            "evidence.import",
            lambda: self._verified(context, intent),
        )

    def _verified(self, context: CommandContext, intent: ImportHistoricalEvidence) -> JsonObject:
        # The verified reader pins the exact generation through the retention transaction.
        # Its bytes remain opaque: a declared external schema does not authenticate its facts.
        with self.artifacts.reader(context, intent.raw_artifact):
            return self.database.execute(
                context,
                "evidence.import_historical",
                intent.model_dump(mode="json"),
                "evidence.import",
                lambda transaction: self._record(transaction, intent),
            )

    @staticmethod
    def _record(transaction: Transaction, intent: ImportHistoricalEvidence) -> JsonObject:
        transaction.get("project", intent.project_id)
        identity = record_digest("research.historical-source-identity-v1", intent.identity())
        identifier = str(uuid5(UUID(transaction.workspace_id), identity))
        digest = intent.digest()
        try:
            prior = transaction.get("imported_evidence", identifier)
        except ResearchError as error:
            if error.code != ErrorCode.NOT_FOUND:
                raise
        else:
            if prior["data"]["import_manifest_digest"] != digest:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT,
                    "Historical source identity has different imported content",
                )
            return prior
        ArtifactCatalog.available(transaction, intent.raw_artifact)
        evidence = ImportedEvidence(
            manifest=intent,
            import_manifest_digest=digest,
            imported_by=transaction.context.principal.id,
            imported_at=utc_now(),
        )
        row = transaction.insert(
            "imported_evidence",
            identifier,
            evidence.model_dump(mode="json"),
            state="historical",
            parent_id=intent.project_id,
        )
        ArtifactCatalog.retain(
            transaction,
            ArtifactRetention(
                artifact_id=intent.raw_artifact.id,
                owner_kind="imported_evidence",
                owner_id=identifier,
                purpose="historical_provenance",
            ),
        )
        transaction.event(
            "evidence.historical_imported",
            row,
            {
                "project_id": intent.project_id,
                "import_manifest_digest": digest,
            },
        )
        return row
