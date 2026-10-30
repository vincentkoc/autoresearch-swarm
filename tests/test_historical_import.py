from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from mcp import Client
from pydantic import ValidationError

from research_swarm import ResearchClient
from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport
from research_swarm.control.import_models import HistoricalObservation, ImportHistoricalEvidence
from research_swarm.control.promotions import AssessPromotion
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from research_swarm.storage.filesystem import FilesystemBlobStore
from tests.test_agent_research_cycle import command
from tests.test_cli_mcp import cli, stdio
from tests.test_evaluation_driver import role
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


@pytest.fixture
def historical(pipeline: Pipeline, tmp_path: Path) -> ImportHistoricalEvidence:
    path = tmp_path / "historical.json"
    path.write_bytes(b'{ "trial": "fictional-a", "cost": null, "status": "unknown" }\n')
    artifact = pipeline.client.upload_file(path, idempotency_key="historical-bytes")
    return ImportHistoricalEvidence(
        project_id=pipeline.campaign["data"]["contract"]["project_id"],
        source_system="synthetic-lab",
        source_schema="endpoint-v2",
        source_record_id="fictional-a",
        raw_artifact=artifact,
        normalizer="synthetic-normalizer-v1",
        source_status="unknown",
        source_evidence_class="historical-unverified",
        comparison_identifiers={"panel": "fictional-search", "units": "radians"},
        observations=(
            HistoricalObservation(
                name="pose_mae", unit="radians", missing_reason="No terminal endpoint"
            ),
        ),
        provenance_findings=("Original native custody is unknown", "Original cost is absent"),
    )


def record(
    pipeline: Pipeline,
    intent: ImportHistoricalEvidence,
    *,
    key: str | None = None,
    client: ResearchClient | None = None,
) -> dict[str, Any]:
    return (client or pipeline.client).command(
        "evidence.import_historical", intent, idempotency_key=key or new_id()
    )


def test_import_preserves_raw_bytes_missingness_and_no_current_authority(
    pipeline: Pipeline, historical: ImportHistoricalEvidence, tmp_path: Path
) -> None:
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    budgets = pipeline.client.budget(campaign["id"])
    result = record(pipeline, historical, key="original")
    assert record(pipeline, historical, key="original") == result
    assert record(pipeline, historical)["result"] == result["result"]
    imported = result["result"]
    assert imported["campaign_id"] is None and imported["parent_id"] == historical.project_id
    assert imported["state"] == "historical"
    data = imported["data"]
    assert data["imported"] and not data["promotion_eligible"]
    assert data["source_authenticity"] == "unverified"
    assert data["execution_authority"] == data["budget_authority"] == "none"
    assert data["import_manifest_digest"] == historical.digest()
    assert data["manifest"] == historical.model_dump(mode="json")
    assert data["manifest"]["observations"][0]["value"] is None
    copied = tmp_path / "recovered.json"
    pipeline.client.download(historical.raw_artifact.id, copied)
    assert content_digest(copied.read_bytes()) == historical.raw_artifact.digest
    assert copied.read_bytes().startswith(b'{ "trial"') and copied.read_bytes().endswith(b"\n")
    assert pipeline.client.get("campaign", campaign["id"]) == campaign
    assert pipeline.client.budget(campaign["id"]) == budgets
    for kind in ("attempt", "run_result", "evaluation_receipt", "funding_settlement"):
        assert pipeline.client.list(RecordQuery(kind=kind))["items"] == []
    read_context = CommandContext(principal=pipeline.runtime.principal(), idempotency_key=new_id())
    artifact = pipeline.runtime.database.read_record(
        read_context, "artifact", historical.raw_artifact.id
    )
    with pytest.raises(ResearchError) as retained:
        pipeline.runtime.artifacts.delete(
            CommandContext(
                principal=pipeline.runtime.principal(),
                idempotency_key="delete-import",
                expected_revision=artifact["revision"],
            ),
            historical.raw_artifact.id,
            "Retention fixture",
        )
    assert retained.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert "retained references" in retained.value.message


@pytest.mark.parametrize(
    "field", ["observations", "comparison_identifiers", "source_status", "normalizer"]
)
def test_changed_manifest_cannot_replace_same_historical_identity(
    pipeline: Pipeline, historical: ImportHistoricalEvidence, field: str
) -> None:
    first = record(pipeline, historical)["result"]
    changed = {
        "observations": (HistoricalObservation(name="pose_mae", value=-999.0, unit="radians"),),
        "comparison_identifiers": {"panel": "another-panel"},
        "source_status": "succeeded",
        "normalizer": "normalizer-v2",
    }[field]
    with pytest.raises(ResearchError) as conflict:
        record(pipeline, historical.model_copy(update={field: changed}))
    assert conflict.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert pipeline.client.get("imported_evidence", first["id"]) == first


def test_schema_and_original_record_id_distinguish_historical_formats(
    pipeline: Pipeline, historical: ImportHistoricalEvidence
) -> None:
    first = record(pipeline, historical)["result"]
    other_schema = record(
        pipeline, historical.model_copy(update={"source_schema": "different-endpoint-v2"})
    )["result"]
    other_record = record(
        pipeline, historical.model_copy(update={"source_record_id": "fictional-b"})
    )["result"]
    assert len({item["id"] for item in (first, other_schema, other_record)}) == 3
    with pytest.raises(ResearchError) as unrankable:
        pipeline.client.command(
            "promotion.assess",
            AssessPromotion(
                campaign_id=pipeline.campaign["id"],
                candidate_evaluation_id=first["id"],
                expected_incumbent_revision=0,
            ),
            idempotency_key="cannot-promote-history",
        )
    assert unrankable.value.code == ErrorCode.NOT_FOUND


def test_import_rollback_retains_neither_record_nor_permanent_artifact_owner(
    pipeline: Pipeline, historical: ImportHistoricalEvidence, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Transaction.event

    def fail(self: Transaction, name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "evidence.historical_imported":
            raise RuntimeError("Synthetic import event failure")
        return original(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="import event"):
            record(pipeline, historical)
    assert pipeline.client.list(RecordQuery(kind="imported_evidence"))["items"] == []
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' "
                "AND state='retained' "
                "AND json_extract(payload,'$.retention.owner_kind')='imported_evidence'"
            ).fetchone()[0]
            == 0
        )
    assert record(pipeline, historical)["result"]["state"] == "historical"


def test_corrupt_raw_evidence_cannot_be_imported_but_original_ack_stays_replayable(
    pipeline: Pipeline, historical: ImportHistoricalEvidence
) -> None:
    first = record(pipeline, historical, key="accepted")
    artifact = pipeline.runtime.artifacts.get(
        CommandContext(principal=pipeline.runtime.principal(), idempotency_key=new_id()),
        historical.raw_artifact.id,
    )
    store = pipeline.runtime.artifacts.store
    assert isinstance(store, FilesystemBlobStore)
    store._object(
        artifact.workspace_id,
        StoredBlob(reference=artifact.reference, generation=artifact.storage_generation),
    ).write_bytes(b"corrupt")
    assert record(pipeline, historical, key="accepted") == first
    with pytest.raises(ResearchError) as corrupted:
        record(pipeline, historical.model_copy(update={"source_record_id": "fresh-import"}))
    assert corrupted.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert len(pipeline.client.list(RecordQuery(kind="imported_evidence"))["items"]) == 1


def test_competing_importers_reopen_and_commit_one_identity(
    pipeline: Pipeline, historical: ImportHistoricalEvidence
) -> None:
    barrier = Barrier(2)

    def import_once() -> dict[str, Any]:
        client = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
        barrier.wait(timeout=10)
        return record(pipeline, historical, client=client)["result"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(lambda _: import_once(), range(2)))
    assert first == second
    assert len(pipeline.client.list(RecordQuery(kind="imported_evidence"))["items"]) == 1


def test_historical_closure_cannot_release_current_attempt_reservations(
    pipeline: Pipeline, historical: ImportHistoricalEvidence
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    budget = pipeline.client.budget(pipeline.campaign["id"])
    assert (
        next(item for item in budget["accounts"] if item["dimension"] == "attempts")["reserved"]
        == "1"
    )
    imported = record(
        pipeline, historical.model_copy(update={"source_status": "ownership_closed"})
    )["result"]
    assert imported["data"]["manifest"]["source_status"] == "ownership_closed"
    assert pipeline.client.get("attempt", attempt["id"]) == attempt
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget


async def test_http_mcp_cli_share_import_identity_and_read_scope(
    pipeline: Pipeline, historical: ImportHistoricalEvidence
) -> None:
    _, reader, _ = role(pipeline, "read")
    with pytest.raises(ResearchError) as denied:
        record(pipeline, historical, client=reader)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    _, _, token = role(pipeline, "read", "evidence.import")
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, token) as remote,
    ):
        first = record(pipeline, historical, client=remote)["result"]
    async with Client(stdio(pipeline.runtime.root), read_timeout_seconds=20, cache=None) as agent:
        received = await command(
            agent, "evidence.import_historical", historical.model_dump(mode="json"), "mcp-import"
        )
        assert received["result"]["id"] == first["id"]
        assert received["result"]["data"]["promotion_eligible"] is False
    imported = cli(
        pipeline.runtime.root,
        "command",
        "evidence.import_historical",
        "--key",
        "cli-import",
        payload=historical.model_dump(mode="json"),
    )
    assert imported["result"] == first


@pytest.mark.parametrize("value", [True, float("nan"), float("inf")])
def test_historical_metrics_reject_nonmeasurements(value: Any) -> None:
    with pytest.raises(ValidationError):
        HistoricalObservation(name="loss", unit="radians", value=value)


def test_import_bounds_and_authority_cannot_be_overridden(
    historical: ImportHistoricalEvidence,
) -> None:
    payload = historical.model_dump(mode="json")
    with pytest.raises(ValidationError):
        ImportHistoricalEvidence.model_validate({**payload, "promotion_eligible": True})
    payload["raw_artifact"]["size_bytes"] = 1024 * 1024 + 1
    with pytest.raises(ValidationError, match="exceeds 1 MiB"):
        ImportHistoricalEvidence.model_validate(payload)
