"""Optional project-local import helper; training/evaluation use only the file protocol."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from receipts import normalize

from research_swarm import ResearchClient
from research_swarm.control.import_models import HistoricalObservation, ImportHistoricalEvidence
from research_swarm.domain.canonical import content_digest, record_digest, strict_json_loads


def import_endpoint(
    client: ResearchClient,
    *,
    project_id: str,
    path: Path,
    source_schema: str,
    source_record_id: str,
    expected_people: list[str],
    expected_identity: dict[str, str],
    key: str,
) -> dict[str, Any]:
    with path.open("rb") as stream:
        raw = stream.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise ValueError("Historical endpoint exceeds the metadata bound")
    endpoint = strict_json_loads(raw)
    panels = normalize(
        endpoint,
        source_schema=source_schema,
        expected_people=expected_people,
        expected_identity=expected_identity,
    )
    artifact = client.upload_file(path, idempotency_key=key + "-bytes")
    if artifact.digest != content_digest(raw):
        raise ValueError("Historical endpoint changed while it was imported")
    root = Path(__file__).resolve().parent
    normalizer = record_digest(
        "synthetic-pose-normalizer-v1",
        {
            name: content_digest((root / name).read_bytes())
            for name in ("history.py", "receipts.py")
        },
    )
    observations = tuple(
        HistoricalObservation(
            name=name, unit="degrees", panel=panel.name, value=getattr(panel, name)
        )
        for panel in panels.values()
        for name in ("macro_mae_deg", "pooled_mae_deg")
    )
    request = ImportHistoricalEvidence(
        project_id=project_id,
        source_system="synthetic-pose-example",
        source_schema=source_schema,
        source_record_id=source_record_id,
        raw_artifact=artifact,
        normalizer="synthetic-pose-normalizer-v1",
        normalizer_digest=normalizer,
        source_status="endpoint_recorded",
        source_evidence_class="historical-endpoint",
        comparison_identifiers=expected_identity,
        observations=observations,
        provenance_findings=(
            "Endpoint bytes do not prove original native closure",
            "Neither endpoint format supplies an authenticated execution seed",
        ),
    )
    return client.command("evidence.import_historical", request, idempotency_key=key)
