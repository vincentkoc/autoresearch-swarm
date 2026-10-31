"""Transfer profiles bound selection before bytes or ownership can move."""

from dataclasses import replace

import pytest

from research_swarm.control.orchestration_transfer import transfer_request_digest
from research_swarm.control.transfer_view import TransferLimits
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Transaction
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_transfer_preparation import counts, selection


def selected(pipeline, context, intent, limits):
    return pipeline.runtime.database.store.read(
        lambda sql: pipeline.runtime.service.orchestration_transfer._prepare_in(
            Transaction(sql, context),
            intent,
            transfer_request_digest(context, intent),
            limits=limits,
        )
    )


@pytest.fixture
def bounded(pipeline, tmp_path):
    _, intent, context = selection(pipeline)
    path = tmp_path / "second-evidence.txt"
    path.write_text("Synthetic required transfer evidence", encoding="utf-8")
    artifact = pipeline.client.upload_file(path, idempotency_key="extra-evidence")
    pipeline.runtime.database.store.write(
        lambda sql: ArtifactCatalog.retain(
            Transaction(sql, context),
            ArtifactRetention(
                artifact_id=artifact.id,
                owner_kind="campaign",
                owner_id=intent.campaign_id,
                purpose="fixture-required-evidence",
            ),
        )
    )
    view = selected(pipeline, context, intent, TransferLimits())
    assert view is not None and len(view.artifacts) >= 2
    limits = TransferLimits(
        max_records=sum(entry["category"] == "record" for entry in view.entries),
        max_entries=len(view.entries),
        max_manifest_bytes=len(canonical_bytes({"entries": view.entries})),
        max_artifacts=len(view.artifacts),
        max_artifact_bytes=sum(item.size_bytes for item in view.artifacts),
    )
    return pipeline, intent, context, view, limits


def test_exact_bounds_preserve_native_view_and_transfer_digest(bounded):
    pipeline, intent, context, native, limits = bounded
    before = counts(pipeline)
    limited = selected(pipeline, context, intent, limits)
    assert limited == native
    assert limited.digest == native.digest and limited.artifact_digest == native.artifact_digest
    assert counts(pipeline) == before


@pytest.mark.parametrize(
    "field",
    ["max_records", "max_entries", "max_manifest_bytes", "max_artifacts", "max_artifact_bytes"],
)
def test_oversized_view_refuses_before_any_transfer_effect(bounded, field):
    pipeline, intent, context, _, exact = bounded
    limit = getattr(exact, field)
    assert limit > 1
    before = counts(pipeline)
    with pytest.raises(ResearchError) as refused:
        selected(pipeline, context, intent, replace(exact, **{field: limit - 1}))
    assert refused.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert counts(pipeline) == before


def test_commit_applies_the_same_profile_bounds_again(bounded):
    pipeline, intent, context, view, exact = bounded
    service = pipeline.runtime.service.orchestration_transfer
    with pytest.raises(ResearchError) as refused:
        pipeline.runtime.database.execute(
            context,
            "orchestration.transfer",
            intent.model_dump(mode="json"),
            "orchestration.manage",
            lambda tx: service._commit(
                tx,
                intent,
                transfer_request_digest(context, intent),
                view,
                limits=replace(exact, max_artifact_bytes=exact.max_artifact_bytes - 1),
            ),
        )
    assert refused.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert pipeline.client.get("orchestration_binding", intent.campaign_id) == view.binding


def test_custody_scan_limits_sql_before_a_bridge_materializes_rows(bounded):
    pipeline, intent, context, _, _ = bounded
    inspected = []

    def inspect(sql):
        class MaterializingBridge:
            def execute(self, statement, parameters=()):
                if statement.startswith("SELECT payload FROM budgets"):
                    # The bridge must receive the limit itself; filtering a large
                    # returned result in Python would be too late.
                    inspected.append(statement)
                    assert statement.endswith(" LIMIT ?") and parameters[-1] == 2
                return sql.execute(statement, parameters)

        return pipeline.runtime.service.orchestration_transfer._prepare_in(
            Transaction(MaterializingBridge(), context),
            intent,
            transfer_request_digest(context, intent),
            limits=TransferLimits(max_records=1),
        )

    with pytest.raises(ResearchError) as refused:
        pipeline.runtime.database.store.read(inspect)
    assert refused.value.code == ErrorCode.RESOURCE_UNAVAILABLE and len(inspected) == 1


@pytest.mark.parametrize(
    "value",
    [
        {"max_records": True},
        {"max_entries": 0},
        {"max_manifest_bytes": 65 * 1024**2},
        {"max_artifacts": 4097},
        {"max_artifact_bytes": -1},
    ],
)
def test_profiles_cannot_expand_native_ceilings_or_accept_invalid_limits(value):
    with pytest.raises(ValueError):
        TransferLimits(**value)
