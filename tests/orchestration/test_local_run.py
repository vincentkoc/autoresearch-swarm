from __future__ import annotations

import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.client import ResearchClient
from research_swarm.control.commands import CompleteCampaign, PauseCampaign
from research_swarm.control.promotions import AssessPromotion, CommitPromotion
from research_swarm.control.queries import RecordQuery
from research_swarm.control.workers import EngineCapabilities
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.orchestration.bootstrap import RunOptions, prepare_run
from tests.api.test_cli_mcp import cli
from tests.paths import REPO_ROOT


@pytest.fixture
def source(tmp_path: Path) -> Path:
    destination = tmp_path / "project"
    shutil.copytree(REPO_ROOT / "examples" / "cpu_bigram", destination)
    return destination


def test_concurrent_bootstrap_creates_one_frozen_run(tmp_path: Path, source: Path) -> None:
    with ResearchClient.local(tmp_path / "state") as client:
        with ThreadPoolExecutor(max_workers=3) as pool:
            runs = list(pool.map(lambda _: prepare_run(client, source, key="baseline"), range(3)))
        assert all(run.plan == runs[0].plan for run in runs)
        for kind in ("project", "campaign", "candidate", "trial"):
            assert len(client.list(RecordQuery(kind=kind))["items"]) == 1
        campaign = client.get("campaign", runs[0].plan.campaign_id)
        assert campaign["data"]["contract"]["search"]["policy"] == "external"
        assert campaign["data"]["contract"]["limits"][1]["limit"] == "300000"


@pytest.mark.parametrize(
    "interruption",
    [
        "upload",
        "project.register",
        "campaign.create",
        "campaign.seal",
        "campaign.start",
        "candidate.propose",
        "candidate.validate",
        "trial.submit",
    ],
)
def test_resume_replays_every_bootstrap_boundary_without_source_checkout(
    tmp_path: Path,
    source: Path,
    interruption: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with ResearchClient.local(tmp_path / "state") as client:
        command, upload = client.command, client.upload_file
        failed = False

        def interrupted_command(operation: str, *args: Any, **kwargs: Any) -> Any:
            nonlocal failed
            result = command(operation, *args, **kwargs)
            if operation == interruption and not failed:
                failed = True
                raise OSError("Synthetic lost bootstrap acknowledgment")
            return result

        def interrupted_upload(*args: Any, **kwargs: Any) -> Any:
            nonlocal failed
            result = upload(*args, **kwargs)
            if interruption == "upload" and not failed:
                failed = True
                raise OSError("Synthetic lost bootstrap acknowledgment")
            return result

        monkeypatch.setattr(client, "command", interrupted_command)
        monkeypatch.setattr(client, "upload_file", interrupted_upload)
        with pytest.raises(OSError, match="acknowledgment"):
            prepare_run(client, source, key="fixed-run")
        shutil.rmtree(source)
        resumed = prepare_run(client, tmp_path / "relocated-and-absent", key="fixed-run")
        assert resumed.plan.campaign_id is not None and resumed.plan.trial_id is not None
        for kind in ("project", "campaign", "candidate", "trial"):
            assert len(client.list(RecordQuery(kind=kind))["items"]) == 1
        assert client.get("trial", resumed.plan.trial_id)["state"] == "queued"


def test_frozen_source_survives_checkout_change_and_cache_loss(
    tmp_path: Path, source: Path
) -> None:
    with ResearchClient.local(tmp_path / "state") as client:
        original = prepare_run(client, source, key="frozen")
        (source / "train.py").write_text("raise SystemExit(99)\n", encoding="utf-8")
        assert prepare_run(client, source, key="frozen").plan == original.plan
        (original.root / "source.zip").unlink()
        assert prepare_run(client, source, key="frozen").plan == original.plan
        with (original.root / "source.zip").open("ab") as stream:
            stream.write(b"modified archive")
        with pytest.raises(ResearchError) as corrupted:
            prepare_run(client, source, key="frozen")
        assert corrupted.value.code == ErrorCode.ARTIFACT_UNVERIFIED
        assert len(client.list(RecordQuery(kind="trial"))["items"]) == 1


def test_changed_options_conflict_and_external_pause_is_not_resumed(
    tmp_path: Path, source: Path
) -> None:
    with ResearchClient.local(tmp_path / "state") as client:
        prepared = prepare_run(client, source, key="external")
        campaign = client.get("campaign", prepared.plan.campaign_id)
        client.command(
            "campaign.pause",
            PauseCampaign(campaign_id=campaign["id"]),
            idempotency_key="external-pause",
            expected_revision=campaign["revision"],
        )
        assert prepare_run(client, source, key="external").plan == prepared.plan
        assert client.get("campaign", campaign["id"])["state"] == "paused"
        with pytest.raises(ResearchError) as changed:
            prepare_run(client, source, key="external", options=RunOptions(seed=1))
        assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT


def test_unsupported_engine_stops_before_campaign_admission(tmp_path: Path, source: Path) -> None:
    manifest = source / "research.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace('profile = "python-cpu"', 'profile = "mlx"'),
        encoding="utf-8",
    )
    profile = EngineCapabilities(
        name="python-cpu",
        backend="python",
        cpu_slots=1,
        execution_fingerprint=content_digest(b"test-profile"),
    )
    with ResearchClient.local(tmp_path / "state") as client:
        with pytest.raises(ResearchError) as failure:
            prepare_run(client, source, profile=profile)
        assert failure.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
        assert not client.list(RecordQuery(kind="campaign"))["items"]


def test_missing_external_input_cannot_seal_or_submit(tmp_path: Path, source: Path) -> None:
    manifest = source / "research.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'output_names = ["model"]',
            'output_names = ["model"]\nrequired_artifacts = ["external_data"]',
        ),
        encoding="utf-8",
    )
    with ResearchClient.local(tmp_path / "state") as client:
        with pytest.raises(ResearchError) as missing:
            prepare_run(client, source)
        assert missing.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
        assert not client.list(RecordQuery(kind="trial"))["items"]
        assert client.list(RecordQuery(kind="campaign"))["items"][0]["state"] == "draft"


def test_external_pause_during_bootstrap_is_not_overridden(
    tmp_path: Path,
    source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with ResearchClient.local(tmp_path / "state") as client:
        command = client.command

        def pause_after_start(operation: str, *args: Any, **kwargs: Any) -> Any:
            result = command(operation, *args, **kwargs)
            if operation == "campaign.start":
                campaign = client.get("campaign", result["result"]["id"])
                if campaign["state"] == "running":
                    command(
                        "campaign.pause",
                        PauseCampaign(campaign_id=campaign["id"]),
                        idempotency_key="operator-pause",
                        expected_revision=campaign["revision"],
                    )
            return result

        monkeypatch.setattr(client, "command", pause_after_start)
        with pytest.raises(ResearchError) as paused:
            prepare_run(client, source)
        assert paused.value.code == ErrorCode.REVISION_CONFLICT
        monkeypatch.setattr(client, "command", command)
        with pytest.raises(ResearchError):
            prepare_run(client, source)
        assert client.list(RecordQuery(kind="campaign"))["items"][0]["state"] == "paused"
        assert not client.list(RecordQuery(kind="trial"))["items"]


@pytest.mark.parametrize("limit,expected", [(10, "awaiting_proposals"), (1, "awaiting_completion")])
def test_run_cli_executes_baseline_and_returns_agent_context_without_promotion(
    tmp_path: Path,
    source: Path,
    limit: int,
    expected: str,
) -> None:
    state, registry = tmp_path / "state", tmp_path / "machine" / "resources.sqlite"
    arguments = (
        "run",
        str(source),
        "--key",
        "cli-run",
        "--machine-registry",
        str(registry),
        "--max-trials",
        str(limit),
    )
    result = cli(state, *arguments)
    assert result["state"] == expected
    assert result["driver"]["executed_attempt_count"] == 2
    assert result["context"]["campaign"]["state"] == "running"
    assert result["context"]["campaign"]["data"]["incumbent_candidate_id"] is None
    assert result["next_action"]["campaign_id"] == result["campaign_id"]
    shutil.rmtree(source)
    resumed = cli(state, *arguments)
    assert resumed["run_id"] == result["run_id"] and resumed["trial_id"] == result["trial_id"]
    assert resumed["driver"]["executed_attempt_count"] == 0
    if limit == 1:
        with ResearchClient.local(state) as agent:
            receipt = agent.list(RecordQuery(kind="evaluation_receipt"))["items"][0]
            assessment = agent.command(
                "promotion.assess",
                AssessPromotion(
                    campaign_id=result["campaign_id"],
                    candidate_evaluation_id=receipt["id"],
                    expected_incumbent_revision=0,
                ),
                idempotency_key="agent-assess-baseline",
            )["result"]
            agent.command(
                "promotion.commit",
                CommitPromotion(decision_id=assessment["id"]),
                idempotency_key="agent-select-baseline",
            )
            campaign = agent.get("campaign", result["campaign_id"])
            completed = agent.command(
                "campaign.complete",
                CompleteCampaign(campaign_id=campaign["id"], reason="Baseline research finished"),
                idempotency_key="agent-finish",
                expected_revision=campaign["revision"],
            )["result"]
            assert (
                completed["report"]["data"]["snapshot"]["incumbent"]["evidence_level"] == "observed"
            )
        terminal = cli(state, *arguments)
        assert terminal["state"] == "terminal" and terminal["driver"]["executed_attempt_count"] == 0


def test_legacy_run_options_keep_original_command_identity() -> None:
    legacy = (
        b'{"input_artifacts":{},"manifest_path":"research.toml","max_trials":10,'
        b'"parallelism":1,"primary_metric":null,"schema_version":"1.0",'
        b'"seed":0,"source_revision":null}'
    )
    assert canonical_bytes(RunOptions().model_dump(mode="json")) == legacy
    assert canonical_bytes(RunOptions(search_policy="external").model_dump(mode="json")) == legacy
    assert RunOptions.model_validate_json(legacy).search_policy == "external"


def test_cli_greedy_bootstrap_plans_without_effects_then_advances_baseline(
    tmp_path: Path, source: Path
) -> None:
    state, registry = tmp_path / "state", tmp_path / "machine" / "resources.sqlite"
    started = cli(
        state,
        "run",
        str(source),
        "--key",
        "greedy-run",
        "--machine-registry",
        str(registry),
        "--max-trials",
        "2",
        "--search-policy",
        "greedy",
    )
    identity = cli(state, "init")
    actor = identity["operator_id"]
    with ResearchClient.local(state) as client:
        campaign = client.get("campaign", started["campaign_id"])
        assert campaign["data"]["contract"]["search"]["policy"] == "greedy"
    arguments = (
        started["campaign_id"],
        "--proposer",
        actor,
        "--baseline-trial",
        started["trial_id"],
    )
    planned = cli(state, "search", "plan", *arguments)
    assert planned["operation"] == "promotion.assess"
    with ResearchClient.local(state) as client:
        assert client.get("campaign", started["campaign_id"]) == campaign
        assert client.list(RecordQuery(kind="promotion"))["items"] == []
    advanced = cli(state, "search", "advance", *arguments)
    assert advanced["state"] == "awaiting_proposal"
    assert [item["operation"] for item in advanced["actions"]] == [
        "promotion.assess",
        "promotion.commit",
        "search.request_proposal",
    ]
    recovered = cli(state, "search", "advance", *arguments)
    assert recovered["evidence_ids"] == advanced["evidence_ids"]
    assert recovered["actions"] == []
