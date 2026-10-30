"""Historical imports and fresh execution of an ordinary, project-owned workload."""

from __future__ import annotations

import copy
import importlib.util
import json
import math
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from research_swarm import ResearchClient
from research_swarm.client import LocalTransport
from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.import_models import HistoricalObservation, ImportHistoricalEvidence
from research_swarm.control.models import StoppingRule
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.types import new_id
from research_swarm.orchestration.local import LocalDriver
from research_swarm.projects.manifest import load_manifest
from research_swarm.projects.workload import (
    WorkloadContext,
    WorkloadInput,
    WorkloadReport,
    read_report,
    verify_outputs,
)
from research_swarm.workers.local_enrollment import ensure_local_enrollment
from tests.support import LIMITS
from tests.test_cli_mcp import process_environment


@dataclass
class Archive:
    source: Path
    root: Path
    trial_id: str
    inputs: dict[str, WorkloadInput]
    endpoints: dict[str, Path]
    report: WorkloadReport


def execute(
    source: Path,
    root: Path,
    *,
    trial_id: str,
    stage: str,
    inputs: dict[str, WorkloadInput] | None = None,
    seed: int = 7,
) -> tuple[subprocess.CompletedProcess[str], WorkloadContext]:
    root.mkdir(parents=True)
    request = WorkloadContext(
        trial_id=trial_id,
        attempt_id=new_id(),
        stage_id=stage,
        spec_digest=content_digest(b"synthetic-archive-producer"),
        seed=seed,
        time_budget_ms=1000,
        inputs=inputs or {},
        output_root=str(root / "outputs"),
        report_path=str(root / "report.json"),
    ).sealed()
    context_path = root / "context.json"
    context_path.write_bytes(canonical_bytes(request.model_dump(mode="json")))
    result = subprocess.run(
        [sys.executable, stage + ".py"],
        cwd=source,
        env={**process_environment(), "RESEARCH_CONTEXT": str(context_path)},
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
    )
    return result, request


@pytest.fixture
def archive(tmp_path: Path) -> Archive:
    source = tmp_path / "ordinary-project"
    shutil.copytree(Path(__file__).resolve().parents[1] / "examples" / "synthetic_pose", source)
    root, trial_id = tmp_path / "historical", new_id()
    manifest = load_manifest(source / "research.toml")
    trained, training = execute(source, root / "train", trial_id=trial_id, stage="train")
    assert trained.returncode == 0, trained.stderr
    train_report = read_report(training, manifest.stages[0])
    inputs = {
        output.output.name: WorkloadInput(
            path=str(Path(training.output_root) / output.output.path),
            digest=output.digest,
            size_bytes=output.size_bytes,
        )
        for output in verify_outputs(training, train_report, maximum_bytes=1024 * 1024)
    }
    evaluated, evaluation = execute(
        source, root / "evaluate", trial_id=trial_id, stage="evaluate", inputs=inputs
    )
    assert evaluated.returncode == 0, evaluated.stderr
    report = read_report(evaluation, manifest.stages[1])
    endpoints = {
        output.output.name: Path(evaluation.output_root) / output.output.path
        for output in verify_outputs(evaluation, report, maximum_bytes=1024 * 1024)
    }
    return Archive(source, root, trial_id, inputs, endpoints, report)


@pytest.fixture
def readers(archive: Archive, monkeypatch: pytest.MonkeyPatch) -> tuple[ModuleType, ModuleType]:
    modules = []
    for name in ("receipts", "history"):
        spec = importlib.util.spec_from_file_location(name, archive.source / (name + ".py"))
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        modules.append(module)
    return modules[0], modules[1]


def expected(archive: Archive) -> list[str]:
    return json.loads((archive.source / "dataset.json").read_bytes())["partition"]["search"]


def identity(endpoint: dict[str, Any]) -> dict[str, str]:
    return {name: endpoint[name] for name in ("trial", "batch_sha256", "checkpoint_sha256")}


def submit_existing(client: ResearchClient, registered: dict[str, Any]) -> tuple[str, str]:
    campaign = client.command(
        "campaign.create",
        CreateCampaign(
            project_id=registered["project"]["id"],
            project_revision_id=registered["project_revision"]["id"],
            name="Disposable pose replay",
            primary_metric="macro_mae_deg",
            limits=LIMITS,
            stopping=StoppingRule(max_trials=1),
        ),
        idempotency_key="replay-campaign",
    )["result"]
    for operation in ("campaign.seal", "campaign.start"):
        campaign = client.command(
            operation,
            CampaignTarget(campaign_id=campaign["id"]),
            idempotency_key=operation,
            expected_revision=campaign["revision"],
        )["result"]
    candidate = client.command(
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"],
            source_artifact=registered["project_revision"]["data"]["source_artifact"],
            hypothesis="Replay the archived synthetic recipe with fresh authority",
        ),
        idempotency_key="replay-candidate",
        expected_revision=campaign["revision"],
    )["result"]
    validation = client.command(
        "candidate.validate",
        ValidateCandidate(candidate_id=candidate["id"]),
        idempotency_key="replay-validation",
        expected_revision=candidate["revision"],
    )["result"]
    trial = client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=candidate["id"],
            validation_id=validation["id"],
            seed=7,
        ),
        idempotency_key="replay-trial",
        expected_revision=campaign["revision"],
    )["result"]
    return campaign["id"], trial["id"]


def driver(client: ResearchClient, root: Path) -> LocalDriver:
    assert isinstance(client.transport, LocalTransport)
    enrollment = ensure_local_enrollment(
        client.transport.runtime, machine_registry=root / "machine" / "resources.sqlite"
    )
    return LocalDriver(client, enrollment)


async def test_imported_receipts_replay_under_fresh_authority_without_a_project_plugin(
    archive: Archive, readers: tuple[ModuleType, ModuleType], tmp_path: Path
) -> None:
    normalizer, importer = readers
    originals = {name: json.loads(path.read_bytes()) for name, path in archive.endpoints.items()}
    state = tmp_path / "state"
    with ResearchClient.local(state) as client:
        registered = client.register_project(archive.source, idempotency_key="ordinary-project")
        project_id = registered["result"]["project"]["id"]
        imported = []
        for name, schema in (
            ("endpoint_schema2", "campaign-schema-2"),
            ("endpoint_candidates_v2", "candidates-v2"),
        ):
            arguments = dict(
                project_id=project_id,
                path=archive.endpoints[name],
                source_schema=schema,
                source_record_id=archive.trial_id,
                expected_people=expected(archive),
                expected_identity=identity(originals[name]),
                key="import-" + name,
            )
            ack = importer.import_endpoint(client, **arguments)
            assert importer.import_endpoint(client, **arguments) == ack
            row = ack["result"]
            imported.append(row)
            assert row["campaign_id"] is None
            assert row["data"]["promotion_eligible"] is False
            assert row["data"]["execution_authority"] == row["data"]["budget_authority"] == "none"
            copied = tmp_path / (name + "-recovered.json")
            client.download(row["data"]["manifest"]["raw_artifact"]["id"], copied)
            assert copied.read_bytes() == archive.endpoints[name].read_bytes()

        missing = tmp_path / "incomplete.json"
        missing.write_bytes(b'{"terminal_status":"unknown","endpoint":null,"cost":null}\n')
        absent = client.command(
            "evidence.import_historical",
            ImportHistoricalEvidence(
                project_id=project_id,
                source_system="synthetic-pose-example",
                source_schema="incomplete-native-v1",
                source_record_id="fictional-incomplete",
                raw_artifact=client.upload_file(missing, idempotency_key="incomplete-bytes"),
                normalizer="preserve-missing-v1",
                source_status="unknown",
                observations=(
                    HistoricalObservation(
                        name="macro_mae_deg", unit="degrees", missing_reason="Endpoint absent"
                    ),
                ),
                provenance_findings=("Original cost and native closure are absent",),
            ),
            idempotency_key="incomplete-import",
        )["result"]
        assert absent["data"]["manifest"]["observations"][0]["value"] is None
        for kind in ("campaign", "trial", "attempt", "evaluation_receipt"):
            assert not client.list(RecordQuery(kind=kind))["items"]

        campaign_id, trial_id = submit_existing(client, registered["result"])
        replay = await driver(client, tmp_path).drain(campaign_id)
        assert replay.state == "idle" and replay.executed_attempt_count == 2
        assert trial_id != archive.trial_id
        campaign = client.get("campaign", campaign_id)
        assert campaign["data"]["contract"]["project_id"] == project_id
        fresh = {}
        for item in client.list(RecordQuery(kind="job", campaign_id=campaign_id))["items"]:
            job = client.get("job", item["id"])
            assert job["state"] == "succeeded"
            for name, reference in job["data"]["outputs"].items():
                path = tmp_path / (name + "-fresh.json")
                client.download(reference["id"], path)
                fresh[name] = json.loads(path.read_bytes())
                if name == "model":
                    assert path.read_bytes() == Path(archive.inputs["model"].path).read_bytes()
        fit = json.loads(Path(archive.inputs["fit_receipt"].path).read_bytes())
        for name in ("checkpoint_sha256", "draw_order_sha256", "draws", "updates", "valid_frames"):
            assert fresh["fit_receipt"][name] == fit[name]
        assert fresh["fit_receipt"]["trial"] == trial_id
        for name in originals:
            assert fresh[name] == {**originals[name], "trial": trial_id}
            assert "seed" not in fresh[name]
        modern = normalizer.normalize(
            fresh["endpoint_schema2"],
            source_schema="campaign-schema-2",
            expected_people=expected(archive),
            expected_identity=identity(fresh["endpoint_schema2"]),
        )["full"]
        old = normalizer.normalize(
            fresh["endpoint_candidates_v2"],
            source_schema="candidates-v2",
            expected_people=expected(archive),
            expected_identity=identity(fresh["endpoint_candidates_v2"]),
        )
        assert old["full"].values() == modern.values()
        assert old["central"].valid_elements == 2 and modern.valid_elements == 8
        assert not math.isclose(modern.macro_mae_deg, modern.pooled_mae_deg)
        sums = fresh["endpoint_schema2"]["people"]
        macro = sum(math.degrees(row["error_sum"] / row["elements"]) for row in sums.values()) / 2
        pooled = math.degrees(sum(row["error_sum"] for row in sums.values()) / 8)
        receipt_rows = client.list(RecordQuery(kind="evaluation_receipt", campaign_id=campaign_id))[
            "items"
        ]
        assert len(receipt_rows) == 1 and receipt_rows[0]["state"] == "valid"
        evidence = client.get("evaluation_receipt", receipt_rows[0]["id"])["data"]["receipt"]
        measured = {metric["name"]: metric["value"] for metric in evidence["metrics"]}
        assert measured == {"macro_mae_deg": macro, "pooled_mae_deg": pooled}
        assert campaign["data"]["incumbent_candidate_id"] is None
        for row in imported:
            assert client.get("imported_evidence", row["id"]) == row
        for item in client.list(RecordQuery(kind="run_result", campaign_id=campaign_id))["items"]:
            result = client.get("run_result", item["id"])["data"]
            assert result["observed_seed"] == 7 and result["status"] == "succeeded"
    shutil.rmtree(archive.source)
    with ResearchClient.local(state) as reopened:
        resumed = await driver(reopened, tmp_path).drain(campaign_id)
        assert resumed.state == "idle" and resumed.executed_attempt_count == 0
        assert reopened.get("trial", trial_id)["state"] == "succeeded"


@pytest.mark.parametrize(
    "case,match",
    [
        ("coverage", "coverage"),
        ("units", "units"),
        ("count", "positive safe integer"),
        ("statistic", "finite"),
        ("aggregate", "aggregate"),
        ("panels", "both declared panels"),
        ("identity", "identity differs"),
        ("unknown", "Unknown endpoint schema"),
    ],
)
def test_project_reader_rejects_incompatible_or_inconsistent_receipts(
    archive: Archive, readers: tuple[ModuleType, ModuleType], case: str, match: str
) -> None:
    schema = "candidates-v2" if case in ("aggregate", "panels") else "campaign-schema-2"
    name = "endpoint_candidates_v2" if schema == "candidates-v2" else "endpoint_schema2"
    endpoint = json.loads(archive.endpoints[name].read_bytes())
    pins = identity(endpoint)
    if case == "coverage":
        endpoint["people"].pop(expected(archive)[0])
    elif case == "units":
        endpoint["units"] = "degrees"
    elif case == "count":
        endpoint["people"][expected(archive)[0]]["elements"] = True
    elif case == "statistic":
        endpoint["people"][expected(archive)[0]]["error_sum"] = float("nan")
    elif case == "aggregate":
        endpoint["scores"]["full"]["person_mae_deg"] += 1
    elif case == "panels":
        del endpoint["scores"]["central"]
    elif case == "identity":
        pins["checkpoint_sha256"] = "different-checkpoint"
    else:
        schema = "guessed-schema"
    with pytest.raises(ValueError, match=match):
        readers[0].normalize(
            endpoint,
            source_schema=schema,
            expected_people=expected(archive),
            expected_identity=pins,
        )


@pytest.mark.parametrize("case", ["seed", "units", "coverage", "partition", "checkpoint"])
def test_protected_evaluator_rejects_semantic_mismatch_even_with_updated_input_digest(
    archive: Archive, case: str, tmp_path: Path
) -> None:
    model = json.loads(Path(archive.inputs["model"].path).read_bytes())
    if case == "seed":
        model["seed"] = 8
    elif case == "units":
        model["units"] = "degrees"
    elif case == "coverage":
        model["fit_people"] = model["fit_people"][:1]
    elif case == "partition":
        model["partition_sha256"] = "changed"
    else:
        model["weight"] += 1
    inputs = copy.deepcopy(archive.inputs)
    path = tmp_path / "changed-model.json"
    path.write_bytes(canonical_bytes(model))
    inputs["model"] = WorkloadInput(
        path=str(path), digest=content_digest(path.read_bytes()), size_bytes=path.stat().st_size
    )
    if case == "partition":
        fit = json.loads(Path(inputs["fit_receipt"].path).read_bytes())
        fit["checkpoint_sha256"] = inputs["model"].digest.removeprefix("sha256:")
        fit_path = tmp_path / "changed-fit.json"
        fit_path.write_bytes(canonical_bytes(fit))
        inputs["fit_receipt"] = WorkloadInput(
            path=str(fit_path),
            digest=content_digest(fit_path.read_bytes()),
            size_bytes=fit_path.stat().st_size,
        )
    failed, context = execute(
        archive.source,
        tmp_path / "negative-evaluation",
        trial_id=archive.trial_id,
        stage="evaluate",
        inputs=inputs,
    )
    assert failed.returncode != 0 and "ValueError" in failed.stderr
    assert not Path(context.report_path).exists()


@pytest.mark.parametrize("raw", [b'{"trial":1,"trial":2}', b'{"trial":NaN}', b"[]"])
def test_import_reader_refuses_ambiguous_json_before_upload(
    archive: Archive,
    readers: tuple[ModuleType, ModuleType],
    tmp_path: Path,
    raw: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "ambiguous.json"
    path.write_bytes(raw)
    with ResearchClient.local(tmp_path / "unused-state") as client:
        monkeypatch.setattr(
            client,
            "upload_file",
            lambda *args, **kwargs: pytest.fail("Invalid metadata was uploaded"),
        )
        with pytest.raises(ValueError):
            readers[1].import_endpoint(
                client,
                project_id=new_id(),
                path=path,
                source_schema="campaign-schema-2",
                source_record_id="ambiguous",
                expected_people=expected(archive),
                expected_identity={},
                key="ambiguous",
            )
