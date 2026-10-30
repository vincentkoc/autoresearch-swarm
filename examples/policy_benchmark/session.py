"""Run real funded CPU campaigns through the public API, then retain their evidence."""

from __future__ import annotations

import inspect
import os
import tempfile
from pathlib import Path

from contract import Definition

import research_swarm
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import (
    CampaignTarget,
    CompleteCampaign,
    ProposeCandidate,
    RetainArtifact,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import ReportCampaign
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import (
    canonical_bytes,
    content_digest,
    record_digest,
    strict_json_loads,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.orchestration.local import LocalDriver
from research_swarm.runtime import (
    LocalRuntime,
    private_state_directory,
    read_private,
    write_private,
)
from research_swarm.workers.local_enrollment import ensure_local_enrollment

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "cpu_bigram"


def limits(trials: int, proposals: int = 0) -> tuple[BudgetLimit, ...]:
    # Native admission includes the outer timeout and closure allowance, not just training.
    return (
        BudgetLimit(dimension="attempts", limit=trials * 2, unit="count"),
        BudgetLimit(dimension="execution_ms", limit=trials * 40_000, unit="ms"),
        BudgetLimit(dimension="trial_admissions", limit=trials, unit="count"),
        BudgetLimit(dimension="proposal_requests", limit=proposals, unit="count"),
    )


class BenchmarkSession:
    """The caller holds one file lock; every command envelope survives response loss."""

    def __init__(self, root: Path, definition: Definition, *, machine_registry: Path | None = None):
        self.root, self.definition = root.resolve(), definition
        with private_state_directory(self.root):
            pass
        self.runtime = LocalRuntime.open(root / "runtime")
        self.client = ResearchClient(LocalTransport(self.runtime))
        self.enrollment = ensure_local_enrollment(
            self.runtime, cpu_slots=2, machine_registry=machine_registry
        )
        self.driver = LocalDriver(self.client, self.enrollment)
        self.actor = self.runtime.principal().id
        frozen = {
            "definition": definition.model_dump(mode="json"),
            "workload": {
                path.name: content_digest(path.read_bytes())
                for path in sorted(SOURCE.iterdir())
                if path.name
                in {
                    "train.py",
                    "common.py",
                    "evaluate.py",
                    "dataset.json",
                    "research.toml",
                    "environment.json",
                }
            },
            "implementation": {
                name: content_digest(path.read_bytes())
                for name, path in {
                    "runner": HERE / "run.py",
                    "contract": HERE / "contract.py",
                    "session": HERE / "session.py",
                    **{
                        "package/"
                        + str(path.relative_to(Path(inspect.getfile(research_swarm)).parent)): path
                        for path in Path(inspect.getfile(research_swarm)).parent.rglob("*.py")
                    },
                }.items()
            },
            "execution_fingerprint": self.enrollment.worker.profile.execution_fingerprint,
            "cell_budget": [item.model_dump(mode="json") for item in limits(4, 2)],
            "proposer": {
                "version": "fixed-rate-sequence-v1",
                "rates": [0.05, 0.35],
                "llm_calls": 0,
            },
            "objective": (
                "fresh-seed audit of the frozen selected candidate; "
                "report cost and missing cells separately"
            ),
        }
        self.definition_digest = record_digest("research.example-policy-benchmark", frozen)
        self._freeze("definition.json", {"digest": self.definition_digest, **frozen})
        with private_state_directory(root / "commands"):
            pass

    def _freeze(self, name: str, value: dict) -> None:
        path = self.root / name
        encoded = canonical_bytes(value)
        if path.exists():
            if read_private(self.root, name, limit=8 * 1024 * 1024) != encoded:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Frozen benchmark inputs differ"
                )
        else:
            write_private(path, encoded)

    def call(self, name: str, operation: str, request, *, revision: int | None = None) -> dict:
        root = self.root / "commands"
        filename = content_digest(name.encode()).split(":")[1] + ".json"
        payload = request.model_dump(mode="json")
        intent = {"operation": operation, "payload": payload, "revision": revision}
        if (root / filename).exists():
            prior = strict_json_loads(read_private(root, filename, limit=2 * 1024 * 1024))
            if prior["operation"] != operation or prior["payload"] != payload:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Benchmark command intent differs"
                )
            intent = prior  # Reuse the pre-delivery revision, including after a lost reply.
        else:
            write_private(root / filename, canonical_bytes(intent))
        return self.client.command(
            operation,
            payload,
            idempotency_key=record_digest(
                "research.benchmark-command", [self.definition_digest, name]
            ),
            expected_revision=intent["revision"],
        )["result"]

    def start(self, name: str, campaign: dict) -> dict:
        for operation in ("campaign.seal", "campaign.start"):
            campaign = self.call(
                name + ":" + operation,
                operation,
                CampaignTarget(campaign_id=campaign["id"]),
                revision=campaign["revision"],
            )
        return campaign

    def source(self, rate: float) -> Path:
        target = self.root / ("source-" + str(rate))
        names = (
            "train.py",
            "common.py",
            "evaluate.py",
            "dataset.json",
            "research.toml",
            "environment.json",
        )
        expected = {name: (SOURCE / name).read_bytes() for name in names}
        expected["train.py"] = expected["train.py"].replace(
            b"LEARNING_RATE = 0.35", f"LEARNING_RATE = {rate}".encode()
        )
        if not target.exists():
            with tempfile.TemporaryDirectory(
                dir=self.root, prefix="preparing-source-"
            ) as temporary:
                staged = Path(temporary) / "source"
                staged.mkdir()
                for name, content in expected.items():
                    (staged / name).write_bytes(content)
                os.rename(staged, target)
        if {path.name for path in target.iterdir()} != set(expected) or any(
            (target / name).read_bytes() != content for name, content in expected.items()
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Prepared benchmark source differs")
        return target

    def candidate(self, name: str, campaign_id: str, artifact) -> tuple[dict, dict]:
        campaign = self.client.get("campaign", campaign_id)
        candidate = self.call(
            name + ":candidate",
            "candidate.propose",
            ProposeCandidate(
                campaign_id=campaign_id,
                source_artifact=artifact,
                hypothesis="Predeclared benchmark realization",
            ),
            revision=campaign["revision"],
        )
        validation = self.call(
            name + ":validation",
            "candidate.validate",
            ValidateCandidate(candidate_id=candidate["id"]),
            revision=candidate["revision"],
        )
        return candidate, validation

    def baseline(self, name: str, campaign_id: str, artifact, seed: int) -> dict:
        candidate, validation = self.candidate(name, campaign_id, artifact)
        return self.call(
            name + ":trial",
            "trial.submit",
            SubmitTrial(
                campaign_id=campaign_id,
                candidate_id=candidate["id"],
                validation_id=validation["id"],
                seed=seed,
            ),
            revision=self.client.get("campaign", campaign_id)["revision"],
        )

    def rows(self, kind: str, campaign_id: str) -> list[dict]:
        page = self.client.list(RecordQuery(kind=kind, campaign_id=campaign_id, limit=100))
        if page["next_cursor"] is not None:
            raise ValueError("Example benchmark exceeded its frozen record bound")
        return [self.client.get(kind, item["id"]) for item in page["items"]]

    def trial_statistics(self, trial: dict) -> dict:
        stages = []
        steps = None
        for job_id in trial["data"]["job_ids"]:
            job = self.client.get("job", job_id)
            attempt_id = job["data"]["current_attempt_id"]
            if attempt_id is None:
                continue
            try:
                result = self.client.get("run_result", attempt_id)["data"]
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
                continue
            stages.append(
                {
                    "stage": job["data"]["stage"]["id"],
                    "attempt_id": attempt_id,
                    **{
                        key: result[key]
                        for key in (
                            "status",
                            "started_at",
                            "finished_at",
                            "elapsed_ms",
                            "phase_timings_ms",
                            "result_digest",
                            "execution_fingerprint",
                        )
                    },
                }
            )
            model = job["data"]["outputs"].get("model")
            if model is not None:
                with tempfile.TemporaryDirectory(
                    dir=self.root, prefix="read-checkpoint-"
                ) as temporary:
                    path = Path(temporary) / "model.json"
                    self.client.download(model["id"], path)
                    steps = strict_json_loads(path.read_bytes())["train_steps"]
        return {
            "id": trial["id"],
            "state": trial["state"],
            "seed": trial["data"]["spec"]["seed"],
            "optimizer_steps": steps,
            "stages": stages,
        }

    def finish(self, name: str, campaign_id: str) -> dict:
        return self.call(
            name + ":complete",
            "campaign.complete",
            CompleteCampaign(
                campaign_id=campaign_id, reason="Frozen benchmark allocation exhausted"
            ),
            revision=self.client.get("campaign", campaign_id)["revision"],
        )["report"]

    def report(self, name: str, campaign_id: str) -> dict:
        watermark = self.client.context(campaign_id)["watermark"]
        return self.call(
            name + ":report:" + str(watermark),
            "report.create",
            ReportCampaign(campaign_id=campaign_id, recent_limit=50),
        )

    def retain_file(self, name: str, path: Path, owner_id: str):
        reference = self.client.upload_file(
            path,
            idempotency_key=name + ":" + self.definition_digest,
            media_type="application/json",
        )
        retained = self.call(
            name + ":retain",
            "artifact.retain",
            RetainArtifact(
                artifact_id=reference.id,
                owner_id=owner_id,
                purpose=name,
            ),
        )
        return {"artifact": reference.model_dump(mode="json"), "retention_id": retained["id"]}

    def incomplete_panel(self, index: int, campaign_id: str, outcome: str) -> dict:
        report = self.report(f"no-op-{index}", campaign_id)
        return {
            "index": index,
            "campaign_id": campaign_id,
            "complete": False,
            "outcome": outcome,
            "report_id": report["id"],
            "report_digest": report["data"]["report_digest"],
            "budget": report["data"]["snapshot"]["budget"],
            "trial_outcomes": [
                {"id": row["id"], "state": row["state"]} for row in self.rows("trial", campaign_id)
            ],
        }
