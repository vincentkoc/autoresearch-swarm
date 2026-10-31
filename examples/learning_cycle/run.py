"""A bounded native learning cycle over the same API available to external agents."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import time
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Self
from uuid import UUID, uuid5

from filelock import FileLock
from pydantic import Field, model_validator

import research_swarm
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.benchmark_assessment_models import AssessPolicyBenchmark
from research_swarm.control.benchmark_execution_models import (
    RequestBenchmarkProposal,
    StartPolicyBenchmark,
)
from research_swarm.control.benchmark_models import FreezePolicyBenchmark
from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.learning_models import (
    CommitPolicyDecision,
    FreezeLearningPlan,
    PlanEpisode,
    QualifyTrajectoryDataset,
)
from research_swarm.control.learning_training_models import (
    AdmitPolicyTraining,
    PreparePolicyInputs,
    PublishPolicyTraining,
    RegisterPolicyTrainingProfile,
)
from research_swarm.control.models import SearchConfiguration, StoppingRule
from research_swarm.control.policy_models import (
    AdoptPolicy,
    CreatePolicyBinding,
    PolicyBinding,
    PolicyBindingRef,
)
from research_swarm.control.promotions import AssessPromotion, CommitPromotion
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import (
    AdmitProposalResponse,
    CancelProposal,
    RequestProposal,
    RespondToProposal,
)
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import (
    canonical_bytes,
    content_digest,
    record_digest,
    strict_json_loads,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import Direction
from research_swarm.domain.learning import (
    CategoricalPolicy,
    LearningRate,
    Logit,
    PolicyAction,
    RewardSpec,
)
from research_swarm.domain.types import VersionedRecord
from research_swarm.evaluation.statistics import FixedPairedPolicy
from research_swarm.orchestration.local import LocalDriver
from research_swarm.runtime import (
    LocalRuntime,
    private_state_directory,
    read_private,
    write_private,
)
from research_swarm.workers.local_enrollment import ensure_local_enrollment

HERE = Path(__file__).resolve().parent
Seed = Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)]
NAMESPACE = UUID("8a247dda-36c7-54d8-b182-421b2e5b7340")


class Definition(VersionedRecord):
    development_seeds: tuple[Seed, ...] = Field(min_length=1, max_length=32)
    gate: FixedPairedPolicy
    baseline_logits: tuple[Logit, Logit]
    action_rates: tuple[float, float]
    reward_scale: float = Field(strict=True, gt=0, allow_inf_nan=False)
    training_rate: LearningRate
    future_probe_seed: Seed

    @model_validator(mode="after")
    def disjoint(self) -> Self:
        seeds = (*self.development_seeds, *self.gate.seeds, self.future_probe_seed)
        if len(set(seeds)) != len(seeds):
            raise ValueError("Development, held-out and future-probe seeds must be distinct")
        if any(not 0 < rate <= 1 for rate in self.action_rates):
            raise ValueError("Action learning rates must be in (0, 1]")
        return self


def limits(trials: int, proposals: int) -> tuple[BudgetLimit, ...]:
    return (
        BudgetLimit(dimension="attempts", limit=trials * 2, unit="count"),
        BudgetLimit(dimension="execution_ms", limit=trials * 40_000, unit="ms"),
        BudgetLimit(dimension="trial_admissions", limit=trials, unit="count"),
        BudgetLimit(dimension="proposal_requests", limit=proposals, unit="count"),
    )


class LearningCycle:
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
        package = Path(inspect.getfile(research_swarm)).parent
        frozen = {
            "definition": definition.model_dump(mode="json"),
            "implementation": {
                "package/" + path.relative_to(package).as_posix(): content_digest(path.read_bytes())
                for path in sorted(package.rglob("*.py"))
            },
            "runner": content_digest(Path(__file__).read_bytes()),
            "projects": {
                source.name + "/" + path.name: content_digest(path.read_bytes())
                for source in (HERE.parent / "cpu_bigram", HERE.parent / "cpu_policy_learning")
                for path in sorted(source.iterdir())
                if path.is_file() and path.suffix in {".py", ".json", ".toml"}
            },
            "execution_fingerprint": self.enrollment.worker.profile.execution_fingerprint,
            "claim": "One predeclared seed panel against a weak categorical prior; zero LLM calls.",
        }
        self.definition_digest = record_digest("research.example-learning-cycle.v1", frozen)
        self.freeze_file("definition.json", {"digest": self.definition_digest, **frozen})
        path = root / "commands.json"
        self.commands = (
            strict_json_loads(read_private(root, path.name, limit=8 * 1024 * 1024))
            if path.exists()
            else {}
        )

    def identifier(self, name: str) -> str:
        return str(uuid5(NAMESPACE, self.definition_digest + ":" + name))

    def freeze_file(self, name: str, value: dict) -> None:
        content = canonical_bytes(value)
        path = self.root / name
        if path.exists():
            if read_private(self.root, name, limit=8 * 1024 * 1024) != content:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Frozen qualification inputs differ"
                )
        else:
            write_private(path, content)

    def call(self, name: str, operation: str, request: VersionedRecord, revision=None) -> dict:
        intent = {
            "operation": operation,
            "payload": request.model_dump(mode="json"),
            "revision": revision,
        }
        previous = self.commands.get(name)
        if previous is not None:
            if (previous["operation"], previous["payload"]) != (operation, intent["payload"]):
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Qualification command changed")
            intent = previous
        else:
            self.commands[name] = intent
            write_private(self.root / "commands.json", canonical_bytes(self.commands))
        return self.client.command(
            operation,
            intent["payload"],
            idempotency_key=self.identifier(name),
            expected_revision=intent["revision"],
        )["result"]

    def source(self, name: str, project: str, *, rate=None, training_rate=None) -> Path:
        original = HERE.parent / project
        expected = {
            path.name: path.read_bytes()
            for path in original.iterdir()
            if path.is_file() and path.suffix in {".py", ".json", ".toml"}
        }
        if rate is not None:
            expected["train.py"] = expected["train.py"].replace(
                b"LEARNING_RATE = 0.35", f"LEARNING_RATE = {rate}".encode()
            )
        if training_rate is not None:
            configuration = strict_json_loads(expected["training.json"])
            configuration["learning_rate"] = training_rate
            expected["training.json"] = canonical_bytes(configuration)
        path = self.root / name
        with private_state_directory(path):
            if any(item.name not in expected or not item.is_file() for item in path.iterdir()):
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Unexpected qualification source"
                )
            for filename, content in expected.items():
                target = path / filename
                if target.exists():
                    if target.read_bytes() != content:
                        raise ResearchError(
                            ErrorCode.ARTIFACT_UNVERIFIED, "Qualification project changed"
                        )
                else:
                    write_private(target, content)
        return path

    def campaign(self, name: str, registered: dict, metric: str, *, inputs=None) -> dict:
        campaign = self.call(
            name + ":create",
            "campaign.create",
            CreateCampaign(
                project_id=registered["project"]["id"],
                project_revision_id=registered["project_revision"]["id"],
                name=name,
                primary_metric=metric,
                limits=limits(2, 1),
                input_artifacts=inputs or {},
                search=SearchConfiguration(
                    policy="external", version="external-v1", max_parallel_trials=1
                ),
                stopping=StoppingRule(max_trials=2),
            ),
        )
        for operation in ("campaign.seal", "campaign.start"):
            campaign = self.call(
                name + ":" + operation,
                operation,
                CampaignTarget(campaign_id=campaign["id"]),
                campaign["revision"],
            )
        return campaign

    def candidate(self, name: str, campaign: dict, source: ArtifactRef) -> tuple[dict, dict]:
        candidate = self.call(
            name + ":candidate",
            "candidate.propose",
            ProposeCandidate(
                campaign_id=campaign["id"],
                source_artifact=source,
                hypothesis="Predeclared learning-cycle realization",
            ),
            campaign["revision"],
        )
        validation = self.call(
            name + ":validate",
            "candidate.validate",
            ValidateCandidate(candidate_id=candidate["id"]),
            candidate["revision"],
        )
        return candidate, validation

    async def execute(self, campaign_id: str) -> None:
        result = await self.driver.drain(campaign_id)
        if result.state not in {"idle", "terminal"}:
            raise ResearchError(
                ErrorCode.EXECUTION_UNKNOWN,
                "Qualification execution did not complete",
                details=result.model_dump(mode="json"),
            )

    def receipt(self, trial: dict) -> dict:
        job = self.client.get("job", trial["data"]["job_ids"][-1])
        request = self.client.get("evaluation_request", job["data"]["evaluation_request_id"])
        return self.client.get("evaluation_receipt", request["data"]["receipt_id"])

    async def baseline(self, name: str, campaign: dict, source: ArtifactRef, seed: int) -> dict:
        candidate, validation = self.candidate(name, campaign, source)
        trial = self.call(
            name + ":trial",
            "trial.submit",
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=candidate["id"],
                validation_id=validation["id"],
                seed=seed,
            ),
            campaign["revision"],
        )
        await self.execute(campaign["id"])
        self.promote(name, campaign["id"], trial)
        return trial

    def promote(self, name: str, campaign_id: str, trial: dict) -> None:
        assessment = self.call(
            name + ":assess",
            "promotion.assess",
            AssessPromotion(
                campaign_id=campaign_id,
                candidate_evaluation_id=self.receipt(trial)["id"],
                expected_incumbent_revision=0,
            ),
        )
        self.call(
            name + ":promote", "promotion.commit", CommitPromotion(decision_id=assessment["id"])
        )

    def proposal(self, name: str, campaign_id: str, seed: int) -> dict:
        context = self.client.context(campaign_id)
        return self.call(
            name + ":proposal",
            "search.request_proposal",
            RequestProposal(
                campaign_id=campaign_id,
                proposer_id=self.actor,
                seed=seed,
                expected_controller_id=context["authority"]["controller_id"],
                expected_control_epoch=context["authority"]["control_epoch"],
                expected_incumbent_revision=1,
                instructions="Sample the assigned categorical policy once.",
            ),
            0,
        )["request"]

    def respond(self, name: str, request: dict) -> None:
        decision = self.call(
            name + ":decision",
            "learning.commit_decision",
            CommitPolicyDecision(request_id=request["id"]),
        )
        self.call(
            name + ":response",
            "search.respond",
            RespondToProposal(
                request_id=request["id"],
                outcome="candidate",
                source_artifact=decision["data"]["source_artifact"],
                hypothesis="Precommitted categorical action; no held-out feedback used",
            ),
        )

    async def admit_response(self, name: str, campaign_id: str, request_id: str) -> dict:
        context = self.client.context(campaign_id)
        admitted = self.call(
            name + ":admit",
            "search.admit_response",
            AdmitProposalResponse(
                campaign_id=campaign_id,
                request_id=request_id,
                expected_controller_id=context["authority"]["controller_id"],
                expected_control_epoch=context["authority"]["control_epoch"],
                expected_incumbent_revision=1,
            ),
            1,
        )
        if admitted["transition"]["data"]["outcome"] != "admitted":
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, "Planned policy action did not admit"
            )
        await self.execute(campaign_id)
        return admitted["trial"]

    async def run(self) -> dict:
        final = self.root / "result.json"
        if final.exists():
            result = strict_json_loads(read_private(self.root, final.name, limit=8 * 1024 * 1024))
            expected = record_digest(
                "research.example-learning-result.v1",
                {key: value for key, value in result.items() if key != "result_digest"},
            )
            if (
                result.get("definition_digest") != self.definition_digest
                or result.get("result_digest") != expected
            ):
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Qualification result differs")
            return result
        started = self.root / "started.json"
        if not started.exists():
            write_private(started, canonical_bytes({"wall_time": time.time()}))
        started_at = strict_json_loads(read_private(self.root, started.name, limit=1024))[
            "wall_time"
        ]
        registered = self.client.register_project(
            self.source("research", "cpu_bigram"),
            idempotency_key=self.identifier("research-project"),
        )["result"]
        source = ArtifactRef.model_validate(
            registered["project_revision"]["data"]["source_artifact"]
        )
        actions = {
            name: self.client.capture_project(
                self.source(name, "cpu_bigram", rate=rate),
                idempotency_key=self.identifier(name),
            )
            for name, rate in zip(("fast", "slow"), self.definition.action_rates, strict=True)
        }
        policy = CategoricalPolicy(
            actions=tuple(
                PolicyAction(id=name, source_digest=ref.digest) for name, ref in actions.items()
            ),
            logits=self.definition.baseline_logits,
        )
        reward = RewardSpec(
            metric_name="val_bpb", direction=Direction.MINIMIZE, scale=self.definition.reward_scale
        )
        plan = await self.development(registered, source, policy, reward, actions)
        benchmark, publication, binding = await self.fit(plan)
        assessment = await self.held_out(benchmark)
        adoption = None
        if assessment["data"]["gate"]["outcome"] == "promote":
            adoption = self.call(
                "adopt",
                "learning.adopt_policy",
                AdoptPolicy(
                    binding_id=binding["id"],
                    benchmark_id=benchmark["id"],
                ),
                binding["revision"],
            )
        future = await self.future_probe(registered, source, binding["id"])
        result = self.report(plan, benchmark, publication, assessment, adoption, future)
        result["wall_elapsed_seconds"] = time.time() - started_at
        result["result_digest"] = record_digest("research.example-learning-result.v1", result)
        self.freeze_file(final.name, result)
        return result

    async def development(self, registered, source, policy, reward, actions) -> dict:
        requests = []
        for seed in self.definition.development_seeds:
            name = f"development-{seed}"
            campaign = self.campaign(name, registered, "val_bpb")
            await self.baseline(name, campaign, source, seed)
            requests.append((name, campaign["id"], self.proposal(name, campaign["id"], seed)))
        plan = self.call(
            "development-plan",
            "learning.freeze_plan",
            FreezeLearningPlan(
                plan_id=self.identifier("development-plan"),
                policy=policy,
                reward_spec=reward,
                action_artifacts=actions,
                episodes=tuple(
                    PlanEpisode(request_id=request["id"], split="development")
                    for _, _, request in requests
                ),
            ),
        )
        for name, campaign_id, request in requests:
            self.respond(name, request)
            await self.admit_response(name, campaign_id, request["id"])
        return plan

    async def fit(self, plan) -> tuple[dict, dict, dict]:
        dataset = self.call(
            "dataset",
            "learning.qualify_dataset",
            QualifyTrajectoryDataset(
                plan_id=plan["id"],
                dataset_id=self.identifier("dataset"),
            ),
        )
        prepared = self.call(
            "inputs",
            "learning.prepare_training_inputs",
            PreparePolicyInputs(
                binding_id=self.identifier("inputs"),
                dataset_id=dataset["id"],
            ),
        )
        registered = self.client.register_project(
            self.source(
                "trainer", "cpu_policy_learning", training_rate=self.definition.training_rate
            ),
            idempotency_key=self.identifier("training-project"),
        )["result"]
        profile = self.call(
            "training-profile",
            "learning.register_training_profile",
            RegisterPolicyTrainingProfile(
                profile_id=self.identifier("training-profile"),
                project_revision_id=registered["project_revision"]["id"],
            ),
        )
        campaign = self.campaign(
            "training",
            registered,
            "update_error",
            inputs={
                name: ArtifactRef.model_validate(prepared["data"][name])
                for name in ("behavior", "trajectories")
            },
        )
        candidate, validation = self.candidate(
            "training",
            campaign,
            ArtifactRef.model_validate(
                registered["project_revision"]["data"]["source_artifact"],
            ),
        )
        benchmark = self.call(
            "benchmark",
            "learning.freeze_benchmark",
            FreezePolicyBenchmark(
                benchmark_id=self.identifier("benchmark"),
                training_admission_id=self.identifier("training-admission"),
                inputs_id=prepared["id"],
                profile_id=profile["id"],
                gate=self.definition.gate,
                per_arm_limits=limits(2, 1),
            ),
        )
        binding = self.call(
            "binding",
            "learning.create_policy_binding",
            CreatePolicyBinding(
                binding_id=self.identifier("binding"),
                benchmark_id=benchmark["id"],
            ),
        )
        admission = self.call(
            "training-admission",
            "learning.admit_training",
            AdmitPolicyTraining(
                admission_id=self.identifier("training-admission"),
                profile_id=profile["id"],
                inputs_id=prepared["id"],
                benchmark_id=benchmark["id"],
                campaign_id=campaign["id"],
                candidate_id=candidate["id"],
                validation_id=validation["id"],
                seed=0,
            ),
            campaign["revision"],
        )
        await self.execute(campaign["id"])
        publication = self.call(
            "publication",
            "learning.publish_training",
            PublishPolicyTraining(admission_id=admission["id"]),
        )
        return benchmark, publication, binding

    async def held_out(self, benchmark) -> dict:
        execution = self.call(
            "benchmark-start",
            "learning.start_benchmark",
            StartPolicyBenchmark(
                benchmark_id=benchmark["id"],
                proposer_id=self.actor,
            ),
        )
        for arm_id in execution["data"]["arm_ids"]:
            arm = self.client.get("benchmark_arm", arm_id)["data"]
            name = f"held-out-{arm['side']}-{arm['seed']}"
            await self.execute(arm["campaign_id"])
            self.promote(
                name, arm["campaign_id"], self.client.get("trial", arm["comparator_trial_id"])
            )
            reserved = self.call(
                name + ":proposal",
                "learning.request_benchmark_proposal",
                RequestBenchmarkProposal(arm_id=arm_id),
            )
            request = self.client.get("proposal_request", reserved["data"]["proposal_request_id"])
            self.respond(name, request)
            await self.admit_response(name, arm["campaign_id"], request["id"])
        return self.call(
            "benchmark-assessment",
            "learning.assess_benchmark",
            AssessPolicyBenchmark(benchmark_id=benchmark["id"]),
        )

    async def future_probe(self, registered, source, binding_id) -> dict:
        name = "future-probe"
        seed = self.definition.future_probe_seed
        campaign = self.campaign(name, registered, "val_bpb")
        await self.baseline(name, campaign, source, seed)
        request = self.proposal(name, campaign["id"], seed)
        current = self.client.get("policy_binding", binding_id)
        binding = PolicyBinding.model_validate(current["data"])
        plan = self.call(
            name + ":plan",
            "learning.freeze_plan",
            FreezeLearningPlan(
                plan_id=self.identifier(name + ":plan"),
                policy=binding.policy,
                reward_spec=binding.family.reward_spec,
                action_artifacts=binding.action_artifacts,
                episodes=(PlanEpisode(request_id=request["id"], split="development"),),
                policy_binding=PolicyBindingRef(
                    binding_id=binding_id,
                    revision=current["revision"],
                    policy_digest=binding.policy.digest,
                ),
            ),
        )
        decision = self.call(
            name + ":decision",
            "learning.commit_decision",
            CommitPolicyDecision(request_id=request["id"]),
        )
        self.call(
            name + ":cancel",
            "search.cancel_proposal",
            CancelProposal(
                campaign_id=campaign["id"],
                request_id=request["id"],
                reason="Binding probe only; no new candidate outcome claimed",
            ),
            self.client.get("search_state", campaign["id"])["revision"],
        )
        return {
            "plan_id": plan["id"],
            "binding": plan["data"]["policy_binding"],
            "decision": decision["data"]["decision"],
            "candidate_executed": False,
        }

    def rows(self, kind) -> list[dict]:
        rows, cursor = [], None
        while True:
            page = self.client.list(RecordQuery(kind=kind, limit=100, cursor=cursor))
            rows.extend(page["items"])
            cursor = page["next_cursor"]
            if cursor is None:
                return rows

    def report(self, plan, benchmark, publication, assessment, adoption, future) -> dict:
        dataset = self.client.get("learning_dataset", self.identifier("dataset"))["data"]["dataset"]
        cases = []
        for arm_id, digest in assessment["data"]["case_digests"].items():
            arm = self.client.get("benchmark_arm", arm_id)["data"]
            case = self.client.get("benchmark_case", arm_id)["data"]
            trajectory = case["trajectory"]
            cases.append(
                {
                    "side": arm["side"],
                    "seed": arm["seed"],
                    "case_digest": digest,
                    "action": trajectory["decision"]["action_id"],
                    "reward": trajectory["reward"],
                    "candidate_value": trajectory["candidate_value"],
                    "comparator_value": trajectory["comparator_value"],
                    "consumed": case["total_consumed"],
                    "trial_id": trajectory["trial_id"],
                }
            )
        budgets = {
            row["id"]: self.client.context(row["id"])["budget"] for row in self.rows("campaign")
        }
        totals = {}
        for accounts in budgets.values():
            for account in accounts:
                if Decimal(account["reserved"]) or Decimal(account["uncertain"]):
                    raise ResearchError(
                        ErrorCode.EXECUTION_UNKNOWN, "Qualification budget remains unsettled"
                    )
                dimension = account["dimension"]
                totals[dimension] = totals.get(dimension, Decimal(0)) + Decimal(account["consumed"])
        return {
            "schema_version": "1.0",
            "definition_digest": self.definition_digest,
            "execution_fingerprint": self.enrollment.worker.profile.execution_fingerprint,
            "plan_id": plan["id"],
            "benchmark_id": benchmark["id"],
            "development": [
                {
                    "seed": row["decision"]["seed"],
                    "action": row["decision"]["action_id"],
                    "reward": row["reward"],
                    "trajectory_digest": record_digest("research.trajectory.v1", row),
                }
                for row in dataset["trajectories"]
            ],
            "training": {
                key: publication["data"][key]
                for key in ("policy", "update_digest", "update_error", "bindings_digest")
            },
            "baseline_policy": benchmark["data"]["baseline_policy"],
            "gate": assessment["data"]["gate"],
            "pairs": assessment["data"]["pairs"],
            "cases": cases,
            "benchmark_consumed": assessment["data"]["total_consumed"],
            "all_campaigns_consumed": {key: str(value) for key, value in totals.items()},
            "trial_states": dict(Counter(row["state"] for row in self.rows("trial"))),
            "attempt_states": dict(Counter(row["state"] for row in self.rows("attempt"))),
            "adoption": None
            if adoption is None
            else {"id": adoption["id"], "revision": adoption["data"]["after_revision"]},
            "future_probe": future,
            "claims": [
                "native CPU; sequential execution; zero LLM calls",
                "one predeclared held-out seed panel; same tiny synthetic workload",
                "weak baseline; no general policy superiority or parallel scaling claim",
                "historical result replay; current use still requires controller evidence checks",
            ],
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--definition", type=Path, default=HERE / "definition.json")
    args = parser.parse_args()
    definition = Definition.model_validate(strict_json_loads(args.definition.read_bytes()))
    with private_state_directory(args.root):
        pass
    with FileLock(args.root / "qualification.lock", timeout=0):
        cycle = LearningCycle(args.root.resolve(), definition)
        result = asyncio.run(cycle.run())
        print(canonical_bytes(result).decode())


if __name__ == "__main__":
    main()
