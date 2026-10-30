"""Run real funded CPU campaigns through the public API, then retain their evidence."""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal
from pathlib import Path

from contract import Definition, summarize
from filelock import FileLock
from session import HERE, BenchmarkSession, limits

from research_swarm.control.child_models import CreateChildCampaign
from research_swarm.control.commands import (
    CreateCampaign,
    SubmitTrial,
)
from research_swarm.control.confirmation import AssessConfirmation, CreateConfirmationPlan
from research_swarm.control.models import (
    DelegationConfiguration,
    PortfolioConfiguration,
    PromotionConfiguration,
    ResearchTheme,
    SearchConfiguration,
    StoppingRule,
)
from research_swarm.control.promotions import AssessPromotion, CommitPromotion
from research_swarm.control.search_models import ReportedAgentUsage, RespondToProposal
from research_swarm.domain.budgets import BudgetKind, BudgetLimit
from research_swarm.domain.canonical import (
    canonical_bytes,
    record_digest,
    strict_json_loads,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.orchestration.local import LocalDriver
from research_swarm.orchestration.search import SearchDriver
from research_swarm.runtime import (
    private_state_directory,
    read_private,
)
from research_swarm.search.greedy import GreedyConfiguration
from research_swarm.search.portfolio import PortfolioDriverConfiguration


class Benchmark(BenchmarkSession):
    async def policy_cell(self, case, repeat: int, policy: str, parent: dict, artifact) -> dict:
        name = f"{case.name}:{repeat}:{policy}"
        seed = case.seed + repeat * 2_000_000
        search = (
            SearchConfiguration()
            if policy == "greedy"
            else SearchConfiguration(
                policy="portfolio",
                version="portfolio-v1",
                max_parallel_trials=2,
                portfolio=PortfolioConfiguration(
                    first_seed=seed + 1,
                    themes=(
                        ResearchTheme(
                            theme_id="optimizer",
                            proposer_id=self.actor,
                            instructions="Use the frozen rate sequence",
                        ),
                    ),
                ),
            )
        )
        created = self.call(
            name + ":child",
            "campaign.create_child",
            CreateChildCampaign(
                parent_campaign_id=parent["id"],
                name=name,
                limits=limits(4, 2),
                max_trials=4,
                search=search,
            ),
            revision=self.client.get("campaign", parent["id"])["revision"],
        )
        child = self.start(name, created["campaign"])
        baseline = self.baseline(name, child["id"], artifact, seed)
        config = (
            GreedyConfiguration(
                proposer_id=self.actor, first_seed=seed + 1, baseline_trial_id=baseline["id"]
            )
            if policy == "greedy"
            else PortfolioDriverConfiguration(baseline_trial_id=baseline["id"])
        )
        planner = SearchDriver(self.client, child["id"], config)
        final_state = "step_limit"
        for _ in range(24):
            driven = await self.driver.drain(child["id"])
            if driven.state == "blocked":
                final_state = "blocked"
                break
            progress = planner.advance()
            final_state = progress.state
            if progress.state in {"budget_exhausted", "terminal", "needs_review", "paused"}:
                break
            for request in self.rows("proposal_request", child["id"]):
                if request["state"] != "pending":
                    continue
                ordinal = request["data"]["seed"] - seed - 1
                if ordinal not in (0, 1):
                    raise ValueError("Proposal escaped its frozen sequence")
                source = self.client.capture_project(
                    self.source((0.05, 0.35)[ordinal]), idempotency_key="variant-" + str(ordinal)
                )
                self.call(
                    name + ":answer:" + request["id"],
                    "search.respond",
                    RespondToProposal(
                        request_id=request["id"],
                        outcome="candidate",
                        source_artifact=source,
                        hypothesis="Predeclared optimizer rate; no held-out feedback used",
                        usage=ReportedAgentUsage(
                            provider="scripted",
                            model="fixed-rate-sequence-v1",
                            amounts={"llm_tokens": 0},
                        ),
                    ),
                )
        audit = None
        selection = None
        if final_state in {"budget_exhausted", "terminal"}:
            current = self.client.get("campaign", child["id"])
            selected = current["data"]["incumbent_evaluation_id"]
            evidence = self.client.get("evaluation_receipt", selected)
            request = self.client.get(
                "evaluation_request", evidence["data"]["receipt"]["request_id"]
            )
            selected_trial = self.client.get("trial", request["data"]["request"]["trial_id"])
            selection = {
                "candidate_id": selected_trial["data"]["candidate_id"],
                "validation_id": selected_trial["data"]["validation_id"],
                "selection_evaluation_id": selected,
                "audit_seed": seed + 20_000_000,
            }
            self._freeze("selection-" + name.replace(":", "-") + ".json", selection)
            audit = self.call(
                name + ":audit",
                "trial.submit",
                SubmitTrial(
                    campaign_id=child["id"],
                    candidate_id=selection["candidate_id"],
                    validation_id=selection["validation_id"],
                    seed=selection["audit_seed"],
                ),
                revision=current["revision"],
            )
            audited = await self.driver.drain(child["id"])
            if audited.state == "blocked":
                final_state = "blocked"
        if final_state in {"budget_exhausted", "terminal"}:
            report = self.finish(name, child["id"])
        else:
            report = self.report(name, child["id"])
        snapshot = report["data"]["snapshot"]
        receipts = snapshot["recent_evaluations"]
        baseline_receipts = [
            item
            for item in receipts
            if self.client.get("evaluation_request", item["data"]["request_id"])["data"]["request"][
                "trial_id"
            ]
            == baseline["id"]
        ]
        trials = self.rows("trial", child["id"])
        complete = (
            len(trials) == 4 and len(receipts) == 4 and all(item["eligible"] for item in receipts)
        )
        audit_receipts = [
            item
            for item in receipts
            if audit
            and self.client.get("evaluation_request", item["data"]["request_id"])["data"][
                "request"
            ]["trial_id"]
            == audit["id"]
        ]
        improvement = None
        if complete and len(audit_receipts) == 1 and len(baseline_receipts) == 1:
            improvement = (
                baseline_receipts[0]["data"]["metrics"][0]["value"]
                - audit_receipts[0]["data"]["metrics"][0]["value"]
            )
        else:
            complete = False
        return {
            "case": case.name,
            "split": case.split,
            "repeat": repeat,
            "policy": policy,
            "seed": seed,
            "campaign_id": child["id"],
            "report_id": report["id"],
            "report_digest": report["data"]["report_digest"],
            "complete": complete,
            "driver_state": final_state,
            "improvement": improvement,
            "frozen_selection": selection,
            "audit_evaluation_id": audit_receipts[0]["id"] if len(audit_receipts) == 1 else None,
            "budget": snapshot["budget"],
            "receipts": receipts,
            "trial_outcomes": [self.trial_statistics(item) for item in trials],
        }

    async def no_op(self, index: int, registered: dict) -> dict:
        name = f"no-op-{index}"
        seeds = tuple(10_000_000 + 10 * index + offset for offset in range(5))
        campaign = self.call(
            name,
            "campaign.create",
            CreateCampaign(
                project_id=registered["project"]["id"],
                name=name,
                primary_metric="val_bpb",
                limits=(
                    *limits(11),
                    BudgetLimit(
                        dimension="evaluation_exposures",
                        limit=10,
                        unit="count",
                        kind=BudgetKind.EXPOSURE,
                    ),
                ),
                stopping=StoppingRule(max_trials=11),
                promotion=PromotionConfiguration(
                    mode="paired_confirmation", version="paired-sign-v1", confirmation_seeds=seeds
                ),
            ),
        )
        campaign = self.start(name, campaign)
        artifact = registered["project_revision"]["data"]["source_artifact"]
        baseline = self.baseline(name, campaign["id"], artifact, 9_000_000 + index)
        driven = await LocalDriver(self.client, self.enrollment, concurrency=1).drain(
            campaign["id"]
        )
        if driven.state == "blocked":
            return self.incomplete_panel(index, campaign["id"], "blocked")
        receipts = self.rows("evaluation_receipt", campaign["id"])
        baseline_receipt = next(
            (
                item
                for item in receipts
                if self.client.get("evaluation_request", item["data"]["receipt"]["request_id"])[
                    "data"
                ]["request"]["trial_id"]
                == baseline["id"]
                and item["state"] == "valid"
            ),
            None,
        )
        if baseline_receipt is None:
            return self.incomplete_panel(index, campaign["id"], "baseline_failed")
        assessment = self.call(
            name + ":baseline-assess",
            "promotion.assess",
            AssessPromotion(
                campaign_id=campaign["id"],
                candidate_evaluation_id=baseline_receipt["id"],
                expected_incumbent_revision=0,
            ),
        )
        self.call(
            name + ":baseline-promote",
            "promotion.commit",
            CommitPromotion(decision_id=assessment["id"]),
        )
        candidate, validation = self.candidate(name + ":no-op", campaign["id"], artifact)
        fingerprint = self.enrollment.worker.profile.execution_fingerprint
        plan = self.call(
            name + ":plan",
            "confirmation.create",
            CreateConfirmationPlan(
                campaign_id=campaign["id"],
                candidate_id=candidate["id"],
                validation_id=validation["id"],
                expected_incumbent_revision=1,
                evaluator_id=self.enrollment.evaluator_id,
                execution_fingerprints={"train": fingerprint, "evaluate": fingerprint},
            ),
            revision=self.client.get("campaign", campaign["id"])["revision"],
        )
        driven = await LocalDriver(self.client, self.enrollment, concurrency=1).drain(
            campaign["id"]
        )
        if driven.state == "blocked":
            return self.incomplete_panel(index, campaign["id"], "blocked")
        outcome = self.call(
            name + ":assess", "confirmation.assess", AssessConfirmation(plan_id=plan["id"])
        )
        report = self.finish(name, campaign["id"])
        receipts = self.rows("evaluation_receipt", campaign["id"])
        by_trial = {
            self.client.get("evaluation_request", item["data"]["receipt"]["request_id"])["data"][
                "request"
            ]["trial_id"]: item
            for item in receipts
        }
        pairs = []
        for seed in seeds:
            members = {
                item["side"]: by_trial.get(item["trial_id"])
                for item in plan["data"]["plan"]["bindings"]
                if item["seed"] == seed
            }
            values = {
                side: item["data"]["receipt"]["metrics"][0]["value"]
                for side, item in members.items()
                if item and item["state"] == "valid"
            }
            pairs.append(
                {
                    "seed": seed,
                    "values": values,
                    "receipts": {
                        side: item["id"] if item else None for side, item in members.items()
                    },
                }
            )
        complete = len(pairs) == 5 and all(
            set(item["values"]) == {"candidate", "comparator"} for item in pairs
        )
        return {
            "index": index,
            "campaign_id": campaign["id"],
            "complete": complete,
            "source_digest": artifact["digest"],
            "outcome": outcome.get("data", {})
            .get("decision", {})
            .get("outcome", outcome.get("outcome", "incomplete")),
            "assessment": outcome,
            "report_id": report["id"],
            "report_digest": report["data"]["report_digest"],
            "pairs": pairs,
            "execution": [
                self.trial_statistics(item) for item in self.rows("trial", campaign["id"])
            ],
            "execution_order": "serial plan order; candidate precedes comparator",
            "budget": report["data"]["snapshot"]["budget"],
        }

    async def run(self) -> dict:
        final = self.root / "result.json"
        if final.exists():
            result = strict_json_loads(read_private(self.root, final.name, limit=8 * 1024 * 1024))
            expected = record_digest(
                "research.example-policy-benchmark-result",
                {key: value for key, value in result.items() if key != "result_digest"},
            )
            if (
                result.get("result_digest") != expected
                or result.get("definition_digest") != self.definition_digest
            ):
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Benchmark result differs")
            reference = self.retain_file("benchmark-result", final, result["parent_campaign_id"])
            self._freeze("artifact.json", reference)
            return result
        registered = self.client.register_project(
            self.source(0.0), idempotency_key="benchmark-project"
        )["result"]
        count = len(self.definition.cells())
        parent = self.call(
            "parent",
            "campaign.create",
            CreateCampaign(
                project_id=registered["project"]["id"],
                name="Frozen policy benchmark",
                primary_metric="val_bpb",
                limits=(
                    *limits(count * 4, count * 2),
                    BudgetLimit(dimension="child_campaigns", limit=count, unit="count"),
                ),
                delegation=DelegationConfiguration(max_depth=1),
                stopping=StoppingRule(max_trials=count * 4),
            ),
        )
        parent = self.start("parent", parent)
        definition_artifact = self.retain_file(
            "benchmark-definition", self.root / "definition.json", parent["id"]
        )
        cells = [
            await self.policy_cell(
                case,
                repeat,
                policy,
                parent,
                registered["project_revision"]["data"]["source_artifact"],
            )
            for case, repeat, policy in self.definition.cells()
        ]
        noop_project = self.client.register_project(
            self.source(0.35), idempotency_key="no-op-project"
        )["result"]
        panels = [
            await self.no_op(index, noop_project) for index in range(self.definition.no_op_panels)
        ]
        complete = all(cell["complete"] for cell in cells) and all(
            panel["complete"] for panel in panels
        )
        parent_report = (
            self.finish("parent", parent["id"])
            if all(cell["complete"] for cell in cells)
            else self.report("parent", parent["id"])
        )
        result = {
            "schema_version": "1.0",
            "definition_digest": self.definition_digest,
            "definition_artifact": definition_artifact,
            "parent_campaign_id": parent["id"],
            "complete": complete,
            "cells": cells,
            "no_op_panels": panels,
            "summary": summarize(cells, panels),
            "parent_report_id": parent_report["id"],
            "parent_budget": parent_report["data"]["snapshot"]["budget"],
            "claims": [
                "local CPU execution",
                "fixed proposer; zero LLM calls",
                "held-out starting seeds, not held-out datasets",
                "no RL training or general policy superiority claim",
            ],
        }
        for dimension in ("attempts", "execution_ms", "trial_admissions", "proposal_requests"):
            parent_total = next(
                item["consumed"]
                for item in result["parent_budget"]
                if item["dimension"] == dimension
            )
            child_total = sum(
                (
                    Decimal(
                        next(
                            item["consumed"]
                            for item in cell["budget"]
                            if item["dimension"] == dimension
                        )
                    )
                    for cell in cells
                ),
                Decimal(0),
            )
            if Decimal(parent_total) != child_total:
                raise ValueError("Parent and child benchmark accounting differ")
        result["result_digest"] = record_digest("research.example-policy-benchmark-result", result)
        if complete:
            self._freeze("result.json", result)
            reference = self.retain_file("benchmark-result", final, parent["id"])
            self._freeze("artifact.json", reference)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--definition", type=Path, default=HERE / "benchmark.json")
    args = parser.parse_args()
    definition = Definition.model_validate(strict_json_loads(args.definition.read_bytes()))
    with private_state_directory(args.state):
        pass
    with FileLock(str(args.state / "benchmark.lock"), timeout=0):
        result = asyncio.run(Benchmark(args.state, definition).run())
    print(canonical_bytes(result).decode())


if __name__ == "__main__":
    main()
