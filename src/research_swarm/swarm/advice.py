"""Read campaign evidence for a proposer, failure diagnosis or final report."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter, deque
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from research_swarm.swarm.agents import SCHEMAS, ask_agent
from research_swarm.swarm.interfaces import CampaignSpec, Result
from research_swarm.swarm.population import Population
from research_swarm.swarm.report import _read, capture
from research_swarm.swarm.store import _ATTEMPT, _attempt


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("role", choices=SCHEMAS)
    parser.add_argument("campaign")
    parser.add_argument("--db", type=Path, help="Local swarm campaign database")
    parser.add_argument("--attempt", help="Failed attempt ID for the diagnoser")
    provider = parser.add_mutually_exclusive_group(required=True)
    provider.add_argument("--model", help="Installed local Ollama model")
    provider.add_argument("--fake", action="store_true", help="Use the deterministic test provider")
    parser.add_argument("--timeout", type=float, default=10, help="Hard deadline in seconds")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--count", type=int, default=1, help="Number of parallel recipe proposals")


def _inputs(database: Path, args: argparse.Namespace) -> dict[str, Any]:
    snapshot = capture(database, args.campaign)
    if (args.role == "diagnoser") != bool(args.attempt):
        raise ValueError("Supply a failed --attempt only for the diagnoser")
    data = {
        "campaign": args.campaign,
        "purpose": "post-campaign analysis" if args.role == "reporter" else args.role,
        "metric": snapshot["research"]["objective"]["primary_metric"],
        "meter": snapshot["meter"],
        "incumbent": snapshot["recorded_selection"],
        "recent_evaluations": snapshot["recent_evaluations"][-10:],
    }
    if args.count != 1 and (args.role != "proposer" or args.count < 1):
        raise ValueError("Only the proposer accepts a positive batch size")
    if args.role != "reporter":
        with _read(database) as db:
            spec = db.execute(
                "SELECT spec FROM swarm_campaigns WHERE campaign_id=?", (args.campaign,)
            ).fetchone()
            definition = TypeAdapter(CampaignSpec).validate_json(spec["spec"], strict=True)
            data["search_space"] = definition.search_space
            if args.count > definition.max_workers:
                raise ValueError("Proposal count exceeds campaign capacity")
            if args.role == "proposer":
                population = Population(definition, min_replicates=definition.min_replicates)
                failures: deque[dict[str, Any]] = deque(maxlen=8)
                operators: dict[str, Counter[str]] = {}
                rows = db.execute(
                    _ATTEMPT + " JOIN swarm_events e ON e.attempt_id=a.attempt_id "
                    "WHERE e.campaign_id=? AND e.kind='finished' AND e.id<=? ORDER BY e.id",
                    (args.campaign, snapshot["evidence_through"]),
                )
                for row in rows:
                    result = Result(attempt=_attempt(row), **json.loads(row["observation"]))
                    population.add(result)
                    candidate = result.attempt.trial.candidate
                    operators.setdefault(candidate.operator, Counter())[result.status] += 1
                    if result.status != "ok":
                        failures.append(
                            {
                                "candidate": candidate.params,
                                "status": result.status,
                                "error": (result.error or "")[:1024],
                            }
                        )
                data["top_k"] = [
                    {
                        "params": entry.result.attempt.trial.candidate.params,
                        "score": entry.score,
                        "replicates": entry.replicates,
                        "fidelity": entry.result.attempt.trial.fidelity,
                    }
                    for entry in population.top()
                ]
                data["operator_outcomes"] = operators
                data["recent_failures"] = list(failures)
            if args.attempt:
                row = db.execute(
                    "SELECT a.observation, c.payload FROM swarm_attempts a JOIN swarm_trials t "
                    "USING(trial_id) JOIN swarm_candidates c USING(campaign_id,candidate_id) "
                    "WHERE t.campaign_id=? AND a.attempt_id=?",
                    (args.campaign, args.attempt),
                ).fetchone()
                observation = json.loads(row["observation"]) if row and row["observation"] else {}
                if observation.get("status") not in ("lost", "failed"):
                    raise ValueError(
                        "Diagnoser requires a recorded failed or lost campaign attempt"
                    )
                data["failure"] = {
                    "attempt_id": args.attempt,
                    "status": observation["status"],
                    "error": (observation.get("error") or "")[:2048],
                    "params": json.loads(row["payload"])["params"],
                }
    return data


async def advise(database: Path, args: argparse.Namespace) -> dict[str, Any]:
    context = _inputs(database, args)
    jobs = database.with_name(database.name + ".agent-jobs")
    replies = await asyncio.gather(
        *(
            ask_agent(
                args.role,
                context,
                model=args.model,
                timeout=args.timeout,
                seed=args.seed + i,
                jobs=jobs,
            )
            for i in range(args.count)
        )
    )
    if args.count == 1:
        return replies[0]
    status = "ok" if all(reply["status"] == "ok" for reply in replies) else "incomplete"
    return {"role": args.role, "status": status, "replies": replies}
