"""Three advisory model roles, each supervised in a disposable local process."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import signal
import sys
import tempfile
import time
from collections import Counter, deque
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from research_swarm.swarm.agent_guard import Role as Role
from research_swarm.swarm.agent_guard import record_outcome
from research_swarm.swarm.process import guard_session


class Advice(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Proposal(Advice):
    params: dict[str, JsonValue]
    rationale: str = Field(min_length=1, max_length=1024)


class Diagnosis(Advice):
    cause: Literal["candidate", "resource", "timeout", "workload", "unknown"]
    explanation: str = Field(min_length=1, max_length=1024)
    suggestion: Proposal | None


class Report(Advice):
    summary: str = Field(min_length=1, max_length=4096)


SCHEMAS: dict[Role, type[Advice]] = {
    "proposer": Proposal,
    "diagnoser": Diagnosis,
    "reporter": Report,
}
INSTRUCTIONS = {
    "proposer": "Propose one new recipe from the supplied search space and explain why.",
    "diagnoser": "Classify the failed attempt. Suggest a new recipe from the search space, "
    "or null when a recipe change cannot address the failure.",
    "reporter": "Summarize the supplied hourly or post-campaign evidence. Preserve units and "
    "distinguish recorded selection, individual measurements and budget estimates.",
}


def _recipe(proposal: Proposal, space: dict[str, Any]) -> None:
    # JSON equality distinguishes false from 0 and handles structured parameter choices.
    def encode(value: object) -> str:
        return json.dumps(value, allow_nan=False, sort_keys=True)

    if (
        not space
        or set(proposal.params) != set(space)
        or any(
            encode(value) not in {encode(choice) for choice in space[name]}
            for name, value in proposal.params.items()
        )
    ):
        raise ValueError("Proposed recipe is outside the declared campaign search space")


def _fake(role: Role, context: dict[str, Any], seed: int) -> str:
    if role == "reporter":
        return json.dumps({"summary": f"{context['campaign']}: {context['purpose']}."})
    if role == "diagnoser":
        return json.dumps(
            {"cause": "unknown", "explanation": "Review the recorded failure.", "suggestion": None}
        )
    params = {}
    for name, choices in sorted(context["search_space"].items()):
        seed, index = divmod(seed, len(choices))
        params[name] = choices[index]
    return json.dumps({"params": params, "rationale": "Exercise a declared recipe."})


async def _reply(job: Path) -> None:
    from research_swarm.swarm.operators.llm import Ollama

    request = json.load(sys.stdin)
    role, data, seed = request["role"], request["context"], request["seed"]
    model = request["model"]

    async def heartbeat() -> None:
        while True:
            os.utime(job, None)
            await asyncio.sleep(1)

    pulse = asyncio.create_task(heartbeat())
    try:
        if model is None:
            raw = _fake(role, data, seed)
        else:
            prompt = json.dumps({"task": INSTRUCTIONS[role], "campaign_data": data})
            raw = await Ollama(model)(prompt, SCHEMAS[role].model_json_schema(), seed)
        advice = SCHEMAS[role].model_validate_json(raw)
        recipe = (
            advice
            if isinstance(advice, Proposal)
            else (advice.suggestion if isinstance(advice, Diagnosis) else None)
        )
        if recipe is not None:
            _recipe(recipe, data["search_space"])
        print(advice.model_dump_json())
    finally:
        pulse.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pulse


async def ask_agent(
    role: Role,
    context: dict[str, Any],
    *,
    model: str | None,
    timeout: float,
    jobs: Path,
    seed: int = 0,
) -> dict[str, Any]:
    """Return typed advice or an explicit failure. A null model selects the deterministic fake."""
    if role not in SCHEMAS or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Choose an advisory role and a positive finite timeout")
    if model is not None and not model.strip():
        raise ValueError("Model name must be nonempty")
    payload = json.dumps(
        dict(role=role, context=context, seed=seed, model=model), allow_nan=False
    ).encode()
    jobs.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=role + "-", dir=jobs) as temporary:
        heartbeat = Path(temporary) / "heartbeat.json"
        deadline = time.monotonic() + timeout
        job_metadata = json.dumps({"role": role, "deadline": time.time() + timeout})
        heartbeat.with_name("job.json").write_text(job_metadata)
        heartbeat.write_text(job_metadata)
        guard = [
            sys.executable,
            "-m",
            "research_swarm.swarm.agent_guard",
            str(heartbeat),
            str(deadline),
        ]
        try:
            async with guard_session(guard) as process:
                try:
                    async with asyncio.timeout(deadline - time.monotonic()):
                        stdout, stderr = await process.communicate(payload)
                    if process.returncode == -signal.SIGALRM:
                        raise TimeoutError
                except TimeoutError:
                    # Commit the terminal record before cancellation closes the owned group.
                    record_outcome(heartbeat, "timed_out")
                    raise
        except TimeoutError:
            return {"role": role, "status": "timed_out", "timeout_seconds": timeout}
        status = "ok" if process.returncode == 0 else "failed"
        if process.returncode == -signal.SIGKILL:
            outcome = heartbeat.with_name("outcome.json")
            if outcome.is_file():
                status = json.loads(outcome.read_text())["status"]
        if status == "timed_out":
            return {"role": role, "status": status, "timeout_seconds": timeout}
        if status != "ok":
            return {
                "role": role,
                "status": "failed",
                "error": stderr.decode(errors="replace").strip()[:512] or "Provider exited",
            }
        return {"role": role, "status": "ok", "advice": json.loads(stdout)}


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
    from research_swarm.swarm.interfaces import CampaignSpec, Result
    from research_swarm.swarm.population import Population
    from research_swarm.swarm.report import _read, capture
    from research_swarm.swarm.store import _ATTEMPT, _attempt

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
    return await _advise(database, args, ask_agent)


async def _advise(
    database: Path,
    args: argparse.Namespace,
    provider: Callable[..., Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    context = _inputs(database, args)
    jobs = database.with_name(database.name + ".agent-jobs")
    replies = await asyncio.gather(
        *(
            provider(
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


if __name__ == "__main__":
    try:
        # The caller may be killed while the provider is blocked inside model code.
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGALRM})
        signal.setitimer(signal.ITIMER_REAL, max(1e-6, float(sys.argv[2]) - time.monotonic()))
        asyncio.run(_reply(Path(sys.argv[1])))
    except Exception as exc:
        # Provider output may contain task data. Report the error class without echoing it.
        print(f"Agent response failed: {type(exc).__name__}", file=sys.stderr)
        sys.exit(1)
