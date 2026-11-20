"""Three advisory model roles, each supervised in a disposable local process."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from research_swarm.swarm.agent_guard import record_outcome

Role = Literal["proposer", "diagnoser", "reporter"]


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
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "research_swarm.swarm.agent_guard",
            str(heartbeat),
            str(deadline),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        completed = False
        try:
            async with asyncio.timeout(deadline - time.monotonic()):
                stdout, stderr = await process.communicate(payload)
            if process.returncode == -signal.SIGALRM:
                raise TimeoutError
            completed = True
            if process.returncode == -signal.SIGKILL:
                outcome = heartbeat.with_name("outcome.json")
                if outcome.is_file() and json.loads(outcome.read_text())["status"] == "timed_out":
                    return {"role": role, "status": "timed_out", "timeout_seconds": timeout}
            if process.returncode:
                return {
                    "role": role,
                    "status": "failed",
                    "error": stderr.decode(errors="replace").strip()[:512] or "Provider exited",
                }
            return {"role": role, "status": "ok", "advice": json.loads(stdout)}
        except TimeoutError:
            record_outcome(heartbeat, "timed_out")
            return {"role": role, "status": "timed_out", "timeout_seconds": timeout}
        finally:
            if not completed:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except PermissionError as error:
                    # Darwin may return EPERM while the guard's own SIGKILL is taking effect.
                    # An actual permission failure on a live group must still surface.
                    try:
                        exit_code = await asyncio.wait_for(process.wait(), timeout=1)
                    except TimeoutError:
                        raise error from None
                    if exit_code != -signal.SIGKILL:
                        raise
                await process.wait()


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
