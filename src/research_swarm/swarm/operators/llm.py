"""Bounded local model proposals for text and single-file source edits."""

from __future__ import annotations

import asyncio
import difflib
import json
import math
import threading
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import Future
from dataclasses import asdict, replace
from pathlib import PurePosixPath
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

from research_swarm.swarm.interfaces import CampaignSpec, Candidate, Result
from research_swarm.swarm.operators.params import child


class ArtifactEdit(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    content: str = Field(min_length=1, max_length=65536)
    rationale: str = Field(min_length=1, max_length=1024)


class SourceEdit(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    before: str = Field(min_length=1, max_length=65536)
    after: str = Field(max_length=65536)
    rationale: str = Field(min_length=1, max_length=1024)


class FailureDiagnosis(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    cause: Literal["workload", "environment", "unknown"]
    reason: str = Field(min_length=1, max_length=1024)


Generate = Callable[[str, dict[str, Any], int], Awaitable[str]]


class Ollama:
    def __init__(self, model: str, *, port: int = 11434) -> None:
        self.model = model
        self.url = f"http://127.0.0.1:{port}/api/generate"

    async def __call__(self, prompt: str, schema: dict[str, Any], seed: int) -> str:
        async with httpx.AsyncClient(trust_env=False, timeout=None) as client:
            response = await client.post(
                self.url,
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "format": schema,
                    "stream": False,
                    "options": {"seed": seed, "num_predict": 2048},
                },
            )
            response.raise_for_status()
            content = response.json()["response"]
            if not isinstance(content, str):
                raise ValueError("expected a JSON string from Ollama")
            return content


class TextRewrite:
    name = "llm-text-rewrite"
    schema = ArtifactEdit.model_json_schema()
    instruction = "Put the complete rewritten text in content and a brief explanation in rationale."

    def __init__(self, generate: Generate, objective: str, *, concurrency: int = 4) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        self.generate = generate
        self.objective = objective
        self.concurrency = concurrency
        self._slots = threading.BoundedSemaphore(concurrency)
        self._sequence = 0
        self._recent: dict[str, deque[dict[str, Any]]] = {}
        self._diagnoses: dict[str, deque[tuple[str, dict[str, str]]]] = {}
        self.errors = 0
        self.timeouts = 0

    def tell(self, result: Result) -> None:
        trial = result.attempt.trial
        recent = self._recent.setdefault(trial.campaign_id, deque(maxlen=8))
        recent.append(
            {
                "candidate": trial.candidate.candidate_id,
                "params": trial.candidate.params,
                "parents": trial.candidate.parents,
                "operator": trial.candidate.operator,
                "metrics": result.metrics,
                "cost": result.cost,
                "status": result.status,
                "error": result.error,
            }
        )

    def _source(self, parent: Candidate) -> dict[str, str | None]:
        return {"original_text": parent.text}

    def _edit(self, parent: Candidate, raw: str) -> Candidate | None:
        content = ArtifactEdit.model_validate_json(raw).content
        if content == parent.text:
            return None
        return child(parent, self.name, text=content, diff=None)

    async def _one(
        self, spec: CampaignSpec, parent: Candidate, sequence: int, failure: Result | None
    ) -> Candidate | None:
        context: dict[str, Any] = {
            "objective": self.objective,
            "metric": spec.metric,
            "direction": spec.direction,
            "operator": self.name,
            "recipe": asdict(parent),
            "source": self._source(parent),
            "recent_results": list(self._recent.get(spec.campaign_id, ())),
            "instruction": self.instruction,
        }
        diagnoses = tuple(self._diagnoses.get(spec.campaign_id, ()))
        if diagnoses:
            context["recent_diagnoses"] = [record for _, record in diagnoses]
        if failure is not None:
            context["failure"] = {
                "attempt_number": failure.attempt.number,
                "error": failure.error,
                "output_tail": [entry[-1024:] for entry in failure.logs[-2:]],
                "seed": failure.attempt.trial.seed,
                "fidelity": failure.attempt.trial.fidelity,
            }
            diagnostic_prompt = {
                "instruction": (
                    "Classify the observed failure and explain the evidence. A defect in the "
                    "candidate source, artifact or parameters is workload. An unavailable "
                    "worker or runtime is environment. Use unknown if the evidence is inconclusive."
                ),
                "recipe": context["recipe"],
                "source": context["source"],
                "failure": context["failure"],
            }
            attempt_id = failure.attempt.attempt_id
            record = next((note for key, note in reversed(diagnoses) if key == attempt_id), None)
            if record is None:
                diagnosis = FailureDiagnosis.model_validate_json(
                    await self.generate(
                        json.dumps(diagnostic_prompt),
                        FailureDiagnosis.model_json_schema(),
                        spec.seed + sequence,
                    )
                )
                record = {
                    "candidate": parent.candidate_id,
                    "cause": diagnosis.cause,
                    "reason": diagnosis.reason,
                }
                history = self._diagnoses.setdefault(spec.campaign_id, deque(maxlen=8))
                history.append((attempt_id, record))
            context["diagnosis"] = record
            if record["cause"] != "workload":
                return None
        raw = await self.generate(json.dumps(context), self.schema, spec.seed + sequence)
        return self._edit(parent, raw)

    def _start(
        self,
        spec: CampaignSpec,
        parent: Candidate,
        sequence: int,
        timeout: float,
        failure: Result | None,
    ) -> asyncio.Future[Candidate | None]:
        result: Future[Candidate | None] = Future()
        result.set_running_or_notify_cancel()

        async def request() -> Candidate | None:
            async with asyncio.timeout(timeout):
                return await self._one(spec, parent, sequence, failure)

        def run() -> None:
            try:
                candidate = asyncio.run(request())
            except BaseException as exc:
                self._slots.release()
                result.set_exception(exc)
            else:
                self._slots.release()
                result.set_result(candidate)

        # A provider may block inside its async API or suppress cancellation.
        # Its thread holds a slot until it exits and cannot delay CLI shutdown.
        try:
            threading.Thread(target=run, name=self.name, daemon=True).start()
        except RuntimeError:
            self._slots.release()
            raise
        wrapped = asyncio.wrap_future(result)
        wrapped.add_done_callback(lambda f: None if f.cancelled() else f.exception())
        return wrapped

    async def propose(
        self,
        spec: CampaignSpec,
        parents: Sequence[Candidate],
        k: int,
        *,
        failure: Result | None = None,
    ) -> list[Candidate]:
        if failure is not None:
            clock = asyncio.get_running_loop()
            end = clock.time() + spec.proposal_timeout_seconds
            repairs = await self.diagnose(spec, failure, k)
            remaining = end - clock.time()
            if repairs or not parents or remaining <= 0:
                return repairs
            # Abstaining from a diagnosis leaves ordinary recipe exploration available.
            spec = replace(spec, proposal_timeout_seconds=remaining)
        return await self._batch(spec, parents, k, None)

    async def diagnose(self, spec: CampaignSpec, failure: Result, k: int) -> list[Candidate]:
        attempt = failure.attempt
        if (
            attempt.trial.campaign_id != spec.campaign_id
            or failure.status != "failed"
            or attempt.number < spec.max_attempts
        ):
            return []
        return await self._batch(spec, [attempt.trial.candidate], k, failure)

    async def _batch(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int, failure: Result | None
    ) -> list[Candidate]:
        timeout = spec.proposal_timeout_seconds
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("proposal timeout must be finite and positive")
        if k <= 0 or not parents:
            return []
        end = asyncio.get_running_loop().time() + timeout
        # Reserve this batch before starting providers. Otherwise a fast
        # sibling can free its slot while the same batch is still being built.
        reserved = 0
        for _ in range(min(k, self.concurrency)):
            if not self._slots.acquire(blocking=False):
                break
            reserved += 1
        jobs = []
        unstarted = reserved
        try:
            for i in range(reserved):
                unstarted -= 1  # _start owns this slot, including thread-start failure
                self._sequence += 1
                jobs.append(
                    self._start(spec, parents[i % len(parents)], self._sequence, timeout, failure)
                )
        finally:
            for _ in range(unstarted):
                self._slots.release()
        if not jobs:
            return []
        done, pending = await asyncio.wait(
            jobs, timeout=max(0, end - asyncio.get_running_loop().time())
        )
        self.timeouts += len(pending)
        batch = []
        for job in jobs:
            if job not in done:
                continue
            error = job.exception()
            if isinstance(error, TimeoutError):
                self.timeouts += 1
            elif error is not None:
                self.errors += 1
            elif (candidate := job.result()) is not None:
                batch.append(candidate)
        return batch


class CodeDiff(TextRewrite):
    name = "llm-code-diff"
    schema = SourceEdit.model_json_schema()
    instruction = (
        "Copy one exact, unique substring of base_file into before. "
        "Put its replacement in after, preserving indentation. "
        "This substitution must produce the complete desired edit against the base."
    )

    def __init__(
        self,
        generate: Generate,
        objective: str,
        *,
        path: str,
        source: str,
        revision: str,
        concurrency: int = 4,
    ) -> None:
        super().__init__(generate, objective, concurrency=concurrency)
        target = PurePosixPath(path)
        if (
            not target.parts
            or target.is_absolute()
            or ".." in target.parts
            or any(c.isspace() or ord(c) < 32 for c in path)
            or not revision
        ):
            raise ValueError("expected a relative source path and base revision")
        self.path = path
        self.source = source
        self.revision = revision

    def _source(self, parent: Candidate) -> dict[str, str | None]:
        return {
            "path": self.path,
            "base_revision": self.revision,
            "base_file": self.source,
            "previous_diff": parent.diff,
        }

    def _edit(self, parent: Candidate, raw: str) -> Candidate | None:
        if parent.source_revision not in (None, self.revision):
            raise ValueError("code parent belongs to another base revision")
        edit = SourceEdit.model_validate_json(raw)
        if self.source.count(edit.before) != 1:
            raise ValueError("source edit must match exactly one location in the base file")
        content = self.source.replace(edit.before, edit.after, 1)
        if self.path.endswith(".py"):
            compile(content, self.path, "exec", dont_inherit=True)
        lines = difflib.unified_diff(
            self.source.splitlines(keepends=True),
            content.splitlines(keepends=True),
            fromfile=f"a/{self.path}",
            tofile=f"b/{self.path}",
        )
        diff = "".join(
            line if line.endswith("\n") else line + "\n\\ No newline at end of file\n"
            for line in lines
        )
        if not diff or diff == parent.diff:
            return None
        return child(parent, self.name, diff=diff, text=None, source_revision=self.revision)
