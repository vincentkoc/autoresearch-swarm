"""Bounded local model proposals for text and single-file source edits."""

from __future__ import annotations

import asyncio
import difflib
import json
import math
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict
from pathlib import PurePosixPath
from typing import Any

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
        self._active: set[asyncio.Task[Candidate | None]] = set()
        self._sequence = 0
        self._recent: dict[str, deque[dict[str, Any]]] = {}
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

    async def _one(self, spec: CampaignSpec, parent: Candidate, sequence: int) -> Candidate | None:
        prompt = json.dumps(
            {
                "objective": self.objective,
                "metric": spec.metric,
                "direction": spec.direction,
                "operator": self.name,
                "recipe": asdict(parent),
                "source": self._source(parent),
                "recent_results": list(self._recent.get(spec.campaign_id, ())),
                "instruction": self.instruction,
            }
        )
        raw = await self.generate(prompt, self.schema, spec.seed + sequence)
        return self._edit(parent, raw)

    def _finished(self, task: asyncio.Task[Candidate | None]) -> None:
        self._active.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.errors += 1

    async def propose(
        self, spec: CampaignSpec, parents: Sequence[Candidate], k: int
    ) -> list[Candidate]:
        timeout = spec.proposal_timeout_seconds
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("proposal timeout must be finite and positive")
        slots = min(k, self.concurrency - len(self._active))
        if slots <= 0 or not parents:
            return []
        tasks = []
        for i in range(slots):
            self._sequence += 1
            job = asyncio.create_task(self._one(spec, parents[i % len(parents)], self._sequence))
            self._active.add(job)
            job.add_done_callback(self._finished)
            tasks.append(job)
        try:
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            self.timeouts += len(pending)
            return [
                candidate
                for task in tasks
                if task in done
                and not task.cancelled()
                and task.exception() is None
                and (candidate := task.result()) is not None
            ]
        finally:
            # Cancellation-resistant transports keep their slot until they actually exit.
            # Never wait for one to acknowledge cancellation on the dispatch path.
            for task in tasks:
                if not task.done():
                    task.cancel()


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
