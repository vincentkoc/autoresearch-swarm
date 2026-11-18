"""A local Ollama proposer for the program and training-code benchmarks."""

from __future__ import annotations

import json
import random
from collections.abc import Sequence

import httpx

from benchmarks.suite import Candidate, Task, Trial, incumbent


class OllamaPolicy:
    def __init__(self, model: str, port: int = 11434, timeout: float = 30.0):
        self.model = model
        self.port = port
        self.timeout = timeout
        self.input_tokens = 0
        self.output_tokens = 0

    def __call__(self, task: Task, history: Sequence[Trial], rng: random.Random) -> Candidate:
        # Give the model an edit to make even when its preferred output is the parent.
        choices = task.space.copy()
        field = rng.choice(list(choices))
        parent = incumbent(task, history)
        choices[field] = tuple(value for value in choices[field] if value != parent[field])
        schema = {
            "type": "object",
            "properties": {k: {"type": "string", "enum": v} for k, v in choices.items()},
            "required": list(task.space),
            "additionalProperties": False,
        }
        prompt = {
            "task": task.objective,
            "metric": task.metric,
            "direction": task.direction,
            "choices": choices,
            "previous_search_trials": [
                {"candidate": t.candidate, "score": t.score, "error": t.error} for t in history
            ],
            "instruction": "Propose one improved candidate. Return exactly the JSON fields.",
        }
        with httpx.Client(trust_env=False, timeout=self.timeout) as client:
            response = client.post(
                f"http://127.0.0.1:{self.port}/api/generate",
                json={
                    "model": self.model,
                    "prompt": json.dumps(prompt),
                    "format": schema,
                    "stream": False,
                    "options": {"seed": rng.randrange(2**31), "num_predict": 256},
                },
            )
            response.raise_for_status()
            body = response.json()
        self.input_tokens += body.get("prompt_eval_count", 0)
        self.output_tokens += body.get("eval_count", 0)
        candidate = json.loads(body["response"])
        if not isinstance(candidate, dict) or not all(
            isinstance(v, str) for v in candidate.values()
        ):
            raise ValueError("model returned an invalid candidate object")
        if candidate.get(field) not in choices[field]:
            raise ValueError(f"model did not edit {field}")
        return {k: str(v) for k, v in candidate.items()}
