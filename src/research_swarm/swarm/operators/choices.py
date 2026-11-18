"""Schema-constrained model rewrite for artifacts represented by named choices."""

import json
from collections.abc import Mapping, Sequence

from research_swarm.swarm.interfaces import Candidate
from research_swarm.swarm.operators.llm import Generate, TextRewrite
from research_swarm.swarm.operators.params import child


class ChoiceRewrite(TextRewrite):
    """Generate a complete structured recipe from the workload's declared grammar."""

    name = "llm-choice-rewrite"

    def __init__(
        self,
        generate: Generate,
        objective: str,
        space: Mapping[str, Sequence[str]],
        *,
        concurrency: int = 4,
    ) -> None:
        super().__init__(generate, objective, concurrency=concurrency)
        self.space = {key: tuple(values) for key, values in space.items()}
        if not self.space or any(not values for values in self.space.values()):
            raise ValueError("empty artifact grammar")
        self.schema = {
            "type": "object",
            "properties": {
                key: {"type": "string", "enum": list(values)} for key, values in self.space.items()
            },
            "required": list(self.space),
            "additionalProperties": False,
        }
        self.instruction = (
            "Choose one allowed value for each field to meet the objective. "
            "Use previous measurements and propose an improved, untried recipe. "
            "Allowed choices: " + json.dumps(self.space)
        )

    def _edit(self, parent: Candidate, raw: str) -> Candidate | None:
        params = json.loads(raw)
        if (
            not isinstance(params, dict)
            or params.keys() != self.space.keys()
            or any(params[key] not in values for key, values in self.space.items())
        ):
            raise ValueError("model returned a recipe outside the artifact grammar")
        if params == parent.params:
            return None
        return child(parent, self.name, params=params)
