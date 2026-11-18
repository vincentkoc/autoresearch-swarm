"""Factories for the swarm run command's proposer and selection_policy hooks."""

import random
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from research_swarm.swarm.interfaces import CampaignSpec, Operator
from research_swarm.swarm.operators import (
    CodeDiff,
    Crossover,
    EnsembleAdd,
    NewSeed,
    Ollama,
    OptunaSampler,
    ParamMutation,
    Range,
    TextRewrite,
)
from research_swarm.swarm.operators.choices import ChoiceRewrite
from research_swarm.swarm.operators.factorial import FiniteMutation
from research_swarm.swarm.operators.params import Scalar
from research_swarm.swarm.operators.portfolio import ReplicatedSelection, SearchPortfolio


class SearchOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    space: dict[str, Range | list[Scalar]] = Field(default_factory=dict)
    operators: list[
        Literal[
            "param-mutate",
            "optuna",
            "crossover",
            "ensemble-add",
            "new-seed",
            "llm-text-rewrite",
            "llm-code-diff",
            "llm-choice-rewrite",
        ]
    ] = ["param-mutate", "crossover"]
    fidelities: list[float] | None = None
    reduction: int = Field(default=3, ge=2)
    family_cap: int = Field(default=2, ge=1)
    exploration: float = Field(default=0.5, ge=0)
    min_replicates: int = Field(default=2, ge=2)
    seed_std: float = Field(default=0.0, ge=0)
    threshold: float = Field(default=1.96, ge=0)
    replicate: bool = True
    model: str | None = None
    port: int = Field(default=11434, gt=0, le=65535)
    objective: str = ""
    path: str | None = None
    source: str | None = None
    revision: str | None = None

    def selection(self, spec: CampaignSpec) -> ReplicatedSelection:
        return ReplicatedSelection(
            spec,
            min_replicates=self.min_replicates,
            seed_std=self.seed_std,
            threshold=self.threshold,
        )

    def build(self, spec: CampaignSpec) -> SearchPortfolio:
        rng = random.Random(spec.seed)
        finite: dict[str, tuple[str, ...]] = {}
        for key, domain in self.space.items():
            if isinstance(domain, list) and all(isinstance(value, str) for value in domain):
                finite[key] = tuple(str(value) for value in domain)
        operators: dict[str, Operator] = {}
        for name in self.operators:
            if name == "param-mutate":
                operators[name] = (
                    FiniteMutation(spec, finite)
                    if finite and finite.keys() == self.space.keys()
                    else ParamMutation(self.space, rng)
                )
            elif name == "optuna":
                operators[name] = OptunaSampler(self.space)
            elif name == "crossover":
                operators[name] = Crossover(rng)
            elif name == "ensemble-add":
                operators[name] = EnsembleAdd()
            elif name == "new-seed":
                operators[name] = NewSeed(rng)
            else:
                if not self.model or not self.objective:
                    raise ValueError("model operators need a local model and objective")
                llm = Ollama(self.model, port=self.port)
                if name == "llm-code-diff":
                    if self.source is None or not self.path or not self.revision:
                        raise ValueError("code-diff requires source, path and revision")
                    operators[name] = CodeDiff(
                        llm,
                        self.objective,
                        path=self.path,
                        source=self.source,
                        revision=self.revision,
                    )
                elif name == "llm-choice-rewrite":
                    if not finite or finite.keys() != self.space.keys():
                        raise ValueError("choice rewrite requires string-valued domains")
                    operators[name] = ChoiceRewrite(llm, self.objective, finite)
                else:
                    operators[name] = TextRewrite(llm, self.objective)
        return SearchPortfolio(
            spec,
            operators,
            fidelities=self.fidelities,
            reduction=self.reduction,
            exploration=self.exploration,
            family_cap=self.family_cap,
            selection=self.selection(spec) if self.replicate else None,
        )


def _options(spec: CampaignSpec) -> SearchOptions:
    options = dict(spec.options.get("search", {}))
    options.setdefault("space", spec.search_space)
    options.setdefault("min_replicates", max(2, spec.min_replicates))
    return SearchOptions.model_validate(options)


def from_spec(spec: CampaignSpec) -> SearchPortfolio:
    return _options(spec).build(spec)


def selection_from_spec(spec: CampaignSpec) -> ReplicatedSelection:
    return _options(spec).selection(spec)
