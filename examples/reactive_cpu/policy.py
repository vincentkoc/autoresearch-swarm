"""A declared deterministic callback with an intentionally delayed decision."""

import asyncio

from research_swarm.domain.canonical import record_digest
from research_swarm.reactive.contracts import (
    DecisionProposal,
    DecisionRequest,
    NumericValue,
    ReactiveContract,
)
from research_swarm.reactive.policies import CallbackPolicy

CONFIG = {"version": "reactive-cpu-v1", "release_step": 12, "learning_rate": 0.2}
POLICY_DIGEST = record_digest("example.reactive-cpu-policy", CONFIG)


def policy(gate: asyncio.Event) -> CallbackPolicy:
    async def decide(request: DecisionRequest, contract: ReactiveContract) -> DecisionProposal:
        await gate.wait()
        return DecisionProposal(
            request_id=request.id,
            request_digest=request.digest,
            base_revision=request.control_revision,
            kind="set",
            values=(NumericValue(name="learning_rate", value=CONFIG["learning_rate"]),),
            rationale="Demonstrate a bounded update while training continues",
        )

    return CallbackPolicy(POLICY_DIGEST, decide)
