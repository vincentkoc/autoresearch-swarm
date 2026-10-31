"""Resolve immutable stage inputs identically for admission and advisory placement."""

from research_swarm.control.models import JobData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Transaction


def resolved_job_inputs(transaction: Transaction, job: JobData) -> dict[str, ArtifactRef]:
    for predecessor_id in job.predecessor_ids:
        if transaction.get("job", predecessor_id)["state"] != "succeeded":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Stage dependency is incomplete")
    inputs = dict(job.inputs)
    for name, predecessor_id in job.input_sources.items():
        parent = JobData.model_validate(transaction.get("job", predecessor_id)["data"])
        if name not in parent.outputs:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Predecessor output is unavailable")
        inputs[name] = parent.outputs[name]
    for reference in inputs.values():
        ArtifactCatalog.available(transaction, reference)
    return inputs
