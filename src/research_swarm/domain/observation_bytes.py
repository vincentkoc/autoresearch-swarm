"""Bounded observation decoding independent of orchestration and storage."""

from typing import Any

from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef

MAX_OBSERVATION_BYTES = 1024 * 1024


def decode_observation_bytes(reference: ArtifactRef, content: bytes) -> dict[str, Any]:
    """Decode one bounded observation, independently bound to its immutable reference."""
    if reference.size_bytes > MAX_OBSERVATION_BYTES or len(content) > MAX_OBSERVATION_BYTES:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Observation exceeds its size bound")
    if len(content) != reference.size_bytes or content_digest(content) != reference.digest:
        raise ResearchError(
            ErrorCode.ARTIFACT_UNVERIFIED, "Observation bytes differ from reference"
        )
    value = strict_json_loads(content)
    if not isinstance(value, dict):
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Observation must be an object")
    return dict(value)
