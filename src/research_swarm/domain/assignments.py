"""Complete, signed assignment contracts for native workers."""

from typing import Annotated, Literal

from pydantic import StringConstraints, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.domain.types import ID, Digest, Name, Timestamp, VersionedRecord
from research_swarm.domain.workers import EngineCapabilities, WorkerBinding

PublicKeyHex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SignatureHex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{128}$")]


class AssignmentPayload(VersionedRecord):
    dispatch: DispatchEnvelope
    spec: ExperimentSpec
    profile: EngineCapabilities
    source_artifact: ArtifactRef
    source_tree_digest: Digest
    inputs: dict[Name, ArtifactRef]
    evaluation_request_id: ID | None = None
    network_policy: Literal["inherit", "denied"]
    offline_policy: Literal["stop_at_hard_deadline"] = "stop_at_hard_deadline"
    issued_at: Timestamp

    @model_validator(mode="after")
    def consistent(self) -> "AssignmentPayload":
        self.spec.verify()
        dispatch, spec = self.dispatch, self.spec
        if (
            dispatch.workspace_id != spec.workspace_id
            or dispatch.campaign_id != spec.campaign_id
            or dispatch.trial_id != spec.trial_id
            or dispatch.spec_digest != spec.spec_digest
            or self.source_artifact.id != spec.source.artifact_id
            or self.source_tree_digest != spec.source.tree_digest
            or not dispatch.reservation_ids
            or self.issued_at > min(dispatch.expires_at, dispatch.hard_deadline_at)
        ):
            raise ValueError("Assignment differs from its sealed experiment or grant")
        stage = next((stage for stage in spec.stages if stage.id == dispatch.stage_id), None)
        if stage is None or set(self.inputs) != set(stage.required_artifacts):
            raise ValueError("Assignment inputs differ from its stage")
        self.profile.require(stage.resources, self.network_policy)
        return self


class AssignmentAuthority(VersionedRecord):
    binding: WorkerBinding
    algorithm: Literal["ed25519-jcs-v1"] = "ed25519-jcs-v1"
    public_key: PublicKeyHex
    key_id: Digest

    @model_validator(mode="after")
    def identified(self) -> "AssignmentAuthority":
        if self.key_id != signing_key_id(self.public_key):
            raise ValueError("Signing key identity differs")
        return self


def signing_key_id(public_key: str) -> str:
    return record_digest(
        "research.assignment-public-key", {"algorithm": "ed25519", "public_key": public_key}
    )


class SignedAssignment(VersionedRecord):
    algorithm: Literal["ed25519-jcs-v1"] = "ed25519-jcs-v1"
    key_id: Digest
    payload: AssignmentPayload
    signature: SignatureHex
