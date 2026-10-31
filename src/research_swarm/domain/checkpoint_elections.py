"""Hosted destination proofs wrap native recovery signatures without changing them."""

from pydantic import model_validator

from research_swarm.domain.assignments import SignatureHex
from research_swarm.domain.backup import SignedRestoreClaim, SignedRestoreRequest
from research_swarm.domain.canonical import MAX_SAFE_INTEGER, record_digest
from research_swarm.domain.checkpoint_images import LogicalImportIntent
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import Digest, VersionedRecord


class LogicalElectionRequest(VersionedRecord):
    intent: LogicalImportIntent
    source_authority: WitnessAuthorityScope
    source_deployment_digest: Digest
    signed_request: SignedRestoreRequest

    @property
    def digest(self) -> str:
        return record_digest("research.logical-election-request.v1", self.model_dump(mode="json"))

    @model_validator(mode="after")
    def bound(self) -> "LogicalElectionRequest":
        request, source, intent = self.signed_request.request, self.source_authority, self.intent
        if (
            (request.restore_id, request.manifest_digest, request.target_public_key)
            != (intent.restore_id, intent.manifest_digest, intent.target_public_key)
            or (request.workspace_id, request.controller_id, request.source_control_epoch)
            != (source.workspace_id, source.controller_id, source.control_epoch)
            or request.target_control_epoch != source.control_epoch + 1
            or request.target_control_epoch > MAX_SAFE_INTEGER
        ):
            raise ValueError("Logical election request bindings differ")
        return self


class SignedLogicalElectionRequest(VersionedRecord):
    request: LogicalElectionRequest
    signature: SignatureHex


class LogicalElection(VersionedRecord):
    signed_request: SignedLogicalElectionRequest
    signed_claim: SignedRestoreClaim

    @property
    def digest(self) -> str:
        return record_digest("research.logical-election.v1", self.model_dump(mode="json"))

    @model_validator(mode="after")
    def bound(self) -> "LogicalElection":
        request = self.signed_request.request.signed_request.request
        claim = self.signed_claim.claim
        if claim.request != request or claim.source_public_key == request.target_public_key:
            raise ValueError("Logical election claim bindings differ")
        return self


class SignedLogicalElection(VersionedRecord):
    election: LogicalElection
    signature: SignatureHex
