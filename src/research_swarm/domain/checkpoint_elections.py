"""Hosted destination proofs wrap native recovery signatures without changing them."""

from typing import Literal

from pydantic import model_validator

from research_swarm.domain.assignments import SignatureHex
from research_swarm.domain.backup import BackupCustodyPin, SignedRestoreClaim, SignedRestoreRequest
from research_swarm.domain.canonical import MAX_SAFE_INTEGER, record_digest
from research_swarm.domain.checkpoint_images import LogicalImportIntent
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import Digest, Name, VersionedRecord


class LogicalElectionAnchor(VersionedRecord):
    profile: Literal["original-authority-election-v1"] = "original-authority-election-v1"
    source_authority: WitnessAuthorityScope
    source_backend: Literal["cloudflare-do"] = "cloudflare-do"
    source_location: Name
    source_deployment_digest: Digest
    custody_pin: BackupCustodyPin

    @property
    def digest(self) -> str:
        return record_digest("research.logical-election-anchor.v1", self.model_dump(mode="json"))

    @model_validator(mode="after")
    def bound(self) -> "LogicalElectionAnchor":
        scope, pin = self.source_authority, self.custody_pin
        if (scope.workspace_id, scope.controller_id, scope.control_epoch) != (
            pin.workspace_id,
            pin.controller_id,
            pin.control_epoch,
        ):
            raise ValueError("Logical election anchor custody differs")
        return self


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

    @property
    def digest(self) -> str:
        return record_digest("research.logical-election-envelope.v1", self.model_dump(mode="json"))
