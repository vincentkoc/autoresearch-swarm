"""Authenticate dispatch payloads before they can enter the native worker journal."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.domain.types import ID, Revision, Timestamp, VersionedRecord, utc_now


class WorkerBinding(VersionedRecord):
    workspace_id: ID
    controller_id: ID
    worker_id: ID
    worker_session_id: ID
    control_epoch: Revision


@dataclass(frozen=True)
class VerifiedDispatch:
    """Internal verification product. Transports never deserialize this type."""

    envelope: DispatchEnvelope
    spec: ExperimentSpec


def _permit(envelope: DispatchEnvelope, key: bytes) -> str:
    if len(key) < 32:
        raise ValueError("Dispatch authorization requires a key with at least 256 bits")
    payload = canonical_bytes(
        {
            "domain": "research.dispatch-authorization",
            "algorithm": "hmac-sha256-v1",
            "payload": envelope.model_dump(mode="json", exclude={"launch_permit_digest"}),
        }
    )
    return "sha256:" + hmac.new(key, payload, hashlib.sha256).hexdigest()


def authorize_dispatch(envelope: DispatchEnvelope, key: bytes) -> DispatchEnvelope:
    validated = DispatchEnvelope.model_validate(envelope)
    return validated.model_copy(update={"launch_permit_digest": _permit(validated, key)})


class DispatchVerifier:
    def __init__(self, binding: WorkerBinding, key: bytes) -> None:
        if len(key) < 32:
            raise ValueError("Dispatch authorization requires a key with at least 256 bits")
        self.binding = WorkerBinding.model_validate(binding)
        self._key = key

    def verify(
        self,
        envelope: DispatchEnvelope,
        spec: ExperimentSpec,
        *,
        authority_now: Timestamp | None = None,
        allow_expired_observation: bool = False,
    ) -> VerifiedDispatch:
        envelope = DispatchEnvelope.model_validate(envelope)
        spec = ExperimentSpec.model_validate(spec)
        spec.verify()
        if not hmac.compare_digest(_permit(envelope, self._key), envelope.launch_permit_digest):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Dispatch authentication failed")
        for field in WorkerBinding.model_fields:
            if field != "schema_version" and getattr(envelope, field) != getattr(
                self.binding, field
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Dispatch does not match enrolled authority"
                )
        if (
            envelope.workspace_id != spec.workspace_id
            or envelope.campaign_id != spec.campaign_id
            or envelope.trial_id != spec.trial_id
            or envelope.spec_digest != spec.spec_digest
            or envelope.stage_id not in {stage.id for stage in spec.stages}
            or not envelope.reservation_ids
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Dispatch and experiment differ"
            )
        now = authority_now or utc_now()
        if (
            not allow_expired_observation
            and min(envelope.expires_at, envelope.hard_deadline_at) <= now
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Dispatch launch authority has expired")
        return VerifiedDispatch(envelope, spec)
