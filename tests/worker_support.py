from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.experiments import (
    DispatchEnvelope,
    ExperimentSpec,
    MetricDefinition,
    SourceRef,
    StageSpec,
)
from research_swarm.domain.types import new_id
from research_swarm.workers.authority import (
    DispatchVerifier,
    VerifiedDispatch,
    WorkerBinding,
    authorize_dispatch,
)
from research_swarm.workers.contracts import NativeCommand


def dispatch(
    tmp_path: Path, code: str = "print('workload')"
) -> tuple[VerifiedDispatch, NativeCommand]:
    digest = content_digest(b"synthetic")
    stage = StageSpec(id="train", kind="train", argv=(sys.executable, "-c", code), timeout_ms=4000)
    spec = ExperimentSpec(
        workspace_id=new_id(),
        project_id=new_id(),
        campaign_id=new_id(),
        candidate_id=new_id(),
        source=SourceRef(artifact_id=new_id(), tree_digest=digest),
        protocol_digest=digest,
        comparison_class="cpu-v1",
        environment_digest=digest,
        evaluator_contract_digest=digest,
        data_manifest_digest=digest,
        metrics=(MetricDefinition(name="loss", unit="mse", direction="minimize"),),
        stages=(stage,),
        seed=7,
        policy_version="test-v1",
    ).sealed()
    binding = WorkerBinding(
        workspace_id=spec.workspace_id,
        controller_id=new_id(),
        worker_id=new_id(),
        worker_session_id=new_id(),
        control_epoch=1,
    )
    expiry = (
        (datetime.now(UTC) + timedelta(minutes=5))
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    envelope = DispatchEnvelope(
        **binding.model_dump(exclude={"schema_version"}),
        campaign_id=spec.campaign_id,
        trial_id=spec.trial_id,
        job_id=new_id(),
        attempt_id=new_id(),
        spec_digest=spec.spec_digest,
        fencing_generation=1,
        reservation_ids=(new_id(),),
        stage_id=stage.id,
        expires_at=expiry,
        hard_deadline_at=expiry,
        launch_permit_digest=digest,
    )
    key = b"synthetic-test-authority-key-32-bytes"
    envelope = authorize_dispatch(envelope, key)
    verified = DispatchVerifier(binding, key).verify(envelope, spec)
    command = NativeCommand(argv=stage.argv, cwd=str(tmp_path), timeout_ms=4000, grace_ms=100)
    return verified, command
