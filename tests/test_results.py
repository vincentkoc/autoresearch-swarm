import pytest

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.evidence import ArtifactRef, RunResult
from research_swarm.domain.types import new_id, utc_now


def result() -> RunResult:
    digest = content_digest(b"synthetic")
    return RunResult(
        campaign_id=new_id(),
        trial_id=new_id(),
        job_id=new_id(),
        attempt_id=new_id(),
        worker_id=new_id(),
        worker_session_id=new_id(),
        spec_digest=digest,
        control_epoch=1,
        fencing_generation=1,
        status="failed",
        exit_code=1,
        started_at=utc_now(),
        finished_at=utc_now(),
        elapsed_ms=1,
        process_closure_ref=ArtifactRef(id=new_id(), digest=digest, size_bytes=1),
        execution_fingerprint=digest,
    )


def test_seal_rejects_copied_success_with_nonzero_exit() -> None:
    invalid = result().model_copy(update={"status": "succeeded"})
    with pytest.raises(ValueError, match="exit code zero"):
        invalid.sealed()


def test_result_digest_covers_spec_reference() -> None:
    sealed = result().sealed()
    sealed.verify()
    changed = sealed.model_copy(update={"spec_digest": content_digest(b"other")})
    with pytest.raises(ValueError, match="digest mismatch"):
        changed.verify()


@pytest.mark.parametrize("timestamp", ["yesterday", "2026-02-30T00:00:00.000Z", "2026-01-01"])
def test_result_rejects_ambiguous_or_invalid_timestamps(timestamp: str) -> None:
    with pytest.raises(ValueError):
        result().model_copy(update={"started_at": timestamp}).sealed()
