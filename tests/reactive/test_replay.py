from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_swarm.domain.types import new_id
from research_swarm.projects.workload import WorkloadContext
from research_swarm.reactive.journal import DecisionJournal
from research_swarm.reactive.replay import ReplayHook
from research_swarm.reactive.transcript import ControlTranscript
from tests.reactive.support import receipt_for, request_for, response_for
from tests.reactive.test_hook import Controls


@pytest.fixture
def transcript(journal: DecisionJournal) -> ControlTranscript:
    with journal.trainer() as trainer:
        for observed, applied in ((10, 14), (20, 22)):
            request = request_for(journal, observed)
            trainer.request(request)
            journal.respond(response_for(request))
            intent = trainer.claim_application(request.id, step=applied)
            trainer.record_application(receipt_for(intent))
        trainer.request(request_for(journal, 30))
    return ControlTranscript.capture(journal)


def replay(tmp_path: Path, transcript: ControlTranscript, target: Controls) -> ReplayHook:
    return ReplayHook(
        transcript,
        transcript.session.contract,
        transcript.session.binding.model_copy(update={"attempt_id": new_id()}),
        target,
        tmp_path / "replay.jsonl",
    )


def test_replay_uses_actual_applications_and_separate_provenance(
    tmp_path: Path, transcript: ControlTranscript
) -> None:
    target = Controls()
    hook = replay(tmp_path, transcript, target)
    assert hook.boundary(10) == () and target.value == 0.1
    assert hook.boundary(13) == ()
    applied = hook.boundary(14)
    assert applied[0].outcome == "applied" and target.value == 0.2
    kept = hook.boundary(22)
    assert kept[0].outcome == "kept" and target.writes == 1
    assert hook.boundary(40) == ()
    summary = hook.close()
    assert summary.status == "complete" and summary.completed_applications == 2
    assert summary.mode == "control_schedule_replay"
    assert summary.source_transcript_digest == transcript.digest
    assert summary.binding != transcript.session.binding
    events = [json.loads(line) for line in (tmp_path / "replay.jsonl").read_text().splitlines()]
    assert [event["kind"] for event in events] == [
        "started",
        "intent",
        "receipt",
        "intent",
        "receipt",
        "closed",
    ]
    assert events[1]["record"]["applied_step"] == 14
    assert events[2]["record"]["source_receipt_digest"] == transcript.applications()[0].digest
    with pytest.raises(ValueError, match="terminal"):
        hook.boundary(41)
    assert hook.close() == summary


@pytest.mark.parametrize(
    "tamper", ["missing", "sequence", "before", "step", "identity", "uncertain", "pending", "final"]
)
def test_transcript_rejects_incomplete_or_inconsistent_evidence(
    transcript: ControlTranscript, tamper: str
) -> None:
    data = transcript.model_dump(mode="json")
    first = data["decisions"][0]
    if tamper == "missing":
        data["decisions"].pop()
    elif tamper == "sequence":
        first["request"]["sequence"] = 2
    elif tamper == "before":
        first["receipt"]["before"][0]["value"] = 0.5
    elif tamper == "step":
        first["receipt"]["applied_step"] = 15
    elif tamper == "identity":
        first["receipt"]["request_id"] = new_id()
    elif tamper == "uncertain":
        first["status"] = first["receipt"]["outcome"] = "uncertain"
        first["receipt"]["after"] = first["receipt"]["new_revision"] = None
    elif tamper == "pending":
        data["decisions"][-1]["status"] = "pending"
    else:
        data["session"]["controls"][0]["value"] = 0.3
    with pytest.raises(ValueError):
        ControlTranscript.model_validate(data)


def test_transcript_requires_closed_settled_session(journal: DecisionJournal) -> None:
    with pytest.raises(ValueError, match="closed session"):
        ControlTranscript.capture(journal)
    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        journal.respond(response_for(request))
        trainer.claim_application(request.id, step=11)
    with pytest.raises(ValueError, match="closed session"):
        ControlTranscript.capture(journal)


def test_duplicate_request_identity_is_rejected(transcript: ControlTranscript) -> None:
    data = transcript.model_dump(mode="json")
    # The last cancelled request has no proposal or receipt digest to invalidate.
    data["decisions"][-1]["request"]["id"] = data["decisions"][0]["request"]["id"]
    with pytest.raises(ValueError, match="IDs must be unique"):
        ControlTranscript.model_validate(data)


def test_last_observation_must_match_terminal_state(transcript: ControlTranscript) -> None:
    data = transcript.model_dump(mode="json")
    data["session"]["last_observed_step"] += 1
    with pytest.raises(ValueError, match="final observation"):
        ControlTranscript.model_validate(data)


def test_transcript_rejects_duplicate_receipt_json_keys(
    tmp_path: Path, transcript: ControlTranscript
) -> None:
    payload = transcript.model_dump_json().replace(
        '"applied_step":14', '"applied_step":99,"applied_step":14', 1
    )
    path = tmp_path / "ambiguous.json"
    path.write_text(payload)
    with pytest.raises(ValueError, match="Duplicate"):
        ControlTranscript.read(path)


def test_transcript_roundtrip_and_bounded_input(
    tmp_path: Path, transcript: ControlTranscript, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "transcript.json"
    path.write_text(transcript.model_dump_json())
    assert ControlTranscript.read(path).digest == transcript.digest
    monkeypatch.setattr("research_swarm.reactive.transcript.MAX_TRANSCRIPT_BYTES", 100)
    with pytest.raises(ValueError, match="byte limit"):
        ControlTranscript.read(path)


def test_replay_requires_same_contract_initials_and_new_binding(
    tmp_path: Path, transcript: ControlTranscript
) -> None:
    target = Controls(value=0.9)
    with pytest.raises(ValueError, match="initial controls"):
        replay(tmp_path, transcript, target)
    target.value = 0.1
    with pytest.raises(ValueError, match="distinct run"):
        ReplayHook(
            transcript,
            transcript.session.contract,
            transcript.session.binding,
            target,
            tmp_path / "trace",
        )
    changed = transcript.session.contract.model_copy(update={"max_decisions": 50})
    with pytest.raises(ValueError, match="contract differs"):
        ReplayHook(
            transcript,
            changed,
            transcript.session.binding.model_copy(update={"attempt_id": new_id()}),
            target,
            tmp_path / "trace",
        )
    assert not (tmp_path / "trace").exists()


def test_context_binding_and_incomplete_close(
    tmp_path: Path, transcript: ControlTranscript
) -> None:
    original = transcript.session.binding
    context = WorkloadContext(
        trial_id=original.trial_id,
        attempt_id=new_id(),
        stage_id="train",
        spec_digest=original.spec_digest,
        seed=1,
        time_budget_ms=100,
        output_root=str(tmp_path),
        report_path=str(tmp_path / "report.json"),
        parameters={"reactive": transcript.session.contract.model_dump(mode="json")},
    ).sealed()
    hook = ReplayHook.from_context(context, Controls(), transcript)
    assert hook.binding.context_digest == context.context_digest
    assert hook.close().status == "incomplete"
    with pytest.raises(FileExistsError):
        ReplayHook.from_context(context, Controls(), transcript)


def test_skipped_boundary_is_terminal(tmp_path: Path, transcript: ControlTranscript) -> None:
    hook = replay(tmp_path, transcript, Controls())
    with pytest.raises(ValueError, match="skipped"):
        hook.boundary(15)
    with pytest.raises(ValueError, match="terminal"):
        hook.boundary(16)
    assert hook.close().status == "incomplete"


def test_missing_close_receipt_does_not_establish_completion(
    tmp_path: Path, transcript: ControlTranscript, monkeypatch: pytest.MonkeyPatch
) -> None:
    hook = replay(tmp_path, transcript, Controls())
    hook.boundary(14)
    hook.boundary(22)

    def fail(kind: str, record: object) -> None:
        raise OSError("fixture disk full")

    monkeypatch.setattr(hook, "_write", fail)
    with pytest.raises(OSError):
        hook.close()
    assert hook.close().status == "incomplete"


@pytest.mark.parametrize("failure", ["throw", "mismatch"])
def test_failed_mutation_stays_uncertain_without_rollback(
    tmp_path: Path, transcript: ControlTranscript, failure: str
) -> None:
    target = Controls(failure=failure)
    hook = replay(tmp_path, transcript, target)
    receipt = hook.boundary(14)[0]
    assert receipt.outcome == "uncertain" and receipt.after is None
    assert target.value != 0.1 and target.writes == 1
    assert hook.close().status == "uncertain"


@pytest.mark.parametrize("fail_kind", ["intent", "receipt"])
def test_trace_failure_never_retries_mutation(
    tmp_path: Path, transcript: ControlTranscript, monkeypatch: pytest.MonkeyPatch, fail_kind: str
) -> None:
    target = Controls()
    hook = replay(tmp_path, transcript, target)
    original = hook._write

    def fail(kind: str, record: object) -> None:
        if kind == fail_kind:
            raise OSError("fixture disk full")
        original(kind, record)

    monkeypatch.setattr(hook, "_write", fail)
    with pytest.raises(OSError):
        hook.boundary(14)
    assert target.writes == (0 if fail_kind == "intent" else 1)
    with pytest.raises(ValueError, match="terminal"):
        hook.boundary(15)
    assert hook.close().status == ("incomplete" if fail_kind == "intent" else "uncertain")
