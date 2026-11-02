from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_swarm.domain.types import new_id
from research_swarm.reactive.calls import fence_call, finish_call, start_call
from research_swarm.reactive.journal import DecisionJournal
from research_swarm.reactive.transcript import ControlTranscript
from tests.reactive.support import receipt_for, request_for, response_for


def settled(journal: DecisionJournal, outcome: str) -> ControlTranscript:
    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        call = start_call(journal, request.id, journal.inspect().contract.policy_digest)
        proposal = response_for(request)
        if outcome in {"duplicate", "conflict", "manual-failure"}:
            journal.respond(response_for(request, 0.3) if outcome == "conflict" else proposal)
            trainer.record_application(receipt_for(trainer.claim_application(request.id, step=11)))
        if outcome == "timeout-late":
            fence_call(journal, request.id, call.id, "timeout")
        if outcome in {"late", "timeout-late"}:
            trainer.close()
        if outcome in {"failed", "cancelled", "manual-failure"}:
            finish_call(
                journal,
                request.id,
                call.id,
                failure="cancelled" if outcome == "cancelled" else "exception",
            )
        else:
            finish_call(journal, request.id, call.id, proposal=proposal)
        if outcome == "accepted":
            trainer.record_application(receipt_for(trainer.claim_application(request.id, step=11)))
    return ControlTranscript.capture(journal)


@pytest.mark.parametrize(
    "outcome",
    [
        "accepted",
        "late",
        "timeout-late",
        "duplicate",
        "conflict",
        "failed",
        "cancelled",
        "manual-failure",
    ],
)
def test_real_settled_calls_preserve_manual_and_late_evidence(
    journal: DecisionJournal, tmp_path: Path, outcome: str
) -> None:
    transcript = settled(journal, outcome)
    path = tmp_path / "transcript.json"
    path.write_bytes(transcript.model_dump_json().encode("utf-8"))
    assert ControlTranscript.read(path) == transcript
    assert len(transcript.applications()) == (
        outcome in {"accepted", "duplicate", "conflict", "manual-failure"}
    )


@pytest.mark.parametrize(
    "tamper",
    [
        "policy",
        "timeout",
        "completed",
        "proposal",
        "disposition",
        "failure",
        "cancelled",
        "request-id",
        "request-digest",
        "revision",
        "values",
        "response",
        "fence",
    ],
)
def test_transcript_rejects_corrupt_call_provenance(
    journal: DecisionJournal, tmp_path: Path, tamper: str
) -> None:
    transcript = settled(journal, "accepted")
    data = transcript.model_dump(mode="json")
    call = data["decisions"][0]["call"]
    if tamper == "policy":
        call["policy_digest"] = "sha256:" + "b" * 64
    elif tamper == "timeout":
        call["timeout_ms"] += 1
    elif tamper == "completed":
        call["completed_at"] = None
    elif tamper in {"proposal", "disposition"}:
        call[tamper] = None
    elif tamper == "failure":
        call["failure_code"] = "exception"
    elif tamper == "cancelled":
        call["outcome"] = "cancelled"
    elif tamper == "request-id":
        call["proposal"]["request_id"] = new_id()
    elif tamper == "request-digest":
        call["proposal"]["request_digest"] = "sha256:" + "b" * 64
    elif tamper == "revision":
        call["proposal"]["base_revision"] += 1
    elif tamper == "values":
        call["proposal"]["values"][0]["value"] = 9.0
    elif tamper == "response":
        call["proposal"]["values"][0]["value"] = 0.3
    else:
        call["fence"] = "timeout"
    with pytest.raises(ValueError):
        ControlTranscript.model_validate(data)
    path = tmp_path / "corrupt.json"
    path.write_bytes(json.dumps(data).encode("utf-8"))
    with pytest.raises(ValueError):
        ControlTranscript.read(path)
