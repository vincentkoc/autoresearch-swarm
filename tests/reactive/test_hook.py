from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

import pytest

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.reactive.contracts import DecisionProposal, NumericValue, Observation
from research_swarm.reactive.hook import ReactiveHook
from research_swarm.reactive.journal import DecisionJournal, JournalConflict
from tests.reactive.support import response_for


@dataclass
class Controls:
    value: float = 0.1
    writes: int = 0
    failure: str = ""

    def read(self) -> tuple[NumericValue, ...]:
        return (NumericValue(name="lr", value=self.value),)

    def apply(self, values: tuple[NumericValue, ...]) -> None:
        self.writes += 1
        self.value = values[0].value
        if self.failure == "throw":
            raise RuntimeError("fixture mutation failed after writing")
        if self.failure == "mismatch":
            self.value += 0.1


def observed(step: int) -> Observation:
    return Observation(step=step, diagnostics=(NumericValue(name="loss", value=2.0),))


async def test_training_continues_while_decision_is_pending(journal: DecisionJournal) -> None:
    target = Controls()
    gate, replied = asyncio.Event(), asyncio.Event()
    with ReactiveHook(journal, target) as hook:
        result = await hook.boundary(observed(5))
        assert result.request is not None

        async def decide() -> None:
            await gate.wait()
            journal.respond(response_for(result.request))  # type: ignore[arg-type]
            replied.set()

        task = asyncio.create_task(decide())
        for step in range(6, 9):
            boundary = await asyncio.wait_for(hook.boundary(observed(step)), timeout=1)
            assert boundary.status == "waiting"
            assert target.writes == 0 and not task.done()
        gate.set()
        await asyncio.wait_for(replied.wait(), timeout=1)
        assert target.writes == 0
        applied = await hook.boundary(observed(9))
        assert applied.receipt is not None
        assert applied.receipt.observed_step == applied.receipt.requested_step == 5
        assert applied.receipt.applied_step == 9
        assert target.value == 0.2 and target.writes == 1
        await task


@pytest.mark.parametrize("kind", ["keep", "set"])
async def test_keep_and_noop_have_identical_application_semantics(
    journal: DecisionJournal, kind: str
) -> None:
    target = Controls()
    with ReactiveHook(journal, target) as hook:
        result = await hook.boundary(observed(5), step=6)
        assert result.request is not None
        journal.respond(
            DecisionProposal.model_validate(
                {
                    "request_id": result.request.id,
                    "request_digest": result.request.digest,
                    "base_revision": 0,
                    "kind": kind,
                    "values": [] if kind == "keep" else [{"name": "lr", "value": 0.1}],
                }
            )
        )
        applied = await hook.boundary(observed(7))
        assert applied.status == "kept" and target.writes == 0
        assert applied.receipt is not None and applied.receipt.new_revision == 1
        assert (
            applied.receipt.observed_step,
            applied.receipt.requested_step,
            applied.receipt.applied_step,
        ) == (5, 6, 7)


@pytest.mark.parametrize(
    "failure,detail", [("throw", "control_apply_failed"), ("mismatch", "readback_mismatch")]
)
async def test_failed_mutation_is_uncertain_and_never_rolled_back(
    journal: DecisionJournal, failure: str, detail: str
) -> None:
    target = Controls(failure=failure)
    with ReactiveHook(journal, target) as hook:
        result = await hook.boundary(observed(5))
        assert result.request is not None
        journal.respond(response_for(result.request))
        failed = await hook.boundary(observed(6))
        assert failed.status == "uncertain"
        assert failed.receipt is not None and failed.receipt.after is None
        assert target.value != 0.1 and target.writes == 1
        record = journal.get(result.request.id)
        assert record is not None and record.detail == detail
        assert (await hook.boundary(observed(7))).status == "uncertain"
        assert target.writes == 1 and journal.inspect().status == "uncertain"


async def test_external_control_drift_fences_intervention(journal: DecisionJournal) -> None:
    target = Controls()
    with ReactiveHook(journal, target) as hook:
        result = await hook.boundary(observed(5))
        assert result.request is not None
        journal.respond(response_for(result.request))
        target.value = 0.3
        assert (await hook.boundary(observed(6))).status == "uncertain"
        assert target.value == 0.3 and target.writes == 0
        assert journal.get(result.request.id).detail == "control_drift"  # type: ignore[union-attr]


async def test_receipt_write_failure_does_not_allow_another_mutation(
    journal: DecisionJournal, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = Controls()
    with ReactiveHook(journal, target) as hook:
        result = await hook.boundary(observed(5))
        assert result.request is not None
        journal.respond(response_for(result.request))

        def fail_receipt(*args: object, **kwargs: object) -> None:
            raise OSError("fixture persistence failure")

        monkeypatch.setattr(hook.session, "record_application", fail_receipt)
        with pytest.raises(OSError, match="persistence failure"):
            await hook.boundary(observed(6))
        assert (await hook.boundary(observed(7))).status == "uncertain"
        assert target.value == 0.2 and target.writes == 1
        assert journal.pending().status == "applying"  # type: ignore[union-attr]
    assert journal.inspect().status == "uncertain"


async def test_stale_decision_expires_without_touching_controls(journal: DecisionJournal) -> None:
    target = Controls()
    with ReactiveHook(journal, target) as hook:
        result = await hook.boundary(observed(5))
        assert result.request is not None
        assert (await hook.boundary(observed(16))).status == "expired"
        late = journal.respond(response_for(result.request))
        assert late.response_disposition == "late" and late.status == "expired"
        assert target.writes == 0


async def test_close_during_a_cooperative_yield_cancels_pending_work(
    journal: DecisionJournal,
) -> None:
    target = Controls()
    hook = ReactiveHook(journal, target)
    result = await hook.boundary(observed(5))
    assert result.request is not None
    task = asyncio.create_task(hook.boundary(observed(6)))
    await asyncio.sleep(0)
    with pytest.raises(JournalConflict, match="Concurrent"):
        await hook.boundary(observed(7))
    hook.close()
    assert (await task).status == "closed"
    assert journal.get(result.request.id).status == "cancelled"  # type: ignore[union-attr]
    assert target.writes == 0


def test_initial_control_mismatch_does_not_claim_ownership(journal: DecisionJournal) -> None:
    with pytest.raises(JournalConflict, match="initial controls"):
        ReactiveHook(journal, Controls(value=0.8))
    assert journal.inspect().trainer_id is None


async def test_history_and_decision_count_stay_bounded(
    journal: DecisionJournal, tmp_path: Path
) -> None:
    original = journal.inspect()
    bounded = DecisionJournal.create(
        tmp_path / "bounded.sqlite",
        original.binding,
        original.contract.model_copy(update={"max_prompt_bytes": 1536, "max_decisions": 5}),
    )
    with ReactiveHook(bounded, Controls()) as hook:
        for step in range(5, 30, 5):
            result = await hook.boundary(observed(step))
            assert result.request is not None
            assert len(canonical_bytes(result.request.model_dump(mode="json"))) <= 1536
            bounded.respond(response_for(result.request, 0.1))
            assert (await hook.boundary(observed(step + 1))).status == "kept"
        assert (await hook.boundary(observed(30))).status == "idle"
        assert bounded.inspect().decision_count == 5
