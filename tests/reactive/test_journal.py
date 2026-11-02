from __future__ import annotations

import pytest

from research_swarm.domain.types import new_id
from research_swarm.reactive.contracts import NumericValue
from research_swarm.reactive.journal import DecisionJournal, JournalConflict
from tests.reactive.support import receipt_for, request_for, response_for


def test_durable_application_and_exact_duplicates(journal: DecisionJournal) -> None:
    with journal.trainer() as trainer:
        item = request_for(journal)
        trainer.request(item)
        assert trainer.request(item).request == item
        response = response_for(item)
        journal.respond(response)
        assert journal.respond(response).proposal == response
        with pytest.raises(JournalConflict, match="differs from the recorded response"):
            journal.respond(response_for(item, 0.3))
        intent = trainer.claim_application(item.id, step=12)
        with pytest.raises(JournalConflict, match="new application claim"):
            trainer.claim_application(item.id, step=12)
        receipt = receipt_for(intent)
        trainer.record_application(receipt)
        assert trainer.record_application(receipt).receipt == receipt
        assert journal.inspect().control_revision == 1
        assert journal.pending() is None
    reopened = DecisionJournal.open(journal.path)
    assert reopened.inspect().controls == (NumericValue(name="lr", value=0.2),)
    assert reopened.inspect().status == "closed"
    assert [event.kind for event in reopened.events()] == [
        "created",
        "owned",
        "requested",
        "responded",
        "claimed",
        "receipted",
        "closed",
    ]


def test_request_order_identity_and_current_controls(journal: DecisionJournal) -> None:
    with journal.trainer() as trainer:
        item = request_for(journal)
        for changed in (
            item.model_copy(
                update={"binding": item.binding.model_copy(update={"attempt_id": new_id()})}
            ),
            item.model_copy(update={"control_revision": 1}),
        ):
            with pytest.raises(JournalConflict, match="current run or controls"):
                trainer.request(changed)
        trainer.request(item)
        with pytest.raises(JournalConflict, match="different content"):
            trainer.request(item.model_copy(update={"requested_step": 11}))
        with pytest.raises(JournalConflict, match="one ordered"):
            trainer.request(request_for(journal, 20))
        trainer.settle(item.id, "failed", detail="fixture failure")
        with pytest.raises(JournalConflict, match="interval ordering"):
            trainer.request(request_for(journal, 11))
        trainer.request(request_for(journal, 15))


@pytest.mark.parametrize(
    "changes",
    [
        {"applied_step": 13},
        {"request_digest": "sha256:" + "b" * 64},
        {"decision_digest": "sha256:" + "b" * 64},
        {"after": (NumericValue(name="lr", value=0.8),)},
    ],
)
def test_receipts_are_bound_to_actual_claim(
    journal: DecisionJournal, changes: dict[str, object]
) -> None:
    with journal.trainer() as trainer:
        item = request_for(journal)
        trainer.request(item)
        journal.respond(response_for(item))
        intent = trainer.claim_application(item.id, step=12)
        with pytest.raises(JournalConflict, match="original claim|proposed controls"):
            trainer.record_application(receipt_for(intent, **changes))
        assert journal.pending().status == "applying"  # type: ignore[union-attr]
    assert journal.inspect().status == "uncertain"


def test_terminal_fencing_preserves_late_response(journal: DecisionJournal) -> None:
    trainer = journal.trainer()
    item = request_for(journal)
    trainer.request(item)
    trainer.close()
    trainer.close()
    late = journal.respond(response_for(item))
    assert late.status == "cancelled" and late.response_disposition == "late"
    assert journal.inspect().status == "closed" and journal.pending() is None
    assert journal.respond(response_for(item)) == late
    with pytest.raises(JournalConflict, match="terminal"):
        journal.trainer()
    with pytest.raises(JournalConflict, match="ownership"):
        trainer.claim_application(item.id, step=12)


def test_create_does_not_replace_existing_session(journal: DecisionJournal) -> None:
    state = journal.inspect()
    with pytest.raises(FileExistsError):
        DecisionJournal.create(journal.path, state.binding, state.contract)
    assert journal.inspect() == state
    assert len(journal.events(limit=1)) == 1
    assert journal.events(after=0) == ()
