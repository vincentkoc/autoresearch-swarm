from __future__ import annotations

import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from filelock import Timeout

from research_swarm.reactive.journal import DecisionJournal, JournalConflict
from tests.reactive.support import receipt_for, request_for, response_for


def test_reader_cannot_recover_or_steal_live_application(journal: DecisionJournal) -> None:
    with journal.trainer() as trainer:
        item = request_for(journal)
        trainer.request(item)
        journal.respond(response_for(item))
        intent = trainer.claim_application(item.id, step=12)
        reader = DecisionJournal.open(journal.path)
        assert reader.pending().status == "applying"  # type: ignore[union-attr]
        with pytest.raises(Timeout):
            reader.trainer()
        assert reader.inspect().status == "active"
        trainer.record_application(receipt_for(intent))


def test_process_interruption_requires_exclusive_recovery(journal: DecisionJournal) -> None:
    item = request_for(journal)
    script = """
import os, sys
from pathlib import Path
from research_swarm.reactive.contracts import DecisionRequest, DecisionProposal, NumericValue
from research_swarm.reactive.journal import DecisionJournal
journal = DecisionJournal.open(Path(sys.argv[1]))
trainer = journal.trainer()
request = DecisionRequest.model_validate_json(sys.argv[2])
trainer.request(request)
journal.respond(DecisionProposal(request_id=request.id, request_digest=request.digest,
    base_revision=0, kind='set', values=(NumericValue(name='lr', value=0.2),)))
trainer.claim_application(request.id, step=12)
os._exit(0)
"""
    subprocess.run(
        [sys.executable, "-c", script, str(journal.path), item.model_dump_json()],
        check=True,
        timeout=10,
    )
    reader = DecisionJournal.open(journal.path)
    assert reader.pending().status == "applying"  # type: ignore[union-attr]
    assert reader.inspect().status == "active"
    with pytest.raises(JournalConflict, match="Interrupted training owner"):
        reader.trainer()
    assert reader.inspect().status == "uncertain"
    record = reader.get(item.id)
    assert record is not None and record.receipt is not None
    assert record.receipt.after is None and record.receipt.new_revision is None
    with pytest.raises(JournalConflict, match="terminal"):
        reader.trainer()


def test_independent_response_connections_have_one_winner(journal: DecisionJournal) -> None:
    with journal.trainer() as trainer:
        item = request_for(journal)
        trainer.request(item)
        barrier = Barrier(2)

        def respond(value: float) -> str:
            connection = DecisionJournal.open(journal.path)
            barrier.wait(timeout=5)
            try:
                connection.respond(response_for(item, value))
                return "accepted"
            except JournalConflict:
                return "conflict"

        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sorted(pool.map(respond, [0.2, 0.3])) == ["accepted", "conflict"]
        assert sum(event.kind == "responded" for event in journal.events()) == 1


def test_projection_and_events_commit_atomically(journal: DecisionJournal) -> None:
    with journal.trainer() as trainer, sqlite3.connect(journal.path) as connection:
        connection.execute(
            "CREATE TRIGGER fail_projection BEFORE UPDATE ON session "
            "BEGIN SELECT RAISE(ABORT, 'fixture failure'); END"
        )
        item = request_for(journal)
        with pytest.raises(sqlite3.IntegrityError, match="fixture failure"):
            trainer.request(item)
        assert journal.get(item.id) is None and journal.inspect().decision_count == 0
        connection.execute("DROP TRIGGER fail_projection")
        trainer.request(item)
        for statement in ("UPDATE events SET request_id=NULL", "DELETE FROM events"):
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                connection.execute(statement)
        connection.rollback()
