from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from research_swarm.reactive.calls import start_call
from research_swarm.reactive.contracts import DecisionProposal, DecisionRequest, ReactiveContract
from research_swarm.reactive.journal import DecisionJournal, JournalConflict
from research_swarm.reactive.journal_records import DecisionRecord
from research_swarm.reactive.policies import CallbackPolicy, ThresholdPolicy
from research_swarm.reactive.runner import DecisionRunner
from tests.reactive.support import receipt_for, request_for, response_for


async def until(
    journal: DecisionJournal, identifier: str, predicate: Callable[[DecisionRecord], bool]
) -> DecisionRecord:
    async def wait() -> DecisionRecord:
        while True:
            record = journal.get(identifier)
            assert record is not None
            if predicate(record):
                return record
            await asyncio.sleep(0)

    return await asyncio.wait_for(wait(), timeout=2)


def timed(journal: DecisionJournal, tmp_path: Path) -> DecisionJournal:
    state = journal.inspect()
    return DecisionJournal.create(
        tmp_path / "timed.sqlite",
        state.binding,
        state.contract.model_copy(update={"decision_timeout_ms": 30}),
    )


async def test_external_runner_persists_before_inference_outside_transaction(
    journal: DecisionJournal,
) -> None:
    called = 0

    async def callback(request: DecisionRequest, contract: ReactiveContract) -> DecisionProposal:
        nonlocal called
        called += 1
        record = journal.get(request.id)
        assert record is not None and record.call is not None and record.call.outcome == "started"
        with sqlite3.connect(journal.path, timeout=0) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.rollback()
        return response_for(request)

    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        external = DecisionJournal.open(journal.path)
        with DecisionRunner(
            external, CallbackPolicy(journal.inspect().contract.policy_digest, callback)
        ) as runner:
            assert runner.poll() == "started"
            assert runner.poll() == "busy"
            record = await until(journal, request.id, lambda item: item.status == "responded")
            assert record.call is not None and record.call.disposition == "accepted"
            assert runner.poll() == "idle" and called == 1


async def test_recorded_call_is_not_repeated_by_another_runner(journal: DecisionJournal) -> None:
    async def forbidden(request: DecisionRequest, contract: ReactiveContract) -> DecisionProposal:
        raise AssertionError("must not be invoked")

    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        digest = journal.inspect().contract.policy_digest
        start_call(journal, request.id, digest)
        with pytest.raises(JournalConflict, match="already recorded"):
            start_call(DecisionJournal.open(journal.path), request.id, digest)
        with DecisionRunner(
            DecisionJournal.open(journal.path), CallbackPolicy(digest, forbidden)
        ) as runner:
            assert runner.poll() == "busy"
    assert journal.get(request.id).call.outcome == "started"  # type: ignore[union-attr]


def test_independent_call_claim_connections_have_one_winner(journal: DecisionJournal) -> None:
    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        digest = journal.inspect().contract.policy_digest
        barrier = Barrier(2)

        def claim(_: int) -> str:
            external = DecisionJournal.open(journal.path)
            barrier.wait(timeout=5)
            try:
                start_call(external, request.id, digest)
                return "claimed"
            except JournalConflict:
                return "conflict"

        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sorted(pool.map(claim, [1, 2])) == ["claimed", "conflict"]
        assert sum(event.kind == "call_started" for event in journal.events()) == 1


async def test_timeout_keeps_cancellation_resistant_call_in_flight(
    journal: DecisionJournal, tmp_path: Path
) -> None:
    journal = timed(journal, tmp_path)
    started, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def stubborn(request: DecisionRequest, contract: ReactiveContract) -> DecisionProposal:
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
        return response_for(request)

    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        policy = CallbackPolicy(journal.inspect().contract.policy_digest, stubborn)
        with DecisionRunner(journal, policy) as runner:
            runner.poll()
            await asyncio.wait_for(started.wait(), timeout=1)
            await asyncio.wait_for(cancelled.wait(), timeout=1)
            assert journal.get(request.id).status == "expired"  # type: ignore[union-attr]
            trainer.request(request_for(journal, 15))
            assert runner.poll() == "busy"
            with DecisionRunner(DecisionJournal.open(journal.path), policy) as other:
                assert other.poll() == "busy"
            runner.close()
            assert journal.inspect().inflight_call_request_id == request.id
            release.set()
            record = await until(
                journal,
                request.id,
                lambda item: item.call is not None and item.call.outcome == "returned",
            )
            assert record.status == "expired" and record.response_disposition == "late"
            assert record.call is not None and record.call.fence == "timeout"
            assert journal.inspect().inflight_call_request_id is None


async def test_timeout_does_not_overwrite_manual_application(
    journal: DecisionJournal, tmp_path: Path
) -> None:
    journal = timed(journal, tmp_path)
    cancelled, release = asyncio.Event(), asyncio.Event()

    async def stubborn(request: DecisionRequest, contract: ReactiveContract) -> DecisionProposal:
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
        return response_for(request, 0.2)

    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        with DecisionRunner(
            journal, CallbackPolicy(journal.inspect().contract.policy_digest, stubborn)
        ) as runner:
            runner.poll()
            journal.respond(response_for(request, 0.3))
            receipt = receipt_for(trainer.claim_application(request.id, step=12))
            trainer.record_application(receipt)
            await asyncio.wait_for(cancelled.wait(), timeout=1)
            assert journal.get(request.id).receipt == receipt  # type: ignore[union-attr]
            release.set()
            record = await until(
                journal,
                request.id,
                lambda item: item.call is not None and item.call.outcome == "returned",
            )
            assert record.status == "applied" and record.receipt == receipt
            assert record.call is not None and record.call.disposition == "conflict"
            assert record.call.proposal == response_for(request, 0.2)
            assert record.proposal == response_for(request, 0.3)


@pytest.mark.parametrize("invalid", [False, True])
async def test_failures_are_preserved_without_secret_exception_text(
    journal: DecisionJournal, invalid: bool
) -> None:
    async def callback(request: DecisionRequest, contract: ReactiveContract) -> DecisionProposal:
        if invalid:
            return response_for(request, 8.0)
        raise RuntimeError("secret-fixture-key")

    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        with DecisionRunner(
            journal, CallbackPolicy(journal.inspect().contract.policy_digest, callback)
        ) as runner:
            runner.poll()
            record = await until(journal, request.id, lambda item: item.status == "failed")
            assert record.call is not None
            assert record.call.failure_code == ("invalid_response" if invalid else "exception")
            assert runner.poll() == "idle"
            assert "secret-fixture-key" not in "".join(
                event.model_dump_json() for event in journal.events()
            )


async def test_close_before_callback_starts_has_a_durable_outcome(journal: DecisionJournal) -> None:
    invoked = False

    async def callback(request: DecisionRequest, contract: ReactiveContract) -> DecisionProposal:
        nonlocal invoked
        invoked = True
        return response_for(request)

    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        runner = DecisionRunner(
            journal, CallbackPolicy(journal.inspect().contract.policy_digest, callback)
        )
        runner.poll()
        runner.close()
        record = await until(
            journal,
            request.id,
            lambda item: item.call is not None and item.call.outcome == "cancelled",
        )
        assert not invoked and record.status == "cancelled"
        assert journal.inspect().inflight_call_request_id is None
        assert runner.poll() == "closed"


async def test_rule_policy_is_configuration_bound(journal: DecisionJournal, tmp_path: Path) -> None:
    policy = ThresholdPolicy(diagnostic="loss", above=1.0, control="lr", value=0.2)
    state = journal.inspect()
    with pytest.raises(JournalConflict, match="fingerprint"):
        DecisionRunner(journal, policy)
    rule_journal = DecisionJournal.create(
        tmp_path / "rule.sqlite",
        state.binding,
        state.contract.model_copy(update={"policy_digest": policy.digest}),
    )
    assert policy.digest != policy.model_copy(update={"value": 0.3}).digest
    with rule_journal.trainer() as trainer, DecisionRunner(rule_journal, policy) as runner:
        request = request_for(rule_journal)
        trainer.request(request)
        runner.poll()
        record = await until(rule_journal, request.id, lambda item: item.status == "responded")
        assert record.proposal is not None and record.proposal.values[0].value == 0.2
