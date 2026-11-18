from __future__ import annotations

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database


@pytest.mark.parametrize("violation", ["skip", "repeat", "rewrite", "retry_hook"])
def test_boundary_contract_violation_rolls_back_every_effect(database, context, violation):
    calls = []

    class BrokenBoundary:
        def invoke(self, transaction, *, name, payload, perform, reject):
            if violation == "skip":
                return {"disposition": "invented"}
            try:
                result = perform()
            except ResearchError as error:
                return reject(error)
            if violation == "repeat":
                perform()
            if violation == "rewrite":
                result["result"]["changed"] = True
            return result

        def rejected(self, transaction, **kwargs):
            calls.append("boundary fence")
            transaction.insert("fixture_fence", new_id(), {}, state="fenced")

    guarded = Database.from_store(database.store, execution_boundary=BrokenBoundary())
    command = context()
    before = guarded.snapshot(command)

    def handler(tx):
        tx.insert("fixture_effect", new_id(), {}, state="saved")
        if violation == "retry_hook":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Injected handler rejection")
        return {}

    def failed_hook(tx, error):
        calls.append("domain hook")
        raise ResearchError(ErrorCode.REVISION_CONFLICT, "Abort outer transaction")

    with pytest.raises(RuntimeError):
        guarded.execute(
            command, "fixture.operation", {}, "write", handler, on_rejection=failed_hook
        )
    assert guarded.snapshot(command) == before
    assert guarded.replay(command, "fixture.operation", {}, "write") is None
    assert calls == (["boundary fence", "domain hook"] if violation == "retry_hook" else [])
