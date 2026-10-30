from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database


@pytest.fixture
def principal() -> Principal:
    return Principal(id=new_id(), workspace_id=new_id(), scopes=frozenset({"*"}))


@pytest.fixture
def context(principal: Principal) -> Callable[..., CommandContext]:
    def create(key: str | None = None, revision: int | None = None) -> CommandContext:
        return CommandContext(
            principal=principal,
            idempotency_key=key or new_id(),
            expected_revision=revision,
        )

    return create


@pytest.fixture
def database(tmp_path: Path, principal: Principal) -> Database:
    database = Database(tmp_path / "research.sqlite")
    database.bootstrap(principal.workspace_id, new_id())
    return database
