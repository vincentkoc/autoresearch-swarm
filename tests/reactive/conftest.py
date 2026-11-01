from pathlib import Path

import pytest

from research_swarm.reactive.journal import DecisionJournal
from tests.reactive.support import create_journal


@pytest.fixture
def journal(tmp_path: Path) -> DecisionJournal:
    return create_journal(tmp_path / "session.sqlite")
