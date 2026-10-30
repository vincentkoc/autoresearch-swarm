"""Actor identities and idempotent command envelopes."""

from __future__ import annotations

from pydantic import Field

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Name, Record, Revision, StringSet, new_id


class Principal(Record):
    id: ID
    workspace_id: ID
    scopes: StringSet

    def require(self, scope: str) -> None:
        if scope not in self.scopes and "*" not in self.scopes:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED,
                f"Required scope: {scope}",
                details={"scope": scope},
            )


class CommandContext(Record):
    principal: Principal
    idempotency_key: Name
    command_id: ID = Field(default_factory=new_id)
    expected_revision: Revision | None = None
