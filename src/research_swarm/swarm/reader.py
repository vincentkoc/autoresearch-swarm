"""Read-only SDK for a local campaign database."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from research_swarm.swarm.report import _read, capture, read_meter


def _limit(limit: int) -> int:
    if not 1 <= limit <= 100:
        raise ValueError("Page size must be between 1 and 100")
    return limit


@dataclass(frozen=True)
class CampaignReader:
    database: Path

    def campaigns(self, cursor: str | None = None, limit: int = 100) -> dict[str, Any]:
        with _read(self.database) as db:
            rows = db.execute(
                "SELECT campaign_id, project_id FROM swarm_campaigns WHERE campaign_id > ? "
                "ORDER BY campaign_id LIMIT ?",
                (cursor or "", _limit(limit) + 1),
            ).fetchall()
        page = rows[:limit]
        return {
            "items": [
                {
                    "id": row["campaign_id"],
                    "data": {"name": row["campaign_id"], "project_id": row["project_id"]},
                }
                for row in page
            ],
            "next_cursor": page[-1]["campaign_id"] if len(rows) > limit else None,
        }

    def snapshot(self, campaign_id: str) -> dict[str, Any]:
        return capture(self.database, campaign_id)

    def budget(self, campaign_id: str) -> dict[str, Any]:
        return read_meter(self.database, campaign_id)

    def events(
        self, after: int = 0, limit: int = 100, campaign_id: str | None = None
    ) -> dict[str, Any]:
        if after < 0:
            raise ValueError("Event cursor must be nonnegative")
        with _read(self.database) as db:
            rows = db.execute(
                "SELECT * FROM swarm_events WHERE id > ? AND (? IS NULL OR campaign_id=?) "
                "ORDER BY id LIMIT ?",
                (after, campaign_id, campaign_id, _limit(limit) + 1),
            ).fetchall()
        page = rows[:limit]
        return {
            "items": [dict(row) for row in page],
            "next_cursor": page[-1]["id"] if len(rows) > limit else None,
        }
