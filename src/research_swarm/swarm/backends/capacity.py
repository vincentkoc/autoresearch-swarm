"""Concurrency limits shared by device-bound views of one backend."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from research_swarm.swarm.interfaces import CampaignSpec


class Slots:
    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("slots must be positive")
        self.limit = limit
        self.active: Counter[str] = Counter()
        self.changed = asyncio.Condition()

    def free(self, spec: CampaignSpec) -> int:
        return max(
            0,
            min(self.limit - self.active.total(), spec.max_workers - self.active[spec.campaign_id]),
        )

    @asynccontextmanager
    async def claim(self, spec: CampaignSpec) -> AsyncIterator[None]:
        if spec.max_workers < 1:
            raise ValueError("max_workers must be positive")
        async with self.changed:
            await self.changed.wait_for(lambda: self.free(spec) > 0)
            self.active[spec.campaign_id] += 1
        try:
            yield
        finally:
            async with self.changed:
                self.active[spec.campaign_id] -= 1
                if not self.active[spec.campaign_id]:
                    del self.active[spec.campaign_id]
                self.changed.notify_all()
