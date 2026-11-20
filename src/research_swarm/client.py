"""Authenticated HTTP and local read clients for campaign state."""

from __future__ import annotations

import asyncio
import ssl
from pathlib import Path
from types import TracebackType
from typing import Any
from urllib.parse import quote

import httpx

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.endpoints import controller_url
from research_swarm.swarm.private_files import campaign_database, default_state_directory
from research_swarm.swarm.report import CampaignReader


class ResearchClient:
    def __init__(self, reader: CampaignReader | httpx.Client) -> None:
        self.reader = reader

    @classmethod
    def local(
        cls, state_dir: Path | None = None, *, token_path: Path | None = None
    ) -> ResearchClient:
        if token_path is not None:
            raise ValueError("Local campaign reads use filesystem access")
        return cls(CampaignReader(campaign_database(state_dir or default_state_directory())))

    @classmethod
    def connect(
        cls, url: str, token: str, *, timeout: float = 30, tls: ssl.SSLContext | None = None
    ) -> ResearchClient:
        if tls is not None and (tls.verify_mode != ssl.CERT_REQUIRED or not tls.check_hostname):
            raise ValueError("TLS context must verify certificates and hostnames")
        return cls(
            httpx.Client(
                base_url=controller_url(url),
                headers={"Authorization": "Bearer " + token},
                timeout=timeout,
                follow_redirects=False,
                trust_env=False,
                verify=tls or True,
            )
        )

    @staticmethod
    def _get(http: httpx.Client, path: str, **params: str | int | None) -> dict[str, Any]:
        query = {key: value for key, value in params.items() if value is not None}
        with http.stream("GET", path, params=query) as response:
            response.raise_for_status()
            content = bytearray()
            for chunk in response.iter_bytes(64 * 1024):
                if len(content) + len(chunk) > 32 * 1024 * 1024:
                    raise ValueError("Campaign response exceeds its size limit")
                content.extend(chunk)
        payload = strict_json_loads(bytes(content))
        if not isinstance(payload, dict):
            raise ValueError("Expected a campaign response object")
        return payload

    def campaigns(self, cursor: str | None = None, limit: int = 100) -> dict[str, Any]:
        if isinstance(self.reader, CampaignReader):
            return self.reader.campaigns(cursor, limit)
        return self._get(self.reader, "v1/campaigns", cursor=cursor, limit=limit)

    def snapshot(self, campaign_id: str) -> dict[str, Any]:
        if isinstance(self.reader, CampaignReader):
            return self.reader.snapshot(campaign_id)
        return self._get(self.reader, f"v1/campaigns/{quote(campaign_id, safe='')}/snapshot")

    def budget(self, campaign_id: str) -> dict[str, Any]:
        if isinstance(self.reader, CampaignReader):
            return self.reader.budget(campaign_id)
        return self._get(self.reader, f"v1/campaigns/{quote(campaign_id, safe='')}/budget")

    def events(
        self, *, after: int = 0, limit: int = 100, campaign_id: str | None = None
    ) -> dict[str, Any]:
        if isinstance(self.reader, CampaignReader):
            return self.reader.events(after, limit, campaign_id)
        return self._get(
            self.reader, "v1/events", after=after, limit=limit, campaign_id=campaign_id
        )

    def close(self) -> None:
        if isinstance(self.reader, httpx.Client):
            self.reader.close()

    def __enter__(self) -> ResearchClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class AsyncResearchClient:
    def __init__(self, client: ResearchClient) -> None:
        self.client = client

    @classmethod
    def local(
        cls, state_dir: Path | None = None, *, token_path: Path | None = None
    ) -> AsyncResearchClient:
        return cls(ResearchClient.local(state_dir, token_path=token_path))

    @classmethod
    def connect(cls, url: str, token: str) -> AsyncResearchClient:
        return cls(ResearchClient.connect(url, token))

    async def campaigns(self, cursor: str | None = None, limit: int = 100) -> dict[str, Any]:
        return await asyncio.to_thread(self.client.campaigns, cursor, limit)

    async def snapshot(self, campaign_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(self.client.snapshot, campaign_id)

    async def budget(self, campaign_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(self.client.budget, campaign_id)

    async def events(
        self, *, after: int = 0, limit: int = 100, campaign_id: str | None = None
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self.client.events, after=after, limit=limit, campaign_id=campaign_id
        )

    async def __aenter__(self) -> AsyncResearchClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await asyncio.to_thread(self.client.close)
