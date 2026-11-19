"""Run a locally configured backend against the controller's scoped worker queue."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

import httpx
from pydantic import TypeAdapter

from research_swarm.domain.endpoints import controller_url
from research_swarm.swarm.command import campaign_backend
from research_swarm.swarm.interfaces import Attempt, Result, Trial
from research_swarm.swarm.spec import load_spec
from research_swarm.swarm.worker import from_spec


class HTTPWorkerQueue:
    def __init__(self, http: httpx.Client, token: str) -> None:
        self.http = http
        self.token = token
        self.claims: dict[str, tuple[str, bool]] = {}

    def _post(self, route: str, token: str, **kwargs: Any) -> httpx.Response:
        return self.http.post(
            "/v1/worker/" + route, headers={"Authorization": "Bearer " + token}, **kwargs
        )

    def requeue_expired(self, campaign_id: str, before: float) -> int:
        # The controller owns the campaign binding, clock and expiry interval.
        return int(self._post("requeue", self.token).raise_for_status().json())

    def claim(
        self,
        campaign_id: str,
        worker_id: str,
        *,
        reserve_cost: float | Callable[[Trial], float] = 0.0,
    ) -> Attempt | None:
        response = self._post(
            "claim", self.token, params={"campaign_id": campaign_id, "worker_id": worker_id}
        )
        data = response.raise_for_status().json()
        if data is None:
            return None
        attempt = TypeAdapter(Attempt).validate_python(data["attempt"])
        if attempt.worker_id != worker_id or attempt.trial.campaign_id != campaign_id:
            raise ValueError("Controller returned an attempt for another worker or campaign")
        self.claims[attempt.attempt_id] = data["token"], data.get("attempt_telemetry") is True
        return attempt

    def workload_started(self, attempt_id: str) -> None:
        token, supported = self.claims[attempt_id]
        if supported:
            response = self._post(attempt_id + "/started", token)
            if response.status_code != 410:
                response.raise_for_status()

    def heartbeat(self, attempt_id: str, *, cost: float = 0.0) -> bool:
        token, _ = self.claims[attempt_id]
        response = self._post(attempt_id + "/heartbeat", token)
        if response.status_code == 410 or response.raise_for_status().json() is False:
            self.claims.pop(attempt_id, None)
            return False
        return True

    def report(self, result: Result) -> bool:
        attempt_id = result.attempt.attempt_id
        route = attempt_id + "/report"
        payload = asdict(result)
        del payload["attempt"], payload["cost"]
        token, supported = self.claims[attempt_id]
        if not supported:
            del payload["failure_class"]
        try:
            response = self._post(route, token, json=payload)
        except httpx.TransportError:
            # Delivery may have succeeded. Replaying the same observation is idempotent.
            response = self._post(route, token, json=payload)
        fresh = response.status_code != 410 and bool(response.raise_for_status().json())
        self.claims.pop(attempt_id, None)
        return fresh


async def run_worker(
    path: Path, controller: str, token: str, worker_id: str, *, once: bool
) -> bool:
    spec = load_spec(path)
    with httpx.Client(base_url=controller_url(controller), trust_env=False, timeout=10) as http:
        worker = from_spec(
            spec, HTTPWorkerQueue(http, token), campaign_backend(spec), worker_id=worker_id
        )
        if once:
            return await worker.run_once(spec)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        try:
            await worker.run(spec, stop=stop)
            return True
        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
