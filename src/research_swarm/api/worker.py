"""Attempt-scoped HTTP writes for one configured campaign worker."""

from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field, FiniteFloat

from research_swarm.swarm.files import CampaignFiles
from research_swarm.swarm.interfaces import Attempt, Outcome, Result
from research_swarm.swarm.meter import CostMeter

MAX_ARTIFACT_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class WorkerGrant:
    campaign_id: str
    worker_id: str
    token: str


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    status: Outcome
    metrics: dict[str, FiniteFloat] = Field(default_factory=dict)
    error: str | None = None
    logs: tuple[str, ...] = ()
    input_tokens: int = Field(default=0, ge=0, strict=True)
    output_tokens: int = Field(default=0, ge=0, strict=True)
    peak_vram_mib: float | None = Field(default=None, ge=0)


def worker_router(database: Path, grant: WorkerGrant) -> APIRouter:
    if (
        not grant.worker_id
        or not grant.token
        or not grant.token.isascii()
        or any(c.isspace() for c in grant.token)
    ):
        raise ValueError("Configure a worker identity and an ASCII bearer without whitespace")
    files = CampaignFiles(database)
    store = files.store
    spec = store.campaign_spec(grant.campaign_id)
    meter = CostMeter()
    router = APIRouter(prefix="/v1/worker")

    def attempt_token(attempt_id: str) -> str:
        return hmac.digest(grant.token.encode(), attempt_id.encode(), "sha256").hex()

    def authorize(request: Request, response: Response) -> None:
        attempt_id = request.path_params.get("attempt_id")
        expected = attempt_token(attempt_id) if attempt_id is not None else grant.token
        scheme, _, presented = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(
            presented.encode(), expected.encode()
        ):
            raise HTTPException(
                401, "Worker bearer required", headers={"WWW-Authenticate": "Bearer"}
            )
        response.headers["Cache-Control"] = "no-store"

    secured = [Depends(authorize)]

    def current(attempt_id: str) -> tuple[Attempt, Result | None]:
        with store.worker_attempt(grant.campaign_id, grant.worker_id, attempt_id) as state:
            if state is None:
                raise HTTPException(410, "Attempt is no longer assigned to this worker")
            return state

    def charge(attempt: Attempt, input_tokens: int = 0, output_tokens: int = 0) -> float:
        elapsed = max(0.0, time.time() - attempt.started_at)
        return meter.charge(spec, attempt, elapsed, input_tokens, output_tokens)

    @router.post("/requeue", dependencies=secured)
    def requeue() -> int:
        return store.requeue_expired(grant.campaign_id, time.time() - 30)

    @router.post("/claim", dependencies=secured)
    def claim(campaign_id: str, worker_id: str) -> dict[str, Any] | None:
        if (campaign_id, worker_id) != (grant.campaign_id, grant.worker_id):
            raise HTTPException(403, "Worker grant does not cover this campaign and identity")
        attempt = store.claim(
            grant.campaign_id,
            grant.worker_id,
            reserve_cost=lambda trial: meter.estimate(spec, trial),
        )
        if attempt is None:
            return None
        return {"attempt": asdict(attempt), "token": attempt_token(attempt.attempt_id)}

    @router.post("/{attempt_id}/heartbeat", dependencies=secured)
    def heartbeat(attempt_id: str) -> bool:
        attempt, _ = current(attempt_id)
        return store.heartbeat(attempt_id, cost=charge(attempt))

    @router.post("/{attempt_id}/report", dependencies=secured)
    def report(attempt_id: str, body: Observation) -> bool:
        attempt, previous = current(attempt_id)
        if body.status == "ok" and spec.metric not in body.metrics:
            raise HTTPException(422, "Successful result requires the campaign metric")
        result = Result(attempt=attempt, **body.model_dump())
        if previous is None:
            cost = charge(attempt, body.input_tokens, body.output_tokens)
            if store.report(replace(result, cost=cost)):
                return True
            _, previous = current(attempt_id)
        if previous is not None and replace(previous, cost=0.0) == result:
            return False
        if (
            previous is not None
            and previous.status == "lost"
            and previous.error == "Heartbeat expired"
        ):
            raise HTTPException(410, "Attempt expired")
        raise HTTPException(409, "Terminal observation differs")

    @router.put("/{attempt_id}/artifact", dependencies=secured)
    async def artifact(
        attempt_id: str,
        request: Request,
        content_sha256: Annotated[str, Header(pattern="^[0-9a-f]{64}$")],
    ) -> dict[str, Any]:
        if current(attempt_id)[1] is not None:
            raise HTTPException(409, "Attempt is no longer running")
        with NamedTemporaryFile() as staged:
            digest = hashlib.sha256()
            size = 0
            async for chunk in request.stream():
                size += len(chunk)
                if size > MAX_ARTIFACT_BYTES:
                    raise HTTPException(413, "Artifact exceeds upload limit")
                digest.update(chunk)
                staged.write(chunk)
            if digest.hexdigest() != content_sha256:
                raise HTTPException(422, "Content digest differs")
            staged.flush()
            # A claim can expire while the client is streaming. Fence the final publication.
            with store.worker_attempt(
                grant.campaign_id, grant.worker_id, attempt_id, write=True
            ) as state:
                if state is None or state[1] is not None:
                    raise HTTPException(409, "Attempt is no longer running")
                return files.put(Path(staged.name)).model_dump(mode="json")

    return router
