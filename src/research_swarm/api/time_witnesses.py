"""Private native time endpoint. It exposes no research state or admission commands."""

import asyncio
import hmac
import math
import re

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import TypeAdapter
from starlette.requests import ClientDisconnect

from research_swarm.api.http import read_body
from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.control.time_witnesses import MAX_WITNESS_BYTES
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import TimeWitnessRequest
from research_swarm.domain.types import Digest


def witness_consumer_digest(token: str) -> str:
    """Hash an explicitly provisioned high-entropy token; never include it in a record."""
    if not isinstance(token, str) or not 43 <= len(token) <= 128:
        raise ValueError("Invalid witness consumer credential")
    if re.fullmatch(r"[A-Za-z0-9_-]+", token) is None:
        raise ValueError("Invalid witness consumer credential")
    return record_digest("research.time-witness-consumer", {"token": token})


def _failure(code: ErrorCode, status: int, *, retryable: bool = False) -> JSONResponse:
    return JSONResponse(
        {"schema_version": "1.0", "error": {"code": code.value, "retryable": retryable}},
        status_code=status,
        headers={"cache-control": "no-store"},
    )


def create_time_witness_app(
    source: NativeWitnessSource, consumer_digest: str, *, body_timeout_seconds: float = 1.0
) -> FastAPI:
    """Pin one consumer digest and one source; provisioning is not an HTTP operation.

    Bind privately or behind authenticated TLS. Credential rotation and source
    replacement follow the deployment's explicit clock requalification lifecycle.
    """
    expected_digest = TypeAdapter(Digest).validate_python(consumer_digest)
    if (
        isinstance(body_timeout_seconds, bool)
        or not math.isfinite(body_timeout_seconds)
        or not 0 < body_timeout_seconds <= 10
    ):
        raise ValueError("Witness body timeout must be positive and at most ten seconds")
    app = FastAPI(
        title="Private native time witness", docs_url=None, redoc_url=None, openapi_url=None
    )

    @app.post("/v1/time-witness")
    async def observe(request: Request) -> Response:
        authorization = request.headers.getlist("authorization")
        if len(authorization) != 1 or not authorization[0].startswith("Bearer "):
            return _failure(ErrorCode.AUTHORIZATION_DENIED, 401)
        try:
            presented = witness_consumer_digest(authorization[0][7:])
        except ValueError:
            return _failure(ErrorCode.AUTHORIZATION_DENIED, 401)
        if not hmac.compare_digest(expected_digest, presented):
            return _failure(ErrorCode.AUTHORIZATION_DENIED, 401)
        media = request.headers.getlist("content-type")
        encoding = request.headers.getlist("content-encoding")
        if (
            len(media) != 1
            or media[0].partition(";")[0].strip().lower() != "application/json"
            or len(encoding) > 1
            or (encoding and encoding[0].lower() != "identity")
        ):
            return _failure(ErrorCode.PROJECT_CONTRACT_INVALID, 415)
        try:
            async with asyncio.timeout(body_timeout_seconds):
                body = await read_body(request, MAX_WITNESS_BYTES)
            payload = TimeWitnessRequest.model_validate(strict_json_loads(body))
        except TimeoutError:
            return _failure(ErrorCode.TRANSPORT_UNAVAILABLE, 408, retryable=True)
        except ClientDisconnect:
            return _failure(ErrorCode.PROJECT_CONTRACT_INVALID, 400)
        except (ValueError, UnicodeError, RecursionError, ResearchError):
            return _failure(ErrorCode.PROJECT_CONTRACT_INVALID, 422)
        try:
            witness = source.observe(payload)
            encoded = canonical_bytes(witness.model_dump(mode="json"))
            if len(encoded) > MAX_WITNESS_BYTES:
                return _failure(ErrorCode.TRANSPORT_UNAVAILABLE, 503, retryable=True)
            return Response(
                encoded, media_type="application/json", headers={"cache-control": "no-store"}
            )
        except ResearchError as error:
            if error.code == ErrorCode.AUTHORIZATION_DENIED:
                return _failure(ErrorCode.AUTHORIZATION_DENIED, 403)
            if error.code == ErrorCode.STALE_FENCE:
                return _failure(ErrorCode.STALE_FENCE, 409)
            return _failure(ErrorCode.TRANSPORT_UNAVAILABLE, 503, retryable=True)
        except Exception:
            return _failure(ErrorCode.TRANSPORT_UNAVAILABLE, 503, retryable=True)

    return app
