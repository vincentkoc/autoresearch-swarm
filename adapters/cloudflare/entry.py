"""Local-only feasibility fixture. This is not the production research API."""

import json
from typing import Literal
from urllib.parse import urlparse

from atomic import atomic
from pydantic import Field
from workers import DurableObject, Response, WorkerEntrypoint, import_from_javascript

from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import VersionedRecord

# RFC 8032 public test vector. No operational key enters this fixture.
FIXTURE_SEED = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
FIXTURE_SIGNATURE = (
    "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555f"
    "b8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
)


class Probe(VersionedRecord):
    key: str = Field(pattern=r"^[a-z0-9-]{1,64}$")
    mode: Literal["accept", "reject", "hook_failure"]
    value: int = Field(strict=True, ge=0, le=1_000_000)


class Qualification(DurableObject):
    async def exercise(self, body):
        request = Probe.model_validate_json(body)
        # Loading may suspend through JSPI; finish it before entering SQL transactions.
        crypto = import_from_javascript("./crypto.mjs")
        storage = self.ctx.storage
        sql = storage.sql
        sql.exec("CREATE TABLE IF NOT EXISTS effects (key TEXT PRIMARY KEY, value INTEGER)")
        sql.exec("CREATE TABLE IF NOT EXISTS fences (key TEXT PRIMARY KEY)")
        sql.exec(
            "CREATE TABLE IF NOT EXISTS acknowledgments "
            "(key TEXT PRIMARY KEY, digest TEXT, body TEXT)"
        )
        payload = canonical_bytes(request.model_dump(mode="json"))
        digest = content_digest(payload)

        def outer():
            prior = list(
                sql.exec("SELECT digest,body FROM acknowledgments WHERE key=?", request.key)
            )
            if prior:
                if prior[0].digest != digest:
                    raise ValueError("IDEMPOTENCY_CONFLICT")
                return str(prior[0].body)

            def inner():
                sql.exec("INSERT INTO effects VALUES (?,?)", request.key, request.value)
                if crypto.sign(FIXTURE_SEED, "") != FIXTURE_SIGNATURE:
                    raise ValueError("RFC 8032 signature vector failed inside the transaction")
                signature = crypto.sign(FIXTURE_SEED, payload.hex())
                if not isinstance(signature, str):
                    raise TypeError("Signing returned a Promise or non-string value")
                if not crypto.verify(crypto.publicKey(FIXTURE_SEED), payload.hex(), signature):
                    raise ValueError("Synchronous signature verification failed")
                if request.mode != "accept":
                    raise ResearchError(ErrorCode.INVALID_TRANSITION, "synthetic domain rejection")
                return signature

            try:
                signature = atomic(storage, inner)
                response = {
                    "state": "accepted",
                    "signature": signature,
                    "payload_hex": payload.hex(),
                }
            except ResearchError as error:
                sql.exec("INSERT INTO fences VALUES (?)", request.key)
                if request.mode == "hook_failure":
                    raise ValueError("synthetic rejection-hook failure") from error
                response = {"state": "rejected", "code": error.code.value}
            encoded = json.dumps(response)
            sql.exec("INSERT INTO acknowledgments VALUES (?,?,?)", request.key, digest, encoded)
            return encoded

        try:
            result = json.loads(atomic(storage, outer))
        except Exception as error:
            result = {"state": "raised", "error": str(error)}
        result["counts"] = {
            table: list(sql.exec(f"SELECT COUNT(*) AS n FROM {table} WHERE key=?", request.key))[
                0
            ].n
            for table in ("effects", "fences", "acknowledgments")
        }
        result["public_key"] = crypto.publicKey(FIXTURE_SEED)
        return json.dumps(result)


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        url = urlparse(request.url)
        if url.hostname not in {"127.0.0.1", "localhost"}:
            return Response("Local qualification only", status=403)
        if request.method != "POST" or url.path != "/qualification":
            return Response("Not found", status=404)
        text = await request.text()
        if len(text.encode()) > 4096:
            return Response("Request too large", status=413)
        stub = self.env.QUALIFICATION.getByName("contract-fixture")
        return Response(await stub.exercise(text), headers={"content-type": "application/json"})
