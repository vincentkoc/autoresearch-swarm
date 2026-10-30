"""Local-only feasibility fixture. This is not the production research API."""

import json
import sys
from typing import Literal
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, uuid5

from durable_sql import DurableObjectStore
from pydantic import Field
from storage_qualification import exercise as exercise_storage
from workers import DurableObject, Response, WorkerEntrypoint, import_from_javascript

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import VersionedRecord
from research_swarm.state.database import SCHEMA, Database

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
    async def storage_contract(self, case):
        return json.dumps(exercise_storage(self.ctx.storage, str(self.ctx.id), case))

    async def exercise(self, body):
        request = Probe.model_validate_json(body)
        # Loading may suspend through JSPI; finish it before entering SQL transactions.
        crypto = import_from_javascript("./crypto.mjs")
        store = DurableObjectStore(
            self.ctx.storage,
            namespace="local-qualification",
            object_id=str(self.ctx.id),
            schema=SCHEMA,
        )
        database = Database.from_store(store)
        workspace, controller, actor = (
            str(uuid5(NAMESPACE_URL, "research-qualification/" + name))
            for name in ("workspace", "controller", "actor")
        )
        database.bootstrap(workspace, controller)
        context = CommandContext(
            principal=Principal(id=actor, workspace_id=workspace, scopes={"*"}),
            idempotency_key=request.key,
        )
        payload = canonical_bytes(request.model_dump(mode="json"))
        effect_id, fence_id = (
            str(uuid5(NAMESPACE_URL, request.key + "/" + name)) for name in ("effect", "fence")
        )

        def operation(transaction):
            row = transaction.insert(
                "fixture_effect", effect_id, {"value": request.value}, state="saved"
            )
            transaction.event("fixture.effect", row, {})
            if crypto.sign(FIXTURE_SEED, "") != FIXTURE_SIGNATURE:
                raise ValueError("RFC 8032 signature vector failed inside the transaction")
            signature = crypto.sign(FIXTURE_SEED, payload.hex())
            if not isinstance(signature, str):
                raise TypeError("Signing returned a Promise or non-string value")
            if not crypto.verify(crypto.publicKey(FIXTURE_SEED), payload.hex(), signature):
                raise ValueError("Synchronous signature verification failed")
            if request.mode != "accept":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "synthetic domain rejection")
            return {"signature": signature, "payload_hex": payload.hex()}

        def fence(transaction, error):
            row = transaction.insert("fixture_fence", fence_id, {}, state="fenced")
            transaction.event("fixture.fenced", row, {})
            if request.mode == "hook_failure":
                raise ValueError("synthetic rejection-hook failure") from error

        try:
            acknowledgment = database.execute(
                context,
                "fixture.exercise",
                request.model_dump(mode="json"),
                "write",
                operation,
                on_rejection=fence,
            )
            result = {
                "state": "accepted",
                "command_id": acknowledgment["command_id"],
                **acknowledgment["result"],
            }
        except ResearchError as error:
            result = (
                {"state": "rejected", "code": error.code.value, "command_id": error.command_id}
                if error.code == ErrorCode.INVALID_TRANSITION
                else {"state": "raised", "error": error.code.value}
            )
        except Exception as error:
            result = {"state": "raised", "error": str(error)}

        def counts(sql):
            return {
                "effects": sql.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='fixture_effect' AND id=?",
                    (effect_id,),
                ).one()[0],
                "fences": sql.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='fixture_fence' AND id=?", (fence_id,)
                ).one()[0],
                "acknowledgments": sql.execute(
                    "SELECT COUNT(*) FROM commands WHERE idempotency_key=?", (request.key,)
                ).one()[0],
            }

        result["counts"] = store.read(counts)
        result["public_key"] = crypto.publicKey(FIXTURE_SEED)
        assert "sqlite3" not in sys.modules and "research_swarm.state.sqlite" not in sys.modules
        return json.dumps(result)


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        url = urlparse(request.url)
        if url.hostname not in {"127.0.0.1", "localhost"}:
            return Response("Local qualification only", status=403)
        if request.method != "POST":
            return Response("Not found", status=404)
        if url.path.startswith("/storage/"):
            case = url.path.removeprefix("/storage/")
            if case not in {"roundtrip", "unknown-version", "foreign-state", "migration-rollback"}:
                return Response("Not found", status=404)
            stub = self.env.QUALIFICATION.getByName("storage-contract-" + case)
            return Response(
                await stub.storage_contract(case), headers={"content-type": "application/json"}
            )
        if url.path != "/qualification":
            return Response("Not found", status=404)
        text = await request.text()
        if len(text.encode()) > 4096:
            return Response("Request too large", status=413)
        stub = self.env.QUALIFICATION.getByName("contract-fixture")
        return Response(await stub.exercise(text), headers={"content-type": "application/json"})
