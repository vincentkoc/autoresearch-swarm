"""Local-only feasibility fixture. This is not the production research API."""

import asyncio
import hashlib
import json
import sys
from typing import Literal
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, uuid5

from delivery_router import delivery_error
from durable_sql import DurableObjectStore
from ed25519 import NobleEd25519
from exclusive import exclusive
from hosted_http import failure, uncertain
from hosted_http import handle as handle_http
from hosted_http import route as http_route
from http_binding import HTTPBodyReader
from native_router_qualification import NativeProbe, open_native_router
from native_router_qualification import exercise as exercise_native_router
from pydantic import Field
from r2_qualification import exercise as exercise_r2
from router_qualification import RouterProbe, configuration, delivery_fixture, open_router
from router_qualification import exercise as exercise_router
from signing_qualification import exercise as exercise_signing
from storage_qualification import exercise as exercise_storage
from witness_qualification import exercise as exercise_witness
from workers import (
    DurableObject,
    Response,
    WorkerEntrypoint,
    import_from_javascript,
    python_to_rpc,
)

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


class OrderingProbe(VersionedRecord):
    key: str = Field(pattern=r"^[a-z0-9-]{1,64}$")
    case: Literal["sample", "changed-source", "domain-reject"] = "sample"
    witness_origin: str = Field(max_length=256)
    timeout_ms: int = Field(strict=True, ge=100, le=10_000, default=8_000)
    bypass_gate: bool = Field(strict=True, default=False)


class Qualification(DurableObject):
    async def fetch(self, request):
        # Local-only HTTP probe: synthetic state must be provisioned explicitly first.
        delivery = urlparse(request.url).path.startswith("/v1/delivery/")
        module = import_from_javascript("./crypto.mjs")
        transport = import_from_javascript("./fixture_transport.mjs")
        bodies = import_from_javascript("./http_io.mjs")
        r2 = import_from_javascript("./r2_io.mjs")
        try:
            key = (
                "http-"
                + hashlib.sha256(
                    (
                        request.method
                        + "\0"
                        + urlparse(request.url).path
                        + "\0"
                        + (request.headers.get("idempotency-key") or "")
                    ).encode()
                ).hexdigest()[:48]
            )
            probe = RouterProbe(
                action="inspect",
                key=key,
                artifact_profile=True,
                witness_origin=request.headers.get("x-fixture-witness-origin") or "",
                r2_fault=request.headers.get("x-fixture-r2-fault") or "none",
            )
            crypto = NobleEd25519(module)
            if delivery or request.headers.get("x-fixture-native-source") == "true":
                native_transport = import_from_javascript("./witness_io.mjs")
                router = await open_native_router(
                    self.ctx,
                    str(self.ctx.id),
                    crypto,
                    native_transport,
                    bucket=python_to_rpc(self.env.ARTIFACTS),
                    r2_module=r2,
                    r2_unavailable=request.headers.get("x-fixture-r2-fault") == "unavailable",
                )
            else:
                config, _ = configuration(str(self.ctx.id), crypto, artifact_profile=True)
                router = await open_router(
                    self.ctx,
                    str(self.ctx.id),
                    probe,
                    config,
                    crypto,
                    transport,
                    bucket=python_to_rpc(self.env.ARTIFACTS),
                    r2_module=r2,
                )
            handler = (await delivery_fixture(router)).handle if delivery else None
            arguments = (
                request.method,
                request.url,
                dict(request.headers),
                HTTPBodyReader(python_to_rpc(request), bodies),
            )
            result = await handler(*arguments) if handler else await handle_http(router, *arguments)
        except ResearchError as error:
            result = delivery_error(error) if delivery else failure(error)
        if (
            request.method == "POST"
            and result.status == 200
            and (
                (
                    request.headers.get("x-fixture-drop-finalize") == "true"
                    and urlparse(request.url).path.endswith("/finalize")
                )
                or urlparse(request.url).path
                == "/v1/commands/" + (request.headers.get("x-fixture-drop-command") or "")
            )
        ):
            return Response("{", headers={"content-type": "application/json"})
        return Response(result.body, status=result.status, headers=dict(result.headers))

    async def native_contract(self, body):
        crypto = NobleEd25519(import_from_javascript("./crypto.mjs"))
        transport = import_from_javascript("./witness_io.mjs")
        return json.dumps(
            await exercise_native_router(
                self.ctx,
                str(self.ctx.id),
                NativeProbe.model_validate_json(body),
                crypto,
                transport,
                bucket=python_to_rpc(self.env.ARTIFACTS),
                r2_module=import_from_javascript("./r2_io.mjs"),
            )
        )

    async def r2_contract(self, case):
        module = import_from_javascript("./r2_io.mjs")
        # Pass the raw JS binding, not a PyProxy whose mapping get shadows R2.get.
        bucket = python_to_rpc(self.env.ARTIFACTS)
        return json.dumps(await exercise_r2(bucket, module, case))

    async def research_contract(self, body):
        probe = RouterProbe.model_validate_json(body)
        crypto = import_from_javascript("./crypto.mjs")
        transport = import_from_javascript("./fixture_transport.mjs")
        artifact_arguments = {}
        if probe.artifact_profile:
            artifact_arguments = {
                "bucket": python_to_rpc(self.env.ARTIFACTS),
                "r2_module": import_from_javascript("./r2_io.mjs"),
            }
        return json.dumps(
            await exercise_router(
                self.ctx, str(self.ctx.id), probe, crypto, transport, **artifact_arguments
            )
        )

    async def witness_contract(self, case, fail_bootstrap):
        module = import_from_javascript("./crypto.mjs")

        async def operation():
            return await exercise_witness(
                self.ctx.storage, str(self.ctx.id), case, module, fail_bootstrap=fail_bootstrap
            )

        return json.dumps(await exclusive(self.ctx, operation))

    async def witness_ordering(self, body):
        request = OrderingProbe.model_validate_json(body)
        # Imports may suspend. Finish loading both modules before entering the gate.
        crypto = import_from_javascript("./crypto.mjs")
        transport = import_from_javascript("./fixture_transport.mjs")

        async def acquire(witness):
            try:
                text = await transport.request(
                    request.witness_origin,
                    "/witness/" + request.key,
                    canonical_bytes(witness.model_dump(mode="json")).decode(),
                    float(request.timeout_ms),
                )
            except Exception as error:
                raise ResearchError(
                    ErrorCode.TRANSPORT_UNAVAILABLE, "Local witness fixture unavailable"
                ) from error
            return text.encode()

        async def operation():
            return await exercise_witness(
                self.ctx.storage,
                str(self.ctx.id),
                request.case,
                crypto,
                key=request.key,
                acquire=acquire,
            )

        # Local-only negative control; not a supported authority configuration.
        return json.dumps(
            await operation() if request.bypass_gate else await exclusive(self.ctx, operation)
        )

    async def signing_contract(self, body):
        module = import_from_javascript("./crypto.mjs")
        try:
            result = exercise_signing(self.ctx.storage, str(self.ctx.id), body, module)
        except ResearchError as error:
            result = {"error": error.code.value}
        return json.dumps(result)

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


async def observed_rpc(invoke_rpc, transport, origin, key):
    submitted = asyncio.Event()

    async def invoke():
        try:
            delivery = invoke_rpc()
        finally:
            submitted.set()
        return await delivery

    delivery = asyncio.create_task(invoke())
    try:
        await submitted.wait()
        if delivery.done():
            delivery.result()
        # Observe RPC submission outside the blocked DO. The bypass control
        # must expose interleaving under this same delivery path.
        await transport.request(origin, "/ingress/" + key, "{}", 10_000.0)
        return Response(await delivery, headers={"content-type": "application/json"})
    finally:
        if not delivery.done():
            delivery.cancel()
            await asyncio.gather(delivery, return_exceptions=True)


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        url = urlparse(request.url)
        if url.hostname not in {"127.0.0.1", "localhost"}:
            return Response("Local qualification only", status=403)
        if url.path.startswith("/v1/delivery/"):
            # Delivery uses one explicitly provisioned authority, regardless of caller headers.
            stub = self.env.QUALIFICATION.getByName("shared-native-research-orchestration")
            try:
                return await stub.fetch(request)
            except Exception:
                result = delivery_error()
                return Response(result.body, status=result.status, headers=dict(result.headers))
        if url.path.startswith("/v1/"):
            name = (
                "shared-native-router-contract"
                if request.headers.get("x-fixture-native-source") == "true"
                else "shared-router-contract-artifacts"
            )
            if request.headers.get("x-fixture-research-profile") == "true":
                name = "shared-native-research-admission"
            if request.headers.get("x-fixture-input-profile") == "true":
                name = "shared-native-research-inputs"
            if request.headers.get("x-fixture-output-profile") == "true":
                name = "shared-native-research-outputs"
            if request.headers.get("x-fixture-publication-profile") == "true":
                name = "shared-native-research-publication"
            if request.headers.get("x-fixture-evaluation-profile") == "true":
                name = "shared-native-research-evaluation"
            if request.headers.get("x-fixture-lifecycle-profile") == "true":
                name = "shared-native-research-lifecycle"
            if request.headers.get("x-fixture-scientific-profile") == "true":
                name = "shared-native-research-scientific"
            if request.headers.get("x-fixture-orchestration-profile") == "true":
                name = "shared-native-research-orchestration"
            stub = self.env.QUALIFICATION.getByName(name)
            try:
                kind, _, _ = http_route(request.method, request.url)
                mode = (
                    "upload_part"
                    if kind in {"upload", "worker-upload"}
                    else "command"
                    if kind in {"command", "finalize"}
                    else "query"
                )
            except ResearchError as error:
                result = failure(error)
                return Response(result.body, status=result.status, headers=dict(result.headers))
            except ValueError:
                result = failure(
                    ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid target")
                )
                return Response(result.body, status=result.status, headers=dict(result.headers))
            try:
                return await stub.fetch(request)
            except Exception:
                result = uncertain(mode)
                return Response(result.body, status=result.status, headers=dict(result.headers))
        if request.method != "POST":
            return Response("Not found", status=404)
        if url.path == "/native-research":
            text = await request.text()
            if len(text.encode()) > 4096:
                return Response("Request too large", status=413)
            probe = NativeProbe.model_validate_json(text)
            stub = self.env.QUALIFICATION.getByName(
                "shared-native-research-orchestration"
                if probe.orchestration_profile
                else "shared-native-research-scientific"
                if probe.scientific_profile
                else "shared-native-research-lifecycle"
                if probe.lifecycle_profile
                else "shared-native-research-evaluation"
                if probe.evaluation_profile
                else "shared-native-research-publication"
                if probe.publication_profile
                else "shared-native-research-outputs"
                if probe.output_profile
                else "shared-native-research-inputs"
                if probe.input_profile
                else "shared-native-research-admission"
                if probe.research_profile
                else "shared-native-router-contract"
            )
            return Response(
                await stub.native_contract(probe.model_dump_json()),
                headers={"content-type": "application/json"},
            )
        if url.path.startswith("/r2-bytes/"):
            case = url.path.removeprefix("/r2-bytes/")
            if case not in {"roundtrip", "limit", "lost-response", "late", "retired", "corrupt"}:
                return Response("Not found", status=404)
            stub = self.env.QUALIFICATION.getByName("async-r2-binding-contract")
            return Response(
                await stub.r2_contract(case), headers={"content-type": "application/json"}
            )
        if url.path == "/research":
            text = await request.text()
            if len(text.encode()) > 65536:
                return Response("Request too large", status=413)
            probe = RouterProbe.model_validate_json(text)
            transport = import_from_javascript("./fixture_transport.mjs")
            suffix = "-bypass" if probe.bypass_gate else ""
            if probe.artifact_profile:
                suffix += "-artifacts"
            stub = self.env.QUALIFICATION.getByName("shared-router-contract" + suffix)
            return await observed_rpc(
                lambda: stub.research_contract(text), transport, probe.witness_origin, probe.key
            )
        if url.path == "/witness-ordering":
            text = await request.text()
            if len(text.encode()) > 4096:
                return Response("Request too large", status=413)
            probe = OrderingProbe.model_validate_json(text)
            transport = import_from_javascript("./fixture_transport.mjs")
            suffix = "-bypass" if probe.bypass_gate else ""
            stub = self.env.QUALIFICATION.getByName("witness-ordering-" + probe.case + suffix)
            return await observed_rpc(
                lambda: stub.witness_ordering(text), transport, probe.witness_origin, probe.key
            )
        if url.path.startswith("/witness/"):
            case = url.path.removeprefix("/witness/")
            if case not in {"sample", "changed-source", "domain-reject"}:
                return Response("Not found", status=404)
            stub = self.env.QUALIFICATION.getByName("witness-contract-" + case)
            return Response(
                await stub.witness_contract(case, url.query == "fail-bootstrap=1"),
                headers={"content-type": "application/json"},
            )
        if url.path == "/signing":
            text = await request.text()
            if len(text.encode()) > 65536:
                return Response("Request too large", status=413)
            stub = self.env.QUALIFICATION.getByName("signing-contract")
            return Response(
                await stub.signing_contract(text), headers={"content-type": "application/json"}
            )
        if url.path.startswith("/storage/"):
            case = url.path.removeprefix("/storage/")
            if case not in {
                "roundtrip",
                "unknown-version",
                "foreign-state",
                "migration-rollback",
                "existing-empty",
                "existing-current",
                "existing-old",
                "existing-column",
                "existing-index",
                "existing-marker",
            }:
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
