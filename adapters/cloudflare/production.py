"""Production Worker: one fixed authority, with a separate private bootstrap entrypoint."""

from checkpoint_admission import admit_target
from checkpoint_election import elect_source
from checkpoint_source import capture_source, seal_source
from checkpoint_target import import_target, request_target
from checkpoint_wire import require_rpc_string
from deployment import load_deployment
from durable_sql import DurableObjectStore
from ed25519 import NobleEd25519
from hosted_http import json_response
from http_binding import HTTPBodyReader
from provisioning import provision
from r2_binding import R2BindingTransport
from runtime import fixed_target, open_router, public_route, runtime_failure, serve
from witness_binding import WitnessHTTPTransport
from workers import DurableObject, Response, WorkerEntrypoint, import_from_javascript, python_to_rpc

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import SCHEMA
from research_swarm.state.sql import StoreIdentity
from research_swarm.storage.async_r2 import BoundedR2Objects


def deployment(env):
    crypto = NobleEd25519(import_from_javascript("./crypto.mjs"))
    manifest, secrets = load_deployment(
        getattr(env, "RESEARCH_MANIFEST", None),
        getattr(env, "RESEARCH_SECRETS", None),
        crypto=crypto,
    )
    return manifest, secrets, crypto


def response(result):
    return Response(result.body, status=result.status, headers=dict(result.headers))


def witness(manifest, secrets):
    return WitnessHTTPTransport(
        import_from_javascript("./witness_io.mjs"),
        manifest.witness_origin,
        secrets.witness_consumer,
        timeout_ms=manifest.witness_timeout_ms,
    )


async def source_checkpoint(state, env, credential, request_json, operation):
    try:
        manifest, secrets, crypto = deployment(env)
        secrets.require_bootstrap(credential)
        fixed_target(env.RESEARCH_AUTHORITY, manifest, actual_id=state.id)
        result = await operation(
            state,
            lambda: DurableObjectStore.open_existing(
                state.storage, namespace=manifest.namespace, object_id=str(state.id)
            ),
            manifest,
            secrets,
            identity=StoreIdentity("cloudflare-do", manifest.namespace + "/" + str(state.id)),
            credential=credential,
            source_json=getattr(env, "RESEARCH_CHECKPOINT_SOURCE", None),
            request_json=request_json,
            crypto=crypto,
        )
        return result if isinstance(result, str) else json_response(result).body.decode()
    except ResearchError as error:
        result = runtime_failure(error, mode="command")
    except Exception:
        result = runtime_failure(mode="command")
    return result.body.decode()


async def target_checkpoint(state, env, credential, operation, **arguments):
    try:
        manifest, secrets, crypto = deployment(env)
        secrets.require_bootstrap(credential)
        fixed_target(env.RESEARCH_AUTHORITY, manifest, actual_id=state.id)
        return await operation(
            state,
            lambda: DurableObjectStore.open_existing(
                state.storage, namespace=manifest.namespace, object_id=str(state.id)
            ),
            manifest,
            secrets,
            identity=StoreIdentity("cloudflare-do", manifest.namespace + "/" + str(state.id)),
            credential=credential,
            target_json=getattr(env, "RESEARCH_CHECKPOINT_TARGET", None),
            crypto=crypto,
            **arguments,
        )
    except ResearchError as error:
        result = runtime_failure(error, mode="command")
    except Exception:
        result = runtime_failure(mode="command")
    return result.body.decode()


class ResearchAuthority(DurableObject):
    async def checkpoint_request(self, credential):
        return await target_checkpoint(self.ctx, self.env, credential, request_target)

    async def checkpoint_admit(self, credential, election_json):
        return await target_checkpoint(
            self.ctx, self.env, credential, admit_target, election_json=election_json
        )

    async def checkpoint_import(self, credential, bundle_json):
        try:
            manifest, secrets, crypto = deployment(self.env)
            secrets.require_bootstrap(credential)
            fixed_target(self.env.RESEARCH_AUTHORITY, manifest, actual_id=self.ctx.id)
            result = await import_target(
                self.ctx,
                lambda: DurableObjectStore.open_for_provisioning(
                    self.ctx.storage,
                    namespace=manifest.namespace,
                    object_id=str(self.ctx.id),
                    schema=SCHEMA,
                ),
                manifest,
                secrets,
                identity=StoreIdentity(
                    "cloudflare-do", manifest.namespace + "/" + str(self.ctx.id)
                ),
                credential=credential,
                target_json=getattr(self.env, "RESEARCH_CHECKPOINT_TARGET", None),
                bundle_json=bundle_json,
                crypto=crypto,
            )
            return json_response(result).body.decode()
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        return result.body.decode()

    async def checkpoint_seal(self, credential, request_json):
        return await source_checkpoint(self.ctx, self.env, credential, request_json, seal_source)

    async def checkpoint_capture(self, credential, request_json):
        return await source_checkpoint(self.ctx, self.env, credential, request_json, capture_source)

    async def checkpoint_elect(self, credential, request_json):
        return await source_checkpoint(self.ctx, self.env, credential, request_json, elect_source)

    async def fetch(self, request):
        mode = "query"
        try:
            mode = public_route(request.method, request.url)
            manifest, secrets, crypto = deployment(self.env)
            fixed_target(self.env.RESEARCH_AUTHORITY, manifest, actual_id=self.ctx.id)
            acquire = witness(manifest, secrets)
            storage = manifest.configuration.storage
            objects = BoundedR2Objects(
                storage,
                R2BindingTransport(
                    python_to_rpc(self.env.ARTIFACTS),
                    import_from_javascript("./r2_io.mjs"),
                    storage,
                ),
            )
            bodies = import_from_javascript("./http_io.mjs")
            router = await open_router(
                self.ctx,
                lambda: DurableObjectStore.open_existing(
                    self.ctx.storage, namespace=manifest.namespace, object_id=str(self.ctx.id)
                ),
                manifest,
                secrets,
                crypto=crypto,
                acquire=acquire,
                objects=objects,
            )
            result = await serve(
                router,
                manifest,
                secrets,
                request.method,
                request.url,
                dict(request.headers),
                HTTPBodyReader(python_to_rpc(request), bodies),
            )
        except ResearchError as error:
            result = runtime_failure(error, mode=mode)
        except Exception:
            result = runtime_failure(mode=mode)
        return response(result)

    async def provision(self, credential):
        # DO RPC is binding-only. Authenticate again at the physical state owner.
        try:
            manifest, secrets, crypto = deployment(self.env)
            secrets.require_bootstrap(credential)
            fixed_target(self.env.RESEARCH_AUTHORITY, manifest, actual_id=self.ctx.id)
            receipt = await provision(
                self.ctx,
                lambda: DurableObjectStore.open_for_provisioning(
                    self.ctx.storage,
                    namespace=manifest.namespace,
                    object_id=str(self.ctx.id),
                    schema=SCHEMA,
                ),
                manifest,
                secrets,
                identity=StoreIdentity(
                    "cloudflare-do", manifest.namespace + "/" + str(self.ctx.id)
                ),
                credential=credential,
                crypto=crypto,
                acquire=witness(manifest, secrets),
            )
            result = json_response({"schema_version": "1.0", "receipt": receipt})
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        # Keep RPC values JSON-native and avoid transferring Python proxies.
        return result.body.decode()


class Bootstrap(WorkerEntrypoint):
    """Bind this named entrypoint only to the operator's private service."""

    async def fetch(self, request):
        return response(runtime_failure(ResearchError(ErrorCode.NOT_FOUND, "RPC only")))

    async def checkpoint_admit(self, credential, election_json):
        try:
            manifest, secrets, _ = deployment(self.env)
            secrets.require_bootstrap(credential)
            identifier = fixed_target(self.env.RESEARCH_AUTHORITY, manifest)
            return await self.env.RESEARCH_AUTHORITY.get(identifier).checkpoint_admit(
                credential, election_json
            )
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        return result.body.decode()

    async def checkpoint_request(self, credential):
        try:
            manifest, secrets, _ = deployment(self.env)
            secrets.require_bootstrap(credential)
            identifier = fixed_target(self.env.RESEARCH_AUTHORITY, manifest)
            return await self.env.RESEARCH_AUTHORITY.get(identifier).checkpoint_request(credential)
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        return result.body.decode()

    async def checkpoint_elect(self, credential, request_json):
        try:
            manifest, secrets, _ = deployment(self.env)
            secrets.require_bootstrap(credential)
            identifier = fixed_target(self.env.RESEARCH_AUTHORITY, manifest)
            return await self.env.RESEARCH_AUTHORITY.get(identifier).checkpoint_elect(
                credential, request_json
            )
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        return result.body.decode()

    async def checkpoint_import(self, credential, bundle_json):
        try:
            manifest, secrets, _ = deployment(self.env)
            secrets.require_bootstrap(credential)
            identifier = fixed_target(self.env.RESEARCH_AUTHORITY, manifest)
            require_rpc_string(bundle_json)
            return await self.env.RESEARCH_AUTHORITY.get(identifier).checkpoint_import(
                credential, bundle_json
            )
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        return result.body.decode()

    async def checkpoint_seal(self, credential, request_json):
        try:
            manifest, secrets, _ = deployment(self.env)
            secrets.require_bootstrap(credential)
            identifier = fixed_target(self.env.RESEARCH_AUTHORITY, manifest)
            return await self.env.RESEARCH_AUTHORITY.get(identifier).checkpoint_seal(
                credential, request_json
            )
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        return result.body.decode()

    async def checkpoint_capture(self, credential, request_json):
        try:
            manifest, secrets, _ = deployment(self.env)
            secrets.require_bootstrap(credential)
            identifier = fixed_target(self.env.RESEARCH_AUTHORITY, manifest)
            return await self.env.RESEARCH_AUTHORITY.get(identifier).checkpoint_capture(
                credential, request_json
            )
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        return result.body.decode()

    async def provision(self, credential):
        try:
            manifest, secrets, _ = deployment(self.env)
            secrets.require_bootstrap(credential)
            identifier = fixed_target(self.env.RESEARCH_AUTHORITY, manifest)
            return await self.env.RESEARCH_AUTHORITY.get(identifier).provision(credential)
        except ResearchError as error:
            result = runtime_failure(error, mode="command")
        except Exception:
            result = runtime_failure(mode="command")
        return result.body.decode()


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        mode = "query"
        try:
            mode = public_route(request.method, request.url)
            manifest, _, _ = deployment(self.env)
            identifier = fixed_target(self.env.RESEARCH_AUTHORITY, manifest)
            return await self.env.RESEARCH_AUTHORITY.get(identifier).fetch(request)
        except ResearchError as error:
            result = runtime_failure(error, mode=mode)
        except Exception:
            result = runtime_failure(mode=mode)
        return response(result)
