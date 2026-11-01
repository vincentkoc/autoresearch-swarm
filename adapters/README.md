# Infrastructure adapters

Adapters connect provider services to the research engine's existing ports.
Implement the ports required by the service. There is no universal adapter base
class or mandatory provider SDK.

| Port | Responsibility | Reference |
|---|---|---|
| `StateStore`, `SqlSession` | Synchronous snapshots and atomic SQL transactions | [SQL contracts](../src/research_swarm/state/sql.py), [callback-only example](../tests/state/test_state_store.py) |
| `BlobStore` | Artifact bytes, generations and retirement | [Storage contract](../src/research_swarm/storage/base.py) |
| `AsyncR2Transport` | Bounded asynchronous object-storage I/O | [Transport contract](../src/research_swarm/storage/async_r2.py) |
| `Ed25519Provider` | Synchronous signing and signature verification | [Cryptography contract](../src/research_swarm/crypto.py) |
| `ExecutionBoundary` | Qualified authority and time for one operation | [Boundary contract](../src/research_swarm/state/boundaries.py) |
| `DeliveryClient` | Deliver saved actions and waits to the authority | [Delivery contract](../src/research_swarm/orchestration/async_delivery.py), [minimal client example](../tests/orchestration/test_async_delivery.py) |

The local implementations ship with the Python package. Temporal delivery lives
in `research_swarm.orchestration`. The [Cloudflare adapter](cloudflare/README.md)
has a separate provider build and dependency environment.

## Contract

1. Keep provider objects and SDK imports outside domain records and wire schemas.
2. Preserve command keys, immutable identities and accepted replay results.
3. Commit state, events, budget entries and outbox effects together.
4. Keep network awaits outside synchronous transaction callbacks.
5. Report unsupported capabilities explicitly. Do not substitute weaker authority,
   clock, storage or process-closure semantics.
6. Configure endpoints and credentials at the deployment boundary. Research
   requests cannot select another provider account or secret.
7. Run shared conformance tests, then qualify the actual provider runtime.

The callback-only store and asynchronous client tests above provide small working
implementations for extension authors. Use them to check a custom implementation
against the shared command and delivery behavior.

Research repositories use manifests and commands. They do not implement these
infrastructure ports. See [project adoption](../docs/project-adoption.md).
