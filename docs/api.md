# Research HTTP API

The HTTP adapter calls the same command service as local Python clients. The
service owns state, idempotency, revisions, budgets, and evidence acceptance.

## Authentication and command identity

All research endpoints require a bearer credential. `/healthz` reports only
process liveness and is public. The credential resolves to a server-owned actor,
workspace, and scopes. Request bodies cannot supply their own authority.

Credential storage contains a domain-separated token hash. Issuance returns the
opaque token once. Tokens can expire or be revoked. Expiry uses canonical UTC
timestamps. A scope manager cannot issue permissions beyond its own scopes or
issue into another workspace.

Mutation commands use `POST /v1/commands/{operation}` with JSON and an
`Idempotency-Key` header. Revision-sensitive operations also require
`X-Expected-Revision`. An accepted duplicate returns the original command result.
Reusing the key for another payload or revision returns `IDEMPOTENCY_CONFLICT`.

Successful command acknowledgments contain `command_id`, `disposition: accepted`,
and `result`. A durable rejection contains that same command identity,
`disposition: rejected`, and `error`. Replaying a stored rejection preserves its
original command ID. Authentication, schema, and preflight refusals can happen
before a command outcome is stored; these have no command ID or disposition.

An unavailable acknowledgment does not prove rejection. Timeout, disconnect,
malformed response, unexpected status, or missing command identity produces SDK
error `DELIVERY_UNCERTAIN`. Retry with the same actor, operation, payload, expected
revision, and idempotency key. A later authentication refusal does not settle an
earlier uncertain command. Only authorized replay can recover its acknowledgment.

Query transport failure produces `TRANSPORT_UNAVAILABLE`, including the read-only
`POST /v1/query`. Neither transport error is a resource or scientific observation.
Upload-part uncertainty requires inspecting upload progress and replaying the
same offset and bytes. It does not grant authority to start a second upload.

`GET /v1/capabilities` lists the current commands and their schemas.
`GET /v1/openapi.json` describes the HTTP surface. Both require read authority.
Unknown fields, duplicate JSON keys, non-finite values, and oversized command
bodies are rejected. The command body limit is 1 MiB.

## Queries and evidence

| Route | Result |
|---|---|
| `GET /v1/records/{kind}/{id}` | One authorized public research record |
| `POST /v1/query` | Up to 100 summarized records and an opaque next cursor |
| `GET /v1/campaigns/{id}/context` | Bounded recent evidence, lifecycle, contract, counts, and budget |
| `GET /v1/campaigns/{id}/budget` | Exact decimal account values and availability |
| `GET /v1/events?after=N` | Up to 1,000 events after a workspace sequence |
| `GET /v1/events/stream?after=N` | Resumable server-sent events with sequence IDs |

List cursors bind their workspace and filters. They freeze the insertion range,
so later insertions do not extend an in-progress page sequence. Record state is
read at each page request. This is not a historical snapshot across pages.
Campaign context reads its evidence and event watermark in one transaction.

`POST /v1/worker/list` accepts `WorkerQuery` and `POST /v1/worker/get` accepts
`WorkerRead`. These read-only operations require `worker.query`, not workspace
`read`. Both bind the request to the authenticated worker principal and current
session. Generic record, context, and event endpoints still require `read`.
See [scoped worker discovery](worker-assignments.md#scoped-discovery-and-recovery).

`POST /v1/worker/clock` accepts `ClockChallenge` and returns `SignedClockReply`
under `worker.query`. It is available only for an active worker enrolled with
`clock_mode="bounded-continuous-v1"`. Sampling and assignment issuance share a
process-owned clock epoch; this endpoint does not itself grant remote execution.
See [controller time admission](worker-assignments.md#controller-time-admission).

`POST /v1/worker/input` returns an assigned input's `ArtifactRef` and
`POST /v1/worker/input/content` streams its verified bytes. Both accept
`WorkerArtifactRead`, require `worker.inputs`, and remain read-only requests.
Authorization binds the exact active assignment and is rechecked when retaining
the storage generation. Generic artifact endpoints still require workspace read
authority. See [attempt-bound inputs](worker-assignments.md#attempt-bound-inputs).

Event streams recheck credentials between polling batches. Events remain durable
when a client disconnects. The current store retains the complete event stream.

## Artifact transfer

Begin an upload with `POST /v1/artifact-uploads`, declaring digest, byte size, and
an ASCII media type without parameters. Finalize with
`POST /v1/artifact-uploads/{id}/finalize`. Both operations use command headers.

`PUT /v1/artifact-uploads/{id}/parts?offset=N` uses a separate byte-range protocol.
It does not require or interpret an `Idempotency-Key`. Upload identity, offset,
and bytes define replay: identical overlapping bytes are accepted and conflicting
bytes are rejected. Each request is limited to 4 MiB. The upload owner must be
authenticated. A changed range is new transfer progress, not a command retry.

Read metadata with `GET /v1/artifacts/{id}` and bytes with
`GET /v1/artifacts/{id}/content`. The server verifies bytes before sending success
headers, holds a retention pin for the response lifetime, and streams an immutable
generation in bounded chunks. It does not create a full temporary copy for every
download. A disconnect releases the reader even when no body chunk was sent.

## Current verification

Tests use a real loopback HTTP server. They cover command replay across HTTP and
Python, credential revocation, cross-workspace rejection, schema references,
strict input limits, range retries, invalid metadata, and failed artifact reads.
They also hold a reply after its database commit, force a client timeout, and
recover the original acknowledgment without a second command or creation event.
Local TLS tests verify trusted success, untrusted certificate rejection, and
hostname mismatch rejection.
The worker, SDK, CLI, and MCP surfaces have separate qualification steps.
