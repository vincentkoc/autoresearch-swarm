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
The worker, SDK, CLI, and MCP surfaces have separate qualification steps.
