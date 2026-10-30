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
| `POST /v1/proposal/get` | Assigned proposal and optional immutable response under `proposal.respond` |
| `POST /v1/query` | Up to 100 summarized records and an opaque next cursor |
| `GET /v1/campaigns/{id}/context` | Bounded recent evidence, lifecycle, contract, counts, and budget |
| `GET /v1/campaigns/{id}/budget` | Exact decimal account values and availability |
| `GET /v1/events?after=N` | Up to 1,000 events after a workspace sequence |
| `GET /v1/events/stream?after=N` | Resumable server-sent events with sequence IDs |
| `POST /v1/snapshot/page` | Up to 100 immutable recovery item descriptors |
| `POST /v1/snapshot/item` | Up to 64 KiB of one frozen canonical JSON item, as base64 |

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

Event streams recheck credentials between polling batches. Disconnecting does
not delete events or acknowledge notifications. Logical stream retention and the
permanent event ledger have separate lifecycles.

## Recover an expired event cursor

Event pages return `events`, `cursor`, `event_floor`, and `watermark` from one read
transaction. `cursor` advances only through returned events. `watermark` is the
latest committed event position, not proof the reader consumed those events.
`after == event_floor - 1` is valid; older cursors return
`EVENT_CURSOR_EXPIRED` with HTTP 410 and the recovery command name. Future
cursors are invalid. An initially stale SSE request returns 410 before stream
headers. If retention overtakes an established SSE connection, it emits one
`event: gap` frame with the structured error and closes, without a synthetic ID.

Create a frozen recovery view through `stream.create_snapshot`, using a new
`snapshot_id` and a stable command key. It requires workspace `read` authority.
The result binds the reader, workspace, projection version, controller identity,
control epoch, content counts, expiry, and exact event watermark `W`. Snapshot
creation freezes public record kinds and budget account/reservation/entry
projections in one transaction. Internal command receipts, credentials, private
ownership records, artifact bytes, and native worker state are excluded. This
is a client read-model checkpoint, not a workspace backup or ownership transfer.

`RecoveryPage` binds its opaque cursor to that snapshot and scope. Each manifest
entry supplies an ordinal, content digest, and byte size. `RecoveryItem` returns
bounded chunks of the corresponding frozen canonical JSON, not a later live
record. Read and verify every chunk before using an item. The synchronous SDK
provides `SnapshotReader`; async clients expose the same page/item methods.

~~~python
from research_swarm.domain.event_recovery import (
    CreateRecoverySnapshot,
    RecoverySnapshot,
    RecoverySnapshotTarget,
)
from research_swarm.recovery import SnapshotReader

snapshot = RecoverySnapshot.model_validate(
    client.command(
        "stream.create_snapshot",
        CreateRecoverySnapshot(snapshot_id=snapshot_id),
        idempotency_key=recovery_command_key,
    )["result"]
)
reader = SnapshotReader(client, snapshot)
staged_items = list(reader.items())  # Use a staging store for large workspaces.
assert reader.completed
install_read_model_atomically(staged_items, watermark=snapshot.watermark)

cursor = snapshot.watermark
while True:
    page = client.events(after=cursor)
    apply_or_refresh_from_events(page["events"])  # Deduplicate by event ID.
    cursor = page["cursor"]
    if cursor == page["watermark"]:
        break

client.command(
    "stream.release_snapshot",
    RecoverySnapshotTarget(snapshot_id=snapshot.id),
    idempotency_key=release_command_key,
)
~~~

Install a complete snapshot atomically in the consuming application. Do not apply
partially fetched pages to its active view. Events can act as invalidations that
trigger named record reads; their payloads do not necessarily contain a complete
replacement record. Persist each consumed cursor with the corresponding local
view update. A report revision is not an event cursor.

An active snapshot pins the interval strictly after `W`. Retention cannot advance
beyond `W + 1` until release or expiry. Fetching the final page does not release
the pin; its response may be lost. Release after installing the snapshot and
catching up. If recovery expires or another gap occurs, obtain a new snapshot.
After release, later disconnects can require another recovery normally.

Defaults are a five-minute pin, at most 100,000 items, and 256 MiB of frozen
content. Callers may narrow these bounds; pin lifetime ranges from 30 seconds to
one hour. Admission caps unreclaimed snapshots at eight per reader, 64 per
workspace, and 1 GiB of workspace content. Exceeding a bound rolls back the
entire snapshot. `SnapshotReader` additionally limits each decoded item to
32 MiB by default, configurable up to 256 MiB. It never releases a pin itself.

`stream.advance_floor` requires `stream.manage`, an exact `previous_floor`, and
the new first-available event sequence. It advances logical retention only.
Permanent events, undelivered outbox messages, and durable wait evidence remain
available to their own protocols. `stream.collect_expired` reclaims a bounded
batch of expired contents and retains terminal metadata. Creation also reclaims
up to eight expired snapshots belonging to that reader. Expired, unreclaimed
content still counts against admission limits. Revoked readers' abandoned views
can be collected by the operator. Release and collection are idempotent commands.

Expiration uses controller UTC. Collection/floor advancement records terminal
expiry; a backward clock adjustment cannot revive that snapshot. Reads also
reject a snapshot if the current floor has crossed its catch-up interval. Each
page or chunk reauthorizes the original reader. A new reader starts its own
snapshot instead of adopting another principal's cursor.

CLI equivalents are `research command stream.create_snapshot`,
`research snapshot page ID`, and `research snapshot item ID ORDINAL`.
MCP exposes the four stream commands plus `research_snapshot_page` and
`research_snapshot_item`. The SQLite store upgrades schema 1 to 2 in one
transaction, retaining original command acknowledgments and event sequences.
Concurrent openers recheck the schema under the migration lock; a failed
migration rolls back, and newer unsupported schemas are rejected.

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
