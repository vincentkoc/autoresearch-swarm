# Python SDK

`ResearchClient` calls the local command service or the authenticated HTTP API.
The request and response contracts are identical. `AsyncResearchClient` offers
the same operations through thread-backed async methods.

~~~python
from pathlib import Path
from research_swarm import ResearchClient
from research_swarm.control.commands import CreateCampaign, CampaignTarget
from research_swarm.domain.budgets import BudgetLimit

with ResearchClient.local(Path(".research-state")) as client:
    registered = client.register_project(
        Path("examples/cpu_bigram"), idempotency_key="register-bigram-v1"
    )
    project_id = registered["result"]["project"]["id"]
    campaign = client.command(
        "campaign.create",
        CreateCampaign(
            project_id=project_id,
            name="First research campaign",
            primary_metric="val_bpb",
            limits=(
                BudgetLimit(dimension="attempts", limit=20, unit="count"),
                BudgetLimit(dimension="execution_ms", limit=300_000, unit="ms"),
            ),
        ),
        idempotency_key="create-first-campaign",
    )["result"]
    for operation in ("campaign.seal", "campaign.start"):
        campaign = client.command(
            operation,
            CampaignTarget(campaign_id=campaign["id"]),
            idempotency_key=operation + "-first-campaign",
            expected_revision=campaign["revision"],
        )["result"]
    print(client.context(campaign["id"]))
~~~

Starting opens trial admission. It does not mean training has completed.

## Remote connection

Use `ResearchClient.connect(controller_url, token)` with a credential supplied by
the caller's secret store. The client does not discover credentials, follow
redirects, or inherit ambient HTTP proxy configuration. Non-loopback URLs require
HTTPS. Plain HTTP is restricted to literal loopback IPs or exactly `localhost`;
DNS resolution cannot grant a hostname permission to use plaintext.

Certificate and hostname verification remain enabled. An optional
`tls=ssl.SSLContext` supports a private trusted CA, provided that both checks stay
enabled. `timeout=` controls request waiting, not native execution authority.

The client methods include `command`, `capabilities`, `get`, `list`, `context`,
`budget`, `events`, `upload_file`, `capture_project`, `register_project`, and
`download`. `RecordQuery` supplies bounded filters and cursors for `list`.

`artifact(identifier)` returns the typed artifact catalog record and current
lifecycle state without transferring bytes. It requires workspace `read` access,
has an async equivalent, and uses the existing artifact metadata HTTP endpoint.
Metadata is descriptive; it does not reserve bytes or authorize a worker assignment.

`worker_list(WorkerQuery)` and `worker_get(WorkerRead)` expose the narrower worker
query contract. They also have async equivalents. A worker credential can use
these views without obtaining generic workspace read access.

`worker_input(WorkerArtifactRead)` returns authorized input metadata.
`worker_download(request, destination)` downloads and verifies those bytes through
`worker.inputs` authority. Both have async equivalents. They cannot fetch
arbitrary project artifacts or replace an existing destination.

Every command requires an explicit idempotency key. Keep it with the request when
retrying after an uncertain response. Async cancellation or closing a client does
not cancel an accepted campaign or prove that native execution stopped.

`ResearchError.code == DELIVERY_UNCERTAIN` means a mutating request lacks a valid
acknowledgment. Its `details.recovery` distinguishes exact command replay from
byte-range upload recovery. Query transport failures use `TRANSPORT_UNAVAILABLE`.
A durable domain rejection has `ResearchError.command_id`; a pre-admission
refusal does not. Do not replace an uncertain command's idempotency key after a
later authentication refusal. See the [acknowledgment contract](api.md).

## Local state and file transfer

The default directory is `~/.local/state/autoresearch-swarm`. An explicit directory
keeps a project or test independent. Initialization uses a native file lock and
preserves the same workspace and controller across restarts.

The directory contains the research database, artifact store, identity metadata,
private operator credential, local dispatch key, and separate assignment signing
key with a pinned public identity. POSIX state directories and private
files require owner-only permission bits. Windows creates protected native DACLs
for the executing user and SYSTEM, with propagating inheritance on directories.
Initialization rejects broad existing access rather than silently changing it.
Use a state location whose parent namespace you trust. Same-user code and system
administrators remain inside the local trust boundary. Native ACL verification
does not make an arbitrary shared parent directory safe.

Windows qualification uses the native security APIs described in Microsoft's
[file security documentation](https://learn.microsoft.com/en-us/windows/win32/fileio/file-security-and-access-rights).

A selected scoped token file is authoritative. An invalid or revoked token does
not fall back to the operator credential.

Project capture follows the manifest's source mode and declared paths. Git capture
uses the selected immutable revision, including its manifest. An explicit revision
selects Git capture. Directory capture preserves selected working
files. The controller resolves and verifies the resulting immutable bundle.
Convenience registration captures source again on each call. To retry without
local source, retain its ArtifactRef and repeat the `project.register` command.

Uploads bind the caller's transfer key to the declared content and resume through
the byte-range protocol. Completed uploads are recognized before writing parts.
Downloads verify size and digest and refuse to overwrite an existing destination.

The tests cover initialization contention, restart, local/HTTP replay, complete
upload retry, scoped-token refusal, and the shared async command acknowledgment.

Both clients expose `worker_clock(ClockChallenge) -> SignedClockReply`. A worker
enrolled for calibrated time uses `ClockSampler.begin()`, sends that challenge,
and completes it with the returned reply. The sampling origin must remain alive;
reconstructing a sampler does not renew an existing attempt's authority. Keep the
controller process alive across this sequence. The stock local worker continues
to use its default local time mode until native calibrated admission is supported.
