# Result acceptance leases

Every new attempt receives a controller-side `acceptance_lease` in the same
transaction as its dispatch and budget reservation. This record determines
whether an ordinary result can still be accepted. It does not replace the signed
dispatch, native deadline, process journal, or machine resource claim.

| Deadline | Meaning |
|---|---|
| Dispatch `expires_at` | Latest time the original native launch may begin |
| Dispatch `hard_deadline_at` | Original native supervision cap |
| Acceptance lease `expires_at` | Latest controller time for ordinary result acceptance |

The `native-window-v1` policy initially permits acceptance until the original
hard deadline plus 120 seconds. It covers the entire authorized execution window
and a publication grace period; it is not a heartbeat-failure detector. Explicit
renewal can extend publication grace up to a fixed final cap of the original hard
deadline plus 600 seconds. It cannot extend native execution or revive an expired
lease. Automated renewal and background expiry scanning are not enabled by this
policy; ordinary publication always performs the authoritative check.

## Reads and renewal

Operators query `acceptance_lease` records using the ordinary record API. A scoped
worker can read its own lease with `WorkerRead(kind="acceptance_lease", id=attempt_id)`.
The response contains the full worker binding, immutable dispatch digest, policy,
time bounds, clock identity, renewal sequence, state, and record revision.

A heartbeat can include at most 100 explicit `lease_renewals`:

```python
from research_swarm.control.acceptance_models import LeaseRenewal
from research_swarm.control.worker_lifecycle import WorkerHeartbeat

request = WorkerHeartbeat(
    binding=worker_binding,
    driver_instance_id=driver_id,
    sequence=next_sequence,
    lease_renewals=(
        LeaseRenewal(
            attempt_id=lease["id"],
            dispatch_digest=lease["data"]["dispatch_digest"],
            expected_lease_revision=lease["revision"],
        ),
    ),
)
ack = client.command("worker.heartbeat", request, idempotency_key=heartbeat_key)
```

Persist the exact request and key before delivery. Retrying an accepted heartbeat
returns the original acknowledgment and expiration, without another extension.
An empty renewal list is omitted from serialization, preserving older persisted
heartbeat identities. Presence and admission retain their existing contracts.

Each renewal checks the current worker/session, original dispatch, publication
state, lease revision, and controller clock. A revision conflict or an expired,
unqualified, or settled lease returns a per-attempt disposition. Those ordinary
races do not discard valid cancellation acknowledgments in the same heartbeat.
A foreign dispatch or malformed request rejects the transaction.

## Expiry and evidence

`attempt.expire_acceptance` takes one attempt ID and the expected lease revision.
It requires `campaign.manage` and uses controller time. A not-yet-expired lease is
returned unchanged. Expiry atomically marks the attempt/job/trial as execution
unknown, moves outstanding reservations to uncertainty, and records the event.
It does not signal a process, release a device, refund costs, or admit replacement
work. The operation is available through SDK, HTTP, CLI, and MCP.

Publication checks the lease again inside the acceptance transaction, even if no
expiry command has run. Rejection uses `ACCEPTANCE_EXPIRED`. Failed publication
effects roll back; a separately authorized conservative fence commits atomically
with that rejected acknowledgment. This observed expiry or clock failure cannot
revive after clock recovery, even if the worker disconnects before quarantine.
If recording the fence fails, neither the rejection acknowledgment nor partial
accounting commits. The worker then submits the identical
publication as quarantined original evidence with a stable key. That committed
submission can record overdue expiry and uncertainty. The local driver reports a
blocked attempt instead of creating a replacement.

Only explicit current-authority reconciliation can accept a late result. It must
verify original session ownership, dispatch, journal, checkpoint/report inputs,
and native closure. The immutable reconciliation receipt includes the original
lease and its disposition. Acceptance settles the original lease and budget once.
Operational-only closure settles the lease without accepting a scientific result.
Previously accepted publication acknowledgments remain replayable after expiry.

The native registry independently releases its capacity when original process
closure is proven. That can happen before controller reconciliation; it does not
release the controller's unsettled budget or make its attempt successful.

## Clocks and upgrades

Calibrated workers use the same controller continuous-clock epoch and policy as
their signed assignment. Loss of that epoch or policy makes ordinary acceptance
unqualified. The original evidence remains reconcilable. Reopening within the
same qualified controller process preserves its clock; a new process cannot
invent continuity with the old epoch.

Compatibility workers retain `local-utc-v1`: controller UTC, never a worker-supplied
timestamp. A time earlier than the lease's last recorded observation makes the
lease unqualified. This profile does not prove wall-clock continuity during an
unobserved interval. Native elapsed time and resource supervision remain separate.

SQLite schema 3 adds the lease query index and prevents old binaries from opening
the upgraded store. Stop old controllers before upgrading; changing a schema
version cannot stop an already-running old process. Schema 1 and 2 migrate in one
transaction without rewriting signed attempts or prior acknowledgments. A legacy
pending attempt with no issued lease is explicitly `legacy-unqualified-v1`, with
absent historical timestamps. Its original closure requires reconciliation; the
upgrade does not invent an acceptance window.

The tests expire acceptance while a real CPU scope is running, preserve native
identity and occupied capacity, observe original closure, quarantine its evidence,
and reconcile once. Additional tests cover exact-boundary expiry, bounded renewal,
clock discontinuity, cancellation acknowledgment, independent-connection races,
scoped HTTP reads, migration, and old command replay. Physical-host clock behavior
remains a separate qualification.
