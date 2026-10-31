# Artifacts and candidate source

The artifact service separates uploaded bytes, verified catalog records, retention,
and scientific acceptance. Uploading a file does not accept a result or evaluation.
The current implementation supports the local SQLite and filesystem profile.

## Upload and publication

`ArtifactService.begin_upload` reserves the declared content size before creating
staging data. The request includes the expected SHA-256 digest and exact byte count.
The caller receives an upload ID. The same command key returns the same ID.

`upload_part` accepts a byte offset and at most 8 MiB. A matching replay succeeds.
A conflicting replay fails. After an interrupted write, a matching partial prefix
can continue from its durable length. The service never appends duplicate bytes.

`finalize_upload` checks the complete size and digest, publishes immutable bytes,
then commits catalog metadata. A crash between these operations leaves recoverable
bytes. Retrying finalization does not invent an accepted experiment result.

Each workspace has a separate storage namespace. Identical content within a
workspace has one catalog identity. Physical generations have separate identities.
Delayed deletion can remove only its captured generation, including after the same
content is published again. A duplicate upload retains its healthy copy when the
existing catalog generation fails verification.

An artifact reference does not grant access. The service checks workspace, actor,
scope, upload ownership, and catalog identity independently of the digest.

## Retention and cleanup

References and the transition to `deleting` share the domain transaction boundary.
An active reference blocks deletion. A deleting artifact rejects new references.
Deletion releases charged capacity only after the physical operation succeeds and
the ledger records a tombstone. The tombstone preserves the digest and reason.

Ordinary callers can release their optional references. Required evidence needs
the owning domain operation to authorize its retention release. The internal
`ArtifactCatalog.release(..., owner_release=(kind, id))` path belongs in that
owner's checked lifecycle handler. It is not an ordinary API override.

Reads pin the selected artifact through verification and use. A read pin records
an immutable owner profile/identity and reader session. `ArtifactService` accepts
a synchronous `ReaderOwnership` provider. Its native default captures the process
identity; existing untagged native records remain readable. A local administrator
can reconcile a crashed session only after same-host inspection proves that the
original process ended. A live process, inaccessible process or different host
does not qualify.

Portable runtimes can explicitly select `OpaqueReaderOwnership`, which creates
an opaque incarnation without importing native process tools. Normal reads still
release their exact pins in `finally`. This provider cannot prove orphan closure:
restarting a controller, replacing an object incarnation or expiring a lease does
not release abandoned pins. Such artifacts stay retained. Native ownership also
refuses to interpret a foreign profile as a process.

New pins require the session's original owner and active state. Reconciliation
checks ownership and revision again in the release transaction, after the external
closure proof. Closed-session replay remains idempotent. A future provider must
prove permanent closure of the exact owner; an async result, boolean or unknown
profile cannot stand in for that proof.

The quota charges retained content, declared staging capacity, and unfinished
deletions. It conservatively reserves the full upload size. Directory, lock-file,
database, and filesystem allocation overhead are separate operational capacity.
These counters are not a filesystem quota or a measurement of exclusive disk blocks.

Workers can use `worker.cleanup_upload` to finish either duplicate publication or
an interrupted abort using only `worker.outputs`. Cleanup cannot create an abort
intent for a pending upload. An abort keeps its quota reservation until staging
and the exact unpublished generation are retired; the cleanup transaction then
marks both the upload and its worker provenance aborted. A failed deletion or
transaction leaves cleanup retryable under the same key. The original abort
acknowledgment remains unchanged even after cleanup finishes. Recovery retains
the original-session and bounded-journal restrictions.

The controller owns its artifact root. Candidate code must not have write access.
Native file locks coordinate trusted processes. They are not a sandbox for hostile
code running as the controller's operating-system user. POSIX publication syncs
the file and directory ancestry. Cross-platform CI tests process interruption.
it does not simulate power loss or qualify network filesystems.

## Source bundles

`bundle_directory` captures explicitly selected working files. `bundle_git` reads
literal Git tree and blob objects without switching or cleaning the checkout. It
bypasses archive export attributes and enforces a deadline across the full capture.
Git metadata and selected blobs have explicit size limits. Both capture methods
produce an uncompressed ZIP with a canonical manifest and content hashes.
The bundle carries a tree identity independent of storage location.

Path patterns use `/` separators. `*` matches within one component. `**` matches
zero or more directory components. For example, `*.py` selects root Python files,
while `src/**/*.py` includes nested source. Selection never implies data acquisition
or dependency installation.

The portable path contract rejects traversal, symlinks, special files, reserved
names, ambiguous case, and non-normalized names. Extraction checks membership,
sizes, the expected tree identity, and every file digest. It creates a new directory
and never overwrites an existing destination. A failed extraction removes only
the directory that operation created.

POSIX capture traverses pinned directory handles without following symlinks.
Windows capture anchors the root and checks each file's resolved native path before
reading its content. The Windows boundary uses
[CreateFile sharing rules](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew)
and [GetFinalPathNameByHandle](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfinalpathnamebyhandlew).

`validate_mutation` compares the base and candidate manifests, including additions,
deletions, content, and executable mode. Deny patterns and protected dependencies
take precedence over allow patterns. A caller must supply the complete protected
evaluator dependency boundary. A path check alone does not make arbitrary evaluator
imports or candidate-controlled timing trustworthy.

Source bundles provide isolated files and provenance. They do not provide an
operating-system security boundary. Restricted or protected execution still needs
a separately qualified execution profile.
