# Sealed native backups

The `sealed-native-v1` profile exports a consistent SQLite snapshot and every
verified filesystem artifact generation. It requires a sealed workspace and full
operator authority (`*`). R2 and other state or storage adapters are not supported
by this profile.

Sealing stops new research mutations. It does not stop native processes, release
capacity or settle their reservations. The source remains sealed after export.
This is a checkpoint for explicit recovery, not a rolling online backup.

## Establish the cut

Use the shared `workspace.seal` command through the SDK, HTTP API, CLI command
dispatcher or agent tools. The command requires the current controller ID and
control epoch, a new backup ID, a reason and expected revision zero. Pending
uploads, artifact deletions and retained readers must close first. If admission
is rejected, use a new command key after resolving the cause; exact rejected
commands keep their original reply.

For a native operator:

```python
from pathlib import Path

from research_swarm.domain.backup import SealWorkspace
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime

runtime = LocalRuntime.open(Path("state"))
context = CommandContext(
    principal=runtime.principal(), idempotency_key=new_id(), expected_revision=0
)
snapshot = runtime.database.snapshot(context)
request = SealWorkspace(
    backup_id=new_id(),
    expected_controller_id=runtime.identity.controller_id,
    expected_control_epoch=snapshot["control_epoch"],
    reason="Planned inactive restoration",
)
runtime.service.seal_workspace(context, request)
print(request.backup_id)
```

Retain the original worker installations, journals, resource registries and
controller directory. A seal is not evidence that their execution has ended.

## Export and verify

```sh
research --state-dir state backup export backup-directory --backup-id BACKUP_UUID
research backup verify backup-directory --digest sha256:MANIFEST_DIGEST
```

The destination must be outside the runtime, its parent must already exist, and
it must not contain another bundle.
The exporter uses SQLite's backup API, checks its schema/integrity/foreign keys,
and derives the artifact inventory from the copied database. It verifies the
operator credential and assignment key before publishing. It streams and checks
every exact artifact generation, including unreferenced verified content that
still occupies storage.

The manifest binds the database and private-file hashes, ordered generation
inventory, exact seal, both evidence watermarks and source custody public key.
The seal watermark precedes its own event; the snapshot watermark includes it.
A signature over the manifest digest detects inconsistent content. To establish
provenance, compare the manifest digest or source custody key with a trusted
record; a self-contained public key is not independent proof of origin.

The bundle layout is fixed:

```text
backup-directory/
  manifest.json
  research.sqlite
  private/
    identity.json
    operator.token
    dispatch.key
    assignment.key
  blobs/
    GENERATION_UUID
```

The private directory contains credentials and signing keys. A principal with
only `service.manage` cannot export it. Private file protections apply on POSIX
and Windows. Verification rejects unexpected files, substituted paths, linked
files, changed bytes, missing retained evidence and unsupported schemas. The
profile bounds the database at 1 GiB and the inventory at 10,000 generations.

The source keeps a separate `backup-custody.json` key. It is never included in the
bundle. Preserve it with the original sealed controller: copied research keys
must not become authority to choose which restored installation may activate.
Worker journals, machine resource registries and supervisor state are excluded.

The manifest is published last. A destination without it is incomplete and
verification refuses it. An ordinary failed export removes its staging directory;
an interrupted destination remains incomplete. Export to a new destination after
inspection. Retrying a completed destination verifies and returns the same
manifest instead of recopying it or changing the cut.

## Qualification boundary

Native tests cover a real SQLite snapshot, artifact and key corruption, missing
files, hardlinks, scoped-authority refusal, incomplete publication and exact
completed-export replay. Verification can run without opening a controller.

This slice supplies verified export. Inactive import, epoch advancement, live
worker reconciliation and single-target activation remain separate work. A
successful export is not yet a qualified restore procedure.
