"""Private atomic inactive import. No target provisioning, artifact I/O or election."""

from research_swarm.checkpoints.capture import CaptureLimits, overflow, preflight_rows
from research_swarm.checkpoints.cut import inspect_sealed_cut
from research_swarm.checkpoints.fingerprint import fingerprint_state
from research_swarm.checkpoints.image_format import TABLES, row_values, verify_image
from research_swarm.checkpoints.image_schema import require_portable_image_schema
from research_swarm.checkpoints.inactive import RestorationCut, restore_inactive_in
from research_swarm.control.auth import CredentialStore
from research_swarm.crypto import public_key
from research_swarm.domain.backup import RestoreReceipt
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.checkpoint_images import LogicalImportIntent
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_providers import TimeProviderData
from research_swarm.state.database import encode
from research_swarm.state.schema import SCHEMA_VERSION
from research_swarm.state.schema_contracts import quoted, unsupported
from research_swarm.state.sql import IntegrityViolation, StoreIdentity, next_counter

if __package__:
    from .deployment import secret_digest
else:
    from deployment import secret_digest

IMPORT_TABLE = "hosted_checkpoint_import"
IMPORT_COLUMNS = ("singleton", "intent", "receipt", "state_digest")


def _target_schema(sql):
    objects = sql.execute(
        "SELECT type,name,tbl_name FROM sqlite_master "
        "WHERE substr(lower(tbl_name),1,7)!='sqlite_' "
        "AND substr(lower(tbl_name),1,5)!='__cf_' "
        "AND substr(lower(tbl_name),1,4)!='_cf_' LIMIT 129"
    ).fetchall()
    allowed = set(TABLES) | {"research_schema", IMPORT_TABLE}
    if len(objects) > 128 or any(
        row["type"] not in {"table", "index"} or row["tbl_name"] not in allowed for row in objects
    ):
        raise unsupported()
    names = {row["name"] for row in objects if row["type"] == "table"}
    if names - {"credentials", IMPORT_TABLE} != allowed - {"credentials", IMPORT_TABLE}:
        raise unsupported()
    metadata = sql.execute(
        "SELECT * FROM pragma_table_xinfo('research_schema') ORDER BY cid LIMIT 3"
    ).fetchall()
    if len(metadata) != 2 or any(
        (
            row["name"],
            row["type"].upper(),
            row["notnull"],
            row["pk"],
            row["hidden"],
            row["dflt_value"],
        )
        != expected
        for row, expected in zip(
            metadata,
            (
                ("singleton", "INTEGER", 0, 1, 0, None),
                ("version", "INTEGER", 1, 0, 0, None),
            ),
            strict=True,
        )
    ):
        raise unsupported()
    count = sql.execute("SELECT COUNT(*) FROM (SELECT 1 FROM research_schema LIMIT 2)").one()[0]
    wrong = sql.execute(
        "SELECT 1 FROM research_schema WHERE typeof(singleton)!='integer' OR singleton!=1 "
        "OR typeof(version)!='integer' OR version!=? LIMIT 1",
        (SCHEMA_VERSION,),
    ).fetchone()
    if count != 1 or wrong is not None:
        raise unsupported()
    return names


def _saved(sql, intent):
    columns = sql.execute(
        f"SELECT * FROM pragma_table_xinfo('{IMPORT_TABLE}') ORDER BY cid LIMIT 5"
    ).fetchall()
    if len(columns) != 4 or any(
        (
            row["name"],
            row["type"].upper(),
            row["notnull"],
            row["pk"],
            row["hidden"],
            row["dflt_value"],
        )
        != (
            name,
            "INTEGER" if ordinal == 0 else "TEXT",
            int(ordinal > 0),
            int(ordinal == 0),
            0,
            None,
        )
        for ordinal, (row, name) in enumerate(zip(columns, IMPORT_COLUMNS, strict=True))
    ):
        raise unsupported()
    if sql.execute(f"SELECT COUNT(*) FROM (SELECT 1 FROM {IMPORT_TABLE} LIMIT 2)").one()[0] != 1:
        raise unsupported()
    row = sql.execute(
        f"SELECT intent,receipt,state_digest FROM {IMPORT_TABLE} WHERE singleton=1 "
        "AND typeof(intent)='text' AND length(CAST(intent AS BLOB))<=4096 "
        "AND typeof(receipt)='text' AND length(CAST(receipt AS BLOB))<=131072 "
        "AND typeof(state_digest)='text' AND length(CAST(state_digest AS BLOB))=71"
    ).fetchone()
    if row is None or strict_json_loads(row["intent"]) != intent.model_dump(mode="json"):
        raise ResearchError(ErrorCode.STALE_FENCE, "Inactive import intent differs")
    receipt = RestoreReceipt.model_validate_json(row["receipt"])
    if fingerprint_state(sql) != row["state_digest"]:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Inactive imported state changed")
    return receipt


def _require_receipt(sql, intent, image, receipt):
    manifest = image.manifest
    scope = manifest.source_authority
    expected = (
        intent.restore_id,
        manifest.digest,
        manifest.seal.backup_id,
        scope.workspace_id,
        scope.controller_id,
        manifest.source_custody.custody_id,
        manifest.source_custody.public_key,
        intent.target_public_key,
        scope.control_epoch,
        next_counter(scope.control_epoch),
        manifest.seal.worker_ids,
        manifest.seal.unresolved_attempt_ids,
    )
    actual = (
        receipt.restore_id,
        receipt.manifest_digest,
        receipt.backup_id,
        receipt.workspace_id,
        receipt.controller_id,
        receipt.source_custody_id,
        receipt.source_public_key,
        receipt.target_public_key,
        receipt.source_control_epoch,
        receipt.target_control_epoch,
        receipt.worker_ids,
        receipt.unresolved_attempt_ids,
    )
    if actual != expected:
        raise ResearchError(ErrorCode.STALE_FENCE, "Inactive import receipt differs")
    workspace = sql.execute("SELECT id,controller_id,control_epoch FROM workspaces LIMIT 2").one()
    if (workspace[0], workspace[1], workspace[2]) != (
        receipt.workspace_id,
        receipt.controller_id,
        receipt.target_control_epoch,
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Inactive import authority differs")
    gate = sql.execute(
        "SELECT state FROM records WHERE kind='workspace_gate' AND id=?", (receipt.workspace_id,)
    ).one()
    stored = sql.execute(
        "SELECT state,payload FROM records WHERE kind='restoration' AND id=?", (receipt.restore_id,)
    ).one()
    if (
        gate[0] != "restoring"
        or stored[0] != "inactive"
        or RestoreReceipt.model_validate_json(stored[1]) != receipt
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Inactive import phase differs")


def import_checkpoint(
    store,
    target_manifest,
    secrets,
    intent,
    *,
    credential,
    target_seed,
    envelope,
    bodies,
    expected_source,
    expected_custody,
    crypto,
):
    """Caller supplies trusted deployment/custody inputs; never take them from an image."""
    secrets.require_bootstrap(credential)
    if secret_digest("bootstrap", credential) != target_manifest.bootstrap_credential_digest:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Import bootstrap belongs to another deployment"
        )
    intent = LogicalImportIntent.model_validate(intent)
    target_manifest.require_store(store.identity)
    authority = target_manifest.configuration.authority
    issuer = next(
        item.principal
        for item in target_manifest.credentials
        if item.principal.id == target_manifest.provisioning_actor_id
    )
    issuer.require("*")
    if (
        store.identity != StoreIdentity(intent.target_backend, intent.target_location)
        or intent.target_deployment_digest != target_manifest.digest
        or intent.operator_id != issuer.id
        or public_key(crypto, target_seed).hex() != intent.target_public_key
        or intent.target_public_key == expected_custody.public_key
        or (authority.workspace_id, authority.controller_id, authority.control_epoch)
        != (
            expected_source.workspace_id,
            expected_source.controller_id,
            next_counter(expected_source.control_epoch),
        )
        or authority.state_authority_id == expected_source.state_authority_id
        or authority.activation_id == expected_source.activation_id
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Inactive import target differs")
    limits = CaptureLimits()
    if (
        not isinstance(envelope, bytes)
        or len(envelope) > limits.manifest_bytes + 256
        or len(bodies) > limits.chunks
        or any(not isinstance(body, bytes) or len(body) > limits.chunk_bytes for body in bodies)
        or sum(map(len, bodies)) > limits.image_bytes
    ):
        raise overflow()
    image = verify_image(
        envelope,
        bodies,
        expected_digest=intent.manifest_digest,
        expected_source=expected_source,
        expected_custody=expected_custody,
        crypto=crypto,
    )
    manifest = image.manifest
    if (
        manifest.operator_id != issuer.id
        or sum(item.row_count for item in manifest.tables) > limits.rows
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Inactive import source profile differs")

    def restore(sql):
        target_manifest.require_store(store.identity)
        names = _target_schema(sql)
        if IMPORT_TABLE in names:
            require_portable_image_schema(sql)
            receipt = _saved(sql, intent)
            _require_receipt(sql, intent, image, receipt)
            return receipt
        for name in TABLES:
            if (
                name in names
                and sql.execute(f"SELECT 1 FROM {quoted(name)} LIMIT 1").fetchone() is not None
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Inactive import target is not empty"
                )
        CredentialStore.initialize_in(sql)
        require_portable_image_schema(sql)
        for chunk in image.chunks:
            table = TABLES[chunk.table]
            fields = ",".join(quoted(column.name) for column in table.columns)
            parameters = ",".join("?" for _ in table.columns)
            for row in chunk.rows:
                sql.execute(
                    f"INSERT INTO {quoted(table.name)} ({fields}) VALUES ({parameters})",
                    row_values(chunk.table, row),
                )
        require_portable_image_schema(sql)
        imported_inventory = preflight_rows(sql, limits)
        if imported_inventory != manifest.tables:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Imported row inventory differs")
        cut = inspect_sealed_cut(
            sql,
            workspace_id=expected_source.workspace_id,
            controller_id=expected_source.controller_id,
            storage_limit_bytes=target_manifest.configuration.storage_limit_bytes,
            expected_backup_id=manifest.seal.backup_id,
            require_custody=True,
            require_seal_history=True,
        )
        if (
            cut.seal != manifest.seal
            or cut.seal_revision != manifest.seal_revision
            or cut.seal_digest != manifest.seal_digest
            or cut.evidence_through != manifest.snapshot_evidence_through
            or cut.custody_pin != expected_custody
            or cut.artifacts != manifest.artifacts
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Imported source cut differs")
        provider = sql.execute(
            "SELECT state,payload FROM records WHERE kind='time_provider' AND id=?",
            (expected_source.activation_id,),
        ).fetchone()
        if (
            provider is None
            or provider[0] not in {"qualified", "unqualified"}
            or TimeProviderData.model_validate_json(provider[1]).scope.authority != expected_source
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Imported source provider differs")
        context = CommandContext(
            principal=issuer, idempotency_key="logical-import:" + intent.restore_id
        )
        receipt = restore_inactive_in(
            sql,
            context,
            RestorationCut(
                seal=cut.seal,
                seal_revision=cut.seal_revision,
                snapshot_evidence_through=cut.evidence_through,
                manifest_digest=manifest.digest,
                source_custody_id=expected_custody.custody_id,
                source_public_key=expected_custody.public_key,
            ),
            restore_id=intent.restore_id,
            target_public_key=intent.target_public_key,
        )
        _require_receipt(sql, intent, image, receipt)
        fingerprint = fingerprint_state(sql)
        sql.execute(
            f"CREATE TABLE {IMPORT_TABLE} (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "intent TEXT NOT NULL,receipt TEXT NOT NULL,state_digest TEXT NOT NULL)"
        )
        sql.execute(
            f"INSERT INTO {IMPORT_TABLE} VALUES (1,?,?,?)",
            (
                encode(intent.model_dump(mode="json")),
                encode(receipt.model_dump(mode="json")),
                fingerprint,
            ),
        )
        return receipt

    try:
        return store.write(restore)
    except (IntegrityViolation, ValueError):
        raise ResearchError(
            ErrorCode.ARTIFACT_UNVERIFIED, "Imported source data is invalid"
        ) from None
