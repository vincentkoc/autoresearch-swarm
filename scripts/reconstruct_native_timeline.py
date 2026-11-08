"""Reconstruct a read-only display manifest from retained, hash-bound native evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import sqlite3
import tarfile
import tempfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research_swarm.control.native_history import NativeHistory

STUDIES = (
    ("direct-feature-bypass-v1", "DIRECT16"),
    ("temporal-residual-gating-v1", "L4-GATE"),
    ("joint-residual-readout-v1", "JOINT-READOUT"),
)


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def read(path: Path, limit: int = 16 * 1024 * 1024) -> bytes:
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), "rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("Retained source exceeds bound")
    return raw


def verified(raw: bytes, pin: dict[str, Any]) -> bytes:
    if len(raw) != pin["bytes"] or digest(raw) != pin["sha256"]:
        raise ValueError("Retained evidence hash or size mismatch")
    return raw


def utc(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat()


def absolute_level_timestamp(row: dict[str, Any]) -> dict[str, Any]:
    """Preserve the paired checkpoint event without inventing an arm finish."""
    if (
        row["study"] != "absolute-level-context-v1"
        or row["kind"] != "native_fit"
        or row["source_revision"] != "4685c1abeab79a40f9d76fdeea40b9759aafe2f0"
        or row["timestamp_basis"] != "final_full_FIT_diagnostic heartbeat; start unavailable"
    ):
        return row
    checkpoint_at = row.get("fit_completed_at")
    if checkpoint_at is None:
        return row
    return {
        **row,
        "fit_completed_at": None,
        "timestamp_basis": (
            f"{row['timestamp_basis']}; "
            "timestamp_semantics=final_full_FIT_diagnostic_observed_at; "
            f"paired_final_checkpoint_at={checkpoint_at}; "
            "per-arm fit completion unavailable; paired checkpoint is not an arm finish"
        ),
    }


def reconstruct(repo: Path, retained: Path, native: Path) -> dict[str, Any]:
    observations: list[dict[str, Any]] = []
    sources: list[dict[str, str | int]] = []
    for study, prefix in STUDIES:
        evidence_raw = read(repo / "experiments/global" / study / "evidence.json")
        evidence = json.loads(evidence_raw)
        archive = verified(
            read(retained / f"{prefix}-REVIEW-ARTIFACTS-20261107.tar.gz"),
            evidence.get("review_artifact_bundle", evidence.get("review_artifacts")),
        )
        terminal = json.loads(
            verified(
                read(retained / f"{prefix}-TERMINAL-CAPTURE-20261107.json"),
                evidence["terminal_capture"],
            )
        )
        native_pin = terminal["files"]["closeout/native-readback.json"]
        readback_raw = verified(
            base64.b64decode(native_pin["raw_base64"], validate=True), native_pin
        )
        sources.append({"study": study, "sha256": digest(archive), "bytes": len(archive)})
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as archive_file:
            members = archive_file.getmembers()
            if len(members) > 500 or sum(member.size for member in members) > 32 * 1024 * 1024:
                raise ValueError("Expanded archive exceeds bound")
            files = {}
            for member in members:
                if member.isfile():
                    stream = archive_file.extractfile(member)
                    if stream is None:
                        raise ValueError("Unreadable archive member")
                    files[member.name] = stream.read()
        expected_readback = evidence["native_readback"]
        original_readback_digest = digest(readback_raw)
        if digest(readback_raw) != expected_readback["sha256"]:
            publication_pin = evidence["campaign_closeout"]["capture"]
            publication = json.loads(
                verified(
                    read(retained / f"{prefix}-PUBLICATION-CAPTURE-20261107.json"),
                    publication_pin,
                )
            )
            correction = publication["files"]["native-readback.json"]
            readback_raw = verified(
                base64.b64decode(correction["raw_base64"], validate=True), correction
            )
        verified(readback_raw, expected_readback)
        readback = json.loads(readback_raw)
        sources.append(
            {
                "study": study + " native-readback",
                "retained_terminal_readback_sha256": original_readback_digest,
                "selected_committed_readback_sha256": digest(readback_raw),
                "selection": "exact committed evidence pin; corrected publication when required",
            }
        )
        for record in readback["records"]:
            seed = record["seed"]
            outputs = record["outputs"]
            metric_pin = outputs["fit_metrics"]["reference"]
            metric_raw = next(
                raw
                for name, raw in files.items()
                if (name == f"{seed}/fit_metrics.json" or f"/{seed}-fit_metrics-" in name)
            )
            verified(
                metric_raw,
                {
                    "bytes": metric_pin["size_bytes"],
                    "sha256": metric_pin["digest"].removeprefix("sha256:"),
                },
            )
            metrics = json.loads(metric_raw)
            if metrics["partition"] != "FIT" or not metrics["in_sample"]:
                raise ValueError("Unexpected native measurement cohort")
            heartbeat_raw = next(
                raw
                for name, raw in files.items()
                if name.endswith("heartbeat.json") and f"/{seed}/" in "/" + name
            )
            heartbeat = json.loads(heartbeat_raw)
            barrier_raw = next(
                raw
                for name, raw in files.items()
                if name == f"{seed}/fit-barrier.json" or name.endswith(f"/fit-barrier/{seed}.json")
            )
            barrier = json.loads(barrier_raw)
            if heartbeat["swarm"]["attempt_id"] != record["attempt_id"]:
                raise ValueError("Heartbeat attempt differs")
            if heartbeat["step"] != 3000 or heartbeat["diagnostic_windows_per_arm"] != 54941:
                raise ValueError("Incomplete native FIT diagnostic")
            cohort = "FIT64 / full54941 / cache " + evidence["cache_manifest_sha256"][:12]
            for arm, values in metrics["arms"].items():
                observations.append(
                    {
                        "id": f"{record['attempt_id']}:{arm}:{seed}",
                        "kind": "native_fit",
                        "study": study,
                        "cohort": cohort,
                        "campaign_id": record["campaign_id"],
                        "trial_id": record["trial_id"],
                        "attempt_id": record["attempt_id"],
                        "arm": arm,
                        "seed": seed,
                        "status": "completed",
                        "started_at": None,
                        "finished_at": None,
                        "observed_at": utc(heartbeat["last_progress_unix"]),
                        "fit_completed_at": utc(barrier["completed_unix"]),
                        "timestamp_basis": "final_full_FIT_diagnostic heartbeat; start unavailable",
                        "source_revision": evidence["source_revision"],
                        "source_sha256": metric_pin["digest"],
                        "completed_updates": heartbeat["step"],
                        "metrics": {
                            "person_mae_deg": values["person_mae_deg"],
                            "pooled_mae_deg": values["pooled_mae_deg"],
                        },
                    }
                )

    db = native / "research.sqlite"
    wal = native / "research.sqlite-wal"
    if wal.exists() and wal.stat().st_size:
        raise ValueError("Use an online backup for a database with a nonempty WAL")
    before = digest(read(db, 32 * 1024 * 1024))
    connection = sqlite3.connect(db.as_uri() + "?mode=ro&immutable=1", uri=True)
    records: dict[str, dict[str, Any]] = defaultdict(dict)
    with connection:
        for kind, identity, state, campaign, payload in connection.execute(
            "SELECT kind,id,state,campaign_id,payload FROM records"
        ):
            records[kind][identity] = {
                "state": state,
                "campaign": campaign,
                "data": json.loads(payload),
            }
    connection.close()
    if before != digest(read(db, 32 * 1024 * 1024)):
        raise ValueError("Native database changed during read")
    sources.append(
        {"study": "legacy-native-database", "sha256": before, "bytes": db.stat().st_size}
    )
    compound = {
        "spectral": ("spectral-shape-context-v1", "decision.json"),
        "boundary": ("boundary-replication-v1", "native-decision.json"),
        "normalization": ("token-normalization-context-v1", "native-decision.json"),
    }
    for result in records["run_result"].values():
        run = result["data"]
        trial = records["trial"][run["trial_id"]]["data"]
        campaign = records["campaign"][run["campaign_id"]]["data"]
        measurements = run["measurements"]
        base = {
            "study": campaign["name"],
            "campaign_id": run["campaign_id"],
            "trial_id": run["trial_id"],
            "attempt_id": run["attempt_id"],
            "seed": trial["spec"]["seed"],
            "started_at": run["started_at"],
            "finished_at": run["finished_at"],
            "timestamp_basis": "native run_result start/finish",
            "source_revision": trial["spec"]["source"]["base_revision"],
            "source_sha256": run["result_digest"],
            "status": "completed" if run["status"] == "succeeded" else run["status"],
        }
        if not measurements:
            observations.append(
                {
                    **base,
                    "id": run["attempt_id"],
                    "kind": "attempt",
                    "arm": "unscored",
                    "cohort": "unscored native attempts",
                }
            )
            continue
        compound_key = next(
            (key for key in compound if key + "_macro" in measurements[0]["name"]), None
        )
        if compound_key:
            study, name = compound[compound_key]
            raw = read(repo / "experiments/global" / study / name)
            decision = json.loads(raw)
            if decision["attempt_id"] != run["attempt_id"]:
                raise ValueError("Compound decision attempt differs")
            for seed, arms in decision["scores"].items():
                for arm, panels in arms.items():
                    full = panels["full"]
                    observations.append(
                        {
                            **base,
                            "id": f"{run['attempt_id']}:{arm}:{seed}",
                            "kind": "native_fit",
                            "study": study,
                            "arm": arm,
                            "seed": int(seed),
                            "cohort": "inner16 / GLOBAL / full-window",
                            "source_sha256": digest(raw),
                            "completed_updates": decision["target_per_arm"],
                            "timestamp_basis": (
                                "native compound run start/finish; arm time unavailable"
                            ),
                            "metrics": {
                                "person_mae_deg": full["person_mae_deg"],
                                "pooled_mae_deg": full["pooled_mae_deg"],
                            },
                        }
                    )
            continue
        arms: dict[str, dict[str, float]] = defaultdict(dict)
        for metric in measurements:
            arm, suffix = metric["name"].split("_", 1)
            if suffix == "macro_mae_deg":
                arms[arm]["person_mae_deg"] = metric["value"]
            elif suffix == "pooled_mae_deg":
                arms[arm]["pooled_mae_deg"] = metric["value"]
        for arm, values in arms.items():
            observations.append(
                {
                    **base,
                    "id": f"{run['attempt_id']}:{arm}:{base['seed']}",
                    "kind": "native_fit",
                    "arm": arm,
                    "cohort": "inner16 / RawEMA / full-window",
                    "completed_updates": 12000,
                    "metrics": values,
                }
            )
    executed = {row["trial_id"] for row in observations}
    for identity, record in records["trial"].items():
        if identity in executed:
            continue
        trial = record["data"]
        observations.append(
            {
                "id": identity,
                "kind": "attempt",
                "study": "legacy queued trial",
                "cohort": "unscored native attempts",
                "campaign_id": record["campaign"],
                "trial_id": identity,
                "attempt_id": None,
                "arm": "unscored",
                "seed": trial["spec"]["seed"],
                "status": record["state"],
                "timestamp_basis": "no execution timestamp retained",
                "source_revision": trial["spec"]["source"]["base_revision"],
                "source_sha256": trial["spec"]["spec_digest"],
            }
        )
    for identity, record in records["imported_evidence"].items():
        manifest = record["data"]["manifest"]
        comparison = manifest["comparison_identifiers"]
        observations.append(
            {
                "id": identity,
                "kind": "historical_import",
                "study": comparison.get("logical_baseline_id", manifest["source_record_id"]),
                "cohort": "historical imports / original execution time unavailable",
                "arm": "historical",
                "status": "historical",
                "timestamp_basis": "unavailable; import time excluded",
                "source_revision": comparison.get("scientific_source_revision", "unknown"),
                "source_sha256": manifest["raw_artifact"]["digest"],
                "metrics": {
                    (
                        "person_mae_deg" if item["name"] == "macro_mae_deg" else "pooled_mae_deg"
                    ): item["value"]
                    for item in manifest["observations"]
                    if item["panel"] == "full_adaptive_inner16"
                    and item["name"] in {"macro_mae_deg", "pooled_mae_deg"}
                    and item["value"] is not None
                },
            }
        )
    counts = {
        "trials": len({row["trial_id"] for row in observations if row.get("trial_id")}),
        "attempts": len({row["attempt_id"] for row in observations if row.get("attempt_id")}),
        "completed_fits": sum(row["kind"] == "native_fit" for row in observations),
        "FIT64_completed_fits": sum(row["cohort"].startswith("FIT64") for row in observations),
        "evaluator_receipts": len(records["evaluation_receipt"]),
        "historical_imports": len(records["imported_evidence"]),
        "failed_attempts": sum(row["status"] == "failed" for row in observations),
        "queued_trials": sum(row["status"] == "queued" for row in observations),
        "scored_native_observations": sum(
            row["kind"] == "native_fit" and bool(row.get("metrics")) for row in observations
        ),
    }
    history = NativeHistory.model_validate(
        {
            "captured_at": datetime.now(UTC).isoformat(),
            "observations": observations,
            "inventory": counts,
            "sources": sources,
            "limitations": [
                "Adaptive research; unresolved teacher exposure; no official score or promotion.",
                "FIT64, inner16 GLOBAL and inner16 RawEMA remain separate comparable cohorts.",
                "Compound scores use real run completion; individual arm times unavailable.",
                "AWS FIT64 timestamps are final diagnostic heartbeats; actual starts unavailable.",
                "Historical imports lack execution timestamps; visible but unplotted.",
                "Captured inventory only; reconstruct new experiments for a fresh snapshot.",
            ],
        }
    )
    return history.model_dump()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-repo", type=Path, required=True)
    parser.add_argument("--retained-dir", type=Path, required=True)
    parser.add_argument("--native-state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--additional-native-history", type=Path, action="append", default=[])
    args = parser.parse_args()
    result = reconstruct(args.evidence_repo, args.retained_dir, args.native_state)
    for additional_path in args.additional_native_history:
        additional = NativeHistory.model_validate_json(read(additional_path))
        rows = {row["id"]: row for row in result["observations"]}
        for item in additional.observations:
            candidate = absolute_level_timestamp(item.model_dump())
            previous = rows.get(item.id)
            if previous and previous["status"] == "completed" and previous != candidate:
                raise ValueError("Completed native history cannot be rewritten")
            rows[item.id] = candidate
        result["observations"] = list(rows.values())
        result["sources"].extend(additional.sources)
        result["limitations"] = list(
            dict.fromkeys([*result["limitations"], *additional.limitations])
        )
        result["inventory"].update(
            {
                "trials": len({row["trial_id"] for row in rows.values() if row["trial_id"]}),
                "attempts": len({row["attempt_id"] for row in rows.values() if row["attempt_id"]}),
                "completed_fits": sum(
                    row["kind"] == "native_fit" and row["status"] == "completed"
                    for row in rows.values()
                ),
                "FIT64_completed_fits": sum(
                    row["kind"] == "native_fit"
                    and row["status"] == "completed"
                    and row["cohort"].startswith("FIT64")
                    for row in rows.values()
                ),
                "scored_native_observations": sum(
                    row["kind"] == "native_fit" and bool(row.get("metrics"))
                    for row in rows.values()
                ),
                "running_observations": sum(row["status"] == "running" for row in rows.values()),
            }
        )
    result = NativeHistory.model_validate(result).model_dump()
    if args.output.exists() and not args.refresh:
        raise ValueError("Output exists; select explicit --refresh for atomic replacement")
    if args.output.exists():
        previous = json.loads(read(args.output))
        if {key: value for key, value in previous.items() if key != "captured_at"} == {
            key: value for key, value in result.items() if key != "captured_at"
        }:
            print(
                json.dumps(
                    {
                        "unchanged": True,
                        "captured_at": previous["captured_at"],
                        "inventory": previous["inventory"],
                    }
                )
            )
            return
    with tempfile.NamedTemporaryFile(mode="w", dir=args.output.parent, delete=False) as stream:
        scratch = Path(stream.name)
        try:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            scratch.replace(args.output)
        finally:
            scratch.unlink(missing_ok=True)
    print(json.dumps(result["inventory"]))


if __name__ == "__main__":
    main()
