"""List and collect prepared campaign datasets."""

from __future__ import annotations

import argparse
import os
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from filelock import FileLock

from research_swarm.swarm.data_stage import Stage, read_stage, stage_path


def register(commands: argparse._SubParsersAction[Any]) -> None:
    campaign = commands.add_parser("campaign", help="Manage swarm campaign resources")
    actions = campaign.add_subparsers(dest="campaign_action", required=True)
    gc = actions.add_parser("gc", help="List campaign resources or apply scoped cleanup")
    gc.add_argument("campaign_id")
    gc.add_argument(
        "--volume", action="append", default=[], type=Path, help="Prepared data volume root"
    )
    gc.add_argument(
        "--keep", action="append", default=[], metavar="ID", help="Resource ID from listing"
    )
    gc.add_argument("--apply", action="store_true", help="Remove listed, eligible resources")


@dataclass
class Entry:
    id: str
    kind: str
    location: str
    state: str
    action: str = "listed"


def _mounted(data: Path) -> bool:
    # In a Linux worker's mount namespace, a bind target aliases the data inode.
    mounts = Path("/proc/self/mountinfo")
    if not mounts.exists():
        return data.is_mount()
    identity = data.stat()
    for line in mounts.read_text().splitlines():
        target = line.split()[4].replace(r"\040", " ").replace(r"\011", "\t").replace(r"\134", "\\")
        if Path(target).is_relative_to(data):
            return True
        try:
            if os.path.samestat(identity, os.stat(target)):
                return True
        except FileNotFoundError:
            continue
    return False


def _remove_stage(path: Path, expected: Stage) -> str:
    with FileLock(path.with_name(f".{path.name}.lock")):
        pending = path.with_name(f".gc.{path.name}.json")
        if not path.exists() and not pending.exists():
            return "absent"
        if path.resolve() != path or read_stage(path, pending_gc=pending.exists()) != expected:
            return "changed"
        if path.exists():
            if set(os.listdir(path)) - {"stage.json", "data", "mount-namespace"}:
                return "changed"
            namespace = path / "mount-namespace"
            if namespace.exists() and namespace.read_text() != os.readlink("/proc/self/ns/mnt"):
                return "mounted-elsewhere"
            data = path / "data"
            if data.exists() and (data.is_symlink() or _mounted(data)):
                return "mounted"
            if not pending.exists():
                (path / "stage.json").rename(pending)
            elif (path / "stage.json").exists():
                return "changed"
            if data.exists():
                shutil.rmtree(data)
            namespace.unlink(missing_ok=True)
            path.rmdir()
        pending.unlink()
        return "removed"


def _stages(campaign: str, volumes: list[Path], keep: set[str], apply: bool) -> list[Entry]:
    result = []
    for volume in sorted({v.resolve() for v in volumes}):
        root = stage_path(volume, campaign, "revision").parent
        if not root.is_dir():
            continue
        for marker in sorted(root.iterdir()):
            if marker.is_symlink():
                continue
            pending = (
                marker.is_file()
                and marker.name.startswith(".gc.")
                and marker.name.endswith(".json")
            )
            if not pending and not (marker / "stage.json").is_file():
                continue
            revision = marker.name[4:-5] if pending else marker.name
            try:
                path = stage_path(volume, campaign, revision)
                stage = read_stage(path, pending_gc=pending)
            except (ValueError, TypeError, OSError):
                continue
            item = Entry(f"data:{stage.revision}", "data", str(volume), stage.sha256)
            if item.id in keep:
                item.action = "kept"
            elif apply:
                try:
                    item.action = _remove_stage(path, stage)
                except (ValueError, OSError) as error:
                    item.action = f"error: {error}"
            result.append(item)
    return result


def collect(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    entries = _stages(args.campaign_id, args.volume, set(args.keep), args.apply)
    pending = any(entry.action not in {"listed", "kept", "removed", "absent"} for entry in entries)
    return {
        "campaign": args.campaign_id,
        "resources": [asdict(entry) for entry in entries],
    }, 3 if pending else 0
