"""List and collect campaign-owned data and managed compute."""

from __future__ import annotations

import argparse
import os
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from research_swarm.swarm.backends.ec2_gc import EC2, CloudResource
from research_swarm.swarm.backends.jobs import Inventory, Job
from research_swarm.swarm.backends.skypilot import TERMINAL, SkyCLI
from research_swarm.swarm.data_stage import Stage, read_stage, revision_lock, stage_path


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
    gc.add_argument("--sky", metavar="CLI", help="SkyPilot CLI used for this inventory")
    gc.add_argument(
        "--aws", default="aws", metavar="CLI", help="EC2 CLI for recorded AWS placements"
    )


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
    with revision_lock(path):
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


async def _cloud_resources(jobs: list[Job], campaign: str, ec2: EC2) -> list[CloudResource]:
    resources = []
    for region in sorted({region for job in jobs for region in job.aws_regions}):
        for kind in ("instance", "volume"):
            resources.extend(await ec2.find(campaign, jobs, region, kind))
    return resources


async def _collect_job(
    job: Job,
    children: list[CloudResource],
    sky: SkyCLI,
    ec2: EC2,
    queue: list[dict[str, Any]],
    keep: set[str],
    apply: bool,
    inventory: Inventory,
) -> list[Entry]:
    if apply:
        fresh = await _cloud_resources([job], job.campaign, ec2)
        if {c.id for c in children} - {c.id for c in fresh}:
            return [Entry(f"sky:{job.name}", "managed-job", "", "changed", "inventory-changed")]
        children = fresh
    row = job.match(queue)
    status = row["status"] if row else ("TERMINAL" if job.terminal else "UNRESOLVED")
    parent = Entry(
        f"sky:{job.name}", "managed-job", str(row["job_id"] if row else job.job_id or ""), status
    )
    entries = [parent, *(Entry(c.id, c.kind, c.region, c.state) for c in children)]
    # Cancelling a job can remove its attached disks. Preserve the ownership group.
    if any(item.id in keep for item in entries):
        for item in entries:
            item.action = "kept"
        return entries
    if not apply:
        return entries
    volumes = {c.id for c in children if c.kind == "volume"}
    if any(not volumes.issuperset(c.delete_with_instance) for c in children):
        parent.action = "foreign-dependent-volume"
        return entries
    if row and status not in TERMINAL:
        if not await sky.cancel(job):
            parent.action = "unresolved"
            return entries
        # The provider owns teardown until its managed job is terminal.
        parent.action = "cancel-requested"
        row = job.match(await sky.queue())
        if row is None or row["status"] not in TERMINAL:
            return entries
        status = row["status"]
    if status not in TERMINAL and status != "TERMINAL":
        parent.action = "unresolved"
        return entries
    job.terminal = True
    inventory.save(job)
    parent.action = "terminal"
    for child, entry in zip(children, entries[1:], strict=True):
        try:
            entry.action = await ec2.remove(child, keep)
        except (OSError, ValueError, RuntimeError, TimeoutError) as error:
            entry.action = f"error: {error}"
    # Keep the ownership record for retry and later provider inventory.
    return entries


async def collect(args: argparse.Namespace, state: Path) -> tuple[dict[str, Any], int]:
    keep = set(args.keep)
    entries = _stages(args.campaign_id, args.volume, keep, args.apply)
    inventory = Inventory(state)
    jobs = inventory.jobs(args.campaign_id)
    if jobs:
        sky, ec2 = SkyCLI(args.sky), EC2(args.aws)
        try:
            queue = await sky.queue()
            resources = await _cloud_resources(jobs, args.campaign_id, ec2)
            for job in jobs:
                try:
                    entries.extend(
                        await _collect_job(
                            job,
                            [c for c in resources if c.job.name == job.name],
                            sky,
                            ec2,
                            queue,
                            keep,
                            args.apply,
                            inventory,
                        )
                    )
                except (OSError, ValueError, RuntimeError, TimeoutError) as error:
                    entries.append(
                        Entry(f"sky:{job.name}", "managed-job", "", "unknown", f"error: {error}")
                    )
        except (OSError, ValueError, RuntimeError, TimeoutError) as error:
            entries.append(Entry("inventory", "provider", "", "unknown", f"error: {error}"))
    pending = any(
        e.action not in {"listed", "kept", "terminal", "removed", "absent", "absent-or-retagged"}
        for e in entries
    )
    return {
        "campaign": args.campaign_id,
        "resources": [asdict(e) for e in entries],
    }, 3 if pending else 0
