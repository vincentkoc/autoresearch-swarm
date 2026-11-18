"""Collect EC2 resources carrying the exact labels of a recorded SkyPilot job."""

from __future__ import annotations

import json
from dataclasses import dataclass

from research_swarm.swarm.backends.jobs import Job, key
from research_swarm.swarm.backends.remote import command


@dataclass(frozen=True)
class CloudResource:
    kind: str
    id: str
    region: str
    state: str
    job: Job
    delete_with_instance: tuple[str, ...] = ()


class EC2:
    def __init__(self, aws: str) -> None:
        self.aws = aws

    async def call(self, region: str, *args: str, timeout: float = 60.0) -> str:
        return await command(
            [self.aws, "ec2", *args, "--region", region, "--output", "json"], timeout=timeout
        )

    async def find(
        self, campaign: str, jobs: list[Job], region: str, kind: str, resource_id: str | None = None
    ) -> list[CloudResource]:
        owners = {job.name: job for job in jobs if region in job.aws_regions}
        # Filters are an optimization. Repeat every membership check on the response.
        argv = [
            f"describe-{kind}s",
            "--filters",
            "Name=tag:swarm-managed,Values=true",
            f"Name=tag:swarm-campaign,Values={key(campaign)[:32]}",
        ]
        if resource_id:
            argv.append(f"Name={kind}-id,Values={resource_id}")
        data = json.loads(await self.call(region, *argv))
        rows = (
            [r for bucket in data["Reservations"] for r in bucket["Instances"]]
            if kind == "instance"
            else data["Volumes"]
        )
        resources = []
        for row in rows:
            tags = {t["Key"]: t["Value"] for t in row.get("Tags", [])}
            owner = owners.get(tags.get("swarm-job", ""))
            if owner is None or not owner.labels.items() <= tags.items():
                continue
            identifier = row["InstanceId" if kind == "instance" else "VolumeId"]
            if resource_id is not None and resource_id != identifier:
                continue
            state = row["State"]["Name"] if kind == "instance" else row["State"]
            if state in {"terminated", "deleted"}:
                continue
            children = tuple(
                b["Ebs"]["VolumeId"]
                for b in row.get("BlockDeviceMappings", [])
                if b.get("Ebs", {}).get("DeleteOnTermination")
            )
            resources.append(CloudResource(kind, identifier, region, state, owner, children))
        return resources

    async def remove(self, item: CloudResource, keep: set[str]) -> str:
        current = await self.find(item.job.campaign, [item.job], item.region, item.kind, item.id)
        if not current:
            return "absent-or-retagged"
        fresh = current[0]
        if fresh.kind == "instance":
            if keep.intersection(fresh.delete_with_instance):
                return "kept"
            for disk in fresh.delete_with_instance:
                if not await self.find(item.job.campaign, [item.job], item.region, "volume", disk):
                    return "foreign-dependent-volume"
            if fresh.state != "shutting-down":
                await self.call(fresh.region, "terminate-instances", "--instance-ids", fresh.id)
            await self.call(
                fresh.region, "wait", "instance-terminated", "--instance-ids", fresh.id, timeout=600
            )
        elif fresh.state == "available":
            await self.call(fresh.region, "delete-volume", "--volume-id", fresh.id)
        else:
            return f"volume-{fresh.state}"
        return "removed"
