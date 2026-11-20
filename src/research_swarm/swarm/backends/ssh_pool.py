"""Dispatch to dedicated, already provisioned EC2 hosts through OpenSSH.

Install the swarm and its keeper systemd service on each Linux host. The SSH
account belongs to the swarm group, can bind the configured read-only mount,
and has the workload checkout at task_dir. Configure a stable SSH alias for
each instance. The location volume and NVMe roots are existing mounted paths.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shlex
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from research_swarm.swarm.backends import remote
from research_swarm.swarm.backends.capacity import Slots
from research_swarm.swarm.backends.keeper import ACTIVITY, active
from research_swarm.swarm.backends.remote import BootData as BootData
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, FailureClass, Result
from research_swarm.swarm.process import run as run_command


@dataclass(frozen=True, kw_only=True)
class PoolHost:
    target: str
    instance_id: str
    region: str
    task_dir: str
    python: str = "python3"
    device: str = "cuda"
    data: BootData | None = None


class SSHPoolBackend:
    startup_timeout_seconds = 2640.0  # EC2 readiness plus the worker-local data copy.

    def __init__(self, hosts: list[PoolHost], *, ssh: str = "ssh", aws: str = "aws") -> None:
        if len({h.target for h in hosts}) != len(hosts) or len(
            {(h.region, h.instance_id) for h in hosts}
        ) != len(hosts):
            raise ValueError("pool hosts and instance identities must be unique")
        if any(
            not h.target or h.target.startswith("-") or any(c.isspace() for c in h.target)
            for h in hosts
        ):
            raise ValueError("invalid SSH target")
        self._slots = Slots(len(hosts))
        self._available = list(hosts)
        self.ssh, self.aws = ssh, aws
        self._stopping = False

    def free_slots(self, spec: CampaignSpec) -> int:
        return 0 if self._stopping else self._slots.free(spec)

    async def _ec2(self, host: PoolHost, *args: str, timeout: float = 60.0) -> str:
        return await remote.command(
            [self.aws, "ec2", *args, "--region", host.region, "--output", "json"], timeout=timeout
        )

    async def _prepare_host(self, host: PoolHost) -> None:
        # Configure both attributes before allowing a stopped instance to boot.
        await self._ec2(
            host,
            "modify-instance-attribute",
            "--instance-id",
            host.instance_id,
            "--disable-api-termination",
        )
        await self._ec2(
            host,
            "modify-instance-attribute",
            "--instance-id",
            host.instance_id,
            "--instance-initiated-shutdown-behavior",
            "Value=stop",
        )
        state = json.loads(
            await self._ec2(
                host,
                "describe-instances",
                "--instance-ids",
                host.instance_id,
                "--query",
                "Reservations[0].Instances[0].State.Name",
            )
        )
        if state == "stopped":
            await self._ec2(host, "start-instances", "--instance-ids", host.instance_id)
        elif state not in {"running", "pending"}:
            raise RuntimeError(f"instance is unavailable: {state}")
        if state != "running":
            await self._ec2(
                host, "wait", "instance-status-ok", "--instance-ids", host.instance_id, timeout=600
            )

    async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
        async with self._slots.claim(spec):
            if self._stopping:
                return Result(
                    attempt=attempt,
                    status="lost",
                    error="pool is stopping",
                    failure_class="backend_setup",
                )
            host = self._available.pop(0)
            failure_class: FailureClass = "backend_setup"
            try:
                await self._prepare_host(host)
                worker = shlex.join(
                    [host.python, "-m", "research_swarm.swarm.backends.ssh_pool", "run"]
                )
                payload = remote.request(spec, attempt, host.task_dir, host.device)
                payload["data"] = asdict(host.data) if host.data else None
                payload["acknowledge_start"] = True
                failure_class = "remote_dispatch"
                output = await remote.command(
                    [
                        self.ssh,
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "ConnectTimeout=15",
                        "-o",
                        "ServerAliveInterval=15",
                        "-o",
                        "ServerAliveCountMax=3",
                        "--",
                        host.target,
                        worker,
                    ],
                    timeout=spec.trial_timeout_seconds + 1800,
                    payload=json.dumps(payload, allow_nan=False).encode(),
                    started_attempt=attempt.attempt_id,
                )
                failure_class = "invalid_output"
                return remote.read_result(output, attempt)
            except (OSError, ValueError, RuntimeError, TimeoutError) as error:
                return Result(
                    attempt=attempt,
                    status="lost",
                    error=str(error) or type(error).__name__,
                    failure_class=failure_class,
                )
            finally:
                self._available.append(host)

    async def stop(self) -> None:
        """Stop this dedicated pool after draining. A failed stop can be retried."""
        self._stopping = True
        if self._slots.active:
            raise RuntimeError("pool is draining active attempts")
        for host in self._available:
            await self._ec2(host, "stop-instances", "--instance-ids", host.instance_id)


def from_spec(spec: CampaignSpec) -> SSHPoolBackend:
    """Build a dedicated pool from options.backend.hosts in campaign YAML."""
    options = dict(getattr(spec, "options", {}).get("backend", {}))
    hosts = []
    for entry in options.pop("hosts"):
        fields = dict(entry)
        data = fields.pop("data", None)
        hosts.append(PoolHost(**fields, data=BootData(**data) if data is not None else None))
    return SSHPoolBackend(hosts, **options)


async def run_on_host(payload: dict[str, Any], activity: Path = ACTIVITY) -> None:
    run_command(["systemctl", "is-active", "--quiet", "research-swarm-keeper.service"], check=True)
    with active(activity):
        await remote.run(payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["run"])
    parser.parse_args()
    asyncio.run(run_on_host(json.load(sys.stdin)))
