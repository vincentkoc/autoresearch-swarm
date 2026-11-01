"""Machine JSON stays canonical UTF-8 even when Python's text streams use ASCII."""

import io
import os
import subprocess
import sys
from contextlib import redirect_stdout

import pytest

from research_swarm.cli import main
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.runtime import LocalRuntime
from tests.projects.test_manifest import project as project


def ascii_process(arguments, payload):
    return subprocess.run(
        [sys.executable, *arguments],
        input=canonical_bytes(payload),
        capture_output=True,
        env={**os.environ, "PYTHONIOENCODING": "ascii:strict"},
        timeout=30,
    )


def test_committed_unicode_command_replays_as_exact_utf8_json(project, tmp_path):
    title = "研究 → café 🦊"
    manifest = project / "research.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace("CPU research example", title),
        encoding="utf-8",
    )
    runtime = LocalRuntime.open(tmp_path / "runtime")
    client = ResearchClient(LocalTransport(runtime))
    acknowledged = client.register_project(project, idempotency_key="source")
    payload = {
        "source_artifact": acknowledged["result"]["project_revision"]["data"]["source_artifact"]
    }
    result = ascii_process(
        [
            "-m",
            "research_swarm.cli",
            "--state-dir",
            str(runtime.root),
            "command",
            "project.register",
            "--key",
            "source",
        ],
        payload,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    assert result.stdout == canonical_bytes(acknowledged) + b"\n"
    assert strict_json_loads(result.stdout)["result"]["project"]["data"]["name"] == title
    assert result.stderr == b""
    capture = io.StringIO()
    with redirect_stdout(capture):
        assert (
            main(
                [
                    "--state-dir",
                    str(runtime.root),
                    "get",
                    "project",
                    acknowledged["result"]["project"]["id"],
                ]
            )
            == 0
        )
    assert (
        capture.getvalue()
        == canonical_bytes(acknowledged["result"]["project"]).decode("utf-8") + "\n"
    )


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_json_streams_preserve_unicode_under_a_restrictive_text_codec(stream):
    payload = {"message": "é → 研究 🦊", "value": "line one\nline two"}
    result = ascii_process(
        [
            "-c",
            "import sys; from research_swarm.cli import _emit_json; "
            "from research_swarm.domain.canonical import strict_json_loads; "
            f"_emit_json(strict_json_loads(sys.stdin.buffer.read()), stream=sys.{stream})",
        ],
        payload,
    )
    assert result.returncode == 0
    assert getattr(result, stream) == canonical_bytes(payload) + b"\n"
    assert getattr(result, "stderr" if stream == "stdout" else "stdout") == b""
