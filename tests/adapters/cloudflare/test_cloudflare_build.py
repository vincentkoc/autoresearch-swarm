"""Production packaging excludes fixture capabilities and preserves shared source."""

import ast
import json
import os
import re
import subprocess
import sys

import pytest

from adapters.cloudflare.build import (
    COMMON,
    PRODUCTION,
    QUALIFICATION,
    ROOT,
    SOURCE,
    SOURCES,
    build,
)


def test_adapter_directory_does_not_shadow_python_standard_library():
    result = subprocess.run(
        [sys.executable, "-c", "import platform; print(platform.python_version())"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()


def test_production_and_qualification_bundles_share_implementation_without_fixture_exports(
    tmp_path,
):
    production = build("production", output=tmp_path / "production")
    qualification = build(output=tmp_path / "qualification")
    assert (production / "entry.py").read_bytes() == (ROOT / "production.py").read_bytes()
    assert (qualification / "entry.py").read_bytes() == (ROOT / "entry.py").read_bytes()
    for name in COMMON:
        assert (production / name).read_bytes() == (qualification / name).read_bytes()
    assert all((production / name).is_file() for name in PRODUCTION)
    assert all(not (production / name).exists() for name in QUALIFICATION)
    assert not (production / "production_operator_fixture.mjs").exists()
    assert not (production / "production_checkpoint_fixture.mjs").exists()
    assert all((qualification / name).is_file() for name in QUALIFICATION)
    assert not tuple(production.rglob("__pycache__"))
    for path in SOURCE.rglob("*.py"):
        copied = production / "research_swarm" / path.relative_to(SOURCE)
        assert copied.read_bytes() == path.read_bytes()
    classes = {
        node.name
        for node in ast.parse((production / "entry.py").read_text()).body
        if isinstance(node, ast.ClassDef)
    }
    assert classes == {"Default", "Bootstrap", "Recovery", "ResearchAuthority"}
    commands = json.loads((production / "commands.json").read_text())
    assert commands == sorted(set(commands))
    assert commands == json.loads((qualification / "commands.json").read_text())
    assert "x-fixture-" not in (production / "entry.py").read_text()


def test_rebuild_cannot_replace_foreign_or_other_target_output(tmp_path):
    output = tmp_path / "build"
    output.mkdir()
    retained = output / "retained.txt"
    retained.write_text("Retain unrelated files")
    with pytest.raises(RuntimeError, match="unowned"):
        build("production", output=output)
    assert retained.read_text() == "Retain unrelated files"
    dedicated = build("production", output=tmp_path / "production")
    with pytest.raises(RuntimeError, match="different-target"):
        build("qualification", output=dedicated)
    stale = dedicated / "old-build-file"
    stale.write_text("Disposable owned build output")
    build("production", output=dedicated)
    assert not stale.exists()


@pytest.mark.skipif(os.name == "nt", reason="Local symlink privilege is not assumed")
def test_build_refuses_linked_output_and_linked_ownership_marker(tmp_path):
    owned = build("production", output=tmp_path / "owned")
    linked = tmp_path / "linked"
    linked.symlink_to(owned, target_is_directory=True)
    with pytest.raises(RuntimeError, match="linked"):
        build("production", output=linked)
    marker = owned / ".research-cloudflare-build"
    foreign = tmp_path / "marker"
    foreign.write_text(marker.read_text())
    marker.unlink()
    marker.symlink_to(foreign)
    with pytest.raises(RuntimeError, match="unowned"):
        build("production", output=owned)
    assert foreign.read_text() == "production-v1\n"


def test_unknown_target_never_creates_output(tmp_path):
    output = tmp_path / "absent"
    with pytest.raises(ValueError, match="Unknown"):
        build("typo", output=output)
    assert not output.exists()


def test_duplicate_module_destination_preserves_existing_output(tmp_path, monkeypatch):
    output = build("production", output=tmp_path / "production")
    retained = output / "retained.txt"
    retained.write_text("Keep the previous build on invalid configuration")
    monkeypatch.setitem(SOURCES, "research/duplicate.py", "atomic.py")
    with pytest.raises(RuntimeError, match="Duplicate generated"):
        build("production", output=output)
    assert retained.read_text() == "Keep the previous build on invalid configuration"


def test_production_template_has_no_public_ingress_or_embedded_secrets():
    # JSONC comments occupy complete lines in this operator template.
    path = ROOT / "wrangler.production.jsonc"
    raw = "\n".join(
        line for line in path.read_text().splitlines() if not line.lstrip().startswith("//")
    )
    config = json.loads(re.sub(r",(\s*[}\]])", r"\1", raw))
    assert config["main"] == ".generated-production/entry.py"
    assert config["workers_dev"] is config["preview_urls"] is False
    assert not config.get("routes") and not config.get("vars")
    assert config["durable_objects"]["bindings"] == [
        {"name": "RESEARCH_AUTHORITY", "class_name": "ResearchAuthority"}
    ]
    assert config["r2_buckets"][0]["binding"] == "ARTIFACTS"
