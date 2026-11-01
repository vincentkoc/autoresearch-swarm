"""Golden identity bytes checked by Python and an independent ECMAScript process."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Any

import pytest

from research_swarm.domain.canonical import (
    canonical_bytes,
    content_digest,
    record_digest,
    strict_json_loads,
)
from research_swarm.workers.signatures import signing_bytes
from tests.domain.test_assignment_signatures import assignment_payload, signer
from tests.paths import TEST_ROOT

FIXTURES = TEST_ROOT / "conformance"
CORPUS = json.loads((FIXTURES / "canonical-v1.json").read_text(encoding="utf-8"))


def node(operation: str, payload: Any, *, accepted: bool = True) -> Any:
    executable = shutil.which("node")
    if executable is None:
        if os.environ.get("RESEARCH_REQUIRE_NODE") == "1":
            pytest.fail("Required cross-language runtime is unavailable")
        pytest.skip("Cross-language qualification requires Node.js 22 or later")
    result = subprocess.run(
        [executable, str(FIXTURES / "canonical_reference.mjs"), operation],
        input=json.dumps(payload, ensure_ascii=True, allow_nan=False),
        text=True,
        encoding="utf-8",
        capture_output=True,
        timeout=10,
    )
    if not accepted:
        assert result.returncode != 0
        return None
    assert result.returncode == 0, result.stderr
    return strict_json_loads(result.stdout)


@pytest.mark.parametrize("fixture", CORPUS["cases"], ids=lambda fixture: fixture["id"])
def test_python_matches_frozen_canonical_corpus(fixture: dict[str, Any]) -> None:
    payload = strict_json_loads(fixture["source_json"])
    encoded = canonical_bytes(payload)
    assert encoded == fixture["canonical_utf8"].encode("utf-8")
    assert content_digest(encoded) == fixture["content_digest"]
    assert record_digest(fixture["domain"], payload) == fixture["record_digest"]


def test_independent_javascript_matches_all_frozen_bytes_and_hashes() -> None:
    expected = [
        {key: fixture[key] for key in ("id", "canonical_utf8", "content_digest", "record_digest")}
        for fixture in CORPUS["cases"]
    ]
    assert node("canonical", CORPUS["cases"]) == expected
    by_id = {fixture["id"]: fixture for fixture in expected}
    assert by_id["empty"]["record_digest"] != by_id["explicit_null"]["record_digest"]
    assert by_id["composed"]["record_digest"] != by_id["decomposed"]["record_digest"]
    assert by_id["empty"]["record_digest"] != by_id["other_domain"]["record_digest"]


@pytest.mark.parametrize("source", ['{"value":"\\ud800"}', '{"\\udfff":1}', '{"value":1e309}'])
def test_both_serializers_reject_invalid_unicode_and_nonfinite_values(source: str) -> None:
    with pytest.raises(ValueError):
        canonical_bytes(strict_json_loads(source))
    node(
        "canonical",
        [{"id": "invalid", "domain": "research.fixture", "source_json": source}],
        accepted=False,
    )


def test_javascript_verifies_python_assignment_signature_and_rejects_tampering() -> None:
    payload = assignment_payload()
    authority = signer(payload)
    signed = authority.sign(payload)
    wire = {
        "authority": authority.authority.model_dump(mode="json"),
        "assignment": signed.model_dump(mode="json"),
    }
    result = node("signature", wire)
    assert result["valid_signature"]
    assert result["preimage_digest"] == content_digest(signing_bytes(payload, signed.key_id))
    assert (
        result["preimage_digest"]
        == "sha256:d2e6abe5eaa8e9c5ebfba1324f5965c0b7ec14752c60754215b1a01515ff6e57"
    )
    wire["assignment"]["payload"]["spec"]["seed"] = 8
    assert not node("signature", wire)["valid_signature"]
