"""Python-to-JavaScript async byte transport against an actual local R2 binding."""

import httpx

from research_swarm.domain.canonical import content_digest
from tests.adapters.cloudflare.test_cloudflare_native import build_current_sources, server
from tests.adapters.cloudflare.test_cloudflare_native import pytestmark as pytestmark


def exercise(url, case):
    with httpx.Client(trust_env=False, timeout=30) as client:
        result = client.post(url + "/r2-bytes/" + case)
    assert result.status_code == 200, result.text[-4000:]
    return result.json()


def test_async_binding_roundtrip_limit_uncertainty_and_permanent_retirement(tmp_path):
    build_current_sources()
    with server(tmp_path, sequence=1) as url:
        small = exercise(url, "roundtrip")
        assert small == {
            "verified": True,
            "size_bytes": 256,
            "digest": content_digest(bytes(range(256))),
        }
        assert exercise(url, "limit") == {
            "verified": True,
            "size_bytes": 1024**2,
            "digest": content_digest(bytes(range(256)) * 4096),
        }
        assert exercise(url, "lost-response") == {"resumed_same_generation": True}
        assert exercise(url, "corrupt") == {"corruption_refused": True}
        assert exercise(url, "late") == {"late_put_refused": True, "permanent_fence": True}
    with server(tmp_path, sequence=2) as url:
        assert exercise(url, "roundtrip") == small
        assert exercise(url, "retired") == {"permanent_fence": True}
