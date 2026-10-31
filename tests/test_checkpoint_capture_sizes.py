"""Scalar admission measures escaped bytes without allocating unbounded payloads."""

import pytest

from research_swarm.checkpoints.capture import CaptureLimits, capture_rows, preflight_rows
from research_swarm.checkpoints.image_format import row_values
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.checkpoint_images import LogicalChunk
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from tests.test_checkpoint_schema import source as source
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap


def add_command(source, payload):
    row = (
        source.manifest.configuration.authority.workspace_id,
        source.manifest.provisioning_actor_id,
        new_id(),
        content_digest(b"synthetic request"),
        new_id(),
        payload,
        "created",
    )
    source.store.write(lambda sql: sql.execute("INSERT INTO commands VALUES (?,?,?,?,?,?,?)", row))
    return row


@pytest.mark.parametrize(
    "payload",
    [
        "x" * 12000,
        '\\"' * 6000,
        "".join(map(chr, range(32))) * 200,
        "\0suffix" * 1500,
        "\\u0000" * 2000,
        "é中\ue000\U00010000\u2028\u2029" * 1000,
        '[1,true,null,{"x":2}]' * 1000,
    ],
    ids=["ascii", "quote-slash", "controls", "nul-suffix", "literal-escape", "unicode", "json"],
)
async def test_exact_escaped_row_limit_preserves_bytes_and_one_less_refuses(source, payload):
    expected = add_command(source, payload)
    size = len(canonical_bytes(expected))

    def read(sql):
        limits = CaptureLimits(row_bytes=size)
        return capture_rows(
            sql, backup_id=new_id(), inventory=preflight_rows(sql, limits), limits=limits
        )

    image = source.store.read(read)
    commands = [
        row_values(chunk.table, row)
        for body in image.bodies
        if (chunk := LogicalChunk.model_validate_json(body)).table == "commands"
        for row in chunk.rows
    ]
    assert expected in commands
    with pytest.raises(ResearchError) as refused:
        source.store.read(lambda sql: preflight_rows(sql, CaptureLimits(row_bytes=size - 1)))
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


@pytest.mark.parametrize("bound", ["raw_row", "raw_image", "escaped_row", "escaped_image"])
async def test_raw_and_escaped_bounds_refuse_before_portable_payload_reads(source, bound):
    payload, count, limits = {
        "raw_row": ("x" * 65536, 1, CaptureLimits()),
        "raw_image": ("x" * 40000, 2, CaptureLimits(image_bytes=75000)),
        "escaped_row": ("\0" * 12000, 1, CaptureLimits()),
        "escaped_image": ("\0" * 6000, 2, CaptureLimits(image_bytes=65000)),
    }[bound]
    for _ in range(count):
        add_command(source, payload)
    quoted = []

    class Guard:
        def __init__(self, sql):
            self.sql = sql

        def execute(self, statement, parameters=()):
            assert not statement.startswith('SELECT "'), "payload read before size admission"
            if 'FROM "commands"' in statement and "json_quote(" in statement:
                assert bound.startswith("escaped"), "raw overflow reached JSON allocation"
                quoted.append(True)
            return self.sql.execute(statement, parameters)

    with pytest.raises(ResearchError) as refused:
        source.store.read(lambda sql: preflight_rows(Guard(sql), limits))
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert bool(quoted) == bound.startswith("escaped")
