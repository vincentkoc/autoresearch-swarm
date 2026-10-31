import assert from "node:assert/strict";
import test from "node:test";

import operator from "./production_checkpoint_fixture.mjs";

const LIMIT = 6 * 1024 * 1024;
const captured =
  '{"schema_version":"1.0","envelope_base64":"eA==","chunks_base64":["eQ=="]}';

function request(action, { letter = "a", drop = false } = {}) {
  return new Request(`http://127.0.0.1/fixture/target-${letter}/${action}`, {
    method: "POST",
    body: JSON.stringify({
      credential: "target-credential",
      source_credential: "source-credential",
      request_json: '{"backup_id":"fixture"}',
    }),
    headers: drop ? { "x-fixture-drop-bootstrap": "true" } : {},
  });
}

for (const [action, expected] of [
  ["import", captured.length],
  ["import-limit", LIMIT],
  ["import-over-limit", LIMIT + 1],
]) {
  test(`${action} retains signed source content and uses the fixed target`, async () => {
    const calls = [];
    const result = {
      schema_version: "1.0",
      receipt: { restore_id: "fixture" },
    };
    const reply = await operator.fetch(request(action, { letter: "b" }), {
      RESEARCH_BOOTSTRAP: {
        checkpoint_capture: async (credential, payload) => {
          assert.equal(credential, "source-credential");
          assert.equal(payload, '{"backup_id":"fixture"}');
          return captured;
        },
      },
      TARGET_A_BOOTSTRAP: {
        checkpoint_import: () => assert.fail("Wrong target selected"),
      },
      TARGET_B_BOOTSTRAP: {
        checkpoint_import: async (credential, bundle) => {
          assert.equal(credential, "target-credential");
          assert.equal(bundle.length, expected);
          assert.equal(bundle.trimEnd(), captured);
          assert.deepEqual(JSON.parse(bundle), JSON.parse(captured));
          calls.push(true);
          return JSON.stringify(result);
        },
      },
    });
    assert.deepEqual(await reply.json(), { bundle_bytes: expected, result });
    assert.deepEqual(calls, [true]);
  });
}

test("dropped import reply still follows the target call", async () => {
  let called = false;
  const reply = await operator.fetch(request("import", { drop: true }), {
    RESEARCH_BOOTSTRAP: { checkpoint_capture: async () => captured },
    TARGET_A_BOOTSTRAP: {
      checkpoint_import: async () => {
        called = true;
        return '{"receipt":{}}';
      },
    },
  });
  assert.equal(called, true);
  assert.equal(await reply.text(), "{");
});

test("source refusal never reaches target import", async () => {
  const reply = await operator.fetch(request("import"), {
    RESEARCH_BOOTSTRAP: {
      checkpoint_capture: async () => '{"error":{"code":"STALE_FENCE"}}',
    },
    TARGET_A_BOOTSTRAP: {
      checkpoint_import: () => assert.fail("Source refusal was ignored"),
    },
  });
  assert.equal(reply.status, 409);
});

test("default import is denied without touching source or private target", async () => {
  const reply = await operator.fetch(request("default-import"), {
    TARGET_A_API: {},
  });
  assert.deepEqual(await reply.json(), { denied: true });
});

test("target capabilities forward only the fixed public route", async () => {
  const reply = await operator.fetch(
    new Request("http://127.0.0.1/fixture/target-a/capabilities", {
      headers: { authorization: "Bearer synthetic" },
    }),
    {
      TARGET_A_API: {
        fetch: async (request) => {
          assert.equal(new URL(request.url).pathname, "/v1/capabilities");
          assert.equal(
            request.headers.get("authorization"),
            "Bearer synthetic",
          );
          return new Response("inactive", { status: 409 });
        },
      },
    },
  );
  assert.equal(reply.status, 409);
  assert.equal((await operator.fetch(request("capabilities"), {})).status, 404);
});

test("the checkpoint operator refuses non-loopback authorities", async () => {
  const reply = await operator.fetch(
    new Request("https://example.invalid/fixture/targets"),
    {},
  );
  assert.equal(reply.status, 403);
});
