import assert from "node:assert/strict";
import test from "node:test";

import operator from "./production_operator_fixture.mjs";

function request(path, payload, drop = false) {
  return new Request(`http://127.0.0.1${path}`, {
    method: "POST",
    body: JSON.stringify(payload),
    headers: drop ? { "x-fixture-drop-bootstrap": "true" } : {},
  });
}

for (const action of ["seal", "capture"]) {
  test(`private ${action} forwards one unchanged JSON string and can lose its reply`, async () => {
    const calls = [];
    const payload = {
      credential: "synthetic",
      request_json: '{"backup_id":"fixture"}',
    };
    const result = '{"schema_version":"1.0","fixture":"result"}';
    const env = {
      RESEARCH_BOOTSTRAP: {
        [`checkpoint_${action}`]: async (...args) => {
          calls.push(args);
          return result;
        },
      },
    };
    const path = `/fixture/checkpoint-${action}`;
    const lost = await operator.fetch(request(path, payload, true), env);
    assert.equal(await lost.text(), "{");
    const reply = await operator.fetch(request(path, payload), env);
    assert.equal(await reply.text(), result);
    assert.deepEqual(calls, [
      [payload.credential, payload.request_json],
      [payload.credential, payload.request_json],
    ]);
  });

  test(`default ${action} denial never falls through to the private service`, async () => {
    const reply = await operator.fetch(
      request(`/fixture/default-checkpoint-${action}`, {
        credential: "synthetic",
      }),
      { RESEARCH_API: {} },
    );
    assert.deepEqual(await reply.json(), { denied: true });
  });
}

test("oversized fixture body refuses before any private call", async () => {
  const reply = await operator.fetch(
    request("/fixture/checkpoint-seal", {
      request_json: "x".repeat(64 * 1024),
    }),
    {},
  );
  assert.equal(reply.status, 400);
});

test("the fixture forwards ordinary public routing unchanged", async () => {
  const original = new Request("http://127.0.0.1/v1/capabilities");
  const result = new Response("public");
  const reply = await operator.fetch(original, {
    RESEARCH_API: {
      fetch: async (received) => {
        assert.equal(received, original);
        return result;
      },
    },
  });
  assert.equal(reply, result);
});

test("the fixture refuses non-loopback request authorities", async () => {
  const reply = await operator.fetch(
    new Request("https://example.invalid/fixture/identity"),
    {},
  );
  assert.equal(reply.status, 403);
});
