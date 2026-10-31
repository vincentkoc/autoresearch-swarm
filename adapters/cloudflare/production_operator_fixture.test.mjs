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

for (const action of ["seal", "capture", "elect", "read-artifact"]) {
  const method = "checkpoint_" + action.replaceAll("-", "_");
  test(`private ${action} forwards one unchanged JSON string and can lose its reply`, async () => {
    const calls = [];
    const payload = {
      credential: "synthetic",
      request_json: '{"backup_id":"fixture"}',
    };
    const result = {
      elect: '{"election":{},"signature":"fixture"}',
      seal: '{"seal":{},"custody":{}}',
      capture: '{"envelope_base64":"eA==","chunks_base64":["eQ=="]}',
      "read-artifact":
        '{"manifest_digest":"fixture","artifact":{"generation":"fixture"},"content_hex":"00"}',
    }[action];
    const env = {
      RESEARCH_BOOTSTRAP: {
        [method]: async (...args) => {
          calls.push(args);
          return result;
        },
      },
    };
    const path = `/fixture/checkpoint-${action}`;
    const lost = await operator.fetch(request(path, payload, true), env);
    assert.equal(await lost.text(), "{");
    if (action === "elect")
      assert.equal(lost.headers.get("x-fixture-election-outcome"), "elected");
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

  test(`failed ${action} remains visible when a lost reply is requested`, async () => {
    const response = await operator.fetch(
      request(`/fixture/checkpoint-${action}`, {}, true),
      {
        RESEARCH_BOOTSTRAP: {
          [method]: async () => '{"error":{"code":"STALE_FENCE"}}',
        },
      },
    );
    assert.equal(response.status, 409);
    assert.deepEqual(await response.json(), { error: { code: "STALE_FENCE" } });
  });
}

test("dropped election distinguishes the expected loser from an unexpected failure", async () => {
  for (const code of ["IDEMPOTENCY_CONFLICT", "CAPABILITY_UNAVAILABLE"]) {
    const response = await operator.fetch(
      request("/fixture/checkpoint-elect", {}, true),
      {
        RESEARCH_BOOTSTRAP: {
          checkpoint_elect: async () => JSON.stringify({ error: { code } }),
        },
      },
    );
    if (code === "IDEMPOTENCY_CONFLICT") {
      assert.equal(await response.text(), "{");
      assert.equal(
        response.headers.get("x-fixture-election-outcome"),
        "conflict",
      );
    } else {
      assert.equal(response.status, 409);
      assert.deepEqual(await response.json(), { error: { code } });
    }
  }
});

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
