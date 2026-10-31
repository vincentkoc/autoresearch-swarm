import assert from "node:assert/strict";
import test from "node:test";

import operator from "./production_operator_fixture.mjs";

for (const operation of ["worker.submit_evidence", "attempt.reconcile"]) {
  test(`${operation} loses only a matching accepted command response`, async () => {
    let body = { disposition: "accepted", result: { id: "synthetic" } };
    let status = 200;
    let calls = 0;
    const env = {
      RESEARCH_API: {
        fetch: async () => {
          calls += 1;
          return Response.json(body, { status });
        },
      },
    };
    const call = (path = operation) =>
      operator.fetch(
        new Request(`http://127.0.0.1/v1/commands/${path}`, {
          method: "POST",
          body: "{}",
          headers: { "x-fixture-drop-command": operation },
        }),
        env,
      );
    assert.equal(await (await call()).text(), "{");
    assert.deepEqual(await (await call("report.create")).json(), body);
    body = { disposition: "rejected", error: { code: "REVISION_CONFLICT" } };
    assert.deepEqual(await (await call()).json(), body);
    status = 403;
    assert.equal((await call()).status, 403);
    assert.equal(calls, 4);
  });
}

test("fresh provider status uses one private argument and preserves default denial", async () => {
  const calls = [];
  const env = {
    RESEARCH_API: {},
    RESEARCH_BOOTSTRAP: {
      async time_provider_status(...args) {
        calls.push(args);
        return '{"provider":{"state":"unqualified"}}';
      },
    },
  };
  const payload = { credential: "synthetic", request_json: "null" };
  const value = await operator.fetch(
    request("/fixture/provider-status", payload),
    env,
  );
  assert.deepEqual(await value.json(), { provider: { state: "unqualified" } });
  const denied = await operator.fetch(
    request("/fixture/default-provider-status", payload),
    env,
  );
  assert.deepEqual(await denied.json(), { denied: true });
  assert.deepEqual(calls, [["synthetic"]]);
});

test("fresh provider requalification can lose only its matching accepted reply", async () => {
  let value = {
    disposition: "accepted",
    result: {
      data: {
        request: { request_id: "fixture" },
        provider: { scope: { provider_generation: "new-generation" } },
      },
    },
  };
  const env = {
    RESEARCH_API: {},
    RESEARCH_BOOTSTRAP: {
      async requalify_time_provider(...args) {
        assert.deepEqual(args, ["synthetic", '{"request_id":"fixture"}']);
        return JSON.stringify(value);
      },
    },
  };
  const payload = {
    credential: "synthetic",
    request_json: '{"request_id":"fixture"}',
  };
  const call = (drop) =>
    operator.fetch(request("/fixture/requalify-provider", payload, drop), env);
  assert.deepEqual(await (await call(false)).json(), value);
  assert.equal(await (await call(true)).text(), "{");
  value.result.data.request.request_id = "other";
  assert.equal((await call(true)).status, 409);
  value = { error: { code: "STALE_FENCE" } };
  const refused = await call(true);
  assert.equal(refused.status, 409);
  assert.deepEqual(await refused.json(), value);
  const denied = await operator.fetch(
    request("/fixture/default-requalify-provider", payload),
    env,
  );
  assert.deepEqual(await denied.json(), { denied: true });
});

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
