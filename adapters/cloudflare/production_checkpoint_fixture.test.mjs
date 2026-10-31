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
      election_json: "synthetic-signed-election",
      turn_json: "synthetic-saved-turn",
      route_json: '{"route_id":"fixture-route"}',
      closure_json: '{"payload":{"attempt_id":"original-attempt"}}',
      artifact_json:
        '{"artifact_id":"artifact","generation":"generation","backup_id":"backup","manifest_digest":"image"}',
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

test("failed target import cannot become a lost committed reply", async () => {
  const reply = await operator.fetch(request("import", { drop: true }), {
    RESEARCH_BOOTSTRAP: { checkpoint_capture: async () => captured },
    TARGET_A_BOOTSTRAP: {
      checkpoint_import: async () => '{"error":{"code":"STALE_FENCE"}}',
    },
  });
  assert.equal(reply.status, 409);
  assert.deepEqual(await reply.json(), { error: { code: "STALE_FENCE" } });
});

test("default import is denied without touching source or private target", async () => {
  const reply = await operator.fetch(request("default-import"), {
    TARGET_A_API: {},
  });
  assert.deepEqual(await reply.json(), { denied: true });
});

test("target request uses only the fixed private target and preserves lost replies", async () => {
  let calls = 0;
  const env = {
    TARGET_B_BOOTSTRAP: {
      checkpoint_request: async (credential) => {
        assert.equal(credential, "target-credential");
        calls++;
        return '{"request":{},"signature":"fixture"}';
      },
    },
  };
  const lost = await operator.fetch(
    request("request", { letter: "b", drop: true }),
    env,
  );
  assert.equal(await lost.text(), "{");
  const result = await operator.fetch(request("request", { letter: "b" }), env);
  assert.deepEqual(await result.json(), { request: {}, signature: "fixture" });
  assert.equal(calls, 2);
});

test("a failed target request cannot masquerade as a lost successful reply", async () => {
  const result = await operator.fetch(request("request", { drop: true }), {
    TARGET_A_BOOTSTRAP: {
      checkpoint_request: async () => '{"error":{"code":"STALE_FENCE"}}',
    },
  });
  assert.equal(result.status, 409);
  assert.deepEqual(await result.json(), { error: { code: "STALE_FENCE" } });
});

test("default target request has no private fallback", async () => {
  const result = await operator.fetch(request("default-request"), {
    TARGET_A_API: {},
  });
  assert.deepEqual(await result.json(), { denied: true });
});

test("target admission uses only its fixed binding and hides only accepted replies", async () => {
  const accepted = {
    disposition: "accepted",
    result: { data: { logical_election_digest: "fixture" } },
  };
  const env = {
    TARGET_B_BOOTSTRAP: {
      checkpoint_admit: async (credential, election) => {
        assert.equal(credential, "target-credential");
        assert.equal(election, "synthetic-signed-election");
        return JSON.stringify(accepted);
      },
    },
  };
  const reply = await operator.fetch(request("admit", { letter: "b" }), env);
  assert.deepEqual(await reply.json(), accepted);
  const lost = await operator.fetch(
    request("admit", { letter: "b", drop: true }),
    env,
  );
  assert.equal(await lost.text(), "{");
  env.TARGET_B_BOOTSTRAP.checkpoint_admit = async () =>
    '{"error":{"code":"STALE_FENCE"}}';
  const refused = await operator.fetch(
    request("admit", { letter: "b", drop: true }),
    env,
  );
  assert.equal(refused.status, 409);
  assert.deepEqual(await refused.json(), { error: { code: "STALE_FENCE" } });
});

test("default target admission has no private fallback", async () => {
  const reply = await operator.fetch(request("default-admit"), {
    TARGET_A_API: {},
  });
  assert.deepEqual(await reply.json(), { denied: true });
});

test("target turn closure preserves failure and fixed-target routing", async () => {
  const accepted = {
    disposition: "accepted",
    result: { turn: { state: "closed_unacknowledged" } },
  };
  const env = {
    TARGET_A_BOOTSTRAP: {
      checkpoint_turn: async (credential, turn) => {
        assert.equal(credential, "target-credential");
        assert.equal(turn, "synthetic-saved-turn");
        return JSON.stringify(accepted);
      },
    },
  };
  assert.deepEqual(
    await (await operator.fetch(request("turn"), env)).json(),
    accepted,
  );
  assert.equal(
    await (await operator.fetch(request("turn", { drop: true }), env)).text(),
    "{",
  );
  env.TARGET_A_BOOTSTRAP.checkpoint_turn = async () =>
    '{"error":{"code":"STALE_FENCE"}}';
  const refused = await operator.fetch(request("turn", { drop: true }), env);
  assert.equal(refused.status, 409);
  assert.deepEqual(await refused.json(), { error: { code: "STALE_FENCE" } });
});

test("route issuance uses only its fixed target and drops only matching accepted proof", async () => {
  const proof = {
    target_signature: "fixture",
    logical_election: { signature: "fixture" },
    route: { route_id: "fixture-route" },
  };
  const accepted = { disposition: "accepted", result: { data: proof } };
  const env = {
    TARGET_A_BOOTSTRAP: { checkpoint_route: () => assert.fail("Wrong target") },
    TARGET_B_BOOTSTRAP: {
      checkpoint_route: async (credential, payload) => {
        assert.equal(credential, "target-credential");
        assert.equal(payload, '{"route_id":"fixture-route"}');
        return JSON.stringify(accepted);
      },
    },
  };
  const call = (drop = false) =>
    operator.fetch(request("route", { letter: "b", drop }), env);
  assert.deepEqual(await (await call()).json(), accepted);
  assert.equal(await (await call(true)).text(), "{");
  for (const invalid of [
    { error: { code: "STALE_FENCE" } },
    {
      ...accepted,
      result: { data: { ...proof, route: { route_id: "other" } } },
    },
  ]) {
    env.TARGET_B_BOOTSTRAP.checkpoint_route = async () =>
      JSON.stringify(invalid);
    const result = await call(true);
    assert.equal(result.status, 409);
    assert.deepEqual(await result.json(), invalid);
  }
});

test("default route issuance cannot reach private bootstrap", async () => {
  const result = await operator.fetch(request("default-route"), {
    TARGET_A_API: {},
  });
  assert.deepEqual(await result.json(), { denied: true });
});

test("default target turn cannot enter private recovery", async () => {
  const result = await operator.fetch(request("default-turn"), {
    TARGET_A_API: {},
  });
  assert.deepEqual(await result.json(), { denied: true });
});

test("artifact copy carries exact source bytes into only the fixed target", async () => {
  const artifact = { reference: { id: "artifact" }, generation: "generation" };
  const exported = { manifest_digest: "image", artifact, content_hex: "00ff" };
  const receipt = {
    restore_id: "restore",
    storage_digest: "storage",
    artifact,
  };
  const calls = [];
  const env = {
    RESEARCH_BOOTSTRAP: {
      checkpoint_read_artifact: async (credential, payload) => {
        assert.equal(credential, "source-credential");
        assert.equal(JSON.parse(payload).manifest_digest, "image");
        calls.push("read");
        return JSON.stringify(exported);
      },
    },
    TARGET_A_BOOTSTRAP: {
      checkpoint_artifact: () => assert.fail("Wrong target"),
    },
    TARGET_B_BOOTSTRAP: {
      checkpoint_artifact: async (credential, payload, content) => {
        assert.equal(credential, "target-credential");
        assert.deepEqual(JSON.parse(payload), {
          artifact_id: "artifact",
          generation: "generation",
        });
        assert.equal(content, exported.content_hex);
        calls.push("restore");
        return JSON.stringify(receipt);
      },
    },
  };
  const call = (drop = false) =>
    operator.fetch(request("artifact", { letter: "b", drop }), env);
  assert.deepEqual(await (await call()).json(), { exported, result: receipt });
  assert.equal(await (await call(true)).text(), "{");
  assert.deepEqual(calls, ["read", "restore", "read", "restore"]);
  for (const invalid of [
    { error: { code: "STALE_FENCE" } },
    { ...receipt, artifact: { ...artifact, generation: "other" } },
  ]) {
    env.TARGET_B_BOOTSTRAP.checkpoint_artifact = async () =>
      JSON.stringify(invalid);
    const failed = await call(true);
    assert.equal(failed.status, 409);
    assert.deepEqual(await failed.json(), invalid);
  }
});

test("source artifact refusal cannot reach the target or become a lost reply", async () => {
  for (const drop of [false, true]) {
    const result = await operator.fetch(request("artifact", { drop }), {
      RESEARCH_BOOTSTRAP: {
        checkpoint_read_artifact: async () =>
          '{"error":{"code":"STALE_FENCE"}}',
      },
      TARGET_A_BOOTSTRAP: {
        checkpoint_artifact: () => assert.fail("Source refused"),
      },
    });
    assert.equal(result.status, 409);
    assert.deepEqual(await result.json(), { error: { code: "STALE_FENCE" } });
  }
});

test("default artifact restoration has no private fallback", async () => {
  const result = await operator.fetch(request("default-artifact"), {
    TARGET_A_API: {},
  });
  assert.deepEqual(await result.json(), { denied: true });
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

test("recovery forwarding preserves streamed request and response on the fixed binding", async () => {
  const bytes = new Uint8Array([0, 255, 1, 2]);
  const input = new Request(
    "http://127.0.0.1/fixture/recovery-b/v1/worker/output/parts?offset=0",
    {
      method: "PUT",
      headers: {
        authorization: "Bearer original",
        "idempotency-key": "original-key",
      },
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(bytes);
          controller.close();
        },
      }),
      duplex: "half",
    },
  );
  const reply = await operator.fetch(input, {
    TARGET_A_RECOVERY: {
      fetch() {
        assert.fail("Wrong target");
      },
    },
    TARGET_B_API: {
      fetch() {
        assert.fail("Ordinary API selected");
      },
    },
    TARGET_B_RECOVERY: {
      async fetch(request) {
        assert.equal(new URL(request.url).pathname, "/v1/worker/output/parts");
        assert.equal(new URL(request.url).search, "?offset=0");
        assert.equal(request.method, "PUT");
        assert.equal(request.headers.get("authorization"), "Bearer original");
        assert.equal(request.headers.get("idempotency-key"), "original-key");
        assert.equal(request.bodyUsed, false);
        assert.deepEqual(new Uint8Array(await request.arrayBuffer()), bytes);
        return new Response(bytes, {
          status: 202,
          headers: { "x-result": "original" },
        });
      },
    },
  });
  assert.equal(reply.status, 202);
  assert.equal(reply.headers.get("x-result"), "original");
  assert.deepEqual(new Uint8Array(await reply.arrayBuffer()), bytes);
});

test("recovery gateway does not consume a request refused by the receiver", async () => {
  let forwarded;
  const reply = await operator.fetch(
    new Request("http://localhost/fixture/recovery-a/v1/worker/get", {
      method: "POST",
      body: "oversized unauthorized fixture",
    }),
    {
      TARGET_A_RECOVERY: {
        async fetch(request) {
          forwarded = request;
          return new Response(null, { status: 401 });
        },
      },
    },
  );
  assert.equal(reply.status, 401);
  assert.equal(forwarded.bodyUsed, false);
});

test("private closure drops only the matching accepted operational receipt", async () => {
  let calls = 0;
  const accepted = {
    disposition: "accepted",
    result: {
      disposition: "closed_unaccepted",
      attempt: { id: "original-attempt" },
      closure: { id: "receipt" },
    },
  };
  const env = {
    TARGET_B_BOOTSTRAP: {
      async checkpoint_closure(credential, payload) {
        calls++;
        assert.equal(credential, "target-credential");
        assert.deepEqual(JSON.parse(payload), {
          payload: { attempt_id: "original-attempt" },
        });
        return JSON.stringify(accepted);
      },
    },
  };
  assert.equal(
    await (
      await operator.fetch(request("closure", { letter: "b", drop: true }), env)
    ).text(),
    "{",
  );
  assert.deepEqual(
    await (
      await operator.fetch(request("closure", { letter: "b" }), env)
    ).json(),
    accepted,
  );
  assert.equal(calls, 2);
  accepted.result.attempt.id = "different-attempt";
  assert.equal(
    (await operator.fetch(request("closure", { letter: "b", drop: true }), env))
      .status,
    409,
  );
});

test("closure failure and default-route refusal cannot masquerade as lost success", async () => {
  const result = await operator.fetch(request("closure", { drop: true }), {
    TARGET_A_BOOTSTRAP: {
      async checkpoint_closure() {
        return '{"error":{"code":"EXECUTION_UNKNOWN"}}';
      },
    },
  });
  assert.equal(result.status, 409);
  assert.equal((await result.json()).error.code, "EXECUTION_UNKNOWN");
  assert.deepEqual(
    await (
      await operator.fetch(request("default-closure"), { TARGET_A_API: {} })
    ).json(),
    { denied: true },
  );
});
