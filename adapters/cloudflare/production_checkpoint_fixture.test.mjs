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
