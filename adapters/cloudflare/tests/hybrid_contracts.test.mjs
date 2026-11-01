import assert from "node:assert/strict";
import test from "node:test";
import {
  instanceId,
  isEventTimeout,
  parseBinding,
  parseParameters,
  parseWork,
  readJSON,
  same,
  strictJSON,
} from "../.generated/hybrid_contracts.mjs";
import { ensureWorkflow } from "../.generated/hybrid_creation.mjs";

const id = (number) =>
  `00000000-0000-0000-0000-${number.toString().padStart(12, "0")}`;
const binding = () =>
  parseBinding({
    workspace_id: id(1),
    campaign_id: id(2),
    epoch: 1,
    configuration_digest: `sha256:${"0".repeat(64)}`,
    actors: { control: id(3), evaluator: id(4) },
  });
const work = () =>
  parseWork({
    binding: binding(),
    kind: "wait",
    record_id: id(5),
    actor_id: id(3),
  });
const parameters = () =>
  parseParameters({
    binding: binding(),
    generation: 0,
    pending: work(),
    options: { poll_seconds: 1, rotate_after: 2, request_timeout_seconds: 10 },
  });

test("provider IDs are deterministic, bounded and change with binding or generation", async () => {
  const original = parameters();
  const first = await instanceId(original);
  assert.match(first, /^[a-zA-Z0-9_-]{1,100}$/);
  assert.equal(
    await instanceId(parseParameters(JSON.parse(JSON.stringify(original)))),
    first,
  );
  assert.notEqual(await instanceId({ ...original, generation: 1 }), first);
  assert.notEqual(
    await instanceId({
      ...original,
      binding: { ...original.binding, epoch: 2 },
    }),
    first,
  );
});

test("references normalize property order while refusing cross-binding continuation", () => {
  const original = binding();
  const reversed = Object.fromEntries(Object.entries(original).reverse());
  reversed.actors = { evaluator: id(4), control: id(3) };
  assert.ok(same(original, parseBinding(reversed)));
  assert.throws(() =>
    parseParameters({ ...parameters(), binding: { ...original, epoch: 2 } }),
  );
  assert.throws(() => parseBinding({ ...original, epoch: true }));
  assert.throws(() =>
    parseBinding({ ...original, epoch: Number.MAX_SAFE_INTEGER + 1 }),
  );
  assert.throws(() => parseBinding({ ...original, campaign_id: id(2) + "\n" }));
  assert.throws(() => parseBinding({ ...original, token: "private-fixture" }));
  assert.throws(() =>
    parseBinding({ ...original, actors: { control: id(3), evaluator: id(3) } }),
  );
  assert.throws(() => parseWork({ ...work(), actor_id: id(99) }));
});

test("bounded JSON rejects duplicate escaped keys, depth and malformed bytes", async () => {
  assert.deepEqual(strictJSON('{"a":[{"b":"quote: \\\""}]}'), {
    a: [{ b: 'quote: "' }],
  });
  for (const value of [
    '{"a":1,"\\u0061":2}',
    '{"a":{"x":1,"x":2}}',
    "[".repeat(65) + "0" + "]".repeat(65),
    " ".repeat(16385),
  ]) {
    assert.throws(() => strictJSON(value));
  }
  await assert.rejects(readJSON(new Response(new Uint8Array([0xff]))));
  await assert.rejects(readJSON(new Response("x".repeat(16385))));
});

test("an observed generation retries a transient lookup without recreating its history", async () => {
  let gets = 0;
  let creates = 0;
  const provider = {
    async get() {
      gets++;
      if (gets === 1) throw new Error("transient provider failure");
      return {
        async status() {
          return { status: "waiting" };
        },
      };
    },
    async create() {
      creates++;
    },
  };
  await assert.rejects(
    ensureWorkflow(provider, "fixture", parameters(), true),
    /HISTORY_UNAVAILABLE/,
  );
  await ensureWorkflow(provider, "fixture", parameters(), true);
  assert.equal(gets, 2);
  assert.equal(creates, 0);
});

test("lost creation acknowledgment recovers the same ID without another creation attempt", async () => {
  const calls = [];
  const provider = {
    async create(input) {
      calls.push(["create", input.id]);
      throw new Error("reply lost");
    },
    async get(id) {
      calls.push(["get", id]);
      return {
        async status() {
          return { status: "queued" };
        },
      };
    },
  };
  await ensureWorkflow(provider, "fixture", parameters(), false);
  await ensureWorkflow(provider, "fixture", parameters(), true);
  assert.deepEqual(calls, [
    ["create", "fixture"],
    ["get", "fixture"],
    ["get", "fixture"],
  ]);
});

test("event timeout guard accepts the qualified RPC shape and refuses other failures", () => {
  assert.ok(
    isEventTimeout(new Error("Execution timed out after 1000ms"), 1000),
  );
  const named = new Error("timeout");
  named.name = "WorkflowTimeoutError";
  assert.ok(isEventTimeout(named, 1000));
  assert.equal(
    isEventTimeout(new Error("Execution timed out after 2000ms"), 1000),
    false,
  );
  assert.equal(isEventTimeout(new Error("provider unavailable"), 1000), false);
  assert.equal(
    isEventTimeout(
      { name: "Error", message: "Execution timed out after 1000ms" },
      1000,
    ),
    false,
  );
});
