import assert from "node:assert/strict";
import test from "node:test";
import {
  RegistryConflict,
  recoverWorkflow,
  reserveGeneration,
} from "./hybrid_recovery.ts";
import { parseRecovery } from "./hybrid_contracts.ts";

function fixture() {
  let rows = new Map();
  const kv = {
    get: (key) => structuredClone(rows.get(key)),
    put: (key, value) => rows.set(key, structuredClone(value)),
  };
  const previous = {
    schema_version: "1.0",
    binding: { campaign_id: "fixture" },
    generation: 0,
    pending: { record_id: "old-hint" },
    options: { poll_seconds: 1, rotate_after: 2, request_timeout_seconds: 10 },
  };
  reserveGeneration(kv, "binding", "configuration", previous, null);
  const request = {
    binding: previous.binding,
    request_id: "fixture-request",
    expected_generation: 0,
    expected_instance_id: "fixture-0",
    reason: "Resume the saved research work",
  };
  const calls = [];
  const port = {
    kv,
    transaction(callback) {
      const before = structuredClone(rows);
      try {
        return callback();
      } catch (error) {
        rows = before;
        throw error;
      }
    },
    instanceId: async (parameters) => "fixture-" + parameters.generation,
    status: async () => "errored",
    ensure: async (parameters) => {
      calls.push(parameters);
      return "fixture-" + parameters.generation;
    },
  };
  return {
    kv,
    previous,
    request,
    port,
    calls,
    recover: (value = request) =>
      recoverWorkflow(value, "binding", "configuration", port),
  };
}

test("explicit recovery retains failed history and atomically reserves one successor", async () => {
  const f = fixture();
  const [one, two] = await Promise.all([f.recover(), f.recover()]);
  assert.deepEqual(one, two);
  assert.deepEqual(one, {
    request_id: "fixture-request",
    predecessor_id: "fixture-0",
    predecessor_generation: 0,
    id: "fixture-1",
    generation: 1,
  });
  assert.deepEqual(f.kv.get("generation:0"), f.previous);
  assert.deepEqual(f.kv.get("generation:1"), {
    ...f.previous,
    generation: 1,
    pending: null,
  });
  assert.equal(f.kv.get("current"), 1);
  await assert.rejects(
    f.recover({ ...f.request, reason: "Changed intent" }),
    /IDEMPOTENCY_CONFLICT/,
  );
});

test("active, stopped, complete and uncertain histories never authorize replacement", async () => {
  for (const status of [
    "running",
    "queued",
    "waiting",
    "paused",
    "terminated",
    "complete",
    "unknown",
    "unavailable",
  ]) {
    const f = fixture();
    f.port.status = async () => status;
    await assert.rejects(f.recover(), /WORKFLOW_NOT_ERRORED/);
    assert.equal(f.kv.get("current"), 0);
    assert.equal(f.kv.get("recovery:fixture-request"), undefined);
    assert.deepEqual(f.calls, []);
  }
  const f = fixture();
  f.port.status = async () => {
    throw new Error("Lookup unavailable");
  };
  await assert.rejects(f.recover(), /Lookup unavailable/);
  assert.equal(f.kv.get("current"), 0);
});

test("handoff or configuration change during provider lookup defeats fresh recovery", async () => {
  for (const fault of ["handoff", "configuration", "binding"]) {
    const f = fixture();
    f.port.status = async () => {
      if (fault === "handoff")
        reserveGeneration(
          f.kv,
          "binding",
          "configuration",
          { ...f.previous, generation: 1, pending: null },
          f.previous,
        );
      else f.kv.put(fault, "changed");
      return "errored";
    };
    await assert.rejects(
      f.recover(),
      /GENERATION_MISMATCH|CONFIGURATION_CHANGED|BINDING_MISMATCH/,
    );
    assert.equal(f.kv.get("recovery:fixture-request"), undefined);
    assert.deepEqual(f.calls, []);
  }
});

test("distinct concurrent recovery requests cannot both advance the same failed generation", async () => {
  const f = fixture();
  const outcomes = await Promise.allSettled([
    f.recover(),
    f.recover({ ...f.request, request_id: "second" }),
  ]);
  assert.equal(
    outcomes.filter((result) => result.status === "fulfilled").length,
    1,
  );
  assert.equal(f.kv.get("current"), 1);
  assert.equal(f.calls.length, 1);
});

test("lost creation reply retries the saved intent even after later generations advance", async () => {
  const f = fixture();
  let statuses = 0;
  f.port.status = async () => {
    statuses++;
    return "errored";
  };
  f.port.ensure = async () => {
    throw new Error("Lost creation reply");
  };
  await assert.rejects(f.recover(), /Lost creation reply/);
  assert.equal(f.kv.get("current"), 1);
  f.port.ensure = async (parameters) => "fixture-" + parameters.generation;
  const receipt = await f.recover();
  const next = f.kv.get("generation:1");
  reserveGeneration(
    f.kv,
    "binding",
    "configuration",
    { ...next, generation: 2 },
    next,
  );
  assert.deepEqual(await f.recover(), receipt);
  assert.equal(statuses, 1);
  assert.equal(f.kv.get("current"), 2);
});

test("failure writing the recovery intent rolls back its successor reservation", async () => {
  const f = fixture();
  const put = f.kv.put;
  f.kv.put = (key, value) => {
    if (key.startsWith("recovery:")) throw new Error("Fixture storage failure");
    return put(key, value);
  };
  await assert.rejects(f.recover(), /Fixture storage failure/);
  assert.equal(f.kv.get("current"), 0);
  assert.equal(f.kv.get("generation:1"), undefined);
  assert.deepEqual(f.calls, []);
});

test("storage failures are distinct from semantic generation conflicts", () => {
  const f = fixture();
  assert.throws(
    () =>
      reserveGeneration(
        f.kv,
        "binding",
        "configuration",
        { ...f.previous, generation: 3 },
        f.previous,
      ),
    RegistryConflict,
  );
  const failure = new Error("Private infrastructure failure");
  f.kv.put = () => {
    throw failure;
  };
  assert.throws(
    () =>
      reserveGeneration(
        f.kv,
        "binding",
        "configuration",
        { ...f.previous, generation: 1 },
        f.previous,
      ),
    (error) => error === failure && !(error instanceof RegistryConflict),
  );
});

test("recovery input is bounded, exact and cannot replace successor parameters", () => {
  const id = "00000000-0000-0000-0000-000000000001";
  const request = {
    binding: {
      schema_version: "1.0",
      workspace_id: id,
      campaign_id: id,
      epoch: 1,
      configuration_digest: "sha256:" + "0".repeat(64),
      actors: { control: id },
    },
    request_id: id,
    expected_generation: 0,
    expected_instance_id: "research-" + "0".repeat(64) + "-0",
    reason: "Resume saved work",
  };
  assert.deepEqual(parseRecovery(request), request);
  for (const invalid of [
    { pending: null },
    { options: {} },
    { reason: " " },
    { reason: "x".repeat(1025) },
    { expected_generation: true },
    { expected_generation: 1_000_000_000 },
    { request_id: id + "\n" },
    { expected_instance_id: request.expected_instance_id + "\n" },
  ]) {
    assert.throws(() => parseRecovery({ ...request, ...invalid }));
  }
});
