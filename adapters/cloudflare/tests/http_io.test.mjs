import assert from "node:assert/strict";
import test from "node:test";
import { Worker } from "node:worker_threads";
import { readBody } from "../runtime/http_io.mjs";

test("binary and empty request bodies preserve exact bytes", async () => {
  const binary = new Request("http://localhost", { method: "POST", body: new Uint8Array([0, 255, 128]) });
  assert.deepEqual(JSON.parse(await readBody(binary, 3)), { content_hex: "00ff80" });
  assert.deepEqual(JSON.parse(await readBody(new Request("http://localhost"), 0)), { content_hex: "" });
});

test("the body bound applies during consumption and cancels the producer", async () => {
  let cancelled = false;
  let pulled = 0;
  const body = new ReadableStream({
    pull(controller) {
      pulled++;
      controller.enqueue(new Uint8Array([1, 2, 3]));
    },
    cancel() { cancelled = true; },
  }, { highWaterMark: 0 });
  assert.deepEqual(JSON.parse(await readBody({ body }, 4)), { error: "limit" });
  assert.equal(pulled, 2);
  assert.equal(cancelled, true);
});

test("a stalled producer cannot delay refusal beyond the read deadline", async () => {
  let cancelled = false;
  const body = new ReadableStream({
    pull() { return new Promise(() => {}); },
    cancel() {
      cancelled = true;
      return new Promise(() => {}); // Cancellation completion is deliberately unknowable.
    },
  });
  assert.deepEqual(JSON.parse(await readBody({ body }, 1)), { error: "unconfirmed" });
  assert.equal(cancelled, true);
});

test("producer errors and invalid chunks cannot become successful input", async () => {
  for (const body of [
    new ReadableStream({ start(controller) { controller.error(new Error("private source detail")); } }),
    new ReadableStream({ start(controller) { controller.enqueue("invalid"); } }),
  ]) {
    assert.deepEqual(JSON.parse(await readBody({ body }, 64)), { error: "unconfirmed" });
  }
});

test("invalid bounds are refused before touching the input", async () => {
  const request = { get body() { throw new Error("must not inspect body"); } };
  for (const limit of [-1, 1.5, 1024 * 1024 + 1]) {
    await assert.rejects(readBody(request, limit), /Invalid request body bound/);
  }
});

test("always-ready empty request and R2 chunks cannot starve deadlines", async () => {
  // Isolate the old infinite-microtask bug so the parent can enforce its own bound.
  const worker = new Worker(`
    const { parentPort, workerData } = require("node:worker_threads");
    Promise.all([import(workerData.http), import(workerData.r2)]).then(async ([http, r2]) => {
      function source() {
        const value = { cancelled: false };
        value.body = new ReadableStream({
          pull(controller) { controller.enqueue(new Uint8Array()); },
          cancel() { value.cancelled = true; },
        }, { highWaterMark: 0 });
        return value;
      }
      const request = source(), object = source(), late = source();
      const bucket = { get: async () => ({ body: object.body, size: 0, customMetadata: {} }) };
      let resolveLate;
      const lateBucket = { get: () => new Promise(resolve => { resolveLate = resolve; }) };
      const [requestResult, objectResult, lateResult] = await Promise.all([
        http.readBody(request, 0).then(JSON.parse),
        r2.get(bucket, "fixture", 0).then(() => false, () => true),
        r2.get(lateBucket, "late", 0).then(() => false, () => true),
      ]);
      resolveLate({ body: late.body, size: 0, customMetadata: {} });
      await new Promise(resolve => setTimeout(resolve, 0));
      parentPort.postMessage({ requestResult, objectResult, lateResult,
        cancelled: [request.cancelled, object.cancelled, late.cancelled] });
    }).catch(error => { throw error; });
  `, { eval: true, workerData: {
    http: new URL("../runtime/http_io.mjs", import.meta.url).href,
    r2: new URL("../runtime/r2_io.mjs", import.meta.url).href,
  } });
  let timer;
  try {
    const outcome = await new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error("Body reader starved its deadline")), 7000);
      worker.once("message", resolve);
      worker.once("error", reject);
      worker.once("exit", code => { if (code) reject(new Error("Body worker exited")); });
    });
    assert.deepEqual(outcome, {
      requestResult: { error: "unconfirmed" }, objectResult: true, lateResult: true,
      cancelled: [true, true, true],
    });
  } finally {
    clearTimeout(timer);
    await worker.terminate();
  }
});
