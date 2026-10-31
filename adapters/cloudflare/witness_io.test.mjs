import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { Worker } from "node:worker_threads";
import { request } from "./witness_io.mjs";

const token = "public_fixture_consumer_" + "x".repeat(32);
const origin = "https://witness.example.test";
const headers = { "content-type": "application/json" };

async function replaceFetch(fetcher, callback) {
  const original = globalThis.fetch;
  globalThis.fetch = fetcher;
  try { await callback(); } finally { globalThis.fetch = original; }
}

test("real HTTP uses the pinned endpoint and refuses redirects without forwarding credentials", async () => {
  let redirected = false;
  const server = createServer(async (incoming, response) => {
    if (incoming.url !== "/v1/time-witness") { redirected = true; response.end(); return; }
    assert.equal(incoming.method, "POST");
    assert.equal(incoming.headers.authorization, "Bearer " + token);
    let body = "";
    for await (const chunk of incoming) body += chunk;
    if (body === "redirect") {
      response.writeHead(307, { location: "/other" }); response.end(); return;
    }
    response.writeHead(200, headers);
    response.write('{"message":"');
    response.end('é"}');
  }).listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    const endpoint = "http://127.0.0.1:" + server.address().port;
    assert.equal(await request(endpoint, token, "{}", 1000, true), '{"message":"é"}');
    await assert.rejects(request(endpoint, token, "redirect", 1000, true), /Time witness transport unavailable/);
    assert.equal(redirected, false);
  } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
});

test("invalid deployment routes, credentials and bounds cause no network request", async () => {
  await replaceFetch(() => assert.fail("Invalid request reached network"), async () => {
    for (const target of ["http://example.test", "http://127.0.0.1", "https://a.test/path", "https://u:p@a.test", "https://a.test?x=1", "https://a.test#x", "https://a.test/", "https://a.test\\other"]) {
      await assert.rejects(request(target, token, "{}", 1000), /Invalid witness/);
    }
    await assert.rejects(request("http://localhost", token, "{}", 1000, true), /Invalid witness/);
    await assert.rejects(request(origin, "short", "{}", 1000), /Invalid witness/);
    await assert.rejects(request(origin, token, "é".repeat(8193), 1000), /Invalid witness/);
    for (const bound of [99, 10001, 1.5, NaN]) {
      await assert.rejects(request(origin, token, "{}", bound), /Invalid witness/);
    }
  });
});

test("status, media, encoding, reported length and consumed bytes are bounded", async () => {
  const responses = [
    () => new Response("private error body", { status: 401, headers }),
    () => new Response("{}"),
    () => new Response("{}", { headers: { ...headers, "content-encoding": "gzip" } }),
    () => new Response("{}", { headers: { ...headers, "content-length": "16385" } }),
    () => new Response("{}", { headers: { ...headers, "content-length": "1" } }),
    () => new Response("{}", { headers: { ...headers, "content-length": "02" } }),
    () => new Response(new Uint8Array([255]), { headers }),
    () => new Response(new ReadableStream({ start(c) {
      c.enqueue(new Uint8Array(8192)); c.enqueue(new Uint8Array(8193)); c.close();
    } }), { headers }),
  ];
  for (const make of responses) {
    await replaceFetch(async () => make(), async () => {
      await assert.rejects(request(origin, token, "{}", 1000), error => {
        assert.equal(error.message, "Time witness transport unavailable"); return true;
      });
    });
  }
});

test("an exact-limit response preserves its UTF-8 bytes", async () => {
  const body = JSON.stringify({ x: "a".repeat(16384 - 8) });
  assert.equal(new TextEncoder().encode(body).length, 16384);
  await replaceFetch(async (_url, options) => {
    assert.equal(options.redirect, "manual");
    return new Response(body, { headers: { ...headers, "content-length": "16384" } });
  }, async () => assert.equal(await request(origin, token, "{}", 1000), body));
});

test("timeout refuses late fetch completion and cancels its body", async () => {
  let complete, cancelled = false, signal;
  await replaceFetch((_url, options) => {
    signal = options.signal;
    return new Promise(resolve => { complete = resolve; });
  }, async () => {
    await assert.rejects(request(origin, token, "{}", 100), /Time witness transport unavailable/);
    assert.equal(signal.aborted, true);
    complete(new Response(new ReadableStream({ cancel() { cancelled = true; } }), { headers }));
    await new Promise(resolve => setTimeout(resolve, 0));
    assert.equal(cancelled, true);
  });
});

test("a stalled body and non-settling cancellation cannot delay refusal", async () => {
  let cancelled = false;
  await replaceFetch(async () => new Response(new ReadableStream({
    pull() { return new Promise(() => {}); },
    cancel() { cancelled = true; return new Promise(() => {}); },
  }), { headers }), async () => {
    await assert.rejects(request(origin, token, "{}", 100), /Time witness transport unavailable/);
    assert.equal(cancelled, true);
  });
});

test("ready empty chunks cannot starve the exchange deadline", async () => {
  const worker = new Worker(`
    const { parentPort, workerData } = require("node:worker_threads");
    import(workerData.module).then(async ({ request }) => {
      let cancelled = false;
      globalThis.fetch = async () => new Response(new ReadableStream({
        pull(c) { c.enqueue(new Uint8Array()); },
        cancel() { cancelled = true; },
      }, { highWaterMark: 0 }), { headers: { "content-type": "application/json" } });
      try { await request(workerData.origin, workerData.token, "{}", 100); parentPort.postMessage(false); }
      catch (error) { parentPort.postMessage({ message: error.message, cancelled }); }
    });
  `, { eval: true, workerData: { module: new URL("./witness_io.mjs", import.meta.url).href, origin, token } });
  let timer;
  try {
    const result = await new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error("Witness reader starved deadline")), 3000);
      worker.once("message", resolve); worker.once("error", reject);
      worker.once("exit", code => { if (code) reject(new Error("Witness test worker exited")); });
    });
    assert.deepEqual(result, { message: "Time witness transport unavailable", cancelled: true });
  } finally { clearTimeout(timer); await worker.terminate(); }
});
