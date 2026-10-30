import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";
import { forward, route } from "./.generated/agent_facade.mjs";

// Node boundary tests use a stream stand-in; real header semantics are tested in workerd.
globalThis.FixedLengthStream = class extends TransformStream {
  constructor(_length) {
    super();
  }
};

const env = { NATIVE_API_ORIGIN: "https://native.example.invalid" };
const token = "Bearer fixture-caller-identity";
const id = "00000000-0000-0000-0000-000000000001";
const request = (path, init = {}) =>
  new Request("https://facade.example.invalid" + path, {
    headers: { authorization: token, ...init.headers },
    ...init,
  });

async function withFetch(replacement, action) {
  const saved = globalThis.fetch;
  globalThis.fetch = replacement;
  try {
    await action();
  } finally {
    globalThis.fetch = saved;
  }
}

test("facade exposes the exact native command catalog and no private gateway routes", async () => {
  const names = JSON.parse(
    await readFile(new URL(".generated/commands.json", import.meta.url)),
  );
  for (const name of names) assert.ok(route("POST", "/v1/commands/" + name));
  for (const path of [
    "/adapter/start",
    "/v1/delivery/execute",
    "/v1/commands/unknown",
    "/v1/commands/%72eport.create",
    "/v1/credentials",
  ]) {
    assert.equal(route("POST", path), null);
  }
  assert.equal(route("DELETE", "/v1/capabilities"), null);
});

test("caller identity, command bytes, key and revision survive while cookies and forwarding headers do not", async () => {
  const bytes = '{ "campaign_id" : "' + id + '" }';
  await withFetch(
    async (url, options) => {
      assert.equal(url, env.NATIVE_API_ORIGIN + "/v1/commands/report.create");
      assert.equal(options.headers.get("authorization"), token);
      assert.equal(options.headers.get("idempotency-key"), "same-key");
      assert.equal(options.headers.get("x-expected-revision"), "7");
      assert.equal(options.headers.get("cookie"), null);
      assert.equal(options.headers.get("x-forwarded-host"), null);
      assert.equal(options.redirect, "manual");
      assert.equal(new TextDecoder().decode(options.body), bytes);
      return new Response('{"native":"conflict"}', { status: 409 });
    },
    async () => {
      const response = await forward(
        request("/v1/commands/report.create", {
          method: "POST",
          body: bytes,
          headers: {
            authorization: token,
            "idempotency-key": "same-key",
            "x-expected-revision": "7",
            cookie: "private",
            "x-forwarded-host": "other.invalid",
          },
        }),
        env,
      );
      assert.equal(response.status, 409);
      assert.equal(await response.text(), '{"native":"conflict"}');
      assert.equal(
        response.headers.get("cache-control"),
        "no-store, no-transform",
      );
    },
  );
});

test("counted request limits reject oversized chunks before any upstream request", async () => {
  await withFetch(
    async () => {
      throw new Error("Must not contact upstream");
    },
    async () => {
      for (const [path, method, limit] of [
        ["/v1/commands/report.create", "POST", 1024 * 1024],
        [`/v1/artifact-uploads/${id}/parts`, "PUT", 4 * 1024 * 1024],
      ]) {
        const stream = new ReadableStream({
          start(controller) {
            controller.enqueue(new Uint8Array(limit));
            controller.enqueue(new Uint8Array(1));
            controller.close();
          },
        });
        const response = await forward(
          request(path, { method, body: stream, duplex: "half" }),
          env,
        );
        assert.equal(response.status, 413);
      }
      assert.equal(
        (await forward(request("/v1/events?after=1&after=2"), env)).status,
        422,
      );
      assert.equal(
        (await forward(request("/v1/events?destination=elsewhere"), env))
          .status,
        422,
      );
    },
  );
});

test("uncertain mutation and redirect never invent an accepted command", async () => {
  for (const mode of ["lost", "redirect"]) {
    await withFetch(
      async () => {
        if (mode === "lost") throw new Error("private native detail");
        return new Response(null, {
          status: 307,
          headers: { location: "https://other.invalid" },
        });
      },
      async () => {
        const response = await forward(
          request("/v1/commands/report.create", { method: "POST", body: "{}" }),
          env,
        );
        const payload = await response.json();
        assert.equal(response.status, 502);
        assert.equal(payload.error.code, "DELIVERY_UNCERTAIN");
        assert.equal(payload.error.details.recovery, "replay_exact_command");
        assert.equal(payload.command_id, undefined);
        assert.ok(!JSON.stringify(payload).includes("private"));
      },
    );
  }
});

test(
  "artifact stream preserves bytes and cancellation closes its upstream custody",
  { timeout: 1000 },
  async () => {
    let canceled = false,
      signal;
    let closed;
    const closure = new Promise((resolve) => {
      closed = resolve;
    });
    await withFetch(
      async (_url, options) => {
        signal = options.signal;
        signal.addEventListener("abort", closed, { once: true });
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new Uint8Array([0, 255, 1]));
            },
            cancel() {
              canceled = true;
            },
          }),
          { headers: { "content-length": "100", etag: '"sha256:fixture"' } },
        );
      },
      async () => {
        const response = await forward(
          request(`/v1/artifacts/${id}/content`),
          env,
        );
        const reader = response.body.getReader();
        assert.deepEqual([...(await reader.read()).value], [0, 255, 1]);
        await reader.cancel();
        await closure;
        assert.ok(canceled && signal.aborted);
      },
    );
  },
);

test("truncated artifacts fail the stream and SSE frames retain exact event IDs and gaps", async () => {
  await withFetch(
    async () => new Response("abc", { headers: { "content-length": "6" } }),
    async () => {
      const response = await forward(
        request(`/v1/artifacts/${id}/content`),
        env,
      );
      await assert.rejects(
        response.arrayBuffer(),
        /RESEARCH_TRANSPORT_INTERRUPTED/,
      );
    },
  );
  const frames =
    ": heartbeat\n\nid: 7\nevent: research\ndata: {}\n\nevent: gap\ndata: {}\n\n";
  await withFetch(
    async () =>
      new Response(frames, {
        headers: { "content-type": "text/event-stream" },
      }),
    async () => {
      const response = await forward(request("/v1/events/stream?after=6"), env);
      assert.equal(await response.text(), frames);
    },
  );
});

test("POST query failures preserve query recovery and bearer scheme casing", async () => {
  await withFetch(
    async (_url, options) => {
      assert.equal(options.headers.get("authorization"), "bEaReR caller-token");
      throw new Error("private failure");
    },
    async () => {
      const response = await forward(
        request("/v1/query", {
          method: "POST",
          body: "{}",
          headers: { authorization: "bEaReR caller-token" },
        }),
        env,
      );
      const payload = await response.json();
      assert.equal(payload.error.code, "TRANSPORT_UNAVAILABLE");
      assert.equal(payload.error.details.recovery, "retry_query");
    },
  );
});
