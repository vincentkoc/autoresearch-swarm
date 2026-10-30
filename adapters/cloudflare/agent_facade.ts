// Transport only. The native service authenticates every caller and owns every decision.
import commands from "./.generated/commands.json";

export interface Env {
  NATIVE_API_ORIGIN: string;
  ALLOW_LOCAL_API?: string;
  STREAM_IDLE_TIMEOUT_SECONDS?: string;
}

const JSON_BYTES = 1024 * 1024;
const PART_BYTES = 4 * JSON_BYTES;
const RESPONSE_BYTES = 32 * JSON_BYTES;
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
type Route = {
  mode: "query" | "command" | "upload_part";
  body: number;
  stream: "json" | "artifact" | "events";
  query: string[];
};

export function route(method: string, path: string): Route | null {
  const json = {
    mode: "query" as const,
    body: JSON_BYTES,
    stream: "json" as const,
    query: [],
  };
  if (method === "POST") {
    if (path.startsWith("/v1/commands/") && commands.includes(path.slice(13)))
      return { ...json, mode: "command" };
    if (
      [
        "query",
        "proposal/get",
        "worker/get",
        "worker/clock",
        "worker/list",
        "worker/input",
        "worker/output",
        "snapshot/page",
        "snapshot/item",
      ].some((name) => path === "/v1/" + name)
    )
      return json;
    if (path === "/v1/worker/input/content")
      return { ...json, stream: "artifact" };
    if (
      path === "/v1/artifact-uploads" ||
      new RegExp(`^/v1/artifact-uploads/${UUID}/finalize$`).test(path)
    )
      return { ...json, mode: "command" };
  }
  if (
    method === "PUT" &&
    (path === "/v1/worker/output/parts" ||
      new RegExp(`^/v1/artifact-uploads/${UUID}/parts$`).test(path))
  ) {
    return {
      mode: "upload_part",
      body: PART_BYTES,
      stream: "json",
      query:
        path === "/v1/worker/output/parts"
          ? ["upload_id", "worker_id", "attempt_id", "session_id", "offset"]
          : ["offset"],
    };
  }
  if (method === "GET") {
    const get = { ...json, body: 0 };
    if (["/v1/capabilities", "/v1/openapi.json"].includes(path)) return get;
    if (path === "/v1/events") return { ...get, query: ["after", "limit"] };
    if (path === "/v1/events/stream")
      return { ...get, stream: "events", query: ["after"] };
    if (new RegExp(`^/v1/campaigns/${UUID}/context$`).test(path))
      return { ...get, query: ["limit"] };
    if (new RegExp(`^/v1/campaigns/${UUID}/budget$`).test(path)) return get;
    if (new RegExp(`^/v1/records/[a-z_]{1,64}/${UUID}$`).test(path)) return get;
    if (new RegExp(`^/v1/(artifact-uploads|artifacts)/${UUID}$`).test(path))
      return get;
    if (new RegExp(`^/v1/artifacts/${UUID}/content$`).test(path))
      return { ...get, stream: "artifact" };
  }
  return null;
}

function origin(env: Env): string {
  const url = new URL(env.NATIVE_API_ORIGIN);
  const local =
    env.ALLOW_LOCAL_API === "true" &&
    url.protocol === "http:" &&
    ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname);
  if (
    (!local && url.protocol !== "https:") ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  )
    throw new Error("INVALID_ORIGIN");
  return url.origin;
}

function failure(
  code: string,
  status: number,
  retryable = false,
  recovery?: string,
): Response {
  return Response.json(
    {
      error: {
        code,
        message: "Research API transport failed",
        retryable,
        details: recovery ? { recovery } : {},
      },
    },
    {
      status,
      headers: {
        "Cache-Control": "no-store",
        ...(status === 401 ? { "WWW-Authenticate": "Bearer" } : {}),
      },
    },
  );
}

function uncertain(mode: Route["mode"]): Response {
  return failure(
    mode === "query" ? "TRANSPORT_UNAVAILABLE" : "DELIVERY_UNCERTAIN",
    502,
    true,
    mode === "query"
      ? "retry_query"
      : mode === "command"
        ? "replay_exact_command"
        : "inspect_upload_then_replay_exact_range",
  );
}

async function body(
  request: Request,
  limit: number,
): Promise<Uint8Array<ArrayBuffer> | undefined> {
  if (!request.body) return undefined;
  const reader = request.body.getReader();
  const blocks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > limit) throw new Error("REQUEST_TOO_LARGE");
      blocks.push(next.value);
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
  const result = new Uint8Array(size);
  let offset = 0;
  for (const block of blocks) {
    result.set(block, offset);
    offset += block.byteLength;
  }
  return result;
}

export async function forward(
  request: Request,
  env: Env,
  context?: Pick<ExecutionContext, "waitUntil">,
): Promise<Response> {
  const url = new URL(request.url);
  const selected = route(request.method, url.pathname);
  if (!selected) return failure("NOT_FOUND", 404);
  const keys = [...url.searchParams.keys()];
  if (
    url.search.length > 2048 ||
    new Set(keys).size !== keys.length ||
    keys.some((key) => !selected.query.includes(key))
  )
    return failure("PROJECT_CONTRACT_INVALID", 422);
  const authorization = request.headers.get("authorization");
  if (!authorization || !/^Bearer [!-~]{1,4096}$/i.test(authorization))
    return failure("AUTHORIZATION_DENIED", 401);
  let upstream: string;
  let idleMilliseconds: number;
  let content: Uint8Array<ArrayBuffer> | undefined;
  try {
    upstream = origin(env);
    const idle = env.STREAM_IDLE_TIMEOUT_SECONDS ?? "30";
    if (!/^[1-9][0-9]{0,2}$/.test(idle) || Number(idle) > 120)
      throw new Error("INVALID_IDLE_TIMEOUT");
    idleMilliseconds = Number(idle) * 1000;
  } catch {
    return failure("ADAPTER_CAPABILITY_UNSUPPORTED", 503);
  }
  try {
    content = await body(request, selected.body);
  } catch {
    return failure("PROJECT_CONTRACT_INVALID", 413);
  }
  const headers = new Headers({
    Authorization: authorization,
    "Accept-Encoding": "identity",
  });
  for (const name of [
    "content-type",
    "accept",
    "idempotency-key",
    "x-expected-revision",
  ]) {
    const value = request.headers.get(name);
    if (value !== null) headers.set(name, value);
  }
  const abort = new AbortController();
  const cancel = () => abort.abort();
  request.signal.addEventListener("abort", cancel, { once: true });
  if (request.signal.aborted) cancel();
  const timer = setTimeout(cancel, 30_000);
  const cleanup = () => {
    clearTimeout(timer);
    request.signal.removeEventListener("abort", cancel);
    abort.abort();
  };
  let response: Response;
  try {
    response = await fetch(upstream + url.pathname + url.search, {
      method: request.method,
      headers,
      body: content,
      redirect: "manual",
      signal: abort.signal,
    });
    clearTimeout(timer);
    if (response.status >= 300 && response.status < 400)
      throw new Error("REDIRECT_REFUSED");
    if (![null, "identity"].includes(response.headers.get("content-encoding")))
      throw new Error("ENCODING_REFUSED");
  } catch {
    cleanup();
    return uncertain(selected.mode);
  }
  const success = response.status >= 200 && response.status < 300;
  const stream = success ? selected.stream : "json";
  const length = response.headers.get("content-length");
  const declared =
    length !== null && /^(0|[1-9][0-9]*)$/.test(length) ? Number(length) : null;
  if (
    (length !== null &&
      (declared === null || !Number.isSafeInteger(declared))) ||
    (stream === "artifact" && declared === null) ||
    (stream === "json" && declared !== null && declared > RESPONSE_BYTES)
  ) {
    await response.body?.cancel().catch(() => undefined);
    cleanup();
    return uncertain(selected.mode);
  }
  const outputHeaders = new Headers({
    "Cache-Control": "no-store, no-transform",
    "Content-Encoding": "identity",
    "X-Content-Type-Options": "nosniff",
  });
  for (const name of [
    "content-type",
    "content-length",
    "content-disposition",
    "etag",
    "www-authenticate",
  ]) {
    const value = response.headers.get(name);
    if (value !== null) outputHeaders.set(name, value);
  }
  if (!response.body) {
    cleanup();
    return new Response(null, {
      status: response.status,
      headers: outputHeaders,
    });
  }
  const reader = response.body.getReader();
  let received = 0;
  let closed = false;
  const close = async () => {
    if (closed) return;
    closed = true;
    const closing = reader
      .cancel()
      .catch(() => undefined)
      .finally(() => {
        reader.releaseLock();
        cleanup();
      });
    context?.waitUntil(closing);
    await closing;
  };
  const output = new ReadableStream<Uint8Array>({
    async pull(controller) {
      const idle = setTimeout(cancel, idleMilliseconds);
      try {
        const next = await reader.read();
        if (next.done) {
          if (declared !== null && received !== declared)
            throw new Error("INCOMPLETE_RESPONSE");
          controller.close();
          await close();
          return;
        }
        received += next.value.byteLength;
        if (
          (declared !== null && received > declared) ||
          (stream === "json" && received > RESPONSE_BYTES)
        )
          throw new Error("RESPONSE_TOO_LARGE");
        controller.enqueue(next.value);
      } catch {
        controller.error(new Error("RESEARCH_TRANSPORT_INTERRUPTED"));
        await close();
      } finally {
        clearTimeout(idle);
      }
    },
    async cancel() {
      await close();
    },
  });
  let responseBody: ReadableStream = output;
  if (declared !== null) {
    const fixed = new FixedLengthStream(declared);
    // Pipe cancellation propagates to the upstream reader; failure errors the response body.
    const piping = output.pipeTo(fixed.writable).catch(() => undefined);
    context?.waitUntil(piping);
    responseBody = fixed.readable;
  }
  return new Response(responseBody, {
    status: response.status,
    headers: outputHeaders,
  });
}

export default { fetch: forward } satisfies ExportedHandler<Env>;
