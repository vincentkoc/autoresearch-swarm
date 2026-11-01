// Local qualification only. This is not a storage gateway or deployment entry point.
interface Env {
  ARTIFACTS: R2Bucket;
  QUALIFICATION: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (
      env.QUALIFICATION !== "local-r2-binding" ||
      url.hostname !== "127.0.0.1" ||
      !url.pathname.startsWith("/object/")
    ) {
      return new Response(null, { status: 404 });
    }
    const key = decodeURIComponent(url.pathname.slice(8));
    if (!key || key.length > 1024) return new Response(null, { status: 400 });
    if (request.method === "GET") {
      const object = await env.ARTIFACTS.get(key);
      if (!object) return new Response(null, { status: 404 });
      return new Response(object.body, {
        headers: {
          "content-type": "application/octet-stream",
          "content-encoding": "identity",
          "cache-control": "no-store, no-transform",
          "content-length": String(object.size),
          "x-object-metadata": JSON.stringify(object.customMetadata ?? {}),
        },
      });
    }
    if (request.method === "PUT") {
      const metadataText = request.headers.get("x-object-metadata") ?? "{}";
      if (metadataText.length > 8192)
        return new Response(null, { status: 413 });
      const customMetadata = JSON.parse(metadataText) as Record<string, string>;
      const body = await request.arrayBuffer();
      if (body.byteLength > 1024 * 1024)
        return new Response(null, { status: 413 });
      const object = await env.ARTIFACTS.put(key, body, {
        customMetadata,
        ...(request.headers.get("if-none-match") === "*"
          ? { onlyIf: new Headers({ "if-none-match": "*" }) }
          : {}),
      });
      return new Response(null, { status: object ? 200 : 412 });
    }
    return new Response(null, { status: 405 });
  },
} satisfies ExportedHandler<Env>;
