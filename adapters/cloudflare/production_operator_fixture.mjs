// Local test operator only. Excluded from the production build.
import { readBody } from "./http_io.mjs";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (!["127.0.0.1", "localhost"].includes(url.hostname)) {
      return new Response("Local qualification only", { status: 403 });
    }
    if (request.method === "GET" && url.pathname === "/fixture/identity") {
      return Response.json({
        object_id: env.RESEARCH_AUTHORITY.idFromName("primary").toString(),
      });
    }
    if (request.method === "POST" && url.pathname.startsWith("/fixture/")) {
      const encoded = JSON.parse(await readBody(request, 1024));
      if (typeof encoded.content_hex !== "string") {
        return new Response("Invalid fixture input", { status: 400 });
      }
      const bytes = Uint8Array.from(
        encoded.content_hex.match(/../g) ?? [],
        (pair) => Number.parseInt(pair, 16),
      );
      const { credential } = JSON.parse(new TextDecoder().decode(bytes));
      if (url.pathname === "/fixture/default-provision") {
        try {
          await env.RESEARCH_API.provision(credential);
          return Response.json({ denied: false });
        } catch {
          return Response.json({ denied: true });
        }
      }
      if (url.pathname === "/fixture/provision") {
        const result = await env.RESEARCH_BOOTSTRAP.provision(credential);
        return new Response(
          request.headers.get("x-fixture-drop-bootstrap") === "true"
            ? "{"
            : result,
          { headers: { "content-type": "application/json" } },
        );
      }
    }
    return env.RESEARCH_API.fetch(request);
  },
};
