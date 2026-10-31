// Local qualification operator only. Every destination is a fixed service binding.
import { readBody } from "./http_io.mjs";
import sourceOperator from "./production_operator_fixture.mjs";

const RPC_LIMIT = 6 * 1024 * 1024;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (!["127.0.0.1", "localhost"].includes(url.hostname)) {
      return new Response("Local qualification only", { status: 403 });
    }
    if (request.method === "GET" && url.pathname === "/fixture/targets") {
      return Response.json({
        a: env.TARGET_A_AUTHORITY.idFromName("primary").toString(),
        b: env.TARGET_B_AUTHORITY.idFromName("primary").toString(),
      });
    }
    const matched = url.pathname.match(
      /^\/fixture\/target-(a|b)\/(import|import-limit|import-over-limit|default-import|request|default-request|admit|default-admit|turn|default-turn|artifact|default-artifact|provision|capabilities)$/,
    );
    if (!matched) return sourceOperator.fetch(request, env);
    const api = matched[1] === "a" ? env.TARGET_A_API : env.TARGET_B_API;
    const bootstrap =
      matched[1] === "a" ? env.TARGET_A_BOOTSTRAP : env.TARGET_B_BOOTSTRAP;
    const action = matched[2];
    if (request.method === "GET" && action === "capabilities") {
      return api.fetch(
        new Request(new URL("/v1/capabilities", request.url), {
          headers: request.headers,
        }),
      );
    }
    if (action === "capabilities")
      return new Response("Not found", { status: 404 });
    if (request.method !== "POST")
      return new Response("Not found", { status: 404 });
    const read = JSON.parse(await readBody(request, 64 * 1024));
    if (typeof read.content_hex !== "string") {
      return new Response("Invalid fixture input", { status: 400 });
    }
    const bytes = Uint8Array.from(read.content_hex.match(/../g) ?? [], (pair) =>
      Number.parseInt(pair, 16),
    );
    const input = JSON.parse(new TextDecoder().decode(bytes));
    if (action === "artifact") {
      const source = await env.RESEARCH_BOOTSTRAP.checkpoint_read_artifact(
        input.source_credential,
        input.artifact_json,
      );
      const exported = JSON.parse(source);
      if (exported.error) return new Response(source, { status: 409 });
      const artifact = JSON.parse(input.artifact_json);
      const result = JSON.parse(
        await bootstrap.checkpoint_artifact(
          input.credential,
          JSON.stringify({
            artifact_id: artifact.artifact_id,
            generation: artifact.generation,
          }),
          exported.content_hex,
        ),
      );
      if (request.headers.get("x-fixture-drop-bootstrap") === "true") {
        if (
          typeof result.restore_id !== "string" ||
          typeof result.storage_digest !== "string" ||
          JSON.stringify(result.artifact) !== JSON.stringify(exported.artifact)
        )
          return Response.json(result, { status: 409 });
        return new Response("{", {
          headers: { "content-type": "application/json" },
        });
      }
      return Response.json({ exported, result });
    }
    if (action === "provision" || action === "request") {
      const result =
        action === "provision"
          ? await bootstrap.provision(input.credential)
          : await bootstrap.checkpoint_request(input.credential);
      if (
        action === "request" &&
        request.headers.get("x-fixture-drop-bootstrap") === "true"
      ) {
        const value = JSON.parse(result);
        if (!value.request || typeof value.signature !== "string") {
          return new Response(result, { status: 409 });
        }
      }
      return new Response(
        request.headers.get("x-fixture-drop-bootstrap") === "true"
          ? "{"
          : result,
        {
          headers: { "content-type": "application/json" },
        },
      );
    }
    if (action === "admit" || action === "turn") {
      const result =
        action === "admit"
          ? await bootstrap.checkpoint_admit(
              input.credential,
              input.election_json,
            )
          : await bootstrap.checkpoint_turn(input.credential, input.turn_json);
      if (request.headers.get("x-fixture-drop-bootstrap") === "true") {
        const value = JSON.parse(result);
        if (
          value.disposition !== "accepted" ||
          (action === "admit"
            ? typeof value.result?.data?.logical_election_digest !== "string"
            : !["settled", "closed_unacknowledged"].includes(
                value.result?.turn?.state,
              ))
        ) {
          return new Response(result, { status: 409 });
        }
        return new Response("{", {
          headers: { "content-type": "application/json" },
        });
      }
      return new Response(result, {
        headers: { "content-type": "application/json" },
      });
    }
    if (
      action === "default-import" ||
      action === "default-request" ||
      action === "default-admit" ||
      action === "default-turn" ||
      action === "default-artifact"
    ) {
      try {
        if (action === "default-import")
          await api.checkpoint_import(input.credential, "{}");
        else if (action === "default-request")
          await api.checkpoint_request(input.credential);
        else if (action === "default-admit")
          await api.checkpoint_admit(input.credential, input.election_json);
        else if (action === "default-artifact")
          await api.checkpoint_artifact(
            input.credential,
            input.artifact_json,
            "",
          );
        else await api.checkpoint_turn(input.credential, input.turn_json);
        return Response.json({ denied: false });
      } catch {
        return Response.json({ denied: true });
      }
    }
    let bundle = await env.RESEARCH_BOOTSTRAP.checkpoint_capture(
      input.source_credential,
      input.request_json,
    );
    if (JSON.parse(bundle).error) return new Response(bundle, { status: 409 });
    if (action === "import-limit" || action === "import-over-limit") {
      const size = RPC_LIMIT + (action === "import-over-limit" ? 1 : 0);
      if (bundle.length > size)
        throw new Error("Fixture image exceeds its transport profile");
      // Legal trailing JSON whitespace exercises the complete RPC cap with real signed bytes.
      bundle += " ".repeat(size - bundle.length);
    }
    const result = await bootstrap.checkpoint_import(input.credential, bundle);
    const parsed = JSON.parse(result);
    if (
      request.headers.get("x-fixture-drop-bootstrap") === "true" &&
      !parsed.receipt
    ) {
      return new Response(result, { status: 409 });
    }
    return new Response(
      request.headers.get("x-fixture-drop-bootstrap") === "true"
        ? "{"
        : JSON.stringify({
            bundle_bytes: bundle.length,
            result: parsed,
          }),
      { headers: { "content-type": "application/json" } },
    );
  },
};
