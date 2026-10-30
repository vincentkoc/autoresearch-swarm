// Portable delivery boundaries. The native Python kernel owns research semantics.
export type Binding = {
  schema_version: "1.0";
  workspace_id: string;
  campaign_id: string;
  epoch: number;
  configuration_digest: string;
  actors: { control: string; evaluator?: string };
};
export type Work = {
  schema_version: "1.0";
  binding: Binding;
  kind: "turn" | "wait";
  record_id: string;
  actor_id: string;
};
export type Options = {
  poll_seconds: number;
  rotate_after: number;
  request_timeout_seconds: number;
};
export type Parameters = {
  schema_version: "1.0";
  binding: Binding;
  generation: number;
  pending: Work | null;
  options: Options;
};
export const MAX_BYTES = 16 * 1024;

export function isEventTimeout(error: unknown, milliseconds: number): boolean {
  // The pinned workerd RPC boundary can erase Miniflare's named error subclass.
  return (
    error instanceof Error &&
    (error.name === "WorkflowTimeoutError" ||
      (error.name === "Error" &&
        error.message === `Execution timed out after ${milliseconds}ms`))
  );
}
export const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export function object(
  value: unknown,
  required: string[],
  optional: string[] = [],
): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("INVALID_REFERENCE");
  const result = value as Record<string, unknown>;
  const allowed = new Set([...required, ...optional]);
  if (
    required.some((key) => !Object.hasOwn(result, key)) ||
    Object.keys(result).some((key) => !allowed.has(key))
  ) {
    throw new Error("INVALID_REFERENCE");
  }
  return result;
}

function version(value: Record<string, unknown>): void {
  if (value.schema_version !== undefined && value.schema_version !== "1.0")
    throw new Error("SCHEMA_UNSUPPORTED");
}

export function identifier(value: unknown): string {
  if (typeof value !== "string" || value.length !== 36 || !UUID.test(value))
    throw new Error("INVALID_REFERENCE");
  return value;
}

export function integer(
  value: unknown,
  minimum: number,
  maximum: number,
): number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    throw new Error("INVALID_REFERENCE");
  }
  return value;
}

export function parseBinding(value: unknown): Binding {
  const row = object(
    value,
    ["workspace_id", "campaign_id", "epoch", "configuration_digest", "actors"],
    ["schema_version"],
  );
  version(row);
  const actors = object(row.actors, ["control"], ["evaluator"]);
  const control = identifier(actors.control);
  const evaluator =
    actors.evaluator === undefined ? undefined : identifier(actors.evaluator);
  if (control === evaluator) throw new Error("INVALID_REFERENCE");
  if (
    typeof row.configuration_digest !== "string" ||
    row.configuration_digest.length !== 71 ||
    !/^sha256:[0-9a-f]{64}$/.test(row.configuration_digest)
  ) {
    throw new Error("INVALID_REFERENCE");
  }
  return {
    schema_version: "1.0",
    workspace_id: identifier(row.workspace_id),
    campaign_id: identifier(row.campaign_id),
    epoch: integer(row.epoch, 1, Number.MAX_SAFE_INTEGER),
    configuration_digest: row.configuration_digest,
    actors: evaluator === undefined ? { control } : { control, evaluator },
  };
}

export function same(left: unknown, right: unknown): boolean {
  // Inputs are normalized by these parsers before comparison or identity creation.
  return JSON.stringify(left) === JSON.stringify(right);
}

export function parseWork(value: unknown): Work {
  const row = object(
    value,
    ["binding", "kind", "record_id", "actor_id"],
    ["schema_version"],
  );
  version(row);
  const binding = parseBinding(row.binding);
  const actor_id = identifier(row.actor_id);
  if (
    (row.kind !== "turn" && row.kind !== "wait") ||
    !Object.values(binding.actors).includes(actor_id)
  ) {
    throw new Error("INVALID_REFERENCE");
  }
  return {
    schema_version: "1.0",
    binding,
    kind: row.kind,
    record_id: identifier(row.record_id),
    actor_id,
  };
}

export function parseOptions(value: unknown): Options {
  const row = object(value, [
    "poll_seconds",
    "rotate_after",
    "request_timeout_seconds",
  ]);
  return {
    poll_seconds: integer(row.poll_seconds, 1, 60),
    rotate_after: integer(row.rotate_after, 2, 128),
    request_timeout_seconds: integer(row.request_timeout_seconds, 1, 120),
  };
}

export function parseParameters(value: unknown): Parameters {
  const row = object(
    value,
    ["binding", "generation", "pending", "options"],
    ["schema_version"],
  );
  version(row);
  const binding = parseBinding(row.binding);
  const pending = row.pending === null ? null : parseWork(row.pending);
  if (pending !== null && !same(pending.binding, binding))
    throw new Error("BINDING_MISMATCH");
  return {
    schema_version: "1.0",
    binding,
    generation: integer(row.generation, 0, 1_000_000_000),
    pending,
    options: parseOptions(row.options),
  };
}

export async function digest(value: unknown): Promise<string> {
  const encoded = new TextEncoder().encode(JSON.stringify(value));
  if (encoded.byteLength > MAX_BYTES) throw new Error("REFERENCE_TOO_LARGE");
  const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", encoded));
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join(
    "",
  );
}

export async function instanceId(parameters: Parameters): Promise<string> {
  return `research-${await digest(parameters.binding)}-${parameters.generation}`;
}

export function strictJSON(text: string): unknown {
  if (new TextEncoder().encode(text).byteLength > MAX_BYTES)
    throw new Error("REFERENCE_TOO_LARGE");
  const parsed: unknown = JSON.parse(text);
  // JSON.parse checks grammar; this bounded lexical pass rejects duplicate keys.
  const tokens =
    text.match(
      /"(?:\\.|[^"\\])*"|[{}\[\],:]|true|false|null|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
    ) ?? [];
  const stack: (Set<string> | null)[] = [];
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index];
    if (token === "{" || token === "[")
      stack.push(token === "{" ? new Set() : null);
    else if (token === "}" || token === "]") stack.pop();
    else if (token.startsWith('"') && tokens[index + 1] === ":") {
      const keys = stack[stack.length - 1];
      const key = JSON.parse(token) as string;
      if (!keys || keys.has(key)) throw new Error("DUPLICATE_KEY");
      keys.add(key);
    }
    if (stack.length > 64) throw new Error("REFERENCE_TOO_DEEP");
  }
  return parsed;
}

export async function readJSON(source: Request | Response): Promise<unknown> {
  if (!source.body) throw new Error("INVALID_REFERENCE");
  const reader = source.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > MAX_BYTES) throw new Error("REFERENCE_TOO_LARGE");
      chunks.push(next.value);
    }
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return strictJSON(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
}
