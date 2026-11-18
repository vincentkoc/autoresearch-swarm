// Independent ECMAScript fixture verifier. This is not a worker or an API parser.
import { createHash, createPublicKey, verify } from "node:crypto";
import { readFileSync } from "node:fs";

function canonical(value) {
  if (value === null || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("non-finite number");
    return JSON.stringify(value);
  }
  if (typeof value === "string") {
    if (!value.isWellFormed()) throw new Error("invalid Unicode");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (typeof value !== "object") throw new Error("unsupported JSON value");
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new Error("not a plain JSON object");
  }
  // Emit pairs directly: constructing another object would reorder integer keys.
  const pairs = Object.keys(value)
    .sort()
    .map((key) => `${canonical(key)}:${canonical(value[key])}`);
  return `{${pairs.join(",")}}`;
}

function digest(value) {
  return `sha256:${createHash("sha256").update(value, "utf8").digest("hex")}`;
}

const input = JSON.parse(readFileSync(0, "utf8"));
let output;
if (process.argv[2] === "canonical") {
  output = input.map((item) => {
    const payload = JSON.parse(item.source_json);
    const encoded = canonical(payload);
    const preimage = canonical({ domain: item.domain, hash_format_version: "1", payload });
    return {
      id: item.id,
      canonical_utf8: encoded,
      content_digest: digest(encoded),
      record_digest: digest(preimage),
    };
  });
} else if (process.argv[2] === "signature") {
  const { authority, assignment } = input;
  const preimage = canonical({
    domain: "research.worker-assignment",
    algorithm: "ed25519-jcs-v1",
    key_id: assignment.key_id,
    payload: assignment.payload,
  });
  const publicKey = createPublicKey({
    key: Buffer.from(`302a300506032b6570032100${authority.public_key}`, "hex"),
    type: "spki",
    format: "der",
  });
  output = {
    preimage_digest: digest(preimage),
    valid_signature: verify(
      null,
      Buffer.from(preimage, "utf8"),
      publicKey,
      Buffer.from(assignment.signature, "hex"),
    ),
  };
} else {
  throw new Error("unknown fixture operation");
}
process.stdout.write(JSON.stringify(output));
