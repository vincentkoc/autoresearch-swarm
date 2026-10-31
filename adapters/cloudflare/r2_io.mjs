// Awaited R2 binding I/O. A timeout is uncertainty, never proof of write closure.
const MAX_BYTES = 1024 * 1024;
const HEX = Array.from({ length: 256 }, (_, n) => n.toString(16).padStart(2, "0"));

function keyValue(key) {
  if (typeof key !== "string" || !key || key.length > 1024) {
    throw new TypeError("Invalid R2 key");
  }
}

function metadataValue(value) {
  if (!value || typeof value !== "object" || Array.isArray(value) ||
      Object.entries(value).some(([key, field]) => typeof field !== "string" || key.length > 128) ||
      JSON.stringify(value).length > 8192) {
    throw new TypeError("Invalid R2 metadata");
  }
  return value;
}

async function bounded(operation) {
  let timer;
  try {
    return await Promise.race([
      operation(),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("R2 operation unconfirmed")), 5000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export async function put(bucket, key, contentHex, metadataJson, ifAbsent) {
  keyValue(key);
  if (typeof contentHex !== "string" || contentHex.length > MAX_BYTES * 2 ||
      contentHex.length % 2 || !/^[0-9a-f]*$/.test(contentHex) ||
      typeof metadataJson !== "string" || metadataJson.length > 8192 ||
      typeof ifAbsent !== "boolean") {
    throw new TypeError("Invalid bounded R2 write");
  }
  const customMetadata = metadataValue(JSON.parse(metadataJson));
  const content = new Uint8Array(contentHex.length / 2);
  for (let i = 0; i < content.length; i++) {
    content[i] = Number.parseInt(contentHex.slice(i * 2, i * 2 + 2), 16);
  }
  return bounded(async () => (await bucket.put(key, content, {
    customMetadata,
    ...(ifAbsent ? { onlyIf: new Headers({ "if-none-match": "*" }) } : {}),
  })) !== null);
}

export async function get(bucket, key, maxBytes) {
  keyValue(key);
  if (!Number.isInteger(maxBytes) || maxBytes < 0 || maxBytes > MAX_BYTES) {
    throw new TypeError("Invalid bounded R2 read");
  }
  return bounded(async () => {
    const object = await bucket.get(key);
    if (object === null) return "null";
    if (!object.body) throw new Error("R2 object has no body");
    const reader = object.body.getReader();
    try {
      if (!Number.isInteger(object.size) || object.size < 0 || object.size > maxBytes) {
        throw new Error("R2 object exceeded its bound");
      }
      const metadata = metadataValue(object.customMetadata ?? {});
      let size = 0;
      const parts = [];
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > maxBytes) throw new Error("R2 body exceeded its bound");
        for (let offset = 0; offset < value.length; offset += 8192) {
          parts.push(Array.from(value.subarray(offset, offset + 8192), byte => HEX[byte]).join(""));
        }
      }
      if (size !== object.size) throw new Error("R2 body length differs");
      return JSON.stringify({ content_hex: parts.join(""), metadata });
    } finally {
      await reader.cancel();
    }
  });
}
