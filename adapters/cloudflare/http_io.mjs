// Consume bounded request bytes before dispatch. Failed reads grant no authority.
const MAX_BYTES = 1024 * 1024;
const HEX = Array.from({ length: 256 }, (_, n) => n.toString(16).padStart(2, "0"));

export async function readBody(request, maxBytes) {
  if (!Number.isInteger(maxBytes) || maxBytes < 0 || maxBytes > MAX_BYTES) {
    throw new TypeError("Invalid request body bound");
  }
  if (!request.body) return JSON.stringify({ content_hex: "" });
  let reader, timer, exceeded = false;
  try {
    reader = request.body.getReader();
    const consume = async () => {
      let size = 0, chunks = 0;
      const parts = [];
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return parts.join("");
        // Already-ready chunks (including empty ones) must not starve the deadline.
        if (++chunks % 64 === 0) await new Promise(resolve => setTimeout(resolve, 0));
        if (!(value instanceof Uint8Array)) throw new TypeError("Invalid body chunk");
        size += value.byteLength;
        if (size > maxBytes) {
          exceeded = true;
          throw new Error("Body bound exceeded");
        }
        for (let offset = 0; offset < value.length; offset += 8192) {
          parts.push(Array.from(value.subarray(offset, offset + 8192), byte => HEX[byte]).join(""));
        }
      }
    };
    const contentHex = await Promise.race([
      consume(),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("Body read unconfirmed")), 5000);
      }),
    ]);
    return JSON.stringify({ content_hex: contentHex });
  } catch {
    return JSON.stringify({ error: exceeded ? "limit" : "unconfirmed" });
  } finally {
    clearTimeout(timer);
    // Request cancellation is best effort. Do not wait forever for an untrusted
    // producer, or claim that cancellation proves transport closure.
    if (reader) reader.cancel().catch(() => {});
  }
}
