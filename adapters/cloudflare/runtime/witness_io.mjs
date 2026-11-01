// Deployment-owned destination and credential. No caller-selected URL or redirect.
const MAX_BYTES = 16384;

function destination(origin, allowLoopback) {
  try {
    const url = new URL(origin);
    if (typeof origin !== "string" || origin !== url.origin ||
        typeof allowLoopback !== "boolean" ||
        (url.protocol !== "https:" &&
         !(allowLoopback && url.protocol === "http:" && url.hostname === "127.0.0.1"))) {
      throw new Error();
    }
    return new URL("/v1/time-witness", url);
  } catch {
    throw new TypeError("Invalid witness destination");
  }
}

function cancel(body) {
  try { Promise.resolve(body?.cancel()).catch(() => {}); } catch { /* best effort */ }
}

export async function request(origin, credential, body, timeoutMs, allowLoopback = false) {
  const url = destination(origin, allowLoopback);
  if (typeof credential !== "string" || !/^[A-Za-z0-9_-]{43,128}$/.test(credential) ||
      typeof body !== "string" || new TextEncoder().encode(body).length > MAX_BYTES ||
      !Number.isInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 10000) {
    throw new TypeError("Invalid witness transport configuration or request");
  }
  const controller = new AbortController();
  let timer, reader, closed = false;
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error("Witness exchange timed out")), timeoutMs);
  });
  try {
    return await Promise.race([deadline, (async () => {
      const response = await fetch(url, {
        method: "POST", body, redirect: "manual", signal: controller.signal,
        headers: { "authorization": "Bearer " + credential, "content-type": "application/json" },
      });
      if (closed) { cancel(response.body); throw new Error(); }
      if (response.status !== 200 || !response.body ||
          response.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase() !== "application/json" ||
          ![null, "identity"].includes(response.headers.get("content-encoding"))) {
        cancel(response.body);
        throw new Error();
      }
      const length = response.headers.get("content-length");
      if (length !== null && (!/^(?:0|[1-9][0-9]*)$/.test(length) || Number(length) > MAX_BYTES)) {
        cancel(response.body);
        throw new Error();
      }
      reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8", { fatal: true });
      let size = 0, chunks = 0, text = "";
      for (;;) {
        if (closed) throw new Error();
        const { done, value } = await reader.read();
        if (closed) throw new Error();
        if (done) break;
        if (!(value instanceof Uint8Array)) throw new Error();
        size += value.length;
        if (size > MAX_BYTES) throw new Error();
        text += decoder.decode(value, { stream: true });
        if (++chunks % 64 === 0) await new Promise(resolve => setTimeout(resolve, 0));
      }
      if (length !== null && Number(length) !== size) throw new Error();
      return text + decoder.decode();
    })()]);
  } catch {
    throw new Error("Time witness transport unavailable");
  } finally {
    closed = true;
    clearTimeout(timer);
    controller.abort();
    cancel(reader);
  }
}
