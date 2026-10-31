// Local qualification only. This is not a production witness client.
export async function request(origin, path, body, timeoutMs) {
  const url = new URL(origin);
  if (url.protocol !== "http:" || url.hostname !== "127.0.0.1" ||
      url.username || url.password || url.pathname !== "/" || url.search || url.hash ||
      !/^\/(?:witness|ingress)\/[a-z0-9-]{1,64}$/.test(path) ||
      !Number.isInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 10000 ||
      typeof body !== "string" || new TextEncoder().encode(body).length > 16384) {
    throw new TypeError("Invalid local witness fixture request");
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(new URL(path, url), {
      // workerd supports manual/follow; every non-200 response below is refused.
      method: "POST", body, redirect: "manual", signal: controller.signal,
      headers: { "content-type": "application/json", "authorization": "Bearer public_fixture_consumer_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" },
    });
    if (response.status !== 200 || !response.body) {
      throw new Error("Witness fixture unavailable");
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let size = 0, text = "";
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > 16384) throw new Error("Witness fixture response exceeded its bound");
        text += decoder.decode(value, { stream: true });
      }
      return text + decoder.decode();
    } finally {
      await reader.cancel();
    }
  } finally {
    clearTimeout(timer);
  }
}
