/** Select transport configuration without changing research authority semantics. */
export interface DeliveryEnvironment {
  AUTHORITY_PROFILE?: string;
  NATIVE_DELIVERY_ORIGIN?: string;
  HOSTED_DELIVERY_ORIGIN?: string;
  ALLOW_LOCAL_GATEWAY?: string;
}

export function deliveryConfiguration(env: DeliveryEnvironment): {
  profile: string;
  origin: string;
} {
  const profile = env.AUTHORITY_PROFILE;
  let selected: string | undefined;
  if (profile === "hybrid-native-v1") {
    selected = env.NATIVE_DELIVERY_ORIGIN;
  } else if (profile === "hosted-do-witness-v1") {
    selected = env.HOSTED_DELIVERY_ORIGIN;
  } else {
    throw new Error("ADAPTER_CAPABILITY_UNSUPPORTED");
  }
  let url: URL;
  try {
    if (!selected) throw new Error();
    url = new URL(selected);
  } catch {
    throw new Error("INVALID_GATEWAY_CONFIGURATION");
  }
  const local =
    env.ALLOW_LOCAL_GATEWAY === "true" &&
    url.protocol === "http:" &&
    ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname);
  if (
    (!local && url.protocol !== "https:") ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/"
  ) {
    throw new Error("INVALID_GATEWAY_CONFIGURATION");
  }
  return { profile, origin: url.origin };
}
