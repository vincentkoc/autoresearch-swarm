import assert from "node:assert/strict";
import test from "node:test";
import { deliveryConfiguration } from "./hybrid_transport.ts";

const base = {
  NATIVE_DELIVERY_ORIGIN: "https://native.example.invalid/",
  HOSTED_DELIVERY_ORIGIN: "https://hosted.example.invalid/",
};

test("profiles select only their explicit origin and retain configuration identity", () => {
  for (const [profile, origin] of [
    ["hybrid-native-v1", "https://native.example.invalid"],
    ["hosted-do-witness-v1", "https://hosted.example.invalid"],
  ]) {
    assert.deepEqual(
      deliveryConfiguration({ ...base, AUTHORITY_PROFILE: profile }),
      { profile, origin },
    );
  }
  for (const [profile, missing] of [
    ["hybrid-native-v1", "NATIVE_DELIVERY_ORIGIN"],
    ["hosted-do-witness-v1", "HOSTED_DELIVERY_ORIGIN"],
  ]) {
    assert.throws(
      () =>
        deliveryConfiguration({
          ...base,
          AUTHORITY_PROFILE: profile,
          [missing]: undefined,
        }),
      /^Error: INVALID_GATEWAY_CONFIGURATION$/,
    );
  }
});

test("unknown authority profiles cannot silently become native delivery", () => {
  for (const profile of [undefined, "", "durable-object-authority-v1"]) {
    assert.throws(
      () => deliveryConfiguration({ ...base, AUTHORITY_PROFILE: profile }),
      /^Error: ADAPTER_CAPABILITY_UNSUPPORTED$/,
    );
  }
});

test("both profiles reject credentials, paths and non-local plaintext routes", () => {
  for (const [profile, field] of [
    ["hybrid-native-v1", "NATIVE_DELIVERY_ORIGIN"],
    ["hosted-do-witness-v1", "HOSTED_DELIVERY_ORIGIN"],
  ]) {
    for (const origin of [
      "not an origin",
      "https://private:secret@example.invalid",
      "https://example.invalid/path",
      "https://example.invalid/?secret=hidden",
      "https://example.invalid/#hidden",
      "http://example.invalid",
      "file:///",
    ]) {
      assert.throws(
        () =>
          deliveryConfiguration({
            ...base,
            AUTHORITY_PROFILE: profile,
            [field]: origin,
            ALLOW_LOCAL_GATEWAY: "true",
          }),
        /^Error: INVALID_GATEWAY_CONFIGURATION$/,
      );
    }
  }
});

test("loopback plaintext requires an explicit local qualification override", () => {
  const env = {
    AUTHORITY_PROFILE: "hosted-do-witness-v1",
    HOSTED_DELIVERY_ORIGIN: "http://127.0.0.1:4321/",
  };
  assert.throws(() => deliveryConfiguration(env));
  assert.deepEqual(
    deliveryConfiguration({ ...env, ALLOW_LOCAL_GATEWAY: "true" }),
    { profile: env.AUTHORITY_PROFILE, origin: "http://127.0.0.1:4321" },
  );
});
