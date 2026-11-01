import type { Parameters, RecoveryRequest } from "./hybrid_contracts";

export class RegistryConflict extends Error {}

export interface RegistryKV {
  get<T>(key: string): T | undefined;
  put<T>(key: string, value: T): void;
}

export type RecoveryReceipt = {
  request_id: string;
  predecessor_id: string;
  predecessor_generation: number;
  id: string;
  generation: number;
};

type RecoveryIntent = {
  request: RecoveryRequest;
  successor: Parameters;
  receipt: RecoveryReceipt;
};

const same = (left: unknown, right: unknown): boolean =>
  JSON.stringify(left) === JSON.stringify(right);

function requireConfiguration(
  kv: RegistryKV,
  binding: string,
  configuration: string,
): void {
  if (kv.get<string>("binding") !== binding)
    throw new RegistryConflict("BINDING_MISMATCH");
  if (kv.get<string>("configuration") !== configuration)
    throw new RegistryConflict("GATEWAY_CONFIGURATION_CHANGED");
}

export function reserveGeneration(
  kv: RegistryKV,
  binding: string,
  configuration: string,
  parameters: Parameters,
  predecessor: Parameters | null,
): void {
  const registered = kv.get<string>("binding");
  if (registered !== undefined && registered !== binding)
    throw new RegistryConflict("BINDING_MISMATCH");
  const configured = kv.get<string>("configuration");
  if (configured !== undefined && configured !== configuration)
    throw new RegistryConflict("GATEWAY_CONFIGURATION_CHANGED");
  if (
    predecessor !== null &&
    (!same(
      kv.get<Parameters>("generation:" + predecessor.generation),
      predecessor,
    ) ||
      parameters.generation !== predecessor.generation + 1)
  )
    throw new RegistryConflict("GENERATION_MISMATCH");
  const key = "generation:" + parameters.generation;
  const existing = kv.get<Parameters>(key);
  if (existing !== undefined && !same(existing, parameters))
    throw new RegistryConflict("IDEMPOTENCY_CONFLICT");
  const current = kv.get<number>("current");
  if (existing === undefined) {
    if (parameters.generation !== (current === undefined ? 0 : current + 1))
      throw new RegistryConflict("GENERATION_MISMATCH");
    kv.put("binding", binding);
    kv.put("configuration", configuration);
    kv.put(key, parameters);
    kv.put("current", parameters.generation);
  }
}

export async function recoverWorkflow(
  request: RecoveryRequest,
  binding: string,
  configuration: string,
  port: {
    kv: RegistryKV;
    transaction<T>(callback: () => T): T;
    instanceId(parameters: Parameters): Promise<string>;
    status(id: string): Promise<string>;
    ensure(parameters: Parameters): Promise<string>;
  },
): Promise<RecoveryReceipt> {
  const { kv } = port;
  const key = "recovery:" + request.request_id;
  const replay = (): RecoveryIntent | undefined => {
    requireConfiguration(kv, binding, configuration);
    const saved = kv.get<RecoveryIntent>(key);
    if (saved !== undefined && !same(saved.request, request))
      throw new RegistryConflict("IDEMPOTENCY_CONFLICT");
    return saved;
  };
  let intent = replay();
  if (intent === undefined) {
    const current = kv.get<number>("current");
    const previous = kv.get<Parameters>("generation:" + current);
    if (
      !previous ||
      current !== request.expected_generation ||
      !same(previous.binding, request.binding) ||
      (await port.instanceId(previous)) !== request.expected_instance_id
    )
      throw new RegistryConflict("GENERATION_MISMATCH");
    if ((await port.status(request.expected_instance_id)) !== "errored")
      throw new RegistryConflict("WORKFLOW_NOT_ERRORED");
    const successor = {
      ...previous,
      generation: previous.generation + 1,
      pending: null,
    };
    const receipt = {
      request_id: request.request_id,
      predecessor_id: request.expected_instance_id,
      predecessor_generation: previous.generation,
      id: await port.instanceId(successor),
      generation: successor.generation,
    };
    intent = port.transaction(() => {
      const saved = replay();
      if (saved !== undefined) return saved;
      // A normal handoff or another recovery may have won during provider I/O.
      if (
        kv.get<number>("current") !== previous.generation ||
        !same(kv.get<Parameters>("generation:" + previous.generation), previous)
      )
        throw new RegistryConflict("GENERATION_MISMATCH");
      reserveGeneration(kv, binding, configuration, successor, previous);
      const accepted = { request, successor, receipt };
      kv.put(key, accepted);
      return accepted;
    });
  }
  await port.ensure(intent.successor);
  return intent.receipt;
}
