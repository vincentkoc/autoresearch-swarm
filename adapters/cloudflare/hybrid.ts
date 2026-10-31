import {
  DurableObject,
  WorkflowEntrypoint,
  type WorkflowEvent,
  type WorkflowStep,
} from "cloudflare:workers";
import { NonRetryableError } from "cloudflare:workflows";
import { ensureWorkflow } from "./hybrid_creation";
import { deliveryConfiguration } from "./hybrid_transport";
import {
  type Binding,
  type Parameters,
  type Work,
  digest,
  identifier,
  instanceId,
  integer,
  isEventTimeout,
  object,
  parseBinding,
  parseOptions,
  parseParameters,
  parseWork,
  readJSON,
  same,
} from "./hybrid_contracts";

interface Env {
  RESEARCH_FLOW: Workflow<Parameters>;
  RESEARCH_REGISTRY: DurableObjectNamespace<DeliveryRegistry>;
  NATIVE_DELIVERY_ORIGIN: string;
  HOSTED_DELIVERY_ORIGIN?: string;
  GATEWAY_TOKEN: string;
  CONTROL_TOKEN: string;
  ALLOW_LOCAL_GATEWAY?: string;
  AUTHORITY_PROFILE?: string;
}

function gatewayConfiguration(env: Env): { profile: string; origin: string } {
  try {
    return deliveryConfiguration(env);
  } catch (error) {
    throw new NonRetryableError(
      error instanceof Error &&
        error.message === "ADAPTER_CAPABILITY_UNSUPPORTED"
        ? error.message
        : "INVALID_GATEWAY_CONFIGURATION",
    );
  }
}

async function nativeCall(
  env: Env,
  operation: string,
  payload: unknown,
  timeout: number,
): Promise<unknown> {
  const { origin } = gatewayConfiguration(env);
  let response: Response;
  try {
    response = await fetch(origin + "/v1/delivery/" + operation, {
      method: "POST",
      headers: {
        authorization: "Bearer " + env.GATEWAY_TOKEN,
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
      redirect: "manual",
      signal: AbortSignal.timeout(timeout * 1000),
    });
  } catch {
    throw new Error("NATIVE_DELIVERY_UNCERTAIN");
  }
  if (response.status >= 300 && response.status < 400) {
    await response.body?.cancel();
    throw new NonRetryableError("NATIVE_GATEWAY_REDIRECT_REFUSED");
  }
  let value: unknown;
  try {
    value = await readJSON(response);
  } catch {
    throw new Error("NATIVE_DELIVERY_RESPONSE_INVALID");
  }
  if (!response.ok) {
    let code = "NATIVE_DELIVERY_UNCERTAIN";
    let retryable = true;
    try {
      const row = object(value, ["schema_version", "error"]);
      const error = object(row.error, ["code", "retryable"]);
      if (
        row.schema_version !== "1.0" ||
        typeof error.code !== "string" ||
        !/^[A-Z_]{3,64}$/.test(error.code) ||
        typeof error.retryable !== "boolean"
      )
        throw new Error();
      code = error.code;
      retryable = error.retryable;
    } catch {
      /* Never copy an upstream response or exception into Workflow history. */
    }
    if (!retryable || response.status === 401 || response.status === 403)
      throw new NonRetryableError(code);
    throw new Error(code);
  }
  return value;
}

function pendingReply(
  value: unknown,
  binding: Binding,
): { state: string; work: Work | null } {
  const row = object(value, ["schema_version", "state", "work"]);
  if (
    row.schema_version !== "1.0" ||
    !["work", "idle", "retired", "terminal"].includes(String(row.state))
  ) {
    throw new NonRetryableError("INVALID_GATEWAY_REFERENCE");
  }
  const work = row.work === null ? null : parseWork(row.work);
  if (
    (row.state === "work") !== (work !== null) ||
    (work !== null && !same(work.binding, binding))
  ) {
    throw new NonRetryableError("INVALID_GATEWAY_REFERENCE");
  }
  return { state: row.state as string, work };
}

function settledReply(value: unknown, work: Work): boolean {
  const row = object(value, [
    "schema_version",
    "state",
    "record_id",
    "disposition",
    "evidence_id",
    "watermark",
  ]);
  if (
    row.schema_version !== "1.0" ||
    !["settled", "pending"].includes(String(row.state)) ||
    row.record_id !== work.record_id ||
    typeof row.disposition !== "string" ||
    row.disposition.length > 128 ||
    (row.evidence_id !== "" && identifier(row.evidence_id) !== row.evidence_id)
  ) {
    throw new NonRetryableError("INVALID_GATEWAY_REFERENCE");
  }
  integer(row.watermark, 0, Number.MAX_SAFE_INTEGER);
  return row.state === "settled";
}

async function registry(
  env: Env,
  binding: Binding,
): Promise<DurableObjectStub<DeliveryRegistry>> {
  return env.RESEARCH_REGISTRY.getByName(await digest(binding));
}

export class DeliveryRegistry extends DurableObject<Env> {
  private async reserve(
    parameters: Parameters,
    predecessor: Parameters | null,
  ): Promise<string> {
    const bindingDigest = await digest(parameters.binding);
    const configuration = await digest(gatewayConfiguration(this.env));
    this.ctx.storage.transactionSync(() => {
      const kv = this.ctx.storage.kv;
      const registered = kv.get<string>("binding");
      if (registered !== undefined && registered !== bindingDigest)
        throw new NonRetryableError("BINDING_MISMATCH");
      const configured = kv.get<string>("configuration");
      if (configured !== undefined && configured !== configuration)
        throw new NonRetryableError("GATEWAY_CONFIGURATION_CHANGED");
      const previous =
        predecessor === null
          ? null
          : kv.get<Parameters>("generation:" + predecessor.generation);
      if (
        predecessor !== null &&
        (!previous ||
          !same(previous, predecessor) ||
          parameters.generation !== predecessor.generation + 1)
      ) {
        throw new NonRetryableError("GENERATION_MISMATCH");
      }
      const key = "generation:" + parameters.generation;
      const existing = kv.get<Parameters>(key);
      if (existing !== undefined && !same(existing, parameters))
        throw new NonRetryableError("IDEMPOTENCY_CONFLICT");
      const current = kv.get<number>("current");
      if (existing === undefined) {
        if (parameters.generation !== (current === undefined ? 0 : current + 1))
          throw new NonRetryableError("GENERATION_MISMATCH");
        kv.put("binding", bindingDigest);
        kv.put("configuration", configuration);
        kv.put(key, parameters);
        kv.put("current", parameters.generation);
      }
    });
    return this.ensure(parameters);
  }

  private async ensure(parameters: Parameters): Promise<string> {
    const id = await instanceId(parameters);
    // Provider I/O follows the durable intent; retries reuse the exact ID and input.
    if (this.ctx.storage.kv.get<number>("current") === parameters.generation) {
      const observed = "observed:" + parameters.generation;
      await ensureWorkflow(
        this.env.RESEARCH_FLOW,
        id,
        parameters,
        this.ctx.storage.kv.get<boolean>(observed) === true,
      );
      this.ctx.storage.kv.put(observed, true);
    }
    return id;
  }

  async start(value: unknown): Promise<string> {
    gatewayConfiguration(this.env);
    const row = object(value, ["binding", "options"]);
    const parameters = parseParameters({
      binding: parseBinding(row.binding),
      options: parseOptions(row.options),
      generation: 0,
      pending: null,
    });
    const id = await this.reserve(parameters, null);
    const currentId = await this.ensure(this.current());
    const current = await this.env.RESEARCH_FLOW.get(currentId);
    const status = (await current.status()).status;
    if (["queued", "running", "waiting"].includes(status)) {
      // Resume existing history after a delivery-runtime restart; never reset it.
      await current.sendEvent({
        type: "research-wake",
        payload: { kind: "delivery-start" },
      });
    }
    return id;
  }

  async authorize(value: unknown, id: string): Promise<boolean> {
    gatewayConfiguration(this.env);
    const parameters = parseParameters(value);
    if ((await instanceId(parameters)) !== id)
      throw new NonRetryableError("WORKFLOW_ID_MISMATCH");
    const saved = this.ctx.storage.kv.get<Parameters>(
      "generation:" + parameters.generation,
    );
    if (!saved || !same(saved, parameters))
      throw new NonRetryableError("WORKFLOW_INPUT_MISMATCH");
    const configuration = await digest(gatewayConfiguration(this.env));
    if (this.ctx.storage.kv.get<string>("configuration") !== configuration)
      throw new NonRetryableError("GATEWAY_CONFIGURATION_CHANGED");
    // Record existence before any native call, even when the create reply was lost.
    this.ctx.storage.kv.put("observed:" + parameters.generation, true);
    return this.ctx.storage.kv.get<number>("current") === parameters.generation;
  }

  async advance(value: unknown, pendingValue: unknown): Promise<string> {
    const previous = parseParameters(value);
    const next = parseParameters({
      ...previous,
      generation: previous.generation + 1,
      pending: pendingValue,
    });
    return this.reserve(next, previous);
  }

  async recoverSuccessor(value: unknown): Promise<string> {
    const previous = parseParameters(value);
    const next = this.ctx.storage.kv.get<Parameters>(
      "generation:" + (previous.generation + 1),
    );
    if (!next) throw new NonRetryableError("SUCCESSOR_NOT_RESERVED");
    return this.reserve(parseParameters(next), previous);
  }

  private current(): Parameters {
    const generation = this.ctx.storage.kv.get<number>("current");
    const saved =
      generation === undefined
        ? undefined
        : this.ctx.storage.kv.get<Parameters>("generation:" + generation);
    if (!saved) throw new NonRetryableError("WORKFLOW_NOT_REGISTERED");
    return saved;
  }

  async status(): Promise<{
    id: string;
    generation: number;
    status: string;
    initial_pending: Work | null;
  }> {
    const parameters = this.current();
    const id = await instanceId(parameters);
    let status: string;
    try {
      status = (await (await this.env.RESEARCH_FLOW.get(id)).status()).status;
    } catch {
      status = this.ctx.storage.kv.get<boolean>(
        "observed:" + parameters.generation,
      )
        ? "unavailable"
        : "creating";
    }
    return {
      id,
      generation: parameters.generation,
      status,
      initial_pending: parameters.pending,
    };
  }

  async wake(value: unknown): Promise<void> {
    const row = object(value, ["binding", "actor_id", "wait_id"]);
    const binding = parseBinding(row.binding);
    const parameters = this.current();
    const actor_id = identifier(row.actor_id);
    const wait_id = identifier(row.wait_id);
    if (
      !same(binding, parameters.binding) ||
      !Object.values(binding.actors).includes(actor_id)
    ) {
      throw new NonRetryableError("BINDING_MISMATCH");
    }
    const instance = await this.env.RESEARCH_FLOW.get(
      await instanceId(parameters),
    );
    await instance.sendEvent({
      type: "research-wake",
      payload: { actor_id, wait_id },
    });
  }
}

export class ResearchWorkflow extends WorkflowEntrypoint<Env, Parameters> {
  async run(
    event: WorkflowEvent<Parameters>,
    step: WorkflowStep,
  ): Promise<unknown> {
    gatewayConfiguration(this.env);
    const parameters = parseParameters(event.payload);
    const owner = await registry(this.env, parameters.binding);
    const requireGeneration = async () => {
      if (!(await owner.authorize(parameters, event.instanceId)))
        throw new NonRetryableError("GENERATION_SUPERSEDED");
    };
    const config = {
      retries: {
        limit: 8,
        delay: "1 second" as const,
        backoff: "exponential" as const,
      },
      timeout: "3 minutes" as const,
    };
    if (!(await owner.authorize(parameters, event.instanceId))) {
      const next = await step.do("recover-handoff", config, () =>
        owner.recoverSuccessor(parameters),
      );
      return { state: "continued", next };
    }
    const recovery_id = await step.do("recovery-id", async () =>
      crypto.randomUUID(),
    );
    const startup = await step.do("bootstrap", config, async () => {
      await requireGeneration();
      const result = object(
        await nativeCall(
          this.env,
          "bootstrap",
          { binding: parameters.binding, recovery_id },
          parameters.options.request_timeout_seconds,
        ),
        ["schema_version", "state"],
      );
      if (
        result.schema_version !== "1.0" ||
        !["active", "retired", "expired"].includes(String(result.state))
      )
        throw new NonRetryableError("INVALID_GATEWAY_REFERENCE");
      return result.state as string;
    });
    if (startup === "retired") return { state: "retired" };
    let pending = parameters.pending;
    if (startup !== "expired") {
      for (
        let iteration = 0;
        iteration < parameters.options.rotate_after;
        iteration++
      ) {
        if (pending === null) {
          const observed = await step.do(
            "inspect-" + iteration,
            config,
            async () => {
              await requireGeneration();
              return pendingReply(
                await nativeCall(
                  this.env,
                  "inspect",
                  parameters.binding,
                  parameters.options.request_timeout_seconds,
                ),
                parameters.binding,
              );
            },
          );
          if (observed.state === "terminal" || observed.state === "retired")
            return { state: observed.state };
          pending = observed.work;
        }
        if (pending !== null) {
          const selected = pending;
          const settled = await step.do(
            "deliver-" + iteration,
            config,
            async () => {
              await requireGeneration();
              return settledReply(
                await nativeCall(
                  this.env,
                  selected.kind === "turn" ? "execute" : "wait",
                  selected,
                  parameters.options.request_timeout_seconds,
                ),
                selected,
              );
            },
          );
          if (settled) pending = null;
          else
            await this.wait(step, iteration, parameters.options.poll_seconds);
        } else
          await this.wait(step, iteration, parameters.options.poll_seconds);
      }
    }
    const next = await step.do("continue", config, () =>
      owner.advance(parameters, pending),
    );
    return { state: "continued", next };
  }

  private async wait(
    step: WorkflowStep,
    iteration: number,
    seconds: number,
  ): Promise<void> {
    try {
      await step.waitForEvent("wake-" + iteration, {
        type: "research-wake",
        timeout: `${seconds} seconds`,
      });
    } catch (error) {
      if (!isEventTimeout(error, seconds * 1000))
        throw new Error("WORKFLOW_WAIT_FAILED");
    }
    // Events only prompt another native reconciliation; their payload grants no authority.
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (
      request.method !== "POST" ||
      !["/adapter/start", "/adapter/status", "/adapter/wake"].includes(
        url.pathname,
      )
    )
      return new Response("Not found", { status: 404 });
    if (
      !env.CONTROL_TOKEN ||
      env.CONTROL_TOKEN.length < 32 ||
      request.headers.get("authorization") !== "Bearer " + env.CONTROL_TOKEN
    ) {
      return Response.json({ error: "AUTHORIZATION_DENIED" }, { status: 401 });
    }
    try {
      gatewayConfiguration(env);
      const value = await readJSON(request);
      const row = object(
        value,
        url.pathname === "/adapter/start"
          ? ["binding", "options"]
          : url.pathname === "/adapter/wake"
            ? ["binding", "actor_id", "wait_id"]
            : ["binding"],
      );
      const owner = await registry(env, parseBinding(row.binding));
      if (url.pathname === "/adapter/start")
        return Response.json({ id: await owner.start(value) });
      if (url.pathname === "/adapter/status")
        return Response.json(await owner.status());
      await owner.wake(value);
      return Response.json({ accepted: true });
    } catch (error) {
      const code =
        error instanceof NonRetryableError &&
        error.message === "ADAPTER_CAPABILITY_UNSUPPORTED"
          ? error.message
          : "ADAPTER_REQUEST_REJECTED";
      return Response.json({ error: code }, { status: 409 });
    }
  },
} satisfies ExportedHandler<Env>;
