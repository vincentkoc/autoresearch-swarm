import type { Parameters } from "./hybrid_contracts";

export async function ensureWorkflow(
  binding: Workflow<Parameters>,
  id: string,
  parameters: Parameters,
  observed: boolean,
): Promise<void> {
  if (observed) {
    // Missing and temporarily inaccessible histories both forbid recreation.
    // Lookup uncertainty stays retryable; it is not proof of permanent deletion.
    try {
      await (await binding.get(id)).status();
    } catch {
      throw new Error("WORKFLOW_HISTORY_UNAVAILABLE");
    }
    return;
  }
  try {
    await binding.create({ id, params: parameters });
  } catch {
    try {
      await (await binding.get(id)).status();
    } catch {
      throw new Error("WORKFLOW_CREATE_UNCERTAIN");
    }
  }
}
