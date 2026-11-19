# Qualify a campaign backend on GPUs

## Rehearse locally

From the repository root with development dependencies installed, run:

```sh
uv run --locked --offline python -m pytest -q tests/swarm/test_backend_qualification.py
```

| Backend | Injected condition | Expected evidence |
| --- | --- | --- |
| Local | Source data changes after staging | The CPU subprocess reads the immutable revision and selects score 13. |
| SSH pool | The first SSH connection exits with status 255 | The protected synthetic host starts once. The queue records a lost attempt and retries the same trial. |
| SkyPilot | The first managed spot job reports `RECOVERING` | The backend cancels that job, submits an on-demand attempt and reads its metric from the job log. |

Each case runs the public campaign CLI twice against the same SQLite queue.
The workload reads a verified worker copy and executes successfully once.
GC lists the campaign revision, removes it on apply, and retains the named
keep revision and another campaign's data. The Sky case also uses the submitted
job labels to collect a synthetic instance and volume. An untagged host survives.
Provider commands, network transport and privileged binds are simulated. Hardware
timing, GPU telemetry, Linux mount enforcement and provider billing require a live run.

## Before a hardware run

- [ ] Record the operator, approved account, hosts or placements, allowed failure
  injection, spend ceiling and stop time. Include the possible on-demand retry
  and idle storage in the budget.
- [ ] Preserve the Git revision, dependency lock, worker image, driver and GPU
  model, campaign YAML, queue path and provider inventory path. Set
  `resource: gpu`, a trial timeout, finite trial and worker limits, and
  `max_attempts` of at least two for recovery. Record prices and `max_cost`.
- [ ] Choose a workload that exercises the GPU long enough to measure busy time.
  Have it log the actual device, UTC start time, trial ID and attempt ID and emit
  a finite metric. Verify clocks across the controller, workers and telemetry.
- [ ] Prepare a named dataset revision once per location. Record the digest,
  file count and byte count from `stage.json`. Confirm each worker reads
  `SWARM_DATA` from its local copy. On Linux verify the worker mount has the
  `ro` flag and a write by the workload account fails.

## Run each backend

| Backend | What to verify |
| --- | --- |
| Local | Set `options.backend.device` to the intended CUDA or MPS device and `data_dir` to the prepared data. Confirm the workload uses hardware acceleration. Measure peak memory per fit before raising concurrency. |
| SSH pool | Use enrolled dedicated hosts with a matching workload checkout, mounted regional storage, local NVMe and an active keeper. Check termination protection and shutdown behavior before dispatch. An idle host should stop after its configured interval and keep its instance identity when it resumes. |
| SkyPilot | Set the accelerator, explicit placements and a durable backend state directory. Verify matching campaign and job labels on compute and storage. During the first spot attempt, induce the approved interruption and observe autonomous recovery. Record both managed job IDs, placements and attempt IDs. |

## Record the acceptance decision

| Check | Measurement and passing result |
| --- | --- |
| Time to first trial | Start the clock when the operator starts the cold campaign workflow, including required preparation and provisioning. Stop at the first workload start on the GPU. Elapsed time must be **under 30 minutes**. Record storage and model cache state. |
| GPU utilization | Sample busy percentage for each allocated GPU at a fixed interval. Divide summed busy GPU-seconds by summed allocated GPU-seconds across the run. Include idle, staging and retry intervals while devices are allocated. The result must be **above 80%**. Report gaps in telemetry and repeat if they prevent a decision. |
| Spot recovery | Match the interrupted trial ID, candidate, seed and fidelity to a distinct successful retry attempt. Verify one accepted result and the same selection after resume. **No operator redispatch** is allowed. |
| Instance lifecycle | Compare provider lifecycle events with job IDs, timestamps and the GC log. The swarm must initiate **zero instance terminations outside GC**. Record provider-initiated spot reclamation separately and verify the borrowed pool remains intact. |

List resources using the same inventory root and region volumes as the campaign:

```sh
research --state-dir "$STATE" campaign gc "$CAMPAIGN" --volume "$VOLUME"
```

- [ ] Match every listed job, instance, volume and revision to the run. Repeat
  `--volume` for each staged root. Before applying, resolve unexpected resources
  and specify `--keep` for retained resource IDs or `data:<revision>`.
- [ ] Apply the reviewed plan with the same arguments and `--apply`. List again.
  Reconcile the actions with provider inventory and billing, including stopped
  hosts and retained storage. Verify kept and foreign resources still exist.
- [ ] Attach the UTC timeline, queue summary and attempts, stage digests, device
  samples, provider lifecycle events and GC outputs. Record pass, fail or
  unmeasured for each criterion and backend. Keep live identifiers in the
  operator's private run record.
