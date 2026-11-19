---
name: run-a-campaign
description: Run or resume an Autoresearch Swarm campaign from a YAML spec and interpret the selected candidate and held-out decision.
---

Read the [campaign guide](../../../docs/run-a-campaign.md) from the repository
checkout. Use the requested task, device, panel split and budget. For a local
interface check, run the included program campaign. Before a real acceptance
run, stop if the authentic inputs or the owner's explicit budget are missing.

Preserve the campaign ID, spec, queue and panel history when resuming.
If the process stopped during calibration, inspect the original study before
invoking the run command again. The read-only query also shows each stored
held-out decision. [Operator guide and panel recovery check](../../../docs/operator-guide.md#inspect-the-panel-study)

Report the stop reason, submitted trials, failed or lost attempts and selected
candidate from the CLI summary. Use the queue status counts for completed
trials. Report the search score and the measured `confirmed_score` separately.
Use the operator guide to diagnose a failed attempt or a campaign that
cannot dispatch.
