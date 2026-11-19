---
name: run-a-campaign
description: Run or resume an Autoresearch Swarm campaign from a YAML spec and interpret the selected candidate and held-out decision.
---

Read the [campaign guide](../../../docs/run-a-campaign.md) from the repository
checkout. Use the requested task, device, panel split and budget. For a local
interface check, run the included program campaign. Before a real acceptance
run, stop if the authentic inputs or the owner's explicit budget are missing.

To stop a foreground local run, press Ctrl-C and inspect the saved queue.
Resume with the same campaign ID, spec and queue path. For a calibrated
campaign, reuse its original panel study.
If the process stopped during calibration, inspect the original study before
invoking the run command again. The read-only query also shows each stored
held-out decision. [Operator guide and panel recovery check](../../../docs/operator-guide.md#inspect-the-panel-study)

When the loop finishes, report the stop reason, submitted trials, failed or
lost attempts and selected candidate from the CLI summary. If interrupted
before a summary, report the exit status and saved queue state. Use the
queue status counts for completed trials. Report the search score and the
measured `confirmed_score` separately.
Use the operator guide to diagnose a failed attempt or a campaign that
cannot dispatch.
