# Campaign control and reports

The SDK, HTTP API, CLI, and MCP expose the same lifecycle commands.
Closing a client does not complete a campaign or prove that native work stopped.

## Pause and resume

Call `campaign.pause` with the campaign's current revision and `mode="drain"`.
The controller stops admission for new trials. Already active trials can finish
their remaining stages. The campaign becomes paused when those trials settle.

A queued trial remains queued. Uncertain execution keeps the campaign pausing.
The pause command does not infer process termination or refund reservations.
Checkpoint and terminate pause modes are not qualified yet.

Use `campaign.start` with the paused campaign's current revision to resume admission.
Evaluate completed outputs through the normal evaluator commands, including while paused.

## Completion

Call `campaign.complete` on a running campaign with its current revision and a reason.
Completion requires all submitted trials to settle and close their resource claims.
Successful execution must have an evaluation outcome. Reserved or uncertain budgets
prevent completion, including unresolved evaluation exposure.

The stopping contract can allow explicit manual completion. Otherwise, the campaign
must reach its trial count or selected metric target. A target uses the mean primary
metric across the selected candidate's supporting replicas. It uses the metric's
declared direction and requires eligible evidence.

The completion transaction updates the campaign, records its event, and creates the
final report. A report failure rolls back completion. The report records the final
campaign revision, stopping reason, and evidence watermark.

Completion freezes ordinary incumbent selection. Replays of accepted commands still
return their original acknowledgment. Evidence corrections remain allowed. They
preserve history and can suspend a completed campaign's incumbent eligibility.

## Reports

Use `report.create` to capture any campaign without changing its lifecycle.
The report contains the same evidence in structured JSON and Markdown:

- Comparison protocol and primary metric.
- Incumbent identity, evidence level, and eligibility.
- Counts of recorded outcomes, including failures.
- Consumed, reserved, and uncertain budgets.
- A bounded sample of recent receipts and their current eligibility.
- Total evaluation count and query fields for retrieving further evidence.

`recent_limit` defaults to 20 and cannot exceed 50. The sample is chronological,
not a cross-hardware leaderboard. Raw measurements retain their receipt identities.
Untrusted labels remain literal text in Markdown.

Each report has a digest and remains immutable. Later corrections do not rewrite
old reports. Generate a new report or inspect correction events for current eligibility.
An observed baseline is not confirmed improvement. A confirmed seed gate does not
establish generalization to an untouched population.
