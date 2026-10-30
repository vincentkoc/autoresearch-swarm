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

## Scientific completion with outstanding resources

Use `campaign.complete_scientific` when the research conclusion is final while
original resource custody remains unresolved. This is an explicit alternative to
strict completion. It does not change `campaign.complete` or infer that a process
has stopped.

First call `campaign.capture_obligations` with a fresh `review_id`, the campaign ID,
and its current revision. The immutable review references bounded chunks containing
unresolved attempts, jobs, trials, reservations, budget accounts, and open child
funding. Read the chunks before making the decision. Each entry records its identity
and revision or exact accounting value; the root records the full manifest digest.
The local profile limits a review to 16,384 entries and 2 MiB of encoded entries.
An oversized review fails atomically.

Then pass that review ID as `obligations_id`, the same campaign revision, and a reason
to `campaign.complete_scientific`. A changed obligation or campaign revision rejects
the decision. Preserve the exact request and key when retrying a lost reply.

Scientific completion:

- Applies the campaign's existing stopping contract.
- Refuses known active attempts that have not drained. Recorded UNKNOWN attempts
  remain admitted under their original custody and reservations.
- Requires child research to be terminal with a final report, but permits its
  unresolved funding allocation to remain open.
- Stops jobs that have no attempt. Only provably unused evaluation exposure is released.
- Freezes the selected candidate, scientific-completion receipt, and final report
  in one transaction. The report says `reconciliation_pending`.
- Retains original accounting, evidence, and funding obligations. It does not refund
  uncertain execution or admit replacement work.

New trials, child campaigns, evaluation authorizations, restarts, and incumbent
promotion remain forbidden. An already-authorized evaluator may complete the
receipt for an original successful scoring attempt. An original attempt captured
by the review may submit evidence for explicit `attempt.reconcile`, including after
a controller epoch change. Reconciliation validates the same native identity,
artifacts, and custody as before. It can record the original result and settle costs,
but cannot start downstream jobs or change the frozen incumbent. When scientific
output is unusable, `attempt.reconcile_closure` preserves `closed_unaccepted`.

Once the original obligations settle, capture a **fresh** review and call
`campaign.close_resources` with its ID and current campaign revision. Closure requires
all attempts, jobs, trials, reservations, uncertainty, occupied capacity, and child
allocations to be resolved. It records one immutable resource-closure receipt and
settles a funded campaign's remaining parent allocation in the same transaction.
Concurrent delivery and replay cannot release the allocation twice.

The original final report and its ID never change. Current context and supplementary
reports expose the closure receipt and `closed` operational status. Artifact retention
and deletion remain separate decisions; they do not block either completion operation.
All three commands use `campaign.manage` through HTTP, Python, CLI, and MCP. Domain
events let orchestration adapters observe them without provider-specific authority.

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
