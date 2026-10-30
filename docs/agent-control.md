# CLI and agent control

The CLI, Python SDK, HTTP API, and MCP tools call the same command service.
Each mutation has an idempotency key. Revision-sensitive commands also require
the revision returned by the latest read. An accepted command is a durable
acknowledgment; it does not mean that an experiment has finished.

## Start locally

~~~sh
uv sync --group dev
uv run research init
uv run research capabilities
uv run research project register examples/cpu_bigram --key bigram-source-v1
~~~

The default private state directory is `~/.local/state/autoresearch-swarm`.
Use `research --state-dir PATH ...` to select another runtime. Initialization
preserves its identity and credentials across invocations.

Send a command as a JSON file or through stdin:

~~~sh
uv run research command campaign.create --file campaign.json --key campaign-v1
uv run research command campaign.seal --file target.json --key seal-v1 --revision 0
uv run research query campaign --limit 10
uv run research context CAMPAIGN_ID
uv run research budget CAMPAIGN_ID
uv run research events --after 0 --limit 50
~~~

`campaign.json` uses the `campaign.create` request schema returned by capabilities.
`target.json` contains `{"campaign_id":"CAMPAIGN_ID"}` with the actual UUID.
Use the actual campaign revision for seal. All successful output is JSON on
stdout. Domain errors are JSON on stderr with exit status 2. Parser usage errors
use argparse's ordinary stderr format. The CLI never prints its credential.

## Expose agent tools

~~~sh
uv run research mcp
~~~

This command serves MCP over stdio. Configure an MCP client to launch it with the
repository as its working directory. For clients that require an absolute project
location, use `uv run --project /path/to/autoresearch-swarm research mcp`.
Do not use the MCP stdout channel for shell banners or logging.

The tool catalog includes typed commands for project registration, campaign
creation/validation/sealing/start, candidate proposal/validation, and trial
submission. Query tools expose capabilities, bounded campaign context, records,
budgets, and events. Command results include the durable command ID. Structured
errors retain the domain error code and retry information.

The local `research_capture_project` tool reads only manifest-selected source
and requires `source.capture` authority. Registration remains a separate command
that accepts the resulting immutable ArtifactRef. Capture preserves the selected
Git revision or declared working files; it does not change the research checkout.

Tools return summaries by default and cap replies at 128 KiB. Use cursors,
smaller pages, or the HTTP API for large details and artifact transfer. A tool
result bound does not imply that the campaign's full history fits in one prompt.

## Remote control

~~~sh
uv run research serve --host 127.0.0.1 --port 8765
uv run research --controller https://controller.example.invalid --token-file PATH capabilities
uv run research --controller https://controller.example.invalid --token-file PATH mcp
~~~

The server requires bearer authentication. The remote CLI reads a private token
file; raw secrets do not belong in argv. Remote MCP forwards the same API commands
and does not expose local source capture. Use a scoped credential for an agent.
Revocation takes effect on the next operation, without operator-token fallback.

The stdio server and CLI are clients of durable state. Closing them does not
cancel an accepted campaign. This implementation currently queues trial graphs;
the integrated worker execution service is still pending.

Real subprocess tests exercise stdio discovery and tool calls, CLI JSON input,
cross-transport replay, populated context/query output, conflict responses, scoped
capture refusal, and credential revocation during a live MCP connection.
