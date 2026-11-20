# Research HTTP API

`research serve` reads a local campaign database. Pass `--token-file` before
`serve` and `--db` after it. The service binds to `127.0.0.1:8765` by default.
The root path serves the campaign browser shell. A successful `GET /healthz`
response confirms only that the HTTP service answered. Both endpoints are public.
Campaign data requires the read bearer token.

If an external supervisor manages the scheduler, consult it for the scheduler's
process status.

## Campaign reads

| GET route | Result |
| --- | --- |
| `/v1/campaigns` | Campaign list with `cursor` and `limit` pagination |
| `/v1/campaigns/{id}/snapshot` | Captured trial history, scores and selection |
| `/v1/campaigns/{id}/view` | Standalone campaign HTML |
| `/v1/campaigns/{id}/budget` | Measured usage and active dispatch estimates |
| `/v1/events` | Events after a sequence number, optionally filtered by `campaign_id` |

The CLI reads the same data with `report`, `budget` and `events`. Supply
`--controller URL --token-file FILE` before the command for HTTP access.
The Python SDK exposes `campaigns`, `snapshot`, `budget` and `events` through
`ResearchClient` and `AsyncResearchClient`.

## Worker access

Enable worker routes on `serve` with `--worker-campaign`, `--worker-id` and a
separate `--worker-token-file`. The worker uses its bearer to claim an attempt
or requeue expired attempts. A successful claim returns a bearer scoped to that
attempt for heartbeats, result reporting and artifact upload. The CLI handles
this exchange with `research --controller URL --token-file FILE swarm worker
SPEC --worker-id ID`.

The retired command-service protocol is documented with its matching
[controller revision](https://github.com/vincentkoc/autoresearch-swarm/blob/4c4a10ac104243e302bc3eb63b7150f9d9422e99/docs/api.md).
