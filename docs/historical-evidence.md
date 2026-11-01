# Historical evidence import

`evidence.import_historical` retains records from an existing research project
without pretending those runs executed under current engine authority. The command
requires `evidence.import` and `read`. It is available through HTTP, Python, CLI,
and MCP; it does not start a campaign or worker.

Register the ordinary project first. Upload the original metadata file through
the artifact API, preserving its exact bytes. A project-local normalizer can then
submit the generic envelope:

```python
from research_swarm.control.import_models import (
    HistoricalObservation,
    ImportHistoricalEvidence,
)

request = ImportHistoricalEvidence(
    project_id=project_id,
    source_system="existing-lab",
    source_schema="endpoint-v2",
    source_record_id="original-trial-17",
    raw_artifact=uploaded_metadata,
    normalizer="project-normalizer-v1",
    source_status="unknown",
    comparison_identifiers={"panel": "search", "units": "radians"},
    observations=(
        HistoricalObservation(
            name="pose_mae",
            unit="radians",
            missing_reason="No terminal endpoint was preserved",
        ),
    ),
    provenance_findings=("Original execution closure is unresolved",),
)
ack = client.command("evidence.import_historical", request, idempotency_key="import-trial-17")
```

The engine verifies and retains the exact raw artifact generation. It records a
separate domain-separated digest for the normalized import manifest. These are
different identities: source bytes are not silently reserialized into canonical
JSON. The raw file can retain publisher fields, original event chains, contradictory
closure observations, and missing costs that a compact normalized view omits.

Each import belongs to a project, with no current campaign ID. Query
`imported_evidence` using `parent_id=project_id`. Summaries show at most eight
observations; request the full record for the remaining manifest. A metadata file
is limited to 1 MiB, the normalized manifest to 64 KiB, and observations to 64.
Large checkpoints and datasets use their normal project artifact paths rather
than this metadata importer.

The immutable identity includes the project, source system, source schema, and
original record ID. Identical concurrent imports return one record, even with
different delivery keys. Changed raw content, normalized observations, comparison
identity, or normalizer under the same identity conflicts. Preserve the exact
command key and request when recovering a missing acknowledgment. Accepted replay
does not require reopening the raw artifact.

All imported records explicitly carry `promotion_eligible=false`, unverified
source authenticity, and no execution or budget authority. A project normalizer's
claims do not authenticate historical evaluator signatures or native closure.
Imported IDs cannot be used as current evaluator receipts. An archived success
cannot settle a current attempt, release its reservation, or grant new exposure.

Missing measurements remain null with a reason. Do not replace them with zero.
Keep panel, units, coverage, and evaluator/checkpoint identifiers distinct. For
pose-estimation records, preserve sufficient statistics in raw evidence. Keep
macro person MAE separate from pooled MAE. The project's normalizer owns that
interpretation and its metric formulas.

Import and fresh replay establish different claims. A disposable ordinary project
may replay a compatible recipe and compare receipt identities and statistics.
Only fresh authorized execution/evaluation can enter a new campaign's promotion
contract. Metadata import alone proves neither replay equivalence nor improvement.

The [ordinary-project adoption guide](project-adoption.md) maps this sequence to
an existing research repository. Its public synthetic pose example qualifies
receipt replay and denominator handling without a project-specific engine plugin.
