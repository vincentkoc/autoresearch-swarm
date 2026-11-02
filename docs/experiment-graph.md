# Offline experiment graph

`research_swarm.control.experiment_graph.render_experiment_graph_html(dataset, title="Swarm · experiment graph")`
returns a standalone, dependency-free HTML inventory. It reads no live state and
does not change native report observation limits. This is a separate inventory
view, not a replacement for qualified score/time reporting.

```python
import json
from pathlib import Path
from research_swarm.control.experiment_graph import render_experiment_graph_html

dataset = json.loads(Path("inventory.json").read_text())
Path("experiment-graph.html").write_text(
    render_experiment_graph_html(dataset), encoding="utf-8"
)
```

The normalized dataset is an object with:

- `nodes`: up to 5,000 objects with unique `id`, `label`, `cluster`, `status`,
  `summary`, `source_url` (string or null), optional finite `score`, and optional
  `node_type` (a display label, not a layout rule).
- `edges`: up to 25,000 objects with `source` and `target` node IDs, `kind`
  (`causal` or `source-reference`), and optional `label`.
- `captured_at` and `source_revision`: displayed source provenance strings.
- Optional `note`: capture-specific limitations and inventory composition.

For source inventories using `parent`, `reference` and `contains` link kinds,
normalize verified `parent` to `causal`, and both `reference` and `contains` to
`source-reference`. Retain the original kind in `label`. Do not infer parents from
containment, names, category membership or source references.

All required text fields are strings. Unknown fields, duplicate IDs, missing
endpoints, unsupported link kinds and oversized captures are rejected, never
silently truncated. Non-HTTPS or non-GitHub source URLs become unavailable links.
Only exact `https://github.com/…` links are clickable. Missing scores stay missing;
scores never control coordinates, color, clustering or ranking.

Causal edges point from parent to child. Selection traverses only explicit
incoming causal edges for ancestry (with cycle protection). Dashed source
references never imply ancestry. Categories are optional layout constraints,
not inferred relationships; grouping creates no edges.

The off-white three-pane workspace includes a paginated searchable inventory,
Canvas force graph, and selection/provenance details. Search and category filters
control visible nodes and edges without modifying the capture. The list reveals
100 rows at a time; every matching node stays on the graph. Selection details
show the first 100 adjacent connections and disclose the full count. Ancestor
counts include filtered nodes. Failed/unscored nodes remain present.

Drag space to pan, scroll or +/− to zoom, Fit (or F on the canvas) to frame matching
nodes. Drag nodes to pin them, double-click or use the detail button to release.
Reset clears pins and restores deterministic initial positions; Pause/Resume
controls simulation. Keyboard users can select inventory items, follow connection
buttons, pan with arrows, zoom with +/− and clear selection with Escape on canvas.
Reduced-motion users start paused, and may explicitly resume.

The renderer uses bounded local spatial-grid repulsion, link springs, category
gravity and cooling; it needs no CDN, npm package, network or WebGL support.
The document uses hash-based CSP and DOM text nodes for capture content.
