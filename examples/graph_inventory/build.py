"""Build an entirely synthetic research inventory and its offline graph."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from research_swarm.control.experiment_graph import ExperimentGraph, render_experiment_graph_html

CATEGORIES = (
    "Architecture",
    "Data pipeline",
    "Optimization",
    "Evaluation",
    "Robustness",
    "Deployment",
)


def build_inventory() -> ExperimentGraph:
    nodes = []
    edges = []
    for category_index, category in enumerate(CATEGORIES):
        prefix = f"area-{category_index}"
        for i in range(100):
            kind = (
                "research index"
                if i == 0
                else "report group"
                if i < 20
                else "trial"
                if i < 60
                else "configuration"
                if i < 80
                else "hypothesis"
            )
            nodes.append(
                {
                    "id": f"{prefix}-{i:03d}",
                    "label": f"{category} · {kind} {i:02d}",
                    "cluster": category,
                    "status": "synthetic",
                    "node_type": kind,
                    "summary": (
                        f"Illustrative {kind.lower()} in {category.lower()}. "
                        "Generated demo data, not a real experiment or measurement."
                    ),
                    "source_url": None,
                }
            )
            if i:
                edges.append(
                    {
                        "source": f"{prefix}-000",
                        "target": f"{prefix}-{i:03d}",
                        "kind": "source-reference",
                        "label": "Inventory membership · synthetic",
                    }
                )
            if 20 <= i < 60:
                if i > 20:
                    edges.append(
                        {
                            "source": f"{prefix}-{20 + (i - 21) // 2:03d}",
                            "target": f"{prefix}-{i:03d}",
                            "kind": "causal",
                            "label": "Illustrative parent · synthetic",
                        }
                    )
                for target in (1 + (i - 20) % 19, 60 + i % 20, 80 + i % 20):
                    edges.append(
                        {
                            "source": f"{prefix}-{i:03d}",
                            "target": f"{prefix}-{target:03d}",
                            "kind": "source-reference",
                            "label": "Illustrative supporting record",
                        }
                    )
            if i >= 60:
                edges.append(
                    {
                        "source": f"{prefix}-{i:03d}",
                        "target": f"{prefix}-{1 + i % 19:03d}",
                        "kind": "source-reference",
                        "label": "Illustrative report association",
                    }
                )
                edges.append(
                    {
                        "source": f"{prefix}-{i:03d}",
                        "target": f"area-{(category_index + 1) % 6}-{1 + i % 19:03d}",
                        "kind": "source-reference",
                        "label": "Illustrative cross-area reference",
                    }
                )
    return ExperimentGraph.model_validate(
        {
            "captured_at": "2026-01-01T00:00:00Z",
            "source_revision": "synthetic-research-atlas-v1",
            "note": (
                "Entirely synthetic: 600 records, six research areas, branching trial "
                "families and cross-area references. No measured scores or private data. "
                "References and category proximity do not assert ancestry."
            ),
            "nodes": nodes,
            "edges": edges,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/graph-inventory"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    graph = build_inventory()
    (args.output / "inventory.json").write_text(
        json.dumps(graph.model_dump(), indent=2) + "\n", encoding="utf-8"
    )
    (args.output / "index.html").write_text(
        render_experiment_graph_html(graph, title="Swarm · synthetic research atlas"),
        encoding="utf-8",
    )
    print(f"{len(graph.nodes)} synthetic records / {len(graph.edges)} links → {args.output}")


if __name__ == "__main__":
    main()
