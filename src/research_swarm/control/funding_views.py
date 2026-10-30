"""Bounded funding views; ancestor totals already contain descendant consumption."""

from __future__ import annotations

from research_swarm.domain.funding import CampaignFunding, FundingProjection
from research_swarm.state.database import JsonObject, Transaction


def funding_context(transaction: Transaction, campaign_id: str) -> JsonObject | None:
    own = transaction.connection.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind='campaign_funding' AND id=?",
        (transaction.workspace_id, campaign_id),
    ).fetchone()
    counts = transaction.connection.execute(
        "SELECT state,COUNT(*) AS count FROM records WHERE workspace_id=? "
        "AND kind='funding_projection' AND parent_id=? GROUP BY state",
        (transaction.workspace_id, campaign_id),
    ).fetchall()
    if own is None and not counts:
        return None
    incoming = None
    if own is not None:
        grant = CampaignFunding.model_validate(
            transaction.get("campaign_funding", campaign_id)["data"]
        )
        row = transaction.get("funding_projection", campaign_id)
        projection = FundingProjection.model_validate(row["data"])
        incoming = {
            "funding_id": campaign_id,
            "parent_campaign_id": grant.parent_campaign_id,
            "root_campaign_id": grant.root_campaign_id,
            "depth": grant.depth,
            "remaining_depth": grant.remaining_depth,
            "state": row["state"],
            "allocation_revision": row["revision"],
            "balances": {
                name: balance.model_dump(mode="json")
                for name, balance in sorted(projection.balances.items())[:16]
            },
            "dimension_count": len(projection.balances),
            "settlement_id": campaign_id if row["state"] == "settled" else None,
        }
    rows = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='funding_projection' "
        "AND parent_id=? ORDER BY rowid DESC LIMIT 8",
        (transaction.workspace_id, campaign_id),
    ).fetchall()
    children = []
    for item in rows:
        child = transaction.get("campaign", item["id"])
        allocation = transaction.get("funding_projection", item["id"])
        children.append(
            {
                "child_campaign_id": child["id"],
                "campaign_state": child["state"],
                "allocation_state": allocation["state"],
                "allocation_revision": allocation["revision"],
                "settlement_id": child["id"] if allocation["state"] == "settled" else None,
            }
        )
    return {
        "incoming": incoming,
        "children": children,
        "child_count": sum(item["count"] for item in counts),
        "open_child_count": sum(item["count"] for item in counts if item["state"] == "open"),
        "budget_totals_include_descendants": True,
        "children_query": {"kind": "campaign_funding", "parent_id": campaign_id},
    }
