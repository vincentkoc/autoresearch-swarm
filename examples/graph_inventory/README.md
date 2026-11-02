# Synthetic research atlas

Generate a populated offline graph without any private research data:

```bash
uv run python examples/graph_inventory/build.py
```

Open `artifacts/graph-inventory/index.html` in a browser. The adjacent
`inventory.json` contains the reproducible source data.

The atlas has 600 records across six research areas: indexes, report groups,
branching trials, configurations and hypotheses. Its 2,028 links include 234
illustrative parent edges and 1,794 non-causal references. All records are marked
synthetic; scores and source URLs are deliberately absent. Neither these trial
families nor their links describe actual research.

Connected clusters is the default. Select Symmetric ring for the README example,
or try Category groups and Spread-out force. Selection, search and the inspector
work on the same data in every layout.

The browser smoke script regenerates this atlas and captures the README view:

```bash
node tests/visual/experiment-graph-smoke.mjs
```

Its screenshot is `artifacts/experiment-graph/graph-ring-example.png`.
