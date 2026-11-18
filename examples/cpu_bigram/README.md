# CPU bigram research

This original example trains a character transition language model with stochastic
gradient descent. It requires Python 3.11 or later and no third-party packages.

`train.py` is the mutation surface. `evaluate.py` scores a saved checkpoint on the
fixed synthetic validation panel. `common.py` defines the grammar and JSON command
protocol. `research.toml` declares the protected files, output dependency, metric,
and 300 ms active-training budget.

The model optimizes `val_bpb`, measured in bits per predicted ASCII byte. Lower values
are better. This small synthetic benchmark has its own comparison class. Its scores
do not compare with Karpathy's dataset or tokenizer.

From the repository root, run the actual isolated training/evaluation proof:

~~~sh
uv run pytest tests/projects/test_workload.py -q
~~~

The native command reads the file named by `RESEARCH_CONTEXT`. The context supplies
its seed, budget, named inputs, output directory, and report path. The project does
not import or depend on the research engine. See
[the workload protocol](../../docs/workload-protocol.md) for the interface.

`research_mlx.toml` selects the native Metal implementation in `train_mlx.py` and
reuses this dataset and scorer. See [the MLX workload guide](../../docs/mlx-workload.md)
for its hash-locked environment, registered worker, explicit GPU timing, and
separate comparison contract. The CPU path still needs no third-party packages.

## Run a swarm campaign

~~~sh
uv run research swarm run examples/cpu_bigram/swarm.yaml --db run/swarm.sqlite
~~~

The YAML declares eight smoothing values, eight CPU slots and a validation metric
to minimize. One value causes the task to exit with an error. The other seven
produce measurements. The JSON summary includes the winner, top three candidates
and failed-attempt count. Repeat the command to resume the same campaign.

The task command runs relative to the YAML file's directory (or its `task_dir`).
Read the trial and candidate parameters from the JSON file named by `SWARM_TRIAL`.
Write a JSON object of finite numeric metrics to the file named by `SWARM_METRICS`.
Use `candidates` for explicit recipes, or `search_space` for a Cartesian grid.
The default ranking averages distinct seeds at the highest comparable fidelity.
Use `min_replicates` and `min_delta` to set the evidence required to change the winner.

Trusted Python plugins can supply `proposer`, `selection_policy`, `panel_policy`
or `backend` as `module:factory`. Each factory receives the campaign spec. A
`worker` factory additionally receives the store and backend. Plugin-specific
settings belong in `options`. Declaring evaluation panels requires a panel policy
that calibrates before dispatch and checks a challenger before selection.
