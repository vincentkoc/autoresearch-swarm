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
uv run pytest tests/test_workload.py -q
~~~

The native command reads the file named by `RESEARCH_CONTEXT`. The context supplies
its seed, budget, named inputs, output directory, and report path. The project does
not import or depend on the research engine. See
[the workload protocol](../../docs/workload-protocol.md) for the interface.

`research_mlx.toml` selects the native Metal implementation in `train_mlx.py` and
reuses this dataset and scorer. See [the MLX workload guide](../../docs/mlx-workload.md)
for its hash-locked environment, registered worker, explicit GPU timing, and
separate comparison contract. The CPU path still needs no third-party packages.
