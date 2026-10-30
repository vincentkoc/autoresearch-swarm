# Upstream reference workload

Autoresearch Swarm is inspired by [Karpathy's autoresearch](https://github.com/karpathy/autoresearch).

The inspected upstream reference is commit
[228791fb499afffb54b46200aca536f79142f117](https://github.com/karpathy/autoresearch/tree/228791fb499afffb54b46200aca536f79142f117).

That loop uses a five-minute active training budget, edits train.py, preserves
prepare.py, and minimizes validation bits-per-byte.

The inspected upstream root did not include a license file. This project does
not bundle or relicense its source. Our MIT license covers our original code.
The integration operates on a user-supplied upstream checkout and records its
source, dataset, evaluator, environment, and timer trust.

The original CUDA workload requires real qualified hardware and data. A small
CPU example establishes the command contract, not upstream numerical or
performance reproduction.
