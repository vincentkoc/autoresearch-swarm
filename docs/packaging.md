# Installed-package qualification

The development checkout is not the installed product. `test_packaged_release.py`
builds a wheel and installs it into a fresh virtual environment with runtime
versions constrained by the existing `uv.lock`. It starts from a directory outside
the source checkout and strips inherited Python import-path settings.

The proof checks:

1. The isolated interpreter imports `research_swarm` from the new installation.
   Distribution metadata identifies a wheel, not an editable source directory.
2. A core install does not contain Temporal, PyTorch, or MLX.
3. The installed `research` entry point captures an ordinary copied CPU project,
   runs both native stages, and obtains an authenticated evaluation receipt.
4. An MCP subprocess launched by the installed interpreter discovers the API,
   selects the observed baseline, and creates the final report.
5. After the original project directory is removed, the same run key resumes
   from durable state with zero additional native attempts.

The wheel, environment, synthetic project, registry, and runtime live in one
temporary fixture directory. The fixture removes it when the check exits. No
host service or remote worker is installed.

Run this proof explicitly:

```sh
RESEARCH_PACKAGE_TESTS=1 uv run pytest -q tests/test_packaged_release.py
```

It requires uv and may download the build backend or missing locked runtime
wheels into the ordinary package cache. The regular unit suite skips this test;
dedicated CI jobs run it on Linux, macOS, and Windows with Python 3.12. The broader
source contract matrix covers the declared Python versions separately.

This establishes packaging and a local CPU path. It does not qualify accelerator
extras, physical remote workers, upstream numerical reproduction, or a hosted
deployment. Publishing a package or release remains a separate action.
