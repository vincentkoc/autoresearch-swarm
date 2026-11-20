# Per-user normalization of generated raw windows

The example fits one mean and population standard deviation per channel from a
user's unlabeled reference windows. It uses a scale of one for constant channels.
Supply floating-point tensors shaped `[window, channel, time]`. Keep the same
channel order at fit and inference.

Run this example in a Python environment with the project's optional PyTorch extra.
The reference windows represent a separate, earlier recording for the generated user.
When selecting that recording, record its fingerprint in the protected dataset manifest.

```python
import json
from pathlib import Path

import torch

from examples.raw_window_normalization.normalization import fit, reference_sha256

user = "generated-user"
reference = torch.tensor([[[1.0, 3.0], [7.0, 7.0]]])
manifest_file = Path("dataset.json")
manifest = {"reference_sha256_by_user": {user: reference_sha256(reference)}}
manifest_file.write_text(json.dumps(manifest), encoding="utf-8")

state_file = Path("generated-user-normalization.json")
state_file.write_bytes(fit(user, reference))
```

At either fidelity, including after a controller restart, read the previously fitted state.

```python
import json
from pathlib import Path

import torch

from examples.raw_window_normalization.normalization import apply

user = "generated-user"
state_file = Path("generated-user-normalization.json")
window = torch.tensor([[[5.0], [8.0]]])
manifest = json.loads(Path("dataset.json").read_bytes())
expected = manifest["reference_sha256_by_user"][user]
normalized = apply(user, window, state_file.read_bytes(), expected_reference_sha256=expected)
assert normalized.tolist() == [[[3.0], [1.0]]]
```

For an existing dataset manifest, add the per-user fingerprints alongside its
split declarations. Set `data.manifest_path` to that file and include the fitted
state files in `evaluation.protected_paths`. Prepare the state once per user.
`fit` fingerprints the windows it actually uses. `apply` compares that fingerprint
with the selected recording's fingerprint before using the saved statistics.
SHA-256 covers a versioned header with the dtype and shape, followed by the
samples in window, channel and time order encoded as little-endian bytes.

Historical JSON receipts remain readable. A state without `reference_sha256` has
unverified provenance. Applying it requires preparing fresh state from the
selected recording. Apply verified state before prediction, then use held-out
targets to score predictions. The workload defines which recording is available
as reference data at evaluation time.
