# Per-user normalization of generated raw windows

The example fits one mean and population standard deviation per channel from a
user's unlabeled reference windows. It uses a scale of one for constant channels.
Supply floating-point tensors shaped `[window, channel, time]`. Keep the same
channel order at fit and inference.

Run this example in a Python environment with the project's optional PyTorch extra.
The reference windows represent a separate, earlier recording for the generated user.

```python
from pathlib import Path

import torch

from examples.raw_window_normalization.normalization import fit

user = "generated-user"
reference = torch.tensor([[[1.0, 3.0], [7.0, 7.0]]])
state_file = Path("generated-user-normalization.json")
state_file.write_bytes(fit(user, reference))
```

At either fidelity, including after a controller restart, read the previously fitted state.

```python
from pathlib import Path

import torch

from examples.raw_window_normalization.normalization import apply

user = "generated-user"
state_file = Path("generated-user-normalization.json")
window = torch.tensor([[[5.0], [8.0]]])
normalized = apply(user, window, state_file.read_bytes())
assert normalized.tolist() == [[[3.0], [1.0]]]
```

Prepare the state once per user before scoring. Include its exact bytes as a
protected workload input for every rung. Apply it before prediction, then use
held-out targets to score predictions. The workload defines which recording is
available as reference data at evaluation time.
