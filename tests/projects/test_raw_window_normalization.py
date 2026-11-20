import json

import pytest


def test_generated_user_reference_normalizes_each_channel_after_reload(tmp_path):
    torch = pytest.importorskip("torch")
    from examples.raw_window_normalization.normalization import apply, fit, reference_sha256

    reference = torch.tensor([[[1.0, 1.0], [7.0, 7.0]], [[3.0, 3.0], [7.0, 7.0]]])
    window = torch.tensor([[[5.0, 1.0], [8.0, 7.0]]])
    path = tmp_path / "user-a.json"
    expected = reference_sha256(reference)
    assert expected == "35f2c4207d569068c08429a9ca100feba6773df99246f7013d10a80bd343789f"
    path.write_bytes(fit("user-a", reference))
    assert json.loads(path.read_bytes())["reference_sha256"] == expected
    output = apply("user-a", window, path.read_bytes(), expected_reference_sha256=expected)
    assert output.tolist() == [[[3.0, -1.0], [1.0, 0.0]]]
    with pytest.raises(ValueError, match="different user"):
        apply("user-b", window, path.read_bytes(), expected_reference_sha256=expected)
    with pytest.raises(ValueError, match="channel layout"):
        apply("user-a", window[:, :1], path.read_bytes(), expected_reference_sha256=expected)


@pytest.mark.parametrize(
    ("selected", "saved", "error"),
    [
        ("A", "A", None),
        ("B", "A", "different reference recording"),
        ("A", None, "unverified reference provenance"),
        (None, "A", "missing or invalid expected reference fingerprint"),
    ],
)
def test_apply_checks_reference_metadata_before_transform(selected, saved, error, monkeypatch):
    torch = pytest.importorskip("torch")
    from examples.raw_window_normalization.normalization import apply, reference_sha256

    references = {name: torch.tensor([[[sample]]]) for name, sample in [("A", 1.0), ("B", 2.0)]}
    expected = reference_sha256(references[selected]) if selected else None
    state = {"user": "same-user", "mean": [1.0], "scale": [1.0]}
    if saved:
        state["reference_sha256"] = reference_sha256(references[saved])
    fitted = json.dumps(state).encode()
    window = torch.tensor([[[3.0]]])
    if error is None:
        assert apply("same-user", window, fitted, expected_reference_sha256=expected).item() == 2.0
        return

    def unexpected_stat_read(*args, **kwargs):
        pytest.fail("transform accessed channel statistics before validating provenance")

    monkeypatch.setattr(torch.Tensor, "new_tensor", unexpected_stat_read)
    with pytest.raises(ValueError, match=error):
        apply("same-user", window, fitted, expected_reference_sha256=expected)
