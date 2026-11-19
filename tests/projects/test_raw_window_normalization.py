import pytest


def test_generated_user_reference_normalizes_each_channel_after_reload(tmp_path):
    torch = pytest.importorskip("torch")
    from examples.raw_window_normalization.normalization import apply, fit

    reference = torch.tensor([[[1.0, 1.0], [7.0, 7.0]], [[3.0, 3.0], [7.0, 7.0]]])
    window = torch.tensor([[[5.0, 1.0], [8.0, 7.0]]])
    path = tmp_path / "user-a.json"
    path.write_bytes(fit("user-a", reference))
    output = apply("user-a", window, path.read_bytes())
    assert output.tolist() == [[[3.0, -1.0], [1.0, 0.0]]]
    with pytest.raises(ValueError, match="different user"):
        apply("user-b", window, path.read_bytes())
    with pytest.raises(ValueError, match="channel layout"):
        apply("user-a", window[:, :1], path.read_bytes())
