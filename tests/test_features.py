import pytest


def test_compute_features_spread_and_rv() -> None:
    pd = pytest.importorskip("pandas")

    from pmkt.data.features import compute_features

    data = [
        {"ts": 0, "token_id": "t1", "mid": 0.4, "spread_bps": 100.0},
        {"ts": 20, "token_id": "t1", "mid": 0.6, "spread_bps": 100.0},
        {"ts": 40, "token_id": "t1", "mid": 0.5, "spread_bps": 100.0},
        {"ts": 0, "token_id": "t2", "mid": 0.5, "spread_bps": 50.0},
        {"ts": 20, "token_id": "t2", "mid": 0.5, "spread_bps": 50.0},
        {"ts": 40, "token_id": "t2", "mid": 0.5, "spread_bps": 50.0},
    ]
    df = pd.DataFrame(data)

    features = compute_features(df, bar_seconds=60, window_seconds=60)

    t1 = features[features["token_id"] == "t1"].iloc[0]
    t2 = features[features["token_id"] == "t2"].iloc[0]

    assert t1["spread_bps_mean"] == pytest.approx(100.0)
    assert t2["spread_bps_mean"] == pytest.approx(50.0)
    assert t1["rv"] > 0
    assert t2["rv"] == pytest.approx(0.0)



def test_default_feature_windows_keep_minute_grain_and_rolling_context() -> None:
    pd = pytest.importorskip("pandas")
    from pmkt.data.features import compute_features

    frame = pd.DataFrame({
        "ts": [0, 20, 60, 600],
        "token_id": ["t1"] * 4,
        "mid": [0.4, 0.6, 0.5, 0.7],
        "spread_bps": [100.0] * 4,
    })
    default = compute_features(frame)
    assert default["n_obs"].tolist() == [2, 1, 1]
    assert default.iloc[1]["rv"] > 0
    # The final 600 s window retains all three returns; a 60 s window has one.
    assert default.iloc[-1]["rv"] == pytest.approx(0.5821755410922375)
    override = compute_features(frame, bar_seconds=120, window_seconds=30)
    assert override["n_obs"].tolist() == [3, 1]
    assert override.iloc[0]["rv"] == pytest.approx(0.0)
