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
