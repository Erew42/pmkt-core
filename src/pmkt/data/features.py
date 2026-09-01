from __future__ import annotations

import math
from typing import Any


def logit(p: Any, eps: float = 1e-6) -> float | None:
    if p is None:
        return None
    try:
        value = float(p)
    except (TypeError, ValueError):
        return None
    value = min(max(value, eps), 1.0 - eps)
    return math.log(value / (1.0 - value))


def compute_features(
    book_df,
    *,
    bar_seconds: int = 60,
    window_seconds: int = 600,
):
    import pandas as pd

    if "ts" not in book_df.columns or "token_id" not in book_df.columns:
        raise ValueError("book_df must include ts and token_id columns")
    if "mid" not in book_df.columns:
        raise ValueError("book_df must include mid column")

    df = book_df.copy()
    if pd.api.types.is_numeric_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True, errors="coerce")
    else:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    if df.empty:
        return pd.DataFrame(
            columns=["bar_start", "token_id", "mid_last", "spread_bps_mean", "rv", "n_obs"]
        )

    df = df.sort_values(["token_id", "ts"])
    df["bar_start"] = df["ts"].dt.floor(f"{int(bar_seconds)}s")
    df["logit_mid"] = df["mid"].apply(logit)
    df["logit_ret"] = df.groupby("token_id")["logit_mid"].diff()

    df = df.set_index("ts")
    rv_series = (
        df.groupby("token_id")["logit_ret"]
        .rolling(f"{int(window_seconds)}s")
        .std(ddof=0)
        .reset_index(level=0, drop=True)
    )
    df["rv"] = rv_series
    df = df.reset_index()

    features = (
        df.groupby(["token_id", "bar_start"], as_index=False)
        .agg(
            mid_last=("mid", "last"),
            spread_bps_mean=("spread_bps", "mean"),
            rv=("rv", "last"),
            n_obs=("mid", "size"),
        )
        .sort_values(["token_id", "bar_start"])
    )
    return features


def join_market_metadata(features_df, markets_df):
    import pandas as pd

    if "token_ids" not in markets_df.columns:
        return features_df

    cols = ["token_ids", "slug", "question", "volume", "liquidity", "closed"]
    cols = [col for col in cols if col in markets_df.columns]
    meta = markets_df[cols].copy()
    meta = meta.explode("token_ids").rename(columns={"token_ids": "token_id"})
    meta = meta.dropna(subset=["token_id"])
    meta["token_id"] = meta["token_id"].astype(str)

    merged = pd.merge(features_df, meta, on="token_id", how="left")
    return merged
