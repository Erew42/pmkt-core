from __future__ import annotations

import argparse
import asyncio
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pmkt.data.market_data import (
    DEFAULT_EVENT_SLUG,
    collect_order_book_summaries_parquet,
    fetch_trade_history,
    find_event_by_slug,
    trade_history_dataframe,
)
from pmkt.exchanges.polymarket.clob import AsyncClobClient
from pmkt.exchanges.polymarket.gamma import AsyncGammaClient
from pmkt.tokens import extract_token_ids


def require_pandas() -> None:
    try:
        import pandas as pd  # noqa: F401
    except ImportError as exc:
        raise SystemExit(
            "pandas is required to save DataFrame outputs. Install with `pip install -e .`."
        ) from exc


def save_dataframe(df, base_path: Path) -> list[Path]:
    base_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = base_path.with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    saved = [csv_path]
    parquet_path = base_path.with_suffix(".parquet")
    try:
        df.to_parquet(parquet_path, index=False)
    except (ImportError, ValueError):
        return saved
    saved.append(parquet_path)
    return saved


def save_trade_history_dataframe(path_base: Path, rows: list[dict[str, Any]]) -> None:
    require_pandas()
    df = trade_history_dataframe(rows)
    save_dataframe(df, path_base)


def _normalize_epoch(ts: float) -> float:
    if ts > 1.0e11:
        return ts / 1000.0
    return ts


def _load_trade_history_csv(path: Path) -> tuple[list[datetime], list[float]]:
    timestamps: list[datetime] = []
    prices: list[float] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ts_raw = row.get("timestamp")
            price_raw = row.get("price")
            if not ts_raw or not price_raw:
                continue
            try:
                ts_val = _normalize_epoch(float(ts_raw))
                price_val = float(price_raw)
            except ValueError:
                continue
            timestamps.append(datetime.fromtimestamp(ts_val))
            prices.append(price_val)
    return timestamps, prices


def plot_trade_history(csv_path: Path, output_path: Path, title: str) -> None:
    import matplotlib.pyplot as plt

    timestamps, prices = _load_trade_history_csv(csv_path)
    if not timestamps:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(timestamps, prices, linewidth=1.6)
    ax.set_title(title)
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    fig.autofmt_xdate()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def _load_order_book_summary(summary_csv: Path) -> dict[str, list[tuple[datetime, float, float]]]:
    rows: dict[str, list[tuple[datetime, float, float]]] = {}
    with summary_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            token_id = row.get("token_id")
            ts_raw = row.get("ts")
            bid_raw = row.get("best_bid")
            ask_raw = row.get("best_ask")
            if not token_id or not ts_raw or not bid_raw or not ask_raw:
                continue
            try:
                ts_val = _normalize_epoch(float(ts_raw))
                bid_val = float(bid_raw)
                ask_val = float(ask_raw)
            except ValueError:
                continue
            rows.setdefault(token_id, []).append(
                (datetime.fromtimestamp(ts_val), bid_val, ask_val)
            )
    return rows


def plot_order_book_summary(summary_csv: Path, output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    rows = _load_order_book_summary(summary_csv)
    if not rows:
        return
    output_dir.mkdir(parents=True, exist_ok=True)
    for token_id, series in rows.items():
        series = sorted(series, key=lambda item: item[0])
        timestamps = [item[0] for item in series]
        bids = [item[1] for item in series]
        asks = [item[2] for item in series]
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(timestamps, bids, label="Best bid", linewidth=1.4)
        ax.plot(timestamps, asks, label="Best ask", linewidth=1.4)
        ax.set_title(f"Order Book Top of Book ({token_id})")
        ax.set_xlabel("Time")
        ax.set_ylabel("Price")
        ax.legend(loc="best")
        fig.autofmt_xdate()
        fig.savefig(output_dir / f"order_book_{token_id}.png", dpi=160, bbox_inches="tight")
        plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Polymarket trade history and order book snapshots."
    )
    parser.add_argument("--slug", default=DEFAULT_EVENT_SLUG, help="Event or market slug.")
    parser.add_argument("--interval", default="1h", help="Price history interval.")
    parser.add_argument("--fidelity", type=int, default=5, help="Price history fidelity.")
    parser.add_argument(
        "--token-id",
        action="append",
        default=[],
        help="Optional token id override (can be repeated).",
    )
    parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=20.0,
        help="Order book polling interval in seconds (< 60s for testing).",
    )
    parser.add_argument(
        "--max-snapshots",
        type=int,
        default=2,
        help="Max order book snapshots to capture.",
    )
    parser.add_argument(
        "--duration-s",
        type=float,
        default=None,
        help="Optional capture duration in seconds.",
    )
    parser.add_argument(
        "--output-dir",
        default="generated/market_data",
        help="Output directory for data and plots.",
    )
    parser.add_argument(
        "--plots",
        action="store_true",
        help="Generate PNG plots from captured data.",
    )
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    data_dir = output_dir / args.slug
    data_dir.mkdir(parents=True, exist_ok=True)
    require_pandas()

    async with AsyncGammaClient() as gamma, AsyncClobClient() as clob:
        payload = await find_event_by_slug(gamma, args.slug)
        (data_dir / "event.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        token_ids = args.token_id or extract_token_ids(payload)
        if not token_ids:
            raise ValueError("No token ids found in event payload.")

        history_dir = data_dir / "trade_history"
        for token_id in token_ids:
            history = await fetch_trade_history(
                clob,
                token_id,
                interval=args.interval,
                fidelity=args.fidelity,
            )
            save_trade_history_dataframe(history_dir / token_id, history)

        order_book_dir = data_dir / "order_books"
        summary_parquet = await collect_order_book_summaries_parquet(
            clob,
            token_ids,
            output_path=order_book_dir / "order_book_summary.parquet",
            poll_interval_s=args.poll_interval_s,
            max_snapshots=args.max_snapshots,
            duration_s=args.duration_s,
            also_jsonl_dir=order_book_dir / "raw_snapshots",
        )
        import pandas as pd

        summary_df = pd.read_parquet(summary_parquet)
        save_dataframe(summary_df, order_book_dir / "order_book_summary")
        summary_csv = order_book_dir / "order_book_summary.csv"
        if summary_csv.exists():
            print(f"Wrote order book summary to {summary_csv}")

    if args.plots:
        try:
            import matplotlib.pyplot as _  # noqa: F401
        except ImportError as exc:
            raise SystemExit(
                "matplotlib is required for --plots. Install with `pip install matplotlib`."
            ) from exc

        plots_dir = data_dir / "plots"
        for token_id in token_ids:
            csv_path = history_dir / f"{token_id}.csv"
            plot_trade_history(
                csv_path,
                plots_dir / f"trade_history_{token_id}.png",
                f"Trade History ({token_id})",
            )
        if summary_csv.exists():
            plot_order_book_summary(summary_csv, plots_dir)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
