from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

import httpx

from pmkt.exchanges.polymarket.ws import collect_market_snapshots


GAMMA_BASE = "https://gamma-api.polymarket.com"


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return [raw]
        return parsed if isinstance(parsed, list) else [parsed]
    return []


def _active_token() -> tuple[str, str]:
    response = httpx.get(
        f"{GAMMA_BASE}/markets",
        params={"limit": 100, "closed": "false"},
        timeout=20,
    )
    response.raise_for_status()
    markets = response.json()
    if not isinstance(markets, list):
        raise RuntimeError("Gamma /markets did not return a list.")
    for market in markets:
        if not isinstance(market, dict):
            continue
        tokens = [str(token) for token in _parse_json_list(market.get("clobTokenIds")) if token]
        if tokens:
            question = str(market.get("question") or market.get("slug") or market.get("id"))
            return tokens[0], question
    raise RuntimeError("No active market with CLOB token ids found.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke-test the Polymarket market websocket.")
    parser.add_argument("--token-id", default=None, help="CLOB asset/token id to subscribe to.")
    parser.add_argument("--updates", type=int, default=2, help="Number of snapshots to collect.")
    parser.add_argument("--timeout", type=float, default=15.0, help="Maximum seconds to wait.")
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()
    token_id = args.token_id
    question = "provided token"
    if not token_id:
        token_id, question = _active_token()
    snapshots = await collect_market_snapshots(
        [token_id],
        max_updates=args.updates,
        timeout_seconds=args.timeout,
    )
    if not snapshots:
        raise RuntimeError("No websocket snapshots received.")
    summary = {
        "token_id": token_id,
        "market": question,
        "snapshots": len(snapshots),
        "event_types": sorted({str(item.get("event_type")) for item in snapshots}),
        "first": snapshots[0],
        "last": snapshots[-1],
    }
    print(json.dumps(summary, indent=2, default=str))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
