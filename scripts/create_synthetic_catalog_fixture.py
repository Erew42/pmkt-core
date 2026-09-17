"""Create a small offline history catalog for examples and reader smoke tests."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from pmkt.catalog import CatalogSnapshot
from pmkt.data.market_catalog.collect import kalshi_snapshot_dataframe
from pmkt.data.market_catalog.fs import _artifact, _atomic_json, sha256_file
from pmkt.data.normalize import markets_dataframe
from pmkt.data.registry import (
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
)
from pmkt.data.storage.parquet import write_parquet


def create_fixture(path_base: Path) -> dict[str, Path]:
    """Create one explicit synthetic release under a new path base."""
    base = path_base.resolve()
    if base.exists():
        raise FileExistsError(f"refusing to overwrite fixture path base: {base}")
    market_root = base / "data" / "markets"
    release = market_root / "history" / "releases" / "synthetic-v1"
    pm_path = release / "POLYMARKET_ALL_MARKETS.parquet"
    kx_path = release / "KALSHI_ALL_MARKETS.parquet"
    observed = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    pm_rows: list[dict[str, Any]] = [
        {
            "id": "synthetic-polymarket-1",
            "slug": "synthetic-question",
            "question": "Will the synthetic fixture remain offline?",
            "createdAt": observed.isoformat(),
            "updatedAt": observed.isoformat(),
            "closed": False,
        }
    ]
    kx_rows: list[dict[str, Any]] = [
        {
            "ticker": "KXSYNTHETIC-1",
            "title": "Will the synthetic fixture remain offline?",
            "created_time": observed.isoformat(),
            "updated_time": observed.isoformat(),
            "close_time": (observed + timedelta(days=1)).isoformat(),
            "status": "active",
        }
    ]
    write_parquet(
        markets_dataframe(pm_rows),
        pm_path,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    write_parquet(
        kalshi_snapshot_dataframe(kx_rows),
        kx_path,
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    artifacts = {
        "polymarket_all_markets": _artifact(
            pm_path,
            repository_root=base,
            rows=1,
            schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        ),
        "kalshi_all_markets": _artifact(
            kx_path,
            repository_root=base,
            rows=1,
            schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        ),
    }
    manifest = {
        "schema_version": "pmkt.market_history_release.v1",
        "dataset_family": "market_history",
        "release_id": "synthetic-v1",
        "release_kind": "offline_synthetic_fixture",
        "status": "completed",
        "grain": "one latest-known row per venue market",
        "published_at_utc": observed.isoformat(),
        "base_row_count": 2,
        "uncompacted_delta_rows": 0,
        "artifacts": artifacts,
        "provenance": {"data_scope": "synthetic", "source": "offline_fixture"},
        "limitations": ["Synthetic rows are not production observations."],
        "network_accessed": False,
        "research_only": True,
        "execution_authority": False,
        "orders_submitted": False,
    }
    manifest_path = release / "PUBLISHED_MANIFEST.json"
    _atomic_json(manifest_path, manifest)
    _atomic_json(
        market_root / "history" / "LATEST.json",
        {
            "dataset_family": "market_history",
            "release_id": "synthetic-v1",
            "manifest": {
                "path": manifest_path.relative_to(base).as_posix(),
                "sha256": sha256_file(manifest_path),
            },
            **artifacts,
        },
    )
    reference_path = base / "CATALOG_REFERENCE.json"
    CatalogSnapshot.open_latest_history(
        Path("data/markets"), path_base=base
    ).reference.write_json(reference_path)
    return {
        "path_base": base,
        "market_root": market_root,
        "manifest": manifest_path,
        "reference": reference_path,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New path base that will contain data/markets and the saved reference.",
    )
    args = parser.parse_args(argv)
    created = create_fixture(args.output)
    for name, path in created.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
