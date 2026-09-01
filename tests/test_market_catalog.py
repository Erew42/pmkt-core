from __future__ import annotations

from datetime import datetime, timedelta, timezone
import ast
import hashlib
import json
from pathlib import Path
import re
from typing import Any

import duckdb
import pandas as pd
import pytest
from typer.testing import CliRunner

import pmkt.data.market_catalog as market_catalog_module
import pmkt.data.market_catalog.fs as market_catalog_fs
from pmkt.cli.market_catalog import markets_app
from pmkt.data.canonical import (
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
)
from pmkt.data.market_catalog import (
    CatalogError,
    DISCOVERY_MANIFEST_SCHEMA,
    DISCOVERY_POINTER_SCHEMA,
    FilterAgreementError,
    MarketCatalogService,
    collect_polymarket_discovery,
    native_family_for_legacy_kalshi,
    polymarket_operational_family,
    register_catalog_views,
    tree_sha256,
    verify_kalshi_filter_agreement,
)
from pmkt.data.normalize import markets_dataframe
from pmkt.data.storage.parquet import write_parquet
from pmkt.exchanges.kalshi.client import kalshi_markets_dataframe


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def _normalized_cli_output(value: str) -> str:
    return "".join(_ANSI_ESCAPE_RE.sub("", value).split())


NOW = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _pm(
    key: str,
    *,
    created: datetime = NOW,
    updated: datetime | None = None,
    closed: bool = False,
    question: str | None = None,
) -> dict[str, Any]:
    return {
        "id": key,
        "slug": f"market-{key}",
        "question": question or f"Question {key}?",
        "createdAt": created.isoformat(),
        "updatedAt": (updated or created).isoformat(),
        "closed": closed,
    }


def _kx(
    key: str,
    *,
    created: datetime = NOW,
    updated: datetime | None = None,
    status: str = "active",
    title: str | None = None,
) -> dict[str, Any]:
    return {
        "ticker": key,
        "title": title or f"Question {key}?",
        "created_time": created.isoformat(),
        "updated_time": (updated or created).isoformat(),
        "close_time": (created + timedelta(days=1)).isoformat(),
        "status": status,
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _history_artifact(path: Path, *, rows: int) -> dict[str, Any]:
    return {
        "format": "parquet",
        "parquet_file_count": 1,
        "path": str(path.resolve()),
        "rows": rows,
        "sha256": _sha(path),
        "tree_sha256": None,
        "size_bytes": path.stat().st_size,
    }


def _history_pointer_and_manifest(
    service: MarketCatalogService,
) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    pointer = json.loads(service.history_pointer_path.read_text(encoding="utf-8"))
    manifest_path = Path(pointer["manifest"]["path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return pointer, manifest_path, manifest


def _write_history_pointer_and_manifest(
    service: MarketCatalogService,
    *,
    pointer: dict[str, Any],
    manifest_path: Path,
    manifest: dict[str, Any],
) -> None:
    _write_json(manifest_path, manifest)
    pointer["manifest"]["sha256"] = _sha(manifest_path)
    _write_json(service.history_pointer_path, pointer)


def _history_keys(path: Path, key_column: str) -> set[str]:
    files = sorted(str(item) for item in path.rglob("*.parquet"))
    if not files and path.is_file():
        files = [str(path)]
    with duckdb.connect(database=":memory:") as connection:
        rows = connection.execute(
            f"SELECT CAST({key_column} AS VARCHAR) "
            "FROM read_parquet(?, union_by_name=true, hive_partitioning=false)",
            [files],
        ).fetchall()
    return {str(row[0]) for row in rows}


def _kalshi_keys_in_same_bucket() -> tuple[str, str]:
    seen: dict[int, str] = {}
    with duckdb.connect(database=":memory:") as connection:
        for index in range(1_000):
            key = f"KX-COLLISION-{index}"
            row = connection.execute(
                "SELECT CAST(hash(?) % 128 AS INTEGER)", [key]
            ).fetchone()
            assert row is not None
            bucket = int(row[0])
            prior = seen.get(bucket)
            if prior is not None:
                return prior, key
            seen[bucket] = key
    raise AssertionError("failed to find two Kalshi keys in one hash bucket")


def _catalog(
    tmp_path: Path,
    *,
    pm_rows: list[dict[str, Any]] | None = None,
    kx_rows: list[dict[str, Any]] | None = None,
) -> MarketCatalogService:
    root = tmp_path / "data" / "markets"
    release = root / "history" / "releases" / "base"
    pm_path = release / "POLYMARKET_ALL_MARKETS.parquet"
    kx_path = release / "KALSHI_ALL_MARKETS.parquet"
    write_parquet(
        markets_dataframe(pm_rows or [_pm("pm-base", created=NOW - timedelta(days=2))]),
        pm_path,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    write_parquet(
        kalshi_markets_dataframe(
            kx_rows or [_kx("KXBASE", created=NOW - timedelta(days=2))]
        ),
        kx_path,
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    manifest = {
        "schema_version": "pmkt.market_history_release.v1",
        "dataset_family": "market_history",
        "release_id": "base",
        "published_at_utc": (NOW - timedelta(days=1)).isoformat(),
        "as_of": {
            "polymarket_fresh_census_date_utc": "2026-08-22",
            "kalshi_cursor_exhausted_at_utc": "2026-08-22T12:00:00+00:00",
        },
        "promotion_layer_count": 0,
        "base_row_count": len(pm_rows or [1]) + len(kx_rows or [1]),
        "uncompacted_delta_rows": 0,
        "base_parquet_file_count": 2,
        "uncompacted_delta_parquet_file_count": 0,
        "artifacts": {
            "polymarket_all_markets": _history_artifact(
                pm_path, rows=len(pm_rows or [1])
            ),
            "kalshi_all_markets": _history_artifact(kx_path, rows=len(kx_rows or [1])),
        },
    }
    manifest_path = release / "PUBLISHED_MANIFEST.json"
    _write_json(manifest_path, manifest)
    _write_json(
        root / "history" / "LATEST.json",
        {
            "manifest": {
                "path": str(manifest_path.resolve()),
                "sha256": _sha(manifest_path),
            },
            "polymarket_all_markets": manifest["artifacts"]["polymarket_all_markets"],
            "kalshi_all_markets": manifest["artifacts"]["kalshi_all_markets"],
        },
    )
    return MarketCatalogService(root)


def _seed_current(
    service: MarketCatalogService,
    *,
    pm_rows: list[dict[str, Any]],
    kx_rows: list[dict[str, Any]],
) -> tuple[str, str]:
    release = service.releases_root / "previous_current"
    pm_path = release / "POLYMARKET_OPEN_MARKETS.parquet"
    kx_path = release / "KALSHI_OPEN_MARKETS.parquet"
    mve_path = release / "KALSHI_MVE_CURRENT_MARKETS.parquet"
    write_parquet(
        markets_dataframe(pm_rows),
        pm_path,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    write_parquet(
        kalshi_markets_dataframe(kx_rows),
        kx_path,
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    write_parquet(
        kalshi_markets_dataframe([_kx("KXMVE-PRIOR")]),
        mve_path,
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    old_mve_as_of = "2026-08-22T00:00:00+00:00"
    _write_json(
        service.current_pointer_path,
        {
            "dataset_family": "markets",
            "release_id": "previous_current",
            "polymarket_open_markets": {
                "path": str(pm_path.resolve()),
                "rows": len(pm_rows),
                "sha256": _sha(pm_path),
            },
            "kalshi_open_markets": {
                "path": str(kx_path.resolve()),
                "rows": len(kx_rows),
                "sha256": _sha(kx_path),
            },
            "kalshi_mve_current_markets": {
                "path": str(mve_path.resolve()),
                "rows": 1,
                "sha256": _sha(mve_path),
                "as_of_utc": old_mve_as_of,
            },
        },
    )
    return _sha(pm_path), old_mve_as_of


class FakeGamma:
    def __init__(
        self,
        pages: dict[tuple[bool, str | None], dict[str, Any]] | None = None,
        *,
        current_pages: dict[str | None, dict[str, Any]] | None = None,
        targets: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.pages = pages or {}
        self.current_pages = current_pages or {}
        self.targets = targets or {}
        self.calls: list[dict[str, Any]] = []

    async def markets_keyset_raw_page(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        cursor = kwargs.get("after_cursor")
        if "order" in kwargs:
            return self.pages.get(
                (bool(kwargs.get("closed")), cursor),
                {"markets": [], "next_cursor": ""},
            )
        return self.current_pages.get(cursor, {"markets": [], "next_cursor": ""})

    async def market(self, key: str) -> dict[str, Any]:
        return self.targets[key]


class FakeKalshi:
    def __init__(
        self,
        rows: dict[tuple[str | None, str | None], list[dict[str, Any]]] | None = None,
        *,
        targets: dict[str, dict[str, Any]] | None = None,
        historical: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.rows = rows or {}
        self.targets = targets or {}
        self.historical = historical or {}
        self.calls: list[dict[str, Any]] = []

    async def markets_page(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        key = (kwargs.get("mve_filter"), kwargs.get("status"))
        return {"markets": self.rows.get(key, []), "cursor": ""}

    async def market(self, key: str) -> dict[str, Any]:
        return self.targets[key]

    async def historical_market(self, key: str) -> dict[str, Any]:
        return self.historical[key]


@pytest.mark.asyncio
async def test_polymarket_discovery_requires_descending_creation_order() -> None:
    client = FakeGamma(
        {
            (False, None): {
                "markets": [
                    _pm("a", created=NOW - timedelta(minutes=2)),
                    _pm("b", created=NOW - timedelta(minutes=1)),
                ],
                "next_cursor": "",
            }
        }
    )
    with pytest.raises(CatalogError, match="not descending"):
        await collect_polymarket_discovery(client, cutoff=NOW - timedelta(hours=1))


@pytest.mark.asyncio
async def test_polymarket_missing_timestamp_targets_then_fails_closed() -> None:
    unresolved = {**_pm("missing"), "createdAt": None}
    client = FakeGamma(
        {(False, None): {"markets": [unresolved], "next_cursor": ""}},
        targets={"missing": _pm("missing")},
    )
    result = await collect_polymarket_discovery(client, cutoff=NOW - timedelta(hours=1))
    assert [row["id"] for row in result.rows] == ["missing"]
    assert result.details["lanes"]["open"]["timestamp_retry_requests"] == 3
    assert result.details["lanes"]["open"]["target_reads"] == 1

    client.targets["missing"] = unresolved
    with pytest.raises(CatalogError, match="unresolved createdAt"):
        await collect_polymarket_discovery(client, cutoff=NOW - timedelta(hours=1))


@pytest.mark.asyncio
async def test_polymarket_open_closed_conflict_uses_newer_update() -> None:
    older = _pm("same", updated=NOW, question="old")
    newer = _pm("same", updated=NOW + timedelta(minutes=1), question="new")
    client = FakeGamma(
        {
            (False, None): {"markets": [older], "next_cursor": ""},
            (True, None): {"markets": [newer], "next_cursor": ""},
        }
    )
    result = await collect_polymarket_discovery(client, cutoff=NOW - timedelta(hours=1))
    assert result.rows[0]["question"] == "new"


@pytest.mark.asyncio
async def test_kalshi_filter_agreement_accepts_exact_partition_and_rejects_overlap() -> (
    None
):
    client = FakeKalshi(
        {
            ("only", None): [_kx("KXMVE-1")],
            ("exclude", None): [_kx("KXONE")],
            (None, None): [_kx("KXMVE-1"), _kx("KXONE")],
        }
    )
    report = await verify_kalshi_filter_agreement(
        client, cutoff=NOW - timedelta(hours=1), window_end=NOW
    )
    assert report["complete"] is True
    client.rows[("exclude", None)] = [_kx("KXMVE-1")]
    with pytest.raises(CatalogError, match="filter agreement failed"):
        await verify_kalshi_filter_agreement(
            client, cutoff=NOW - timedelta(hours=1), window_end=NOW
        )


def test_bootstrap_watermarks_and_family_rules(tmp_path: Path) -> None:
    service = _catalog(tmp_path)
    assert service.bootstrap_cutoff("polymarket") == datetime(
        2026, 8, 21, tzinfo=timezone.utc
    )
    assert service.bootstrap_cutoff("kalshi-mve") == datetime(
        2026, 8, 21, 12, tzinfo=timezone.utc
    )
    assert native_family_for_legacy_kalshi("KXMVE-ABC") == (
        "kalshi_mve",
        "legacy_ticker_compat",
    )
    assert native_family_for_legacy_kalshi(None)[0] == "family_unknown"
    assert polymarket_operational_family("btc-updown-5m-1787486400") == (
        "polymarket_updown_5m"
    )
    assert polymarket_operational_family("ordinary-market") == (
        "polymarket_conventional"
    )


def test_python_and_duckdb_kalshi_classifier_parity() -> None:
    cases = [
        (None, None),
        ("", None),
        ("   ", None),
        ("  kXmVe-MIXED  ", None),
        ("KXMVE", None),
        ("KX-CONVENTIONAL", None),
        ("KXMVE-OVERRIDDEN", "kalshi_conventional"),
        ("KX-OVERRIDDEN", "kalshi_mve"),
        ("KX-MIXED-PARTITION", "KaLsHi_MvE"),
        ("KX-NONFAMILY-SUFFIX", "kalshi_mve_old"),
        (None, "kalshi_mve"),
    ]
    frame = pd.DataFrame(cases, columns=["market_key", "partition_family"])
    frame["case_index"] = range(len(frame))
    frame["filename"] = frame["partition_family"].map(
        lambda value: (
            None
            if value is None
            else f"root/native_family={value}/bucket=1/part-000000.parquet"
        )
    )
    with duckdb.connect(database=":memory:") as connection:
        connection.register("classifier_cases", frame)
        sql_rows = connection.execute(
            f"SELECT "
            f"{market_catalog_module._kalshi_family_sql('market_key', filename_sql='filename')}, "
            f"{market_catalog_module._kalshi_family_provenance_sql('market_key', filename_sql='filename')} "
            "FROM classifier_cases ORDER BY case_index"
        ).fetchall()

    python_rows = [
        native_family_for_legacy_kalshi(key, native_family=partition_family)
        for key, partition_family in cases
    ]
    assert sql_rows == python_rows


def test_python_and_duckdb_polymarket_classifier_parity() -> None:
    supported = [
        f"asset-{form}-{duration}-{epoch}"
        for form in market_catalog_module.POLYMARKET_RECURRING_FORMS
        for duration in market_catalog_module.POLYMARKET_RECURRING_DURATIONS
        for epoch in ("123456789", "123456789012")
    ]
    slugs: list[str | None] = [
        *supported,
        "ASSET-UPDOWN-5M-123456789",
        "asset-updown-5m-12345678",
        "asset-updown-5m-1234567890123",
        "asset-updown-1h-123456789",
        "ordinary-market",
        "",
        None,
    ]
    frame = pd.DataFrame({"slug": slugs})
    frame["case_index"] = range(len(frame))
    with duckdb.connect(database=":memory:") as connection:
        connection.register("classifier_cases", frame)
        sql_rows = [
            str(row[0])
            for row in connection.execute(
                "SELECT "
                f"{market_catalog_module._polymarket_operational_family_sql('slug')} "
                "FROM classifier_cases ORDER BY case_index"
            ).fetchall()
        ]

    assert sql_rows == [polymarket_operational_family(slug) for slug in slugs]


@pytest.mark.asyncio
async def test_no_publish_leaves_pointer_and_release_namespace_unchanged(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    client = FakeGamma(
        {
            (False, None): {"markets": [_pm("new")], "next_cursor": ""},
            (True, None): {"markets": [], "next_cursor": ""},
        }
    )
    result = await service.discover("polymarket", client=client, publish=False)
    assert result["counts"]["new"] == 1
    assert not service.discovery_pointer_path.exists()
    assert not service.releases_root.exists()


@pytest.mark.asyncio
async def test_discovery_pointer_ignores_orphan_and_cache_rebuilds_when_stale(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    orphan = service.releases_root / "market_discovery_polymarket_orphan"
    orphan.mkdir(parents=True)
    client = FakeGamma(
        {
            (False, None): {"markets": [_pm("new")], "next_cursor": ""},
            (True, None): {"markets": [], "next_cursor": ""},
        }
    )
    await service.discover("polymarket", client=client)
    pointer = service.read_discovery_pointer()
    assert pointer["schema_version"] == DISCOVERY_POINTER_SCHEMA
    assert "orphan" not in pointer["streams"]["polymarket"]["release_id"]
    index = service.ensure_known_key_index()
    with duckdb.connect(str(index)) as connection:
        connection.execute(
            "UPDATE catalog_meta SET value='stale' WHERE key='pointer_fingerprint'"
        )
    service.ensure_known_key_index()
    assert service.status()["known_key_cache"]["state"] == "current"


@pytest.mark.asyncio
async def test_discovery_pointer_rejects_corrupted_artifact(tmp_path: Path) -> None:
    service = _catalog(tmp_path)
    await service.discover(
        "polymarket",
        client=FakeGamma(
            {
                (False, None): {"markets": [_pm("new")], "next_cursor": ""},
                (True, None): {"markets": [], "next_cursor": ""},
            }
        ),
    )
    pointer = json.loads(service.discovery_pointer_path.read_text(encoding="utf-8"))
    manifest_path = Path(pointer["streams"]["polymarket"]["path"])
    if not manifest_path.is_absolute():
        manifest_path = service.repository_root / manifest_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact_path = Path(manifest["artifacts"]["new_markets"]["path"])
    if not artifact_path.is_absolute():
        artifact_path = service.repository_root / artifact_path
    artifact_path.write_bytes(artifact_path.read_bytes() + b"corrupt")
    with pytest.raises(CatalogError, match="artifact is invalid"):
        service.read_discovery_pointer()


@pytest.mark.asyncio
async def test_failed_discovery_does_not_advance_pointer(tmp_path: Path) -> None:
    service = _catalog(tmp_path)
    good = FakeGamma(
        {
            (False, None): {"markets": [_pm("good")], "next_cursor": ""},
            (True, None): {"markets": [], "next_cursor": ""},
        }
    )
    await service.discover("polymarket", client=good)
    before = service.discovery_pointer_path.read_bytes()
    bad_row = {**_pm("bad"), "createdAt": None}
    bad = FakeGamma(
        {(False, None): {"markets": [bad_row], "next_cursor": ""}},
        targets={"bad": bad_row},
    )
    with pytest.raises(CatalogError):
        await service.discover("polymarket", client=bad)
    assert service.discovery_pointer_path.read_bytes() == before


@pytest.mark.asyncio
async def test_filter_disagreement_records_failed_manifest_without_pointer(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    conflict = _kx("KXMVE-CONFLICT")
    client = FakeKalshi(
        {
            ("only", None): [conflict],
            ("exclude", None): [conflict],
            (None, None): [conflict],
        }
    )
    with pytest.raises(FilterAgreementError):
        await service.discover("kalshi-mve", client=client)
    assert not service.discovery_pointer_path.exists()
    failed = list(service.releases_root.glob("*_failed_*/DISCOVERY_MANIFEST.json"))
    assert len(failed) == 1
    manifest = json.loads(failed[0].read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["filter_agreement"]["conflicting_keys"] == ["KXMVE-CONFLICT"]
    assert manifest["pointer_updated"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pm_target_closed", "kx_target_status", "expected_terminal", "expected_reopened"),
    [(True, "finalized", 2, 0), (False, "active", 0, 2)],
)
async def test_current_absence_is_reconciled_without_history_deletion(
    tmp_path: Path,
    pm_target_closed: bool,
    kx_target_status: str,
    expected_terminal: int,
    expected_reopened: int,
) -> None:
    service = _catalog(
        tmp_path,
        pm_rows=[_pm("pm-old", created=NOW - timedelta(days=2))],
        kx_rows=[_kx("KXOLD", created=NOW - timedelta(days=2))],
    )
    history_manifest_path, _manifest = service._history_manifest()
    history_hash = _sha(history_manifest_path)
    _seed_current(
        service,
        pm_rows=[_pm("pm-old")],
        kx_rows=[_kx("KXOLD")],
    )
    pm_client = FakeGamma(
        current_pages={None: {"markets": [_pm("pm-live")], "next_cursor": ""}},
        targets={"pm-old": _pm("pm-old", closed=pm_target_closed)},
    )
    kx_client = FakeKalshi(
        {
            ("exclude", "open"): [_kx("KXLIVE")],
            ("exclude", "unopened"): [],
            ("exclude", "paused"): [],
        },
        targets={"KXOLD": _kx("KXOLD", status=kx_target_status)},
    )
    result = await service.refresh_current(
        scope="standard",
        polymarket_client=pm_client,
        kalshi_client=kx_client,
    )
    reconciliation = result["lifecycle_reconciliation"]
    terminal = (
        reconciliation["polymarket_terminal_updates"]
        + reconciliation["kalshi_terminal_updates"]
    )
    reopened = (
        reconciliation["polymarket_reopenings"] + reconciliation["kalshi_reopenings"]
    )
    assert terminal == expected_terminal
    assert reopened == expected_reopened
    assert reconciliation["historical_rows_deleted"] == 0
    assert _sha(history_manifest_path) == history_hash


@pytest.mark.asyncio
async def test_current_lifecycle_lineage_survives_later_refresh_and_compaction(
    tmp_path: Path,
) -> None:
    service = _catalog(
        tmp_path,
        pm_rows=[_pm("pm-old", created=NOW - timedelta(days=2))],
        kx_rows=[_kx("KXOLD", created=NOW - timedelta(days=2))],
    )
    await service.refresh_current(
        scope="all",
        polymarket_client=FakeGamma(
            current_pages={None: {"markets": [_pm("pm-old")], "next_cursor": ""}}
        ),
        kalshi_client=FakeKalshi(
            {
                ("exclude", "open"): [_kx("KXOLD")],
                ("exclude", "unopened"): [],
                ("exclude", "paused"): [],
                ("only", "open"): [],
                ("only", "unopened"): [],
                ("only", "paused"): [],
                ("only", "closed"): [],
                ("only", "settled"): [],
            }
        ),
    )
    first_correction = await service.refresh_current(
        scope="standard",
        polymarket_client=FakeGamma(
            current_pages={None: {"markets": [_pm("pm-live")], "next_cursor": ""}},
            targets={
                "pm-old": _pm("pm-old", closed=True, updated=NOW + timedelta(hours=1))
            },
        ),
        kalshi_client=FakeKalshi(
            {
                ("exclude", "open"): [_kx("KXLIVE")],
                ("exclude", "unopened"): [],
                ("exclude", "paused"): [],
            },
            targets={
                "KXOLD": _kx(
                    "KXOLD", status="finalized", updated=NOW + timedelta(hours=1)
                )
            },
        ),
    )
    assert (
        first_correction["lifecycle_reconciliation"]["polymarket_terminal_updates"] == 1
    )
    assert first_correction["lifecycle_reconciliation"]["kalshi_terminal_updates"] == 1

    later_refresh = await service.refresh_current(
        scope="standard",
        polymarket_client=FakeGamma(
            current_pages={None: {"markets": [_pm("pm-live")], "next_cursor": ""}}
        ),
        kalshi_client=FakeKalshi(
            {
                ("exclude", "open"): [_kx("KXLIVE")],
                ("exclude", "unopened"): [],
                ("exclude", "paused"): [],
            }
        ),
    )
    assert later_refresh["lifecycle_reconciliation"]["polymarket_terminal_updates"] == 0
    assert later_refresh["lifecycle_reconciliation"]["kalshi_terminal_updates"] == 0
    assert len(service.reachable_current_manifests()) == 3

    await service.compact_history(force=True)
    _manifest_path, manifest = service._history_manifest()
    pm = pd.read_parquet(service._history_artifact_path(manifest, "polymarket"))
    kx = pd.read_parquet(service._history_artifact_path(manifest, "kalshi"))
    assert bool(pm.loc[pm["market_id"] == "pm-old", "closed"].iloc[0]) is True
    assert kx.loc[kx["market_key"] == "KXOLD", "status"].iloc[0] == "finalized"
    assert (
        manifest["current_lifecycle_watermark"]["release_id"]
        == later_refresh["release_id"]
    )
    assert (
        service.reachable_current_manifests(
            after_hash=manifest["current_lifecycle_watermark"]["sha256"]
        )
        == []
    )


def test_legacy_current_manifest_is_a_baseline_not_a_lifecycle_delta(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    legacy_path = service.releases_root / "legacy_current" / "PUBLISHED_MANIFEST.json"
    _write_json(
        legacy_path,
        {
            "schema_version": "pmkt.market_catalog_release.v1",
            "dataset_family": "markets",
            "release_id": "legacy_current",
            "release_kind": "full",
        },
    )
    pointer = {"manifest": service._manifest_ref(legacy_path)}
    _write_json(service.current_pointer_path, pointer)

    assert service._current_predecessor_reference(pointer) is None
    assert service.reachable_current_manifests() == []


@pytest.mark.asyncio
async def test_standard_current_refresh_retains_original_mve_as_of(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    _history_hash, old_mve_as_of = _seed_current(
        service,
        pm_rows=[],
        kx_rows=[],
    )
    result = await service.refresh_current(
        scope="standard",
        polymarket_client=FakeGamma(
            current_pages={None: {"markets": [], "next_cursor": ""}}
        ),
        kalshi_client=FakeKalshi(
            {
                ("exclude", "open"): [],
                ("exclude", "unopened"): [],
                ("exclude", "paused"): [],
            }
        ),
    )
    assert result["artifacts"]["kalshi_mve_current_markets"]["as_of_utc"] == (
        old_mve_as_of
    )


@pytest.mark.asyncio
async def test_full_current_refresh_exhausts_mve_close_and_settlement_windows(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    kalshi = FakeKalshi(
        {
            ("exclude", "open"): [],
            ("exclude", "unopened"): [],
            ("exclude", "paused"): [],
            ("only", "open"): [_kx("KXMVE-LIVE")],
            ("only", "unopened"): [],
            ("only", "paused"): [],
            ("only", "closed"): [_kx("KXMVE-CLOSED", status="closed")],
            ("only", "settled"): [_kx("KXMVE-SETTLED", status="finalized")],
        }
    )
    result = await service.refresh_current(
        scope="all",
        polymarket_client=FakeGamma(
            current_pages={None: {"markets": [], "next_cursor": ""}}
        ),
        kalshi_client=kalshi,
    )
    assert result["artifacts"]["kalshi_mve_current_markets"]["rows"] == 1
    assert any("min_close_ts" in call for call in kalshi.calls)
    assert any("min_settled_ts" in call for call in kalshi.calls)


@pytest.mark.asyncio
async def test_full_current_refresh_bootstraps_empty_catalog_with_explicit_cutoff(
    tmp_path: Path,
) -> None:
    service = MarketCatalogService(tmp_path / "empty" / "markets")
    cutoff = NOW - timedelta(days=2)
    kalshi = FakeKalshi(
        {
            ("exclude", "open"): [],
            ("exclude", "unopened"): [],
            ("exclude", "paused"): [],
            ("only", "open"): [_kx("KXMVE-LIVE")],
            ("only", "unopened"): [],
            ("only", "paused"): [],
            ("only", "closed"): [],
            ("only", "settled"): [],
        }
    )

    result = await service.refresh_current(
        scope="all",
        bootstrap_cutoff=cutoff,
        polymarket_client=FakeGamma(
            current_pages={None: {"markets": [_pm("pm-live")], "next_cursor": ""}}
        ),
        kalshi_client=kalshi,
    )

    manifest = json.loads(
        (service.repository_root / result["manifest"]["path"]).read_text(
            encoding="utf-8"
        )
    )
    lifecycle = manifest["collection"]["kalshi_mve_lifecycle"]
    assert lifecycle["cutoff_utc"] == cutoff.isoformat()
    assert lifecycle["cutoff_source"] == "explicit_bootstrap"
    assert service.current_pointer_path.is_file()
    assert manifest["orders_submitted"] is False


@pytest.mark.asyncio
async def test_empty_current_refresh_requires_explicit_or_history_cutoff(
    tmp_path: Path,
) -> None:
    service = MarketCatalogService(tmp_path / "empty" / "markets")

    with pytest.raises(CatalogError, match="history/LATEST.json is required"):
        await service.refresh_current(
            scope="all",
            polymarket_client=FakeGamma(
                current_pages={None: {"markets": [], "next_cursor": ""}}
            ),
            kalshi_client=FakeKalshi(
                {
                    ("exclude", "open"): [],
                    ("exclude", "unopened"): [],
                    ("exclude", "paused"): [],
                    ("only", "open"): [],
                    ("only", "unopened"): [],
                    ("only", "paused"): [],
                }
            ),
        )

    assert not service.current_pointer_path.exists()
    assert not service.releases_root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope", "cutoff", "message"),
    [
        ("standard", NOW - timedelta(days=1), "only valid with scope='all'"),
        ("all", NOW.replace(tzinfo=None), "must be timezone-aware"),
        (
            "all",
            datetime.now(timezone.utc) + timedelta(days=1),
            "cannot be in the future",
        ),
    ],
)
async def test_current_refresh_rejects_invalid_explicit_bootstrap_cutoff(
    tmp_path: Path,
    scope: str,
    cutoff: datetime,
    message: str,
) -> None:
    service = MarketCatalogService(tmp_path / "empty" / "markets")

    with pytest.raises(CatalogError, match=message):
        await service.refresh_current(
            scope=scope,  # type: ignore[arg-type]
            bootstrap_cutoff=cutoff,
            polymarket_client=FakeGamma(),
            kalshi_client=FakeKalshi(),
        )

    assert not service.market_root.exists()


def test_refresh_current_help_exposes_explicit_bootstrap_cutoff() -> None:
    result = CliRunner().invoke(
        markets_app,
        ["refresh-current", "--help"],
        env={"COLUMNS": "200"},
    )

    assert result.exit_code == 0
    assert "--bootstrap-cutoff" in _normalized_cli_output(result.stdout)


@pytest.mark.asyncio
async def test_failed_current_refresh_preserves_pointer(tmp_path: Path) -> None:
    service = _catalog(tmp_path)
    _seed_current(service, pm_rows=[], kx_rows=[])
    before = service.current_pointer_path.read_bytes()
    repeated = FakeGamma(
        current_pages={
            None: {"markets": [], "next_cursor": "repeat"},
            "repeat": {"markets": [], "next_cursor": "repeat"},
        }
    )
    with pytest.raises(CatalogError, match="cursor repeated"):
        await service.refresh_current(
            scope="standard",
            polymarket_client=repeated,
            kalshi_client=FakeKalshi(),
        )
    assert service.current_pointer_path.read_bytes() == before


def test_catalog_reader_injects_new_and_legacy_family_provenance(
    tmp_path: Path,
) -> None:
    service = _catalog(
        tmp_path,
        pm_rows=[
            {
                **_pm("pm-up"),
                "slug": "eth-up-or-down-15m-1787486400",
            }
        ],
        kx_rows=[_kx("KXMVE-OLD"), _kx("KXNORMAL")],
    )
    with duckdb.connect(database=":memory:") as connection:
        register_catalog_views(connection, service.market_root)
        pm = connection.execute(
            "SELECT operational_family FROM market_catalog_polymarket"
        ).fetchone()
        kx = dict(
            connection.execute(
                "SELECT market_key, native_family FROM market_catalog_kalshi"
            ).fetchall()
        )
    assert pm == ("polymarket_updown_15m",)
    assert kx == {"KXMVE-OLD": "kalshi_mve", "KXNORMAL": "kalshi_conventional"}


@pytest.mark.asyncio
async def test_partition_family_overrides_legacy_ticker_through_compaction(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    pointer, manifest_path, manifest = _history_pointer_and_manifest(service)
    kx_path = Path(manifest["artifacts"]["kalshi_all_markets"]["path"])
    kx_path.unlink()
    rows_by_family = {
        "kalshi_conventional": [_kx("KXMVE-EXPLICIT-CONVENTIONAL")],
        "kalshi_mve": [_kx("KX-EXPLICIT-MVE")],
    }
    parts = []
    for bucket, (family, rows) in enumerate(rows_by_family.items()):
        part = (
            kx_path
            / "source=compacted_base"
            / f"native_family={family}"
            / f"bucket={bucket}"
            / "part-000000.parquet"
        )
        write_parquet(
            kalshi_markets_dataframe(rows),
            part,
            schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            strict=True,
        )
        parts.append(part)
    descriptor = {
        "format": "partitioned_parquet",
        "parquet_file_count": 2,
        "path": str(kx_path.resolve()),
        "rows": 2,
        "sha256": None,
        "tree_sha256": tree_sha256(kx_path),
        "size_bytes": sum(path.stat().st_size for path in parts),
    }
    manifest["artifacts"]["kalshi_all_markets"] = descriptor
    manifest["base_row_count"] = 3
    pointer["kalshi_all_markets"] = dict(descriptor)
    _write_history_pointer_and_manifest(
        service,
        pointer=pointer,
        manifest_path=manifest_path,
        manifest=manifest,
    )

    index_path = service.ensure_known_key_index()
    with duckdb.connect(str(index_path), read_only=True) as connection:
        indexed = dict(
            connection.execute(
                "SELECT market_key, native_family FROM known_keys WHERE venue='kalshi'"
            ).fetchall()
        )
    assert indexed == {
        "KXMVE-EXPLICIT-CONVENTIONAL": "kalshi_conventional",
        "KX-EXPLICIT-MVE": "kalshi_mve",
    }

    compacted = await service.compact_history(force=True)
    assert compacted["published"] is True
    with duckdb.connect(database=":memory:") as connection:
        register_catalog_views(connection, service.market_root)
        classified = {
            row[0]: (row[1], row[2], row[3])
            for row in connection.execute(
                "SELECT market_key, native_family, family_provenance, operational_family "
                "FROM market_catalog_kalshi"
            ).fetchall()
        }
    assert classified == {
        "KXMVE-EXPLICIT-CONVENTIONAL": (
            "kalshi_conventional",
            "partition_provenance",
            "kalshi_conventional",
        ),
        "KX-EXPLICIT-MVE": (
            "kalshi_mve",
            "partition_provenance",
            "kalshi_mve",
        ),
    }


@pytest.mark.asyncio
async def test_weekly_promotion_is_unique_and_compaction_selects_latest_upsert(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    pm_first = FakeGamma(
        {
            (False, None): {"markets": [_pm("pm-new")], "next_cursor": ""},
            (True, None): {"markets": [], "next_cursor": ""},
        }
    )
    await service.discover("polymarket", client=pm_first)
    await service.discover(
        "kalshi-conventional",
        client=FakeKalshi(
            {
                ("only", None): [],
                ("exclude", None): [_kx("KXNEW")],
                (None, None): [_kx("KXNEW")],
            }
        ),
    )
    await service.discover(
        "kalshi-mve",
        client=FakeKalshi({("only", None): [_kx("KXMVE-NEW")]}),
    )
    promotion = service.promote_history()
    assert promotion["promoted_rows"] == {"polymarket": 1, "kalshi": 2}
    _manifest_path, promoted_manifest = service._history_manifest()
    for name, key in (
        ("polymarket_all_markets", "market_id"),
        ("kalshi_all_markets", "market_key"),
    ):
        profile = service._history_artifact_path(
            promoted_manifest, "polymarket" if key == "market_id" else "kalshi"
        )
        files = sorted(str(item) for item in profile.rglob("*.parquet"))
        with duckdb.connect(database=":memory:") as connection:
            row = connection.execute(
                f"SELECT count(*), count(DISTINCT {key}) "
                "FROM read_parquet(?, union_by_name=true, hive_partitioning=false)",
                [files],
            ).fetchone()
        assert row is not None and row[0] == row[1]

    newer = _pm(
        "pm-new",
        created=NOW,
        updated=NOW + timedelta(hours=1),
        question="Updated question",
    )
    await service.discover(
        "polymarket",
        client=FakeGamma(
            {
                (False, None): {"markets": [newer], "next_cursor": ""},
                (True, None): {"markets": [], "next_cursor": ""},
            }
        ),
    )
    compacted = await service.compact_history(force=True)
    assert compacted["published"] is True
    _path, compacted_manifest = service._history_manifest()
    pm_path = service._history_artifact_path(compacted_manifest, "polymarket")
    frame = pd.read_parquet(pm_path)
    selected = frame.loc[frame["market_id"] == "pm-new"].iloc[0]
    assert selected["question"] == "Updated question"
    assert compacted_manifest["promotion_layer_count"] == 0
    assert compacted_manifest["metadata_compacted_through_utc"]


@pytest.mark.asyncio
async def test_two_promotions_preserve_parent_and_accumulate_unique_layers(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    first_kx, second_kx = _kalshi_keys_in_same_bucket()

    await service.discover(
        "polymarket",
        client=FakeGamma(
            {
                (False, None): {"markets": [_pm("pm-one")], "next_cursor": ""},
                (True, None): {"markets": [], "next_cursor": ""},
            }
        ),
    )
    await service.discover(
        "kalshi-conventional",
        client=FakeKalshi(
            {
                ("only", None): [],
                ("exclude", None): [_kx(first_kx)],
                (None, None): [_kx(first_kx)],
            }
        ),
    )
    await service.discover(
        "kalshi-mve",
        client=FakeKalshi({("only", None): [_kx("KXMVE-ONE")]}),
    )
    first_result = service.promote_history()
    first_manifest_path, first_manifest = service._history_manifest()
    first_manifest_bytes = first_manifest_path.read_bytes()
    first_pm = service._history_artifact_path(first_manifest, "polymarket")
    first_kx_path = service._history_artifact_path(first_manifest, "kalshi")
    first_pm_tree = tree_sha256(first_pm)
    first_kx_tree = tree_sha256(first_kx_path)

    later = NOW + timedelta(hours=1)
    await service.discover(
        "polymarket",
        client=FakeGamma(
            {
                (False, None): {
                    "markets": [_pm("pm-two", created=later)],
                    "next_cursor": "",
                },
                (True, None): {"markets": [], "next_cursor": ""},
            }
        ),
    )
    await service.discover(
        "kalshi-conventional",
        client=FakeKalshi({("exclude", None): [_kx(second_kx, created=later)]}),
    )
    await service.discover(
        "kalshi-mve",
        client=FakeKalshi({("only", None): [_kx("KXMVE-TWO", created=later)]}),
    )
    second_result = service.promote_history()
    _second_manifest_path, second_manifest = service._history_manifest()
    second_pm = service._history_artifact_path(second_manifest, "polymarket")
    second_kx_path = service._history_artifact_path(second_manifest, "kalshi")

    assert first_result["release_id"] != second_result["release_id"]
    assert first_manifest_path.read_bytes() == first_manifest_bytes
    assert tree_sha256(first_pm) == first_pm_tree
    assert tree_sha256(first_kx_path) == first_kx_tree
    assert _history_keys(first_pm, "market_id") == {"pm-base", "pm-one"}
    assert _history_keys(first_kx_path, "market_key") == {
        "KXBASE",
        first_kx,
        "KXMVE-ONE",
    }
    assert _history_keys(second_pm, "market_id") == {
        "pm-base",
        "pm-one",
        "pm-two",
    }
    assert _history_keys(second_kx_path, "market_key") == {
        "KXBASE",
        first_kx,
        second_kx,
        "KXMVE-ONE",
        "KXMVE-TWO",
    }
    assert len(list(second_pm.glob("source=discovery_delta/promotion_id=*"))) == 2
    assert len(list(second_kx_path.glob("source=discovery_delta/promotion_id=*"))) == 2
    assert second_manifest["base_row_count"] == 2
    assert second_manifest["uncompacted_delta_rows"] == 6
    assert (
        sum(
            int(profile["row_count"])
            for profile in second_manifest["profiles"].values()
        )
        == 8
    )


@pytest.mark.asyncio
async def test_promotion_collision_preserves_parent_and_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _catalog(tmp_path)
    await service.discover(
        "polymarket",
        client=FakeGamma(
            {
                (False, None): {"markets": [_pm("pm-new")], "next_cursor": ""},
                (True, None): {"markets": [], "next_cursor": ""},
            }
        ),
    )
    await service.discover(
        "kalshi-conventional",
        client=FakeKalshi(
            {
                ("only", None): [],
                ("exclude", None): [],
                (None, None): [],
            }
        ),
    )
    await service.discover("kalshi-mve", client=FakeKalshi({("only", None): []}))
    pointer_before = service.history_pointer_path.read_bytes()
    parent_manifest_path, parent_manifest = service._history_manifest()
    parent_pm = service._history_artifact_path(parent_manifest, "polymarket")
    parent_hash = _sha(parent_pm)
    original_hardlink = market_catalog_fs._hardlink_artifact

    monkeypatch.setattr(
        market_catalog_fs,
        "_run_id",
        lambda prefix: (
            "market_history_collision"
            if prefix == "market_history"
            else f"{prefix}_unused"
        ),
    )

    def hardlink_with_collision(source: Path, target: Path) -> int:
        linked = original_hardlink(source, target)
        if target.name == "POLYMARKET_ALL_MARKETS.parquet":
            write_parquet(
                markets_dataframe([_pm("pm-collision")]),
                target
                / "source=discovery_delta"
                / "promotion_id=market_history_collision"
                / "native_family=polymarket"
                / "part-000000.parquet",
                schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
                strict=True,
            )
        return linked

    monkeypatch.setattr(
        market_catalog_fs, "_hardlink_artifact", hardlink_with_collision
    )
    with pytest.raises(CatalogError, match="collides with linked parent"):
        service.promote_history()

    assert service.history_pointer_path.read_bytes() == pointer_before
    assert parent_manifest_path.is_file()
    assert _sha(parent_pm) == parent_hash


def test_status_reports_metadata_and_deep_history_integrity(tmp_path: Path) -> None:
    service = _catalog(tmp_path)

    metadata = service.status()["history_integrity"]
    deep = service.status(deep=True)["history_integrity"]

    assert metadata["mode"] == "metadata"
    assert metadata["status"] == "valid"
    assert metadata["manifest_sha256"]
    assert set(metadata["artifacts"]) == {"polymarket", "kalshi"}
    assert all(
        artifact["content_hash_verified"] is False
        for artifact in metadata["artifacts"].values()
    )
    assert deep["mode"] == "deep"
    assert all(
        artifact["content_hash_verified"] is True
        for artifact in deep["artifacts"].values()
    )


def test_normal_status_detects_history_row_count_drift(tmp_path: Path) -> None:
    service = _catalog(tmp_path)
    _pointer, _manifest_path, manifest = _history_pointer_and_manifest(service)
    pm_path = Path(manifest["artifacts"]["polymarket_all_markets"]["path"])
    write_parquet(
        markets_dataframe([_pm("pm-base"), _pm("pm-extra")]),
        pm_path,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )

    with pytest.raises(CatalogError, match="row count is invalid"):
        service.status()


def test_normal_status_detects_history_file_count_drift(tmp_path: Path) -> None:
    service = _catalog(tmp_path)
    pointer, manifest_path, manifest = _history_pointer_and_manifest(service)
    kx_path = Path(manifest["artifacts"]["kalshi_all_markets"]["path"])
    frame = pd.read_parquet(kx_path)
    kx_path.unlink()
    first_part = (
        kx_path
        / "source=compacted_base"
        / "native_family=kalshi_conventional"
        / "bucket=1"
        / "part-000000.parquet"
    )
    write_parquet(
        frame,
        first_part,
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    descriptor = {
        "format": "partitioned_parquet",
        "parquet_file_count": 1,
        "path": str(kx_path.resolve()),
        "rows": 1,
        "sha256": None,
        "tree_sha256": tree_sha256(kx_path),
        "size_bytes": first_part.stat().st_size,
    }
    manifest["artifacts"]["kalshi_all_markets"] = descriptor
    pointer["kalshi_all_markets"] = dict(descriptor)
    _write_history_pointer_and_manifest(
        service,
        pointer=pointer,
        manifest_path=manifest_path,
        manifest=manifest,
    )
    write_parquet(
        kalshi_markets_dataframe([]),
        kx_path
        / "source=compacted_base"
        / "native_family=kalshi_conventional"
        / "bucket=2"
        / "part-000000.parquet",
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )

    with pytest.raises(CatalogError, match="file count is invalid"):
        service.status()


def test_deep_status_detects_same_size_payload_substitution(tmp_path: Path) -> None:
    service = _catalog(tmp_path)
    _pointer, _manifest_path, manifest = _history_pointer_and_manifest(service)
    pm_path = Path(manifest["artifacts"]["polymarket_all_markets"]["path"])
    with pm_path.open("r+b") as handle:
        handle.seek(16)
        original = handle.read(1)
        assert original
        handle.seek(16)
        handle.write(bytes([original[0] ^ 1]))

    assert service.status()["history_integrity"]["status"] == "valid"
    with pytest.raises(CatalogError, match="content hash is invalid"):
        service.status(deep=True)


def test_valid_partitioned_history_topology_passes_deep_validation(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    pointer, manifest_path, manifest = _history_pointer_and_manifest(service)
    kx_path = Path(manifest["artifacts"]["kalshi_all_markets"]["path"])
    frame = pd.read_parquet(kx_path)
    kx_path.unlink()
    parts = [
        kx_path
        / "source=compacted_base"
        / "native_family=kalshi_conventional"
        / f"bucket={bucket}"
        / "part-000000.parquet"
        for bucket in (7, 91)
    ]
    write_parquet(
        frame,
        parts[0],
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    write_parquet(
        kalshi_markets_dataframe([]),
        parts[1],
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    descriptor = {
        "format": "partitioned_parquet",
        "parquet_file_count": 2,
        "path": str(kx_path.resolve()),
        "rows": 1,
        "sha256": None,
        "tree_sha256": tree_sha256(kx_path),
        "size_bytes": sum(path.stat().st_size for path in parts),
    }
    manifest["artifacts"]["kalshi_all_markets"] = descriptor
    pointer["kalshi_all_markets"] = dict(descriptor)
    _write_history_pointer_and_manifest(
        service,
        pointer=pointer,
        manifest_path=manifest_path,
        manifest=manifest,
    )

    integrity = service.status(deep=True)["history_integrity"]
    assert integrity["artifacts"]["kalshi"] == {
        "rows": 1,
        "parquet_file_count": 2,
        "size_bytes": descriptor["size_bytes"],
        "content_hash_verified": True,
    }


@pytest.mark.asyncio
async def test_promotion_rechecks_parent_hashes_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _catalog(tmp_path)
    await service.discover(
        "polymarket",
        client=FakeGamma(
            {
                (False, None): {"markets": [_pm("pm-new")], "next_cursor": ""},
                (True, None): {"markets": [], "next_cursor": ""},
            }
        ),
    )
    await service.discover(
        "kalshi-conventional",
        client=FakeKalshi(
            {("only", None): [], ("exclude", None): [], (None, None): []}
        ),
    )
    await service.discover("kalshi-mve", client=FakeKalshi({("only", None): []}))
    pointer_before = service.history_pointer_path.read_bytes()
    _pointer, _manifest_path, parent = _history_pointer_and_manifest(service)
    pm_path = Path(parent["artifacts"]["polymarket_all_markets"]["path"])
    original_atomic_json = market_catalog_fs._atomic_json
    mutated = False

    def write_then_mutate_parent(path: Path, value: dict[str, Any]) -> None:
        nonlocal mutated
        original_atomic_json(path, value)
        if path.name == "PUBLISHED_MANIFEST.json" and ".staging" in path.parts:
            with pm_path.open("r+b") as handle:
                handle.seek(16)
                original = handle.read(1)
                assert original
                handle.seek(16)
                handle.write(bytes([original[0] ^ 1]))
            mutated = True

    monkeypatch.setattr(market_catalog_fs, "_atomic_json", write_then_mutate_parent)
    with pytest.raises(CatalogError, match="content hash is invalid"):
        service.promote_history()

    assert mutated is True
    assert service.history_pointer_path.read_bytes() == pointer_before


@pytest.mark.asyncio
async def test_compaction_rejects_stale_parent_before_staging(tmp_path: Path) -> None:
    service = _catalog(tmp_path)
    pointer_before = service.history_pointer_path.read_bytes()
    _pointer, _manifest_path, manifest = _history_pointer_and_manifest(service)
    pm_path = Path(manifest["artifacts"]["polymarket_all_markets"]["path"])
    with pm_path.open("r+b") as handle:
        handle.seek(16)
        original = handle.read(1)
        assert original
        handle.seek(16)
        handle.write(bytes([original[0] ^ 1]))

    with pytest.raises(CatalogError, match="content hash is invalid"):
        await service.compact_history(force=True)

    assert service.history_pointer_path.read_bytes() == pointer_before
    assert not (service.history_root / ".staging").exists()


@pytest.mark.asyncio
async def test_promotion_accounting_failure_preserves_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _catalog(tmp_path)
    await service.discover(
        "polymarket",
        client=FakeGamma(
            {
                (False, None): {"markets": [_pm("pm-new")], "next_cursor": ""},
                (True, None): {"markets": [], "next_cursor": ""},
            }
        ),
    )
    await service.discover(
        "kalshi-conventional",
        client=FakeKalshi(
            {("only", None): [], ("exclude", None): [], (None, None): []}
        ),
    )
    await service.discover("kalshi-mve", client=FakeKalshi({("only", None): []}))
    pointer_before = service.history_pointer_path.read_bytes()
    original_profile = market_catalog_fs._profile_history_artifact

    def miscount(path: Path, *, key_column: str) -> dict[str, int]:
        profile = original_profile(path, key_column=key_column)
        if key_column == "market_id" and ".staging" in path.parts:
            profile["row_count"] += 1
        return profile

    monkeypatch.setattr(market_catalog_fs, "_profile_history_artifact", miscount)
    with pytest.raises(CatalogError, match="row accounting mismatch"):
        service.promote_history()

    assert service.history_pointer_path.read_bytes() == pointer_before


def test_status_cli_reports_integrity_and_fails_for_invalid_history(
    tmp_path: Path,
) -> None:
    service = _catalog(tmp_path)
    runner = CliRunner()
    common = ["status", "--market-root", str(service.market_root)]

    normal = runner.invoke(markets_app, common)
    deep = runner.invoke(markets_app, [*common, "--deep"])
    assert normal.exit_code == 0
    assert deep.exit_code == 0
    assert json.loads(normal.stdout)["history_integrity"]["mode"] == "metadata"
    assert json.loads(deep.stdout)["history_integrity"]["mode"] == "deep"

    _pointer, _manifest_path, manifest = _history_pointer_and_manifest(service)
    pm_path = Path(manifest["artifacts"]["polymarket_all_markets"]["path"])
    write_parquet(
        markets_dataframe([_pm("pm-base"), _pm("pm-extra")]),
        pm_path,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    invalid = runner.invoke(markets_app, common)
    assert invalid.exit_code != 0


def test_discovery_manifest_contract_is_json_only() -> None:
    assert DISCOVERY_MANIFEST_SCHEMA == "pmkt.market_discovery_manifest.v1"


def test_catalog_module_has_no_trading_or_order_submission_imports() -> None:
    catalog_module = Path("src/pmkt/data/market_catalog.py")
    catalog_package = Path("src/pmkt/data/market_catalog")
    source_paths = (
        [catalog_module]
        if catalog_module.exists()
        else sorted(catalog_package.rglob("*.py"))
    )
    assert source_paths

    imported: set[str] = set()
    sources: list[str] = []
    for source_path in source_paths:
        source = source_path.read_text(encoding="utf-8")
        sources.append(source)
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported.add(node.module)

    assert not any(
        name.startswith(("pmkt.execution", "pmkt.trading", "pmkt.oms"))
        for name in imported
    )
    assert "submit_order" not in "\n".join(sources)
