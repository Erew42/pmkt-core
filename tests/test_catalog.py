from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import builtins
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
from typing import Any

import pytest

from pmkt.catalog import CatalogReference, CatalogSnapshot
from pmkt.data.market_catalog.fs import _artifact
from pmkt.data.market_catalog.reader import load_latest_history
from pmkt.data.market_catalog.service import MarketCatalogService
from pmkt.data.market_catalog.types import CatalogError
from pmkt.errors import ResultLimitExceededError
from pmkt.data.normalize import markets_dataframe
from pmkt.data.registry import (
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
)
from pmkt.data.storage.parquet import write_parquet
from pmkt.exchanges.kalshi.client import kalshi_markets_dataframe


NOW = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _pm(key: str) -> dict[str, Any]:
    return {
        "id": key,
        "slug": f"market-{key}",
        "question": f"Question {key}?",
        "createdAt": NOW.isoformat(),
        "updatedAt": NOW.isoformat(),
        "closed": False,
    }


def _kx(key: str) -> dict[str, Any]:
    return {
        "ticker": key,
        "title": f"Question {key}?",
        "created_time": NOW.isoformat(),
        "updated_time": NOW.isoformat(),
        "close_time": (NOW + timedelta(days=1)).isoformat(),
        "status": "active",
    }


def _catalog_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    path_base = tmp_path / "publisher"
    market_root = path_base / "data" / "markets"
    release = market_root / "history" / "releases" / "history-base"
    pm_path = release / "POLYMARKET_ALL_MARKETS.parquet"
    kx_path = release / "KALSHI_ALL_MARKETS.parquet"
    write_parquet(
        markets_dataframe([_pm("pm-one")]),
        pm_path,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    # This retained writer produces nullable all-null columns with Arrow null
    # physical types; the public validator must use canonical validation semantics.
    write_parquet(
        kalshi_markets_dataframe([_kx("KX-ONE")]),
        kx_path,
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    manifest = {
        "schema_version": "pmkt.market_history_release.v1",
        "dataset_family": "market_history",
        "release_id": "history-base",
        "release_kind": "synthetic_offline_fixture",
        "status": "completed",
        "grain": "one latest-known row per venue market",
        "published_at_utc": NOW.isoformat(),
        "base_row_count": 2,
        "uncompacted_delta_rows": 0,
        "artifacts": {
            "polymarket_all_markets": _artifact(
                pm_path,
                repository_root=path_base,
                rows=1,
                schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
            ),
            "kalshi_all_markets": _artifact(
                kx_path,
                repository_root=path_base,
                rows=1,
                schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            ),
        },
        "network_accessed": False,
        "research_only": True,
        "execution_authority": False,
        "orders_submitted": False,
    }
    manifest_path = release / "PUBLISHED_MANIFEST.json"
    _write_json(manifest_path, manifest)
    pointer = {
        "dataset_family": "market_history",
        "release_id": "history-base",
        "manifest": {
            "path": manifest_path.relative_to(path_base).as_posix(),
            "sha256": _sha(manifest_path),
        },
        **manifest["artifacts"],
    }
    _write_json(market_root / "history" / "LATEST.json", pointer)
    _write_json(market_root / "LATEST.json", {"release_id": "current-decoy"})
    _write_json(
        market_root / "DISCOVERY_LATEST.json",
        {
            "schema_version": "pmkt.market_discovery_pointer.v1",
            "dataset_family": "market_discovery",
            "release_id": "discovery-decoy",
            "streams": {},
        },
    )
    return path_base, market_root, manifest_path


def _rewrite_manifest_and_pointer(
    market_root: Path, manifest_path: Path, manifest: dict[str, Any]
) -> None:
    _write_json(manifest_path, manifest)
    pointer_path = market_root / "history" / "LATEST.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest"]["sha256"] = _sha(manifest_path)
    for artifact_name in ("polymarket_all_markets", "kalshi_all_markets"):
        pointer[artifact_name] = manifest["artifacts"][artifact_name]
    _write_json(pointer_path, pointer)


def _make_directory_link(link: Path, target: Path) -> None:
    if os.name == "nt":
        created = subprocess.run(
            ["cmd.exe", "/c", "mklink", "/J", str(link), str(target)],
            capture_output=True,
            text=True,
            check=False,
        )
        if created.returncode != 0:
            pytest.skip(f"junction creation unavailable: {created.stderr}")
    else:
        link.symlink_to(target, target_is_directory=True)


def test_open_latest_history_selects_only_history_pointer_and_validates_full(
    tmp_path: Path,
) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)

    with CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base, validation="full"
    ) as catalog:
        assert catalog.reference.release_id == "history-base"
        assert catalog.reference.manifest_locator == manifest_path.relative_to(
            path_base
        ).as_posix()
        assert catalog.reference.identity_verification == "history_pointer"
        assert catalog.reference.dataset_schema_versions == (
            ("kalshi", KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION),
            ("polymarket", POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION),
        )
        assert catalog.validation.valid is True
        assert catalog.validation.row_count == 2
        assert catalog.validation.parquet_file_count == 2
        assert "artifact_content_hashes" in catalog.validation.performed_checks
        assert catalog.validation.skipped_checks == ()


def test_relative_market_root_is_resolved_from_explicit_path_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path_base, _market_root, _manifest_path = _catalog_fixture(tmp_path)
    unrelated_cwd = tmp_path / "unrelated-cwd"
    unrelated_cwd.mkdir()
    monkeypatch.chdir(unrelated_cwd)

    catalog = CatalogSnapshot.open_latest_history(
        Path("data/markets"), path_base=path_base
    )

    assert catalog.reference.release_id == "history-base"


def test_missing_history_does_not_fallback_to_other_pointers(tmp_path: Path) -> None:
    path_base = tmp_path / "publisher"
    market_root = path_base / "data" / "markets"
    _write_json(market_root / "LATEST.json", {"release_id": "current"})
    _write_json(market_root / "DISCOVERY_LATEST.json", {"release_id": "discovery"})

    with pytest.raises(CatalogError, match="promote-history"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)


def test_latest_pointer_and_manifest_are_each_read_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    import pmkt.data.market_catalog.reader as reader

    original = reader._read_json_object
    reads: list[Path] = []

    def counted(path: Path) -> tuple[bytes, dict[str, Any]]:
        reads.append(path)
        return original(path)

    monkeypatch.setattr(reader, "_read_json_object", counted)

    state = load_latest_history(market_root, path_base=path_base)

    assert state.row_count == 2
    assert len(reads) == 2
    assert reads[0].name == "LATEST.json"
    assert reads[1].name == "PUBLISHED_MANIFEST.json"


def test_reference_json_is_deterministic_exclusive_and_reopenable(
    tmp_path: Path,
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    catalog = CatalogSnapshot.open_latest_history(market_root, path_base=path_base)
    reference_path = tmp_path / "pins" / "catalog.json"

    catalog.reference.write_json(reference_path)
    first_bytes = reference_path.read_bytes()
    with pytest.raises(FileExistsError):
        catalog.reference.write_json(reference_path)
    catalog.reference.write_json(reference_path, overwrite=True)

    assert reference_path.read_bytes() == first_bytes
    assert CatalogReference.read_json(reference_path) == catalog.reference
    reopened = CatalogSnapshot.open(reference_path)
    assert reopened.reference == catalog.reference
    assert reopened.validation.identity_verification == "saved_reference"


def test_open_manifest_records_external_or_internal_identity(tmp_path: Path) -> None:
    path_base, _market_root, manifest_path = _catalog_fixture(tmp_path)

    internal = CatalogSnapshot.open_manifest(manifest_path, path_base=path_base)
    external = CatalogSnapshot.open_manifest(
        manifest_path, expected_sha256=_sha(manifest_path), path_base=path_base
    )

    assert internal.reference.identity_verification == "internal_consistency"
    assert external.reference.identity_verification == "external_sha256"
    with pytest.raises(CatalogError, match="manifest hash is invalid"):
        CatalogSnapshot.open_manifest(
            manifest_path, expected_sha256="0" * 64, path_base=path_base
        )


def test_metadata_validation_records_skipped_content_checks(tmp_path: Path) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)

    catalog = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base, validation="metadata"
    )

    assert catalog.validation.valid is True
    assert "artifact_content_hashes" in catalog.validation.skipped_checks
    assert "artifact_canonical_schemas" in catalog.validation.skipped_checks


def test_full_rejects_same_size_tampering_that_metadata_skips(tmp_path: Path) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    pm_path = path_base / manifest["artifacts"]["polymarket_all_markets"]["path"]
    with pm_path.open("r+b") as handle:
        handle.seek(16)
        original = handle.read(1)
        assert original
        handle.seek(16)
        handle.write(bytes([original[0] ^ 1]))

    metadata = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base, validation="metadata"
    )
    assert "artifact_content_hashes" in metadata.validation.skipped_checks
    with pytest.raises(CatalogError):
        CatalogSnapshot.open_latest_history(
            market_root, path_base=path_base, validation="full"
        )


def test_pointer_manifest_artifact_disagreement_is_rejected(tmp_path: Path) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    pointer_path = market_root / "history" / "LATEST.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["kalshi_all_markets"]["rows"] = 999
    _write_json(pointer_path, pointer)

    with pytest.raises(CatalogError, match="pointer and manifest disagree"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)


def test_full_validates_direct_parent_manifest_hash_and_metadata_records_skip(
    tmp_path: Path,
) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    parent_path = path_base / "history-parent" / "PUBLISHED_MANIFEST.json"
    _write_json(
        parent_path,
        {
            "schema_version": "pmkt.market_history_release.v1",
            "dataset_family": "market_history",
            "release_id": "parent-release",
        },
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["parent_release"] = {
        "release_id": "parent-release",
        "manifest_path": parent_path.relative_to(path_base).as_posix(),
        "manifest_sha256": _sha(parent_path),
    }
    _rewrite_manifest_and_pointer(market_root, manifest_path, manifest)
    parent_path.write_text(parent_path.read_text(encoding="utf-8") + " ", encoding="utf-8")

    metadata = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base, validation="metadata"
    )
    assert "parent_manifest_content_hashes" in metadata.validation.skipped_checks
    with pytest.raises(CatalogError, match="parent history manifest hash is invalid"):
        CatalogSnapshot.open_latest_history(
            market_root, path_base=path_base, validation="full"
        )


def test_parent_manifest_path_escape_is_rejected(tmp_path: Path) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["parent_release"] = {
        "release_id": "outside",
        "manifest_path": "../outside/PUBLISHED_MANIFEST.json",
        "manifest_sha256": "0" * 64,
    }
    _rewrite_manifest_and_pointer(market_root, manifest_path, manifest)

    with pytest.raises(CatalogError, match="parent history manifest escapes"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)


def test_saved_reference_relocates_after_recorded_base_is_gone(tmp_path: Path) -> None:
    old_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=old_base
    ).reference
    new_base = tmp_path / "relocated-publisher"
    old_base.rename(new_base)

    reopened = CatalogSnapshot.open(reference, relocation={old_base: new_base})

    assert not old_base.exists()
    assert reopened.reference == reference
    assert reopened.validation.row_count == 2


def test_saved_reference_relocates_from_opposite_path_flavor(tmp_path: Path) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base
    ).reference
    foreign_base = "/publisher" if os.name == "nt" else r"C:\publisher"
    foreign = replace(reference, path_base=foreign_base)
    saved = tmp_path / "foreign-reference.json"
    foreign.write_json(saved)

    reopened = CatalogSnapshot.open(
        saved, relocation={foreign_base: path_base}
    )

    assert reopened.reference == foreign
    assert not Path(foreign.manifest_locator).is_absolute()
    assert reopened.validation.row_count == 2


def test_absolute_posix_manifest_locator_relocates_on_every_host(tmp_path: Path) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base
    ).reference
    recorded_base = PurePosixPath("/publisher")
    recorded_locator = recorded_base / PurePosixPath(reference.manifest_locator)
    portable = replace(
        reference,
        path_base=str(recorded_base),
        manifest_locator=str(recorded_locator),
    )
    saved = tmp_path / "absolute-posix-reference.json"
    portable.write_json(saved)

    reopened = CatalogSnapshot.open(
        saved, relocation={str(recorded_base): path_base}
    )

    assert reopened.reference == portable
    assert reopened.validation.row_count == 2


def test_unc_effective_base_and_network_locator_are_rejected_before_io(
    tmp_path: Path,
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base
    ).reference
    network_path = (
        r"\\server\share\catalog" if os.name == "nt" else "//server/share/catalog"
    )

    with pytest.raises(CatalogError, match="local filesystem path"):
        CatalogSnapshot.open(
            reference, relocation={reference.path_base: network_path}
        )
    with pytest.raises(CatalogError, match="local filesystem path"):
        CatalogSnapshot.open(replace(reference, manifest_locator=network_path))
    with pytest.raises(CatalogError, match="local filesystem path"):
        CatalogSnapshot.open(replace(reference, manifest_locator="s3://bucket/catalog"))


def test_mixed_absolute_locator_flavor_is_rejected(tmp_path: Path) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base
    ).reference
    if os.name == "nt":
        foreign_base = "/publisher"
        incompatible_locator = r"C:\publisher\manifest.json"
    else:
        foreign_base = r"C:\publisher"
        incompatible_locator = "/publisher/manifest.json"
    foreign = replace(
        reference,
        path_base=foreign_base,
        manifest_locator=incompatible_locator,
    )

    with pytest.raises(CatalogError, match="incompatible path flavor"):
        CatalogSnapshot.open(foreign, relocation={foreign_base: path_base})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("reference_format_version", "pmkt.catalog_reference.v999", "reference"),
        ("view_contract_version", "pmkt.catalog_views.v999", "view contract"),
        ("manifest_sha256", "not-a-hash", "manifest hash"),
        ("path_base", "relative/path", "path_base"),
        ("dataset_schema_versions", None, "schema versions"),
    ],
)
def test_malformed_reference_json_is_rejected(
    tmp_path: Path, field: str, value: Any, message: str
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base
    ).reference.to_dict()
    reference[field] = value
    path = tmp_path / "malformed-reference.json"
    _write_json(path, reference)

    with pytest.raises(CatalogError, match=message):
        CatalogReference.read_json(path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("release_id", "different-supported-shape"),
        (
            "dataset_schema_versions",
            {
                "kalshi": "kalshi_market_snapshot.v999",
                "polymarket": POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
            },
        ),
    ],
)
def test_supported_shape_reference_identity_mismatch_is_rejected(
    tmp_path: Path, field: str, value: Any
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base
    ).reference.to_dict()
    reference[field] = value
    path = tmp_path / "mismatched-reference.json"
    _write_json(path, reference)

    with pytest.raises(CatalogError, match="disagrees with manifest identity"):
        CatalogSnapshot.open(path)


def test_artifact_path_escape_is_rejected_before_traversal(tmp_path: Path) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"]["polymarket_all_markets"]["path"] = "../outside"
    _write_json(manifest_path, manifest)
    pointer_path = market_root / "history" / "LATEST.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest"]["sha256"] = _sha(manifest_path)
    pointer["polymarket_all_markets"] = manifest["artifacts"][
        "polymarket_all_markets"
    ]
    _write_json(pointer_path, pointer)

    with pytest.raises(CatalogError, match="escapes declared path base"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)


def test_reordered_canonical_columns_remain_compatible(tmp_path: Path) -> None:
    import pandas as pd

    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    pm_path = (
        path_base
        / "data"
        / "markets"
        / "history"
        / "releases"
        / "history-base"
        / "POLYMARKET_ALL_MARKETS.parquet"
    )
    frame = pd.read_parquet(pm_path)
    write_parquet(
        frame[list(reversed(frame.columns))],
        pm_path,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"]["polymarket_all_markets"] = _artifact(
        pm_path,
        repository_root=path_base,
        rows=1,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    _rewrite_manifest_and_pointer(market_root, manifest_path, manifest)

    with CatalogSnapshot.open_latest_history(
        market_root, path_base=path_base
    ) as catalog:
        result = catalog.query("SELECT market_id FROM market_catalog_polymarket")

    assert result.to_arrow().to_pylist() == [{"market_id": "pm-one"}]


def test_repository_fixture_generator_creates_reopenable_offline_catalog(
    tmp_path: Path,
) -> None:
    output = tmp_path / "synthetic"
    command = [
        sys.executable,
        str(ROOT / "scripts" / "create_synthetic_catalog_fixture.py"),
        "--output",
        str(output),
    ]

    created = subprocess.run(
        command, cwd=ROOT, capture_output=True, text=True, check=False
    )
    duplicate = subprocess.run(
        command, cwd=ROOT, capture_output=True, text=True, check=False
    )

    assert created.returncode == 0, created.stderr
    assert duplicate.returncode != 0
    with CatalogSnapshot.open(output / "CATALOG_REFERENCE.json") as catalog:
        result = catalog.query(
            "SELECT venue, market_key, question FROM market_catalog "
            "ORDER BY venue, market_key",
            max_result_rows=10_000,
            max_result_bytes=16 * 1024 * 1024,
        )
    assert result.to_arrow().to_pylist() == [
        {
            "venue": "kalshi",
            "market_key": "KXSYNTHETIC-1",
            "question": "Will the synthetic fixture remain offline?",
        },
        {
            "venue": "polymarket",
            "market_key": "synthetic-polymarket-1",
            "question": "Will the synthetic fixture remain offline?",
        },
    ]


def test_legacy_service_retains_absolute_external_artifact_behavior(
    tmp_path: Path,
) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    descriptor = manifest["artifacts"]["polymarket_all_markets"]
    source = path_base / descriptor["path"]
    outside = tmp_path / "legacy-external" / source.name
    outside.parent.mkdir()
    source.replace(outside)
    descriptor["path"] = str(outside.resolve())
    _rewrite_manifest_and_pointer(market_root, manifest_path, manifest)

    assert MarketCatalogService(market_root).status(deep=True)["history_integrity"][
        "status"
    ] == "valid"
    with pytest.raises(CatalogError, match="escapes declared path base"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)


def test_artifact_junction_or_symlink_escape_is_rejected(tmp_path: Path) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    outside = tmp_path / "outside-evidence"
    outside.mkdir()
    target = outside / "POLYMARKET_ALL_MARKETS.parquet"
    source = (
        path_base
        / "data"
        / "markets"
        / "history"
        / "releases"
        / "history-base"
        / "POLYMARKET_ALL_MARKETS.parquet"
    )
    shutil.copy2(source, target)
    link = path_base / "escaped-artifact"
    _make_directory_link(link, outside)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    descriptor = manifest["artifacts"]["polymarket_all_markets"]
    descriptor["format"] = "partitioned_parquet"
    descriptor["path"] = "escaped-artifact"
    descriptor["tree_sha256"] = descriptor.pop("sha256")
    _write_json(manifest_path, manifest)
    pointer_path = market_root / "history" / "LATEST.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest"]["sha256"] = _sha(manifest_path)
    pointer["polymarket_all_markets"] = descriptor
    _write_json(pointer_path, pointer)

    with pytest.raises(CatalogError, match="escapes declared path base"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)


def test_nested_link_escape_is_rejected_without_scanning_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    outside = tmp_path / "outside-nested"
    outside.mkdir()
    (outside / "do-not-scan").mkdir()
    artifact_root = path_base / "nested-artifact"
    artifact_root.mkdir()
    _make_directory_link(artifact_root / "escape", outside)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    descriptor = manifest["artifacts"]["polymarket_all_markets"]
    descriptor.update(
        {
            "format": "partitioned_parquet",
            "path": "nested-artifact",
            "tree_sha256": descriptor.pop("sha256"),
        }
    )
    _rewrite_manifest_and_pointer(market_root, manifest_path, manifest)
    import pmkt.data.market_catalog.reader as reader

    original_scandir = reader.os.scandir
    scanned: list[Path] = []

    def tracked(path: str | Path) -> Any:
        scanned.append(Path(path).resolve())
        return original_scandir(path)

    monkeypatch.setattr(reader.os, "scandir", tracked)

    with pytest.raises(CatalogError, match="escapes declared path base"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)
    assert outside.resolve() not in scanned


def test_directory_link_cycle_is_rejected(tmp_path: Path) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    artifact_root = path_base / "cyclic-artifact"
    artifact_root.mkdir()
    _make_directory_link(artifact_root / "cycle", artifact_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    descriptor = manifest["artifacts"]["polymarket_all_markets"]
    descriptor.update(
        {
            "format": "partitioned_parquet",
            "path": "cyclic-artifact",
            "tree_sha256": descriptor.pop("sha256"),
        }
    )
    _rewrite_manifest_and_pointer(market_root, manifest_path, manifest)

    with pytest.raises(CatalogError, match="link cycle or alias"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)


def test_managed_query_registers_native_and_common_views(tmp_path: Path) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)

    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        result = catalog.query(
            "SELECT venue, count(*) AS n FROM market_catalog "
            "GROUP BY venue ORDER BY venue"
        )
        polymarket = catalog.query(
            "SELECT market_id, family_provenance FROM market_catalog_polymarket"
        )
        kalshi = catalog.query(
            "SELECT market_key, native_family, family_provenance "
            "FROM market_catalog_kalshi"
        )

    assert result.to_arrow().to_pylist() == [
        {"venue": "kalshi", "n": 1},
        {"venue": "polymarket", "n": 1},
    ]
    assert polymarket.to_arrow().to_pylist() == [
        {"market_id": "pm-one", "family_provenance": "venue_identity"}
    ]
    assert kalshi.to_arrow().to_pylist() == [
        {
            "market_key": "KX-ONE",
            "native_family": "kalshi_conventional",
            "family_provenance": "legacy_ticker_compat",
        }
    ]
    assert result.row_count == 2
    assert result.reference.release_id == "history-base"
    assert result.to_arrow().to_pylist()


def test_managed_query_uses_root_relative_partition_provenance(
    tmp_path: Path,
) -> None:
    path_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    old_file = (
        path_base
        / "data"
        / "markets"
        / "history"
        / "releases"
        / "history-base"
        / "KALSHI_ALL_MARKETS.parquet"
    )
    partition_root = old_file.with_suffix("")
    part = (
        partition_root
        / "source=compacted_base"
        / "native_family=kalshi_conventional"
        / "bucket=1"
        / "part-000000.parquet"
    )
    part.parent.mkdir(parents=True)
    old_file.replace(part)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"]["kalshi_all_markets"] = _artifact(
        partition_root,
        repository_root=path_base,
        rows=1,
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    _rewrite_manifest_and_pointer(market_root, manifest_path, manifest)

    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        result = catalog.query(
            "SELECT native_family, family_provenance FROM market_catalog_kalshi"
        )

    assert result.to_arrow().to_pylist() == [
        {
            "native_family": "kalshi_conventional",
            "family_provenance": "partition_provenance",
        }
    ]


def test_misleading_absolute_ancestor_does_not_change_family(tmp_path: Path) -> None:
    old_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=old_base
    ).reference
    new_base = tmp_path / "native_family=kalshi_mve" / "relocated"
    new_base.parent.mkdir()
    old_base.rename(new_base)

    with CatalogSnapshot.open(
        reference, relocation={old_base: new_base}
    ) as catalog:
        result = catalog.query(
            "SELECT native_family, family_provenance FROM market_catalog_kalshi"
        )

    assert result.to_arrow().to_pylist() == [
        {
            "native_family": "kalshi_conventional",
            "family_provenance": "legacy_ticker_compat",
        }
    ]


def test_unicode_ancestor_preserves_explicit_partition_family(tmp_path: Path) -> None:
    old_base, market_root, manifest_path = _catalog_fixture(tmp_path)
    old_file = (
        old_base
        / "data"
        / "markets"
        / "history"
        / "releases"
        / "history-base"
        / "KALSHI_ALL_MARKETS.parquet"
    )
    partition_root = old_file.with_suffix("")
    part = (
        partition_root
        / "native_family=kalshi_mve"
        / "part-000000.parquet"
    )
    part.parent.mkdir(parents=True)
    old_file.replace(part)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"]["kalshi_all_markets"] = _artifact(
        partition_root,
        repository_root=old_base,
        rows=1,
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    _rewrite_manifest_and_pointer(market_root, manifest_path, manifest)
    reference = CatalogSnapshot.open_latest_history(
        market_root, path_base=old_base
    ).reference
    new_base = tmp_path / "Straße" / "relocated"
    new_base.parent.mkdir()
    old_base.rename(new_base)

    with CatalogSnapshot.open(
        reference, relocation={old_base: new_base}
    ) as catalog:
        result = catalog.query(
            "SELECT market_key, native_family, family_provenance "
            "FROM market_catalog_kalshi"
        )

    assert result.to_arrow().to_pylist() == [
        {
            "market_key": "KX-ONE",
            "native_family": "kalshi_mve",
            "family_provenance": "partition_provenance",
        }
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; SELECT 2",
        "CREATE TABLE nope(i INTEGER)",
        "COPY (SELECT 1) TO 'nope.parquet'",
        "ATTACH 'nope.duckdb' AS nope",
        "SET threads=1",
        "PRAGMA threads=1",
        "INSTALL httpfs",
        "LOAD httpfs",
    ],
)
def test_managed_query_rejects_non_select_statements(
    tmp_path: Path, sql: str
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        with pytest.raises(ValueError, match="one parsed SELECT"):
            catalog.query(sql)


def test_parser_classified_metadata_pragma_and_with_select_are_allowed(
    tmp_path: Path,
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        pragma = catalog.query("PRAGMA version")
        selected = catalog.query("WITH x AS (SELECT 7 AS n) SELECT n FROM x")

    assert pragma.row_count == 1
    assert selected.to_arrow().to_pylist() == [{"n": 7}]


def test_exact_allowlist_blocks_undeclared_local_network_and_replacements(
    tmp_path: Path,
) -> None:
    import duckdb
    import pyarrow as pa

    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    undeclared = path_base / "undeclared.parquet"
    write_parquet(markets_dataframe([_pm("hidden")]), undeclared)
    undeclared_csv = path_base / "undeclared.csv"
    undeclared_csv.write_text("value\nsecret\n", encoding="utf-8")
    ambient_arrow = pa.table({"ambient": [1, 2]})  # noqa: F841
    with duckdb.connect(database=":memory:") as unmanaged:
        assert unmanaged.execute("SELECT count(*) FROM ambient_arrow").fetchone() == (
            2,
        )
    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        for sql in (
            f"SELECT * FROM read_parquet('{undeclared.as_posix()}')",
            f"SELECT * FROM read_csv_auto('{undeclared_csv.as_posix()}')",
            "SELECT * FROM read_parquet('https://example.invalid/catalog.parquet')",
            "SELECT * FROM read_parquet('s3://pmkt-invalid/catalog.parquet')",
            "SELECT * FROM ambient_arrow",
        ):
            with pytest.raises(Exception):
                catalog.query(sql)
        settings = catalog.query(
            "SELECT name, value FROM duckdb_settings() WHERE name IN "
            "('enable_external_access', 'autoinstall_known_extensions', "
            "'autoload_known_extensions', 'python_enable_replacements') "
            "ORDER BY name"
        )

    assert settings.to_arrow().to_pylist() == [
        {"name": "autoinstall_known_extensions", "value": "false"},
        {"name": "autoload_known_extensions", "value": "false"},
        {"name": "enable_external_access", "value": "false"},
        {"name": "python_enable_replacements", "value": "false"},
    ]


def test_query_caps_raise_without_truncation_and_connection_is_reusable(
    tmp_path: Path,
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        with pytest.raises(ResultLimitExceededError, match="max_result_rows=2"):
            catalog.query("SELECT * FROM range(10)", max_result_rows=2)
        assert catalog.query("SELECT 42 AS answer").to_arrow().to_pylist() == [
            {"answer": 42}
        ]
        with pytest.raises(ResultLimitExceededError, match="max_result_bytes=1"):
            catalog.query("SELECT repeat('x', 100) AS payload", max_result_bytes=1)
        assert catalog.query("SELECT 43 AS answer").to_arrow().to_pylist() == [
            {"answer": 43}
        ]


def test_multi_batch_overflow_stops_before_materialization_and_reuses_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    import pmkt.catalog as catalog_module

    materialized = False
    original = catalog_module._table_from_batches

    def tracked(pa: Any, batches: list[Any], schema: Any) -> Any:
        nonlocal materialized
        materialized = True
        return original(pa, batches, schema)

    monkeypatch.setattr(catalog_module, "_table_from_batches", tracked)
    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        with pytest.raises(ResultLimitExceededError, match="max_result_rows=70000"):
            catalog.query(
                "SELECT i FROM range(150000) AS t(i)", max_result_rows=70_000
            )
        assert materialized is False
        assert catalog.query("SELECT 44 AS answer").to_arrow().to_pylist() == [
            {"answer": 44}
        ]
        assert materialized is True


def test_query_result_outlives_context_and_captures_normalized_parameters(
    tmp_path: Path,
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    offset_time = datetime(2026, 9, 11, 14, 0, tzinfo=timezone(timedelta(hours=2)))
    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        result = catalog.query(
            "SELECT ? AS flag, ? AS n, ? AS observed, ? AS payload",
            parameters=(True, 7, offset_time, b"bytes"),
        )

    assert result.parameters == (
        True,
        7,
        datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
        b"bytes",
    )
    assert result.row_count == 1
    assert result.duckdb_version
    assert result.pyarrow_version
    assert result.to_pandas().iloc[0]["n"] == 7


@pytest.mark.parametrize(
    ("parameters", "error"),
    [
        ((float("nan"),), ValueError),
        ((float("inf"),), ValueError),
        ((datetime(2026, 9, 11),), ValueError),
        ((object(),), TypeError),
    ],
)
def test_query_rejects_unsupported_parameters(
    tmp_path: Path, parameters: tuple[Any, ...], error: type[Exception]
) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    with CatalogSnapshot.open_latest_history(market_root, path_base=path_base) as catalog:
        with pytest.raises(error):
            catalog.query("SELECT ?", parameters=parameters)


def test_closed_snapshot_refuses_query_and_reentry(tmp_path: Path) -> None:
    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    catalog = CatalogSnapshot.open_latest_history(market_root, path_base=path_base)
    catalog.close()

    with pytest.raises(RuntimeError, match="closed"):
        catalog.query("SELECT 1")
    with pytest.raises(RuntimeError, match="closed"):
        catalog.__enter__()


def test_catalog_open_reports_missing_arrow_as_optional_dependency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pmkt.errors import OptionalDependencyError

    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    original_import = builtins.__import__

    def blocked(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "pyarrow.parquet":
            raise ImportError("blocked for base-install simulation")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    with pytest.raises(OptionalDependencyError, match=r"pmkt\[data\]"):
        CatalogSnapshot.open_latest_history(market_root, path_base=path_base)


@pytest.mark.parametrize("version", ["1.5.4", "development-build"])
def test_managed_query_rejects_unqualified_duckdb_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    import duckdb
    from pmkt.errors import OptionalDependencyError

    path_base, market_root, _manifest_path = _catalog_fixture(tmp_path)
    catalog = CatalogSnapshot.open_latest_history(market_root, path_base=path_base)
    monkeypatch.setattr(duckdb, "__version__", version)

    with pytest.raises(OptionalDependencyError, match="duckdb>=1.5.5"):
        catalog.query("SELECT 1")
