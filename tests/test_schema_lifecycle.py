from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pmkt.data.registry import list_table_specs
from scripts.inventory_schema_usage import (
    inventory_schema_usage,
    lifecycle_entries,
    load_lifecycle_catalog,
    main,
    validate_lifecycle_catalog,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "docs" / "schema_lifecycle.json"


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_versioned_parquet(path: Path, versions: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "schema_version": versions,
                "value": list(range(len(versions))),
            }
        ),
        path,
    )
    return path


def test_lifecycle_catalog_covers_registry_exactly_once() -> None:
    catalog = load_lifecycle_catalog(CATALOG_PATH)
    registered = [spec.version for spec in list_table_specs()]

    entries = validate_lifecycle_catalog(catalog, registered)

    assert set(entries) == set(registered)
    assert len(entries) == len(registered) == 54
    assert entries["market_match.v2"]["status"] == "active_core"
    assert entries["market_match.v1"]["status"] == "compatibility_legacy"
    assert entries["instrument.v1"]["status"] == "provisional_unintegrated"
    assert entries["polymarket_market_snapshot.v2"]["status"] == "removal_candidate"
    assert entries["market_taxonomy_evidence.v1"]["status"] == "active_experiment"


def test_candidate_catalog_entries_have_concrete_evidence_and_decisions() -> None:
    entries = lifecycle_entries(load_lifecycle_catalog(CATALOG_PATH))
    candidates = {
        schema: entry
        for schema, entry in entries.items()
        if entry["status"] == "removal_candidate"
    }

    assert candidates
    for entry in candidates.values():
        assert entry["persistence"].strip()
        assert entry["tests"]
        assert entry["decision"].strip()
        assert entry["stop_conditions"]
        assert entry["rollback"].strip()
        assert entry["semantic_review"].strip()
        assert isinstance(entry["producers"], list)
        assert isinstance(entry["readers"], list)

    taxonomy = entries["market_taxonomy_evidence.v1"]
    assert "36,997-row" in taxonomy["persistence"]
    assert "Retain" in taxonomy["decision"]


def test_lifecycle_catalog_rejects_duplicate_and_missing_versions() -> None:
    duplicate = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    duplicate["groups"][0]["schemas"].append("event.v1")
    with pytest.raises(ValueError, match="more than one lifecycle group"):
        lifecycle_entries(duplicate)

    missing = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    dimensions = next(group for group in missing["groups"] if group["id"] == "unintegrated_dimensions")
    dimensions["schemas"].remove("event.v1")
    missing["evidence_overrides"].pop("event.v1")
    with pytest.raises(ValueError, match="missing registry schemas: event.v1"):
        validate_lifecycle_catalog(
            missing,
            [spec.version for spec in list_table_specs()],
        )


def test_lifecycle_catalog_rejects_future_candidate_without_evidence() -> None:
    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    core = next(group for group in catalog["groups"] if group["id"] == "capture_matching_core")
    core["status"] = "removal_candidate"

    with pytest.raises(ValueError, match="removal candidate 'book_tape_control.v1'"):
        lifecycle_entries(catalog)


def test_lifecycle_catalog_requires_structured_generic_consumers() -> None:
    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    catalog["generic_consumers"][0].pop("removal_effect")

    with pytest.raises(
        ValueError,
        match="each generic consumer requires non-empty 'removal_effect'",
    ):
        lifecycle_entries(catalog)


def test_schema_inventory_separates_surfaces_and_counts_parquet_rows(
    tmp_path: Path,
) -> None:
    source = _write(
        tmp_path / "src" / "pmkt" / "producer.py",
        'SCHEMA = "market_match.v2"\n',
    )
    test_file = _write(
        tmp_path / "tests" / "test_consumer.py",
        'SCHEMA = "instrument.v1"\n',
    )
    manifest = _write(
        tmp_path / "data" / "run-1" / "RUN_MANIFEST.json",
        json.dumps({"schema_version": "market_match.v2"}),
    )
    parquet = _write_versioned_parquet(
        tmp_path / "data" / "run-1" / "matches.parquet",
        ["market_match.v2", "market_match.v2"],
    )

    first = inventory_schema_usage(
        tmp_path,
        artifact_roots=(tmp_path / "data",),
        text_roots=(tmp_path / "src", tmp_path / "tests"),
        catalog_path=CATALOG_PATH,
        parquet_workers=2,
    )
    second = inventory_schema_usage(
        tmp_path,
        artifact_roots=(tmp_path / "data",),
        text_roots=(tmp_path / "src", tmp_path / "tests"),
        catalog_path=CATALOG_PATH,
        parquet_workers=2,
    )

    assert first == second
    assert first["scan"]["complete"] is True
    match_report = first["schemas"]["market_match.v2"]
    assert match_report["text_references"] == {
        "manifest": [manifest.relative_to(tmp_path).as_posix()],
        "source": [source.relative_to(tmp_path).as_posix()],
    }
    assert match_report["artifact_evidence"] == {
        "file_count": 1,
        "row_count": 2,
        "samples": [parquet.relative_to(tmp_path).as_posix()],
    }
    assert first["schemas"]["instrument.v1"]["text_references"] == {
        "tests": [test_file.relative_to(tmp_path).as_posix()]
    }


def test_schema_inventory_reports_mixed_and_unknown_versions(tmp_path: Path) -> None:
    parquet = _write_versioned_parquet(
        tmp_path / "data" / "mixed.parquet",
        ["market_match.v2", "private_experiment.v1", "market_match.v2"],
    )

    report = inventory_schema_usage(
        tmp_path,
        artifact_roots=(tmp_path / "data",),
        text_roots=(),
        catalog_path=CATALOG_PATH,
    )

    assert report["schemas"]["market_match.v2"]["artifact_evidence"] == {
        "file_count": 1,
        "row_count": 2,
        "samples": [parquet.relative_to(tmp_path).as_posix()],
    }
    assert report["unknown_schema_versions"] == {
        "private_experiment.v1": {
            "file_count": 1,
            "row_count": 1,
            "samples": [parquet.relative_to(tmp_path).as_posix()],
        }
    }
    assert report["removal_evidence"]["artifact_attribution_complete"] is False
    assert report["removal_evidence"]["blockers"][0]["kind"] == "unknown_schema_versions"


def test_schema_inventory_reads_named_version_after_nested_columns(tmp_path: Path) -> None:
    parquet = tmp_path / "data" / "nested.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "nested": pa.array(
                    [{"left": 7, "right": 8}, {"left": 7, "right": 8}],
                    type=pa.struct(
                        [("left", pa.int64()), ("right", pa.int64())]
                    ),
                ),
                "schema_version": ["market_match.v2", "market_match.v2"],
            }
        ),
        parquet,
    )

    report = inventory_schema_usage(
        tmp_path,
        artifact_roots=(tmp_path / "data",),
        text_roots=(),
        catalog_path=CATALOG_PATH,
    )

    assert report["schemas"]["market_match.v2"]["artifact_evidence"] == {
        "file_count": 1,
        "row_count": 2,
        "samples": [parquet.relative_to(tmp_path).as_posix()],
    }
    assert report["unknown_schema_versions"] == {}


def test_schema_inventory_distinguishes_empty_versioned_parquet(tmp_path: Path) -> None:
    parquet = tmp_path / "data" / "empty.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "schema_version": pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.int64()),
            }
        ),
        parquet,
    )

    report = inventory_schema_usage(
        tmp_path,
        artifact_roots=(tmp_path / "data",),
        text_roots=(),
        catalog_path=CATALOG_PATH,
    )

    assert report["scan"]["complete"] is True
    assert report["unversioned_parquet"]["file_count"] == 0
    assert report["empty_versioned_parquet"] == {
        "file_count": 1,
        "samples": [parquet.relative_to(tmp_path).as_posix()],
    }
    assert report["removal_evidence"]["artifact_attribution_complete"] is False
    assert report["removal_evidence"]["blockers"] == [
        {
            "kind": "schema_version_column_present_but_empty",
            "file_count": 1,
            "message": (
                "Empty versioned Parquet proves the column exists but cannot attribute "
                "the artifact to a concrete version."
            ),
        }
    ]


def test_schema_inventory_matches_complete_schema_tokens_only(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "src" / "producer.py",
        'SCHEMA = "book_tape_event.v1"\n',
    )

    report = inventory_schema_usage(
        tmp_path,
        artifact_roots=(),
        text_roots=(tmp_path / "src",),
        catalog_path=CATALOG_PATH,
    )

    expected = [source.relative_to(tmp_path).as_posix()]
    assert report["schemas"]["book_tape_event.v1"]["text_references"] == {
        "source": expected
    }
    assert report["schemas"]["event.v1"]["text_references"] == {}


def test_schema_inventory_cli_fails_closed_on_unreadable_parquet(tmp_path: Path) -> None:
    _write(tmp_path / "data" / "broken.parquet", "not parquet")
    output = tmp_path / "local_data" / "schema_usage.json"

    result = main(
        [
            "--root",
            str(tmp_path),
            "--catalog",
            str(CATALOG_PATH),
            "--artifact-root",
            "data",
            "--text-root",
            "data",
            "--output",
            str(output),
        ]
    )

    assert result == 2
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["scan"]["complete"] is False
    assert report["errors"][0]["kind"] == "parquet_read_error"

    allowed = main(
        [
            "--root",
            str(tmp_path),
            "--catalog",
            str(CATALOG_PATH),
            "--artifact-root",
            "data",
            "--text-root",
            "data",
            "--output",
            str(output),
            "--allow-incomplete",
        ]
    )
    assert allowed == 0


def test_schema_inventory_cli_default_text_roots_include_apps(tmp_path: Path) -> None:
    for relative in (
        "src",
        "apps/dashboard",
        "scripts",
        "tests",
        "docs",
        "data",
        "generated",
        "local_data",
    ):
        (tmp_path / relative).mkdir(parents=True, exist_ok=True)
    application = _write(
        tmp_path / "apps" / "dashboard" / "consumer.py",
        'SCHEMA = "market_match.v2"\n',
    )
    output = tmp_path / "local_data" / "schema_usage.json"

    result = main(
        [
            "--root",
            str(tmp_path),
            "--catalog",
            str(CATALOG_PATH),
            "--output",
            str(output),
        ]
    )

    assert result == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["schemas"]["market_match.v2"]["text_references"] == {
        "application": [application.relative_to(tmp_path).as_posix()]
    }
    assert report["scan"]["text_reference_method"] == (
        "literal_registered_schema_version_tokens"
    )
    assert report["semantic_evidence"]["generic_consumers"][0]["path"] == (
        "apps/dashboard/data/artifacts.py:_validate_manifest_candidate"
    )
