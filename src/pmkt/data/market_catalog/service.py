"""Catalog lineage, publication, history, and index service."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
import hashlib
import httpx
import os
from pathlib import Path
import tempfile
from typing import Any, Literal, cast

from pmkt.data.canonical import (
    KALSHI_MARKET_SNAPSHOT_COLUMNS,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
)
from pmkt.data.normalize import markets_dataframe
from pmkt.data.normalize_kalshi import (
    normalize_kalshi_market_status,
)
from pmkt.data.storage.parquet import write_parquet

from . import fs
from . import history
from .collect import (
    _KX_CREATED_KEYS,
    _KX_UPDATED_KEYS,
    _PM_CREATED_KEYS,
    _PM_UPDATED_KEYS,
    _collect_kalshi_pages,
    _dedupe_rows,
    collect_kalshi_current_family,
    collect_kalshi_discovery,
    collect_polymarket_current,
    collect_polymarket_discovery,
    kalshi_snapshot_dataframe,
    verify_kalshi_filter_agreement,
)
from .families import _kalshi_family_sql
from .fs import (
    _artifact_from_staged,
    _parquet_paths_sql,
    _parquet_sql,
    _quote_sql,
    _raw_key,
    _read_json,
    _repository_root,
    _resolve_stored_path,
    _stored_path,
    _timestamp_range,
    iso_utc,
    parquet_files,
    parquet_row_count,
    parse_timestamp,
    row_timestamp,
    sha256_file,
    tree_sha256,
    utc_now,
)
from .types import (
    CURRENT_MANIFEST_SCHEMA,
    CatalogError,
    DISCOVERY_MANIFEST_SCHEMA,
    DISCOVERY_POINTER_SCHEMA,
    DiscoveryStream,
    FilterAgreementError,
    HISTORY_MANIFEST_SCHEMA,
)


class MarketCatalogService:
    """Own catalog lineage, collection, publication, and reader registration."""

    def __init__(self, market_root: str | Path = "data/markets") -> None:
        self.market_root = Path(market_root).resolve()
        self.repository_root = _repository_root(self.market_root)
        self.releases_root = self.market_root / "releases"
        self.current_root = self.market_root / "current"
        self.history_root = self.market_root / "history"
        self.discovery_pointer_path = self.market_root / "DISCOVERY_LATEST.json"
        self.current_pointer_path = self.market_root / "LATEST.json"
        self.history_pointer_path = self.history_root / "LATEST.json"
        self.index_path = self.current_root / "catalog_index.duckdb"

    def _manifest_ref(self, path: Path) -> dict[str, Any]:
        return {
            "path": _stored_path(path, repository_root=self.repository_root),
            "sha256": sha256_file(path),
        }

    def _resolve_ref(self, reference: Mapping[str, Any]) -> Path:
        value = str(reference.get("path") or "")
        if not value:
            raise CatalogError("manifest reference has no path")
        path = _resolve_stored_path(value, repository_root=self.repository_root)
        expected = str(reference.get("sha256") or "")
        if not path.is_file() or not expected or sha256_file(path) != expected:
            raise CatalogError(f"manifest reference is missing or hash-invalid: {path}")
        return path

    def read_discovery_pointer(self) -> dict[str, Any]:
        if not self.discovery_pointer_path.exists():
            return {
                "schema_version": DISCOVERY_POINTER_SCHEMA,
                "dataset_family": "market_discovery",
                "streams": {},
            }
        pointer = _read_json(self.discovery_pointer_path)
        if pointer.get("schema_version") != DISCOVERY_POINTER_SCHEMA:
            raise CatalogError("unsupported market discovery pointer schema")
        streams = pointer.get("streams")
        if not isinstance(streams, dict):
            raise CatalogError("market discovery pointer streams must be an object")
        for stream, reference in streams.items():
            if stream not in {"polymarket", "kalshi-conventional", "kalshi-mve"}:
                raise CatalogError(f"unknown discovery stream in pointer: {stream}")
            if not isinstance(reference, dict):
                raise CatalogError(f"invalid discovery pointer reference for {stream}")
            manifest_path = self._resolve_ref(reference)
            manifest = _read_json(manifest_path)
            self._validate_discovery_manifest(manifest, expected_stream=stream)
        return pointer

    def _validate_discovery_manifest(
        self, manifest: Mapping[str, Any], *, expected_stream: str
    ) -> None:
        if manifest.get("schema_version") != DISCOVERY_MANIFEST_SCHEMA:
            raise CatalogError(
                f"invalid discovery manifest schema for {expected_stream}"
            )
        if (
            manifest.get("stream") != expected_stream
            or manifest.get("status") != "completed"
            or manifest.get("collection_complete") is not True
        ):
            raise CatalogError(
                f"invalid completed discovery manifest for {expected_stream}"
            )
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, dict):
            raise CatalogError(
                f"discovery manifest has no artifacts for {expected_stream}"
            )
        for name in ("new_markets", "market_upserts"):
            artifact = artifacts.get(name)
            if not isinstance(artifact, dict):
                raise CatalogError(f"discovery manifest has no {name} artifact")
            path = _resolve_stored_path(
                str(artifact.get("path") or ""), repository_root=self.repository_root
            )
            if (
                not path.is_file()
                or sha256_file(path) != artifact.get("sha256")
                or parquet_row_count(path) != int(artifact.get("rows") or 0)
            ):
                raise CatalogError(f"discovery artifact is invalid: {path}")
        predecessor = manifest.get("predecessor_manifest")
        if predecessor is not None:
            if not isinstance(predecessor, dict):
                raise CatalogError("discovery predecessor reference must be an object")
            self._resolve_ref(predecessor)

    def _validate_current_manifest(self, manifest: Mapping[str, Any]) -> None:
        if manifest.get("schema_version") != CURRENT_MANIFEST_SCHEMA:
            raise CatalogError("unsupported market current manifest schema")
        if (
            manifest.get("release_kind") != "catalog_current"
            or manifest.get("status") != "completed"
        ):
            raise CatalogError("invalid completed market current manifest")
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, dict):
            raise CatalogError("market current manifest has no artifacts")
        for name in (
            "polymarket_lifecycle_upserts",
            "kalshi_lifecycle_upserts",
        ):
            artifact = artifacts.get(name)
            if not isinstance(artifact, dict):
                raise CatalogError(f"market current manifest has no {name} artifact")
            path = _resolve_stored_path(
                str(artifact.get("path") or ""),
                repository_root=self.repository_root,
            )
            if (
                not path.is_file()
                or sha256_file(path) != artifact.get("sha256")
                or parquet_row_count(path) != int(artifact.get("rows") or 0)
            ):
                raise CatalogError(
                    f"market current lifecycle artifact is invalid: {path}"
                )
        predecessor = manifest.get("predecessor_manifest")
        if predecessor is not None:
            if not isinstance(predecessor, dict):
                raise CatalogError("current predecessor reference must be an object")
            self._resolve_ref(predecessor)

    def _current_predecessor_reference(
        self, pointer: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        reference = pointer.get("manifest")
        if not isinstance(reference, dict):
            return None
        path = self._resolve_ref(reference)
        manifest = _read_json(path)
        # The pre-lifecycle census pointer is a valid baseline, but it did not
        # publish append-only lifecycle artifacts and therefore cannot be part
        # of the current-release delta chain.
        if manifest.get("schema_version") != CURRENT_MANIFEST_SCHEMA:
            return None
        self._validate_current_manifest(manifest)
        return dict(reference)

    def _write_discovery_pointer(
        self,
        *,
        stream: DiscoveryStream,
        manifest_path: Path,
        manifest: Mapping[str, Any],
    ) -> None:
        pointer = self.read_discovery_pointer()
        streams = dict(pointer.get("streams") or {})
        reference = self._manifest_ref(manifest_path)
        reference.update(
            {
                "release_id": manifest["release_id"],
                "high_watermark_utc": manifest["resulting_high_watermark_utc"],
            }
        )
        streams[stream] = reference
        next_pointer = {
            **pointer,
            "schema_version": DISCOVERY_POINTER_SCHEMA,
            "dataset_family": "market_discovery",
            "streams": streams,
            "updated_at_utc": iso_utc(utc_now()),
        }
        agreement = manifest.get("filter_agreement")
        if isinstance(agreement, dict) and agreement.get("complete") is True:
            next_pointer["kalshi_filter_agreement"] = {
                **self._manifest_ref(manifest_path),
                "cutoff_utc": agreement.get("cutoff_utc"),
                "window_end_utc": agreement.get("window_end_utc"),
            }
        fs._atomic_json(
            self.discovery_pointer_path,
            next_pointer,
        )

    def _prior_filter_agreement(self) -> dict[str, Any] | None:
        pointer = self.read_discovery_pointer()
        reference = pointer.get("kalshi_filter_agreement")
        if not isinstance(reference, dict):
            return None
        path = self._resolve_ref(reference)
        manifest = _read_json(path)
        agreement = manifest.get("filter_agreement")
        if not isinstance(agreement, dict) or agreement.get("complete") is not True:
            raise CatalogError("recorded Kalshi filter-agreement evidence is invalid")
        return {
            "status": "reused_prior_completed_evidence",
            "evidence_manifest": self._manifest_ref(path),
            "cutoff_utc": agreement.get("cutoff_utc"),
            "window_end_utc": agreement.get("window_end_utc"),
            "complete": True,
        }

    def _record_failed_filter_agreement(
        self,
        *,
        stream: DiscoveryStream,
        started: datetime,
        cutoff: datetime,
        report: Mapping[str, Any],
    ) -> Path:
        release_id = fs._run_id(f"market_discovery_{stream.replace('-', '_')}_failed")
        staging = self.market_root / ".staging" / release_id
        release = self.releases_root / release_id
        staging.mkdir(parents=True)
        manifest = {
            "schema_version": DISCOVERY_MANIFEST_SCHEMA,
            "dataset_family": "market_discovery",
            "release_id": release_id,
            "stream": stream,
            "status": "failed",
            "started_at_utc": iso_utc(started),
            "failed_at_utc": iso_utc(utc_now()),
            "previous_cutoff_utc": iso_utc(cutoff),
            "failure": "kalshi_filter_agreement",
            "filter_agreement": dict(report),
            "collection_complete": False,
            "network_accessed": True,
            "execution_authority": False,
            "orders_submitted": False,
            "pointer_updated": False,
        }
        path = staging / "DISCOVERY_MANIFEST.json"
        fs._atomic_json(path, manifest)
        self.releases_root.mkdir(parents=True, exist_ok=True)
        os.replace(staging, release)
        return release / path.name

    def reachable_discovery_manifests(
        self,
        stream: DiscoveryStream,
        *,
        after_hash: str | None = None,
    ) -> list[tuple[Path, dict[str, Any]]]:
        pointer = self.read_discovery_pointer()
        reference = (pointer.get("streams") or {}).get(stream)
        if not isinstance(reference, dict):
            return []
        newest_first: list[tuple[Path, dict[str, Any]]] = []
        seen: set[str] = set()
        while reference:
            digest = str(reference.get("sha256") or "")
            if digest == after_hash:
                break
            if not digest or digest in seen:
                raise CatalogError(f"broken or cyclic discovery chain for {stream}")
            seen.add(digest)
            path = self._resolve_ref(reference)
            manifest = _read_json(path)
            self._validate_discovery_manifest(manifest, expected_stream=stream)
            newest_first.append((path, manifest))
            predecessor = manifest.get("predecessor_manifest")
            reference = predecessor if isinstance(predecessor, dict) else {}
        if after_hash is not None and not reference:
            raise CatalogError(
                f"recorded discovery watermark {after_hash} is not reachable for {stream}"
            )
        return list(reversed(newest_first))

    def reachable_current_manifests(
        self, *, after_hash: str | None = None
    ) -> list[tuple[Path, dict[str, Any]]]:
        """Return completed current releases after one history watermark."""
        if not self.current_pointer_path.is_file():
            if after_hash is not None:
                raise CatalogError(
                    f"recorded current lifecycle watermark {after_hash} is not reachable"
                )
            return []
        pointer = _read_json(self.current_pointer_path)
        reference = pointer.get("manifest")
        if not isinstance(reference, dict):
            if after_hash is not None:
                raise CatalogError(
                    f"recorded current lifecycle watermark {after_hash} is not reachable"
                )
            return []
        newest_first: list[tuple[Path, dict[str, Any]]] = []
        seen: set[str] = set()
        while reference:
            digest = str(reference.get("sha256") or "")
            if digest == after_hash:
                break
            if not digest or digest in seen:
                raise CatalogError("broken or cyclic market current release chain")
            seen.add(digest)
            path = self._resolve_ref(reference)
            manifest = _read_json(path)
            if manifest.get("schema_version") != CURRENT_MANIFEST_SCHEMA:
                if after_hash is not None:
                    raise CatalogError(
                        f"recorded current lifecycle watermark {after_hash} is not reachable"
                    )
                break
            self._validate_current_manifest(manifest)
            newest_first.append((path, manifest))
            predecessor = manifest.get("predecessor_manifest")
            reference = predecessor if isinstance(predecessor, dict) else {}
        if after_hash is not None and not reference:
            raise CatalogError(
                f"recorded current lifecycle watermark {after_hash} is not reachable"
            )
        return list(reversed(newest_first))

    @staticmethod
    def _recorded_artifact_count(
        reference: Mapping[str, Any], field: str, *, artifact_name: str
    ) -> int:
        value = reference.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CatalogError(
                f"history artifact {artifact_name} has invalid {field}: {value!r}"
            )
        return value

    def _validate_history_integrity(
        self,
        *,
        pointer: Mapping[str, Any],
        manifest_path: Path,
        manifest: Mapping[str, Any],
        deep: bool,
    ) -> dict[str, Any]:
        if manifest.get("schema_version") != HISTORY_MANIFEST_SCHEMA:
            raise CatalogError("unsupported market history manifest schema")
        manifest_artifacts = manifest.get("artifacts")
        if not isinstance(manifest_artifacts, dict):
            raise CatalogError("history manifest has no artifacts")

        integrity_artifacts: dict[str, dict[str, Any]] = {}
        total_rows = 0
        for venue, artifact_name in (
            ("polymarket", "polymarket_all_markets"),
            ("kalshi", "kalshi_all_markets"),
        ):
            manifest_reference = manifest_artifacts.get(artifact_name)
            pointer_reference = pointer.get(artifact_name)
            if not isinstance(manifest_reference, dict):
                raise CatalogError(
                    f"history manifest has no {artifact_name} artifact reference"
                )
            if not isinstance(pointer_reference, dict):
                raise CatalogError(
                    f"history pointer has no {artifact_name} artifact reference"
                )
            if dict(pointer_reference) != dict(manifest_reference):
                raise CatalogError(
                    f"history pointer and manifest disagree for {artifact_name}"
                )

            stored_path = str(manifest_reference.get("path") or "")
            if not stored_path:
                raise CatalogError(f"history artifact {artifact_name} has no path")
            path = _resolve_stored_path(
                stored_path, repository_root=self.repository_root
            )
            files = parquet_files(path)
            if not files:
                raise CatalogError(f"history artifact is missing: {path}")

            expected_rows = self._recorded_artifact_count(
                manifest_reference, "rows", artifact_name=artifact_name
            )
            expected_file_count = self._recorded_artifact_count(
                manifest_reference,
                "parquet_file_count",
                artifact_name=artifact_name,
            )
            expected_size = self._recorded_artifact_count(
                manifest_reference, "size_bytes", artifact_name=artifact_name
            )
            actual_rows = parquet_row_count(path)
            actual_file_count = len(files)
            actual_size = sum(item.stat().st_size for item in files)
            if actual_rows != expected_rows:
                raise CatalogError(
                    f"history artifact row count is invalid: {path}; "
                    f"actual={actual_rows}, expected={expected_rows}"
                )
            if actual_file_count != expected_file_count:
                raise CatalogError(
                    f"history artifact file count is invalid: {path}; "
                    f"actual={actual_file_count}, expected={expected_file_count}"
                )
            if actual_size != expected_size:
                raise CatalogError(
                    f"history artifact size is invalid: {path}; "
                    f"actual={actual_size}, expected={expected_size}"
                )

            if deep:
                if path.is_file():
                    expected_hash = str(manifest_reference.get("sha256") or "")
                    if not expected_hash or sha256_file(path) != expected_hash:
                        raise CatalogError(
                            f"history artifact content hash is invalid: {path}"
                        )
                else:
                    expected_tree = str(manifest_reference.get("tree_sha256") or "")
                    if not expected_tree or tree_sha256(path) != expected_tree:
                        raise CatalogError(
                            f"history artifact tree hash is invalid: {path}"
                        )

            total_rows += actual_rows
            integrity_artifacts[venue] = {
                "rows": actual_rows,
                "parquet_file_count": actual_file_count,
                "size_bytes": actual_size,
                "content_hash_verified": deep,
            }

        accounting_fields = ("base_row_count", "uncompacted_delta_rows")
        if any(field in manifest for field in accounting_fields):
            if not all(field in manifest for field in accounting_fields):
                raise CatalogError("history promotion row accounting is incomplete")
            base_rows = self._recorded_artifact_count(
                manifest, "base_row_count", artifact_name="promotion_accounting"
            )
            delta_rows = self._recorded_artifact_count(
                manifest,
                "uncompacted_delta_rows",
                artifact_name="promotion_accounting",
            )
            expected_total = base_rows + delta_rows
            if total_rows != expected_total:
                raise CatalogError(
                    "history base-plus-delta row accounting is invalid: "
                    f"actual={total_rows}, expected={expected_total}"
                )

        return {
            "mode": "deep" if deep else "metadata",
            "status": "valid",
            "manifest_sha256": sha256_file(manifest_path),
            "artifacts": integrity_artifacts,
        }

    def _history_state(
        self, *, deep: bool = False
    ) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        if not self.history_pointer_path.is_file():
            raise CatalogError("history/LATEST.json is required")
        pointer = _read_json(self.history_pointer_path)
        reference = pointer.get("manifest")
        if not isinstance(reference, dict):
            raise CatalogError("history pointer has no manifest reference")
        path = self._resolve_ref(reference)
        manifest = _read_json(path)
        integrity = self._validate_history_integrity(
            pointer=pointer,
            manifest_path=path,
            manifest=manifest,
            deep=deep,
        )
        return path, manifest, integrity

    def _history_manifest(self, *, deep: bool = False) -> tuple[Path, dict[str, Any]]:
        path, manifest, _integrity = self._history_state(deep=deep)
        return path, manifest

    def bootstrap_cutoff(self, stream: DiscoveryStream) -> datetime:
        _path, manifest = self._history_manifest()
        evidence = manifest.get("as_of")
        if not isinstance(evidence, dict):
            raise CatalogError("history manifest has no bootstrap as_of evidence")
        value: datetime | None
        if stream == "polymarket":
            raw = evidence.get("polymarket_fresh_census_date_utc")
            try:
                value = datetime.combine(
                    date.fromisoformat(str(raw)),
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                )
            except (TypeError, ValueError) as exc:
                raise CatalogError(
                    "Polymarket bootstrap needs --bootstrap-cutoff; history date is absent"
                ) from exc
        else:
            value = parse_timestamp(evidence.get("kalshi_cursor_exhausted_at_utc"))
            if value is None:
                raise CatalogError(
                    "Kalshi bootstrap needs --bootstrap-cutoff; cursor evidence is absent"
                )
        return value - timedelta(hours=24)

    def discovery_cutoff(
        self,
        stream: DiscoveryStream,
        *,
        overlap_seconds: int,
        bootstrap_cutoff: datetime | None,
    ) -> tuple[datetime, dict[str, Any] | None]:
        if overlap_seconds < 0:
            raise ValueError("overlap_seconds must be nonnegative")
        pointer = self.read_discovery_pointer()
        reference = (pointer.get("streams") or {}).get(stream)
        if isinstance(reference, dict):
            watermark = parse_timestamp(reference.get("high_watermark_utc"))
            if watermark is None:
                raise CatalogError(
                    f"discovery pointer watermark is invalid for {stream}"
                )
            return watermark - timedelta(seconds=overlap_seconds), reference
        watermark = bootstrap_cutoff or self.bootstrap_cutoff(stream)
        return watermark, None

    def _index_fingerprint(self) -> str:
        digest = hashlib.sha256()
        for pointer_path in (self.history_pointer_path, self.discovery_pointer_path):
            if pointer_path.is_file():
                digest.update(pointer_path.name.encode("utf-8"))
                digest.update(bytes.fromhex(sha256_file(pointer_path)))
        return digest.hexdigest()

    def _history_artifact_path(self, manifest: Mapping[str, Any], venue: str) -> Path:
        artifacts = manifest.get("artifacts")
        key = (
            "polymarket_all_markets" if venue == "polymarket" else "kalshi_all_markets"
        )
        if not isinstance(artifacts, dict) or not isinstance(artifacts.get(key), dict):
            raise CatalogError(f"history manifest has no {key} artifact")
        value = str(artifacts[key].get("path") or "")
        path = _resolve_stored_path(value, repository_root=self.repository_root)
        if not parquet_files(path):
            raise CatalogError(f"history artifact is missing: {path}")
        return path

    def ensure_known_key_index(self, *, index_path: Path | None = None) -> Path:
        """Build a replaceable cache bound to exact pointer file hashes."""
        import duckdb

        target = (index_path or self.index_path).resolve()
        fingerprint = self._index_fingerprint()
        if target.is_file():
            try:
                with duckdb.connect(str(target), read_only=True) as connection:
                    stored = connection.execute(
                        "SELECT value FROM catalog_meta WHERE key='pointer_fingerprint'"
                    ).fetchone()
                if stored and stored[0] == fingerprint:
                    return target
            except Exception:
                pass
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        if temporary.exists():
            temporary.unlink()
        _history_path, history_manifest = self._history_manifest()
        with duckdb.connect(str(temporary)) as connection:
            connection.execute(
                "CREATE TABLE catalog_meta(key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE known_keys(venue VARCHAR NOT NULL, market_key VARCHAR NOT NULL, "
                "payload_hash VARCHAR, updated_at_utc TIMESTAMPTZ, native_family VARCHAR, "
                "PRIMARY KEY(venue, market_key))"
            )
            pm_path = self._history_artifact_path(history_manifest, "polymarket")
            kx_path = self._history_artifact_path(history_manifest, "kalshi")
            connection.execute(
                f"""
                INSERT INTO known_keys
                SELECT 'polymarket', CAST(market_id AS VARCHAR), raw_json_sha256,
                       try_cast(coalesce(
                           json_extract_string(raw_json, '$.updatedAt'),
                           json_extract_string(raw_json, '$.updated_at')
                       ) AS TIMESTAMPTZ), 'polymarket'
                FROM {_parquet_sql(pm_path)}
                WHERE market_id IS NOT NULL
                """
            )
            connection.execute(
                f"""
                INSERT INTO known_keys
                SELECT 'kalshi', CAST(market_key AS VARCHAR), raw_json_sha256,
                       try_cast(updated_time AS TIMESTAMPTZ),
                       {_kalshi_family_sql("market_key", filename_sql="filename")}
                FROM {_parquet_sql(kx_path, filename=True)}
                WHERE market_key IS NOT NULL
                """
            )
            for stream in ("polymarket", "kalshi-conventional", "kalshi-mve"):
                for _manifest_path, manifest in self.reachable_discovery_manifests(
                    stream
                ):
                    artifacts = manifest.get("artifacts")
                    if not isinstance(artifacts, dict):
                        continue
                    for name in ("new_markets", "market_upserts"):
                        artifact = artifacts.get(name)
                        if not isinstance(artifact, dict) or not artifact.get("rows"):
                            continue
                        path = _resolve_stored_path(
                            str(artifact["path"]), repository_root=self.repository_root
                        )
                        venue = "polymarket" if stream == "polymarket" else "kalshi"
                        family = {
                            "polymarket": "polymarket",
                            "kalshi-conventional": "kalshi_conventional",
                            "kalshi-mve": "kalshi_mve",
                        }[stream]
                        key_column = (
                            "market_id" if venue == "polymarket" else "market_key"
                        )
                        updated_sql = (
                            "try_cast(json_extract_string(raw_json, '$.updatedAt') AS TIMESTAMPTZ)"
                            if venue == "polymarket"
                            else "try_cast(updated_time AS TIMESTAMPTZ)"
                        )
                        connection.execute(
                            f"""
                            INSERT OR REPLACE INTO known_keys
                            SELECT {_quote_sql(venue)}, CAST({key_column} AS VARCHAR),
                                   raw_json_sha256, {updated_sql}, {_quote_sql(family)}
                            FROM {_parquet_sql(path)}
                            WHERE {key_column} IS NOT NULL
                            """
                        )
            connection.execute(
                "INSERT INTO catalog_meta VALUES ('pointer_fingerprint', ?)",
                [fingerprint],
            )
        os.replace(temporary, target)
        return target

    def _split_new_and_upserts(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        stream: DiscoveryStream,
        index_path: Path,
    ) -> tuple[Any, Any, dict[str, int]]:
        import duckdb
        import pandas as pd

        venue = "polymarket" if stream == "polymarket" else "kalshi"
        normalized = (
            markets_dataframe([dict(row) for row in rows])
            if venue == "polymarket"
            else kalshi_snapshot_dataframe([dict(row) for row in rows])
        )
        key_column = "market_id" if venue == "polymarket" else "market_key"
        raw_by_key = {_raw_key(row, venue): row for row in rows}
        observations = pd.DataFrame(
            {
                "market_key": normalized[key_column].astype(str),
                "observed_updated_at": [
                    iso_utc(stamp) if stamp is not None else None
                    for stamp in (
                        row_timestamp(raw_by_key[str(key)], _PM_UPDATED_KEYS)
                        if venue == "polymarket"
                        else row_timestamp(raw_by_key[str(key)], _KX_UPDATED_KEYS)
                        for key in normalized[key_column]
                    )
                ],
                "observed_hash": normalized["raw_json_sha256"],
            }
        )
        with duckdb.connect(str(index_path), read_only=True) as connection:
            connection.register("observations", observations)
            classified = connection.execute(
                """
                SELECT o.*, k.payload_hash AS known_hash, k.updated_at_utc AS known_updated_at
                FROM observations o
                LEFT JOIN known_keys k
                  ON k.venue = ? AND k.market_key = o.market_key
                """,
                [venue],
            ).df()
        is_new = classified["known_hash"].isna()
        observed_time = pd.to_datetime(classified["observed_updated_at"], utc=True)
        known_time = pd.to_datetime(classified["known_updated_at"], utc=True)
        is_upsert = (
            ~is_new
            & classified["observed_hash"].ne(classified["known_hash"])
            & observed_time.notna()
            & (known_time.isna() | observed_time.gt(known_time))
        )
        new_keys = set(classified.loc[is_new, "market_key"])
        upsert_keys = set(classified.loc[is_upsert, "market_key"])
        new_frame = normalized[normalized[key_column].astype(str).isin(new_keys)].copy()
        upsert_frame = normalized[
            normalized[key_column].astype(str).isin(upsert_keys)
        ].copy()
        return (
            new_frame,
            upsert_frame,
            {
                "valid": len(normalized),
                "unique": len(normalized),
                "known": int((~is_new).sum()),
                "new": len(new_frame),
                "upsert": len(upsert_frame),
                "unchanged_or_not_newer": int((~is_new & ~is_upsert).sum()),
            },
        )

    async def discover(
        self,
        stream: DiscoveryStream,
        *,
        bootstrap_cutoff: datetime | None = None,
        overlap_seconds: int | None = None,
        max_pages: int = 10_000,
        publish: bool = True,
        client: Any,
    ) -> dict[str, Any]:
        defaults = {
            "polymarket": 1800,
            "kalshi-conventional": 1800,
            "kalshi-mve": 900,
        }
        overlap = defaults[stream] if overlap_seconds is None else overlap_seconds
        cutoff, predecessor = self.discovery_cutoff(
            stream,
            overlap_seconds=overlap,
            bootstrap_cutoff=bootstrap_cutoff,
        )
        started = utc_now()
        if stream == "polymarket":
            active_client = client
            result = await collect_polymarket_discovery(
                active_client, cutoff=cutoff, max_pages=max_pages
            )
            agreement = None
            venue = "polymarket"
            native_family = "polymarket"
            schema = POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION
        else:
            active_client = client
            native_family = (
                "kalshi_mve" if stream == "kalshi-mve" else "kalshi_conventional"
            )
            result = await collect_kalshi_discovery(
                active_client,
                cutoff=cutoff,
                native_family=cast(
                    Literal["kalshi_conventional", "kalshi_mve"],
                    native_family,
                ),
                max_pages=max_pages,
            )
            prior_agreement = self._prior_filter_agreement() if publish else None
            if prior_agreement is not None:
                agreement = prior_agreement
            else:
                agreement_cutoff = max(cutoff, started - timedelta(minutes=2))
                try:
                    agreement = await verify_kalshi_filter_agreement(
                        active_client,
                        cutoff=agreement_cutoff,
                        window_end=started,
                        max_pages=max_pages,
                    )
                except FilterAgreementError as exc:
                    if publish:
                        self._record_failed_filter_agreement(
                            stream=stream,
                            started=started,
                            cutoff=cutoff,
                            report=exc.report,
                        )
                    raise
            venue = "kalshi"
            schema = KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION
        if not result.details.get("complete"):
            raise CatalogError(f"{stream} collection did not establish completeness")

        temporary_directory: tempfile.TemporaryDirectory[str] | None = None
        if publish:
            index_path = self.ensure_known_key_index()
        else:
            temporary_directory = tempfile.TemporaryDirectory(
                prefix="pmkt-catalog-index-"
            )
            index_path = self.ensure_known_key_index(
                index_path=Path(temporary_directory.name) / "catalog.duckdb"
            )
        try:
            new_frame, upsert_frame, counts = self._split_new_and_upserts(
                result.rows, stream=stream, index_path=index_path
            )
        finally:
            if temporary_directory is not None:
                temporary_directory.cleanup()
        summary = {
            "stream": stream,
            "cutoff_utc": iso_utc(cutoff),
            "resulting_high_watermark_utc": iso_utc(result.high_watermark),
            "counts": {
                "raw": result.details.get("raw_rows", len(result.rows)),
                **counts,
            },
            "collection": result.details,
            "filter_agreement": agreement,
            "published": False,
        }
        if not publish:
            return summary

        release_id = fs._run_id(f"market_discovery_{stream.replace('-', '_')}")
        staging = self.market_root / ".staging" / release_id
        release = self.releases_root / release_id
        if staging.exists() or release.exists():
            raise FileExistsError(f"refusing to overwrite catalog release {release_id}")
        staging.mkdir(parents=True)
        new_path = staging / "NEW_MARKETS.parquet"
        upsert_path = staging / "MARKET_UPSERTS.parquet"
        write_parquet(new_frame, new_path, schema=schema, strict=True)
        write_parquet(upsert_frame, upsert_path, schema=schema, strict=True)
        observed_created = [
            row_timestamp(
                row, _PM_CREATED_KEYS if venue == "polymarket" else _KX_CREATED_KEYS
            )
            for row in result.rows
        ]
        observed_updated = [
            row_timestamp(
                row, _PM_UPDATED_KEYS if venue == "polymarket" else _KX_UPDATED_KEYS
            )
            for row in result.rows
        ]
        manifest = {
            "schema_version": DISCOVERY_MANIFEST_SCHEMA,
            "dataset_family": "market_discovery",
            "release_id": release_id,
            "stream": stream,
            "venue": venue,
            "native_family": native_family,
            "status": "completed",
            "started_at_utc": iso_utc(started),
            "observed_at_utc": iso_utc(utc_now()),
            "previous_cutoff_utc": iso_utc(cutoff),
            "resulting_high_watermark_utc": iso_utc(result.high_watermark),
            "overlap_seconds": overlap,
            "predecessor_manifest": predecessor,
            "request": result.details.get("request"),
            "endpoint": result.details.get("endpoint"),
            "collection": result.details,
            "filter_agreement": agreement,
            "counts": summary["counts"],
            "timestamp_ranges": {
                "created": _timestamp_range(observed_created),
                "updated": _timestamp_range(observed_updated),
            },
            "artifacts": {
                "new_markets": _artifact_from_staged(
                    new_path,
                    final_path=release / new_path.name,
                    repository_root=self.repository_root,
                    rows=len(new_frame),
                    schema=schema,
                ),
                "market_upserts": _artifact_from_staged(
                    upsert_path,
                    final_path=release / upsert_path.name,
                    repository_root=self.repository_root,
                    rows=len(upsert_frame),
                    schema=schema,
                ),
            },
            "collection_complete": True,
            "network_accessed": True,
            "execution_authority": False,
            "orders_submitted": False,
        }
        manifest_path = staging / "DISCOVERY_MANIFEST.json"
        fs._atomic_json(manifest_path, manifest)
        self.releases_root.mkdir(parents=True, exist_ok=True)
        os.replace(staging, release)
        final_manifest = release / manifest_path.name
        self._write_discovery_pointer(
            stream=stream, manifest_path=final_manifest, manifest=manifest
        )
        summary.update(
            {
                "published": True,
                "release_id": release_id,
                "manifest": self._manifest_ref(final_manifest),
            }
        )
        return summary

    def _pointer_artifact_path(
        self, pointer: Mapping[str, Any], name: str
    ) -> Path | None:
        reference = pointer.get(name)
        if not isinstance(reference, dict) or not reference.get("path"):
            return None
        path = _resolve_stored_path(
            str(reference["path"]), repository_root=self.repository_root
        )
        files = parquet_files(path)
        if not files:
            return None
        expected_hash = reference.get("sha256")
        expected_tree = reference.get("tree_sha256")
        if expected_hash and (not path.is_file() or sha256_file(path) != expected_hash):
            raise CatalogError(f"current artifact hash is invalid: {path}")
        if expected_tree and (path.is_file() or tree_sha256(path) != expected_tree):
            raise CatalogError(f"current artifact tree hash is invalid: {path}")
        if reference.get("rows") is not None and parquet_row_count(path) != int(
            reference["rows"]
        ):
            raise CatalogError(f"current artifact row count is invalid: {path}")
        return path

    @staticmethod
    def _missing_keys(
        previous_path: Path | Sequence[Path] | None,
        current_frame: Any,
        *,
        key_column: str,
    ) -> list[str]:
        if previous_path is None:
            return []
        import duckdb

        previous_paths = (
            [previous_path] if isinstance(previous_path, Path) else list(previous_path)
        )
        with duckdb.connect(database=":memory:") as connection:
            connection.register("fresh", current_frame[[key_column]])
            rows = connection.execute(
                f"""
                SELECT DISTINCT CAST(previous.{key_column} AS VARCHAR)
                FROM {_parquet_paths_sql(previous_paths)} AS previous
                WHERE previous.{key_column} IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM fresh
                    WHERE CAST(fresh.{key_column} AS VARCHAR) =
                          CAST(previous.{key_column} AS VARCHAR)
                  )
                ORDER BY 1
                """
            ).fetchall()
        return [str(row[0]) for row in rows]

    async def _reconcile_polymarket_absence(
        self,
        client: Any,
        missing: Sequence[str],
        *,
        max_target_reads: int = 500,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
        reopenings: list[dict[str, Any]] = []
        upserts: list[dict[str, Any]] = []
        unresolved = list(missing[max_target_reads:])
        for key in missing[:max_target_reads]:
            try:
                row = await client.market(key)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    unresolved.append(key)
                    continue
                raise
            if not isinstance(row, dict):
                unresolved.append(key)
            elif row.get("closed") is False:
                reopenings.append(row)
            else:
                upserts.append(row)
        return reopenings, upserts, sorted(set(unresolved))

    async def _reconcile_kalshi_absence(
        self,
        client: Any,
        missing: Sequence[str],
        *,
        max_target_reads: int = 500,
    ) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[str]]:
        reopenings: dict[str, list[dict[str, Any]]] = {
            "open": [],
            "unopened": [],
            "paused": [],
        }
        upserts: list[dict[str, Any]] = []
        unresolved = list(missing[max_target_reads:])
        status_bucket = {
            "active": "open",
            "initialized": "unopened",
            "inactive": "paused",
        }
        for key in missing[:max_target_reads]:
            try:
                row = await client.market(key)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
                try:
                    row = await client.historical_market(key)
                except httpx.HTTPStatusError as historical_exc:
                    if historical_exc.response.status_code == 404:
                        unresolved.append(key)
                        continue
                    raise
            if not isinstance(row, dict):
                unresolved.append(key)
                continue
            canonical = normalize_kalshi_market_status(row.get("status"))
            bucket = status_bucket.get(str(canonical))
            if bucket is None:
                upserts.append(row)
            else:
                reopenings[bucket].append(row)
        return reopenings, upserts, sorted(set(unresolved))

    async def refresh_current(
        self,
        *,
        scope: Literal["standard", "all"] = "standard",
        bootstrap_cutoff: datetime | None = None,
        max_pages: int = 10_000,
        polymarket_client: Any,
        kalshi_client: Any,
    ) -> dict[str, Any]:
        """Publish one complete nonterminal census without deleting history."""
        if scope not in {"standard", "all"}:
            raise ValueError("scope must be 'standard' or 'all'")
        if bootstrap_cutoff is not None:
            if scope != "all":
                raise CatalogError("bootstrap_cutoff is only valid with scope='all'")
            if bootstrap_cutoff.tzinfo is None or bootstrap_cutoff.utcoffset() is None:
                raise CatalogError("bootstrap_cutoff must be timezone-aware")
            bootstrap_cutoff = bootstrap_cutoff.astimezone(timezone.utc)
        previous = (
            _read_json(self.current_pointer_path)
            if self.current_pointer_path.is_file()
            else {}
        )
        if scope == "standard" and not isinstance(
            previous.get("kalshi_mve_current_markets"), dict
        ):
            raise CatalogError(
                "the first catalog-quality current refresh must use --scope all"
            )
        if (
            scope == "standard"
            and self._pointer_artifact_path(previous, "kalshi_mve_current_markets")
            is None
        ):
            raise CatalogError("the retained Kalshi MVE current artifact is invalid")
        started = utc_now()
        if bootstrap_cutoff is not None and bootstrap_cutoff > started:
            raise CatalogError("bootstrap_cutoff cannot be in the future")
        pm_client = polymarket_client
        kx_client = kalshi_client
        try:
            pm_rows, pm_collection = await collect_polymarket_current(
                pm_client, max_pages=max_pages
            )
            conventional, kx_collection = await collect_kalshi_current_family(
                kx_client,
                native_family="kalshi_conventional",
                max_pages=max_pages,
            )
            mve: dict[str, list[dict[str, Any]]] | None = None
            mve_collection: dict[str, Any] | None = None
            mve_lifecycle: list[dict[str, Any]] = []
            mve_lifecycle_details: dict[str, Any] | None = None
            if scope == "all":
                mve, mve_collection = await collect_kalshi_current_family(
                    kx_client,
                    native_family="kalshi_mve",
                    max_pages=max_pages,
                )
                prior_mve = previous.get("kalshi_mve_current_markets")
                prior_as_of = (
                    parse_timestamp(prior_mve.get("as_of_utc"))
                    if isinstance(prior_mve, dict)
                    else None
                )
                if prior_as_of is not None:
                    lifecycle_cutoff = prior_as_of
                    lifecycle_cutoff_source = "prior_current_mve"
                elif bootstrap_cutoff is not None:
                    lifecycle_cutoff = bootstrap_cutoff
                    lifecycle_cutoff_source = "explicit_bootstrap"
                else:
                    lifecycle_cutoff = self.bootstrap_cutoff("kalshi-mve")
                    lifecycle_cutoff_source = "history_evidence"
                closed, closed_details = await _collect_kalshi_pages(
                    kx_client,
                    max_pages=max_pages,
                    status="closed",
                    mve_filter="only",
                    min_close_ts=int(lifecycle_cutoff.timestamp()),
                )
                settled, settled_details = await _collect_kalshi_pages(
                    kx_client,
                    max_pages=max_pages,
                    status="settled",
                    mve_filter="only",
                    min_settled_ts=int(lifecycle_cutoff.timestamp()),
                )
                mve_lifecycle = _dedupe_rows(
                    [*closed, *settled],
                    venue="kalshi",
                    timestamp_keys=(*_KX_UPDATED_KEYS, *_KX_CREATED_KEYS),
                )
                mve_lifecycle_details = {
                    "cutoff_utc": iso_utc(lifecycle_cutoff),
                    "cutoff_source": lifecycle_cutoff_source,
                    "closed": closed_details,
                    "settled": settled_details,
                    "complete": closed_details["complete"]
                    and settled_details["complete"],
                }
        finally:
            # Network-client lifetime belongs to the CLI/caller. Keeping it out
            # of the data service preserves the package layering boundary.
            pass
        if not pm_collection["complete"] or not kx_collection["complete"]:
            raise CatalogError("a standard current lane did not exhaust its cursor")
        if scope == "all" and (
            not mve_collection
            or not mve_collection["complete"]
            or not mve_lifecycle_details
            or not mve_lifecycle_details["complete"]
        ):
            raise CatalogError(
                "an MVE current or lifecycle lane did not exhaust its cursor"
            )

        pm_frame = markets_dataframe(pm_rows)
        conventional_frames = {
            status: kalshi_snapshot_dataframe(rows)
            for status, rows in conventional.items()
        }
        previous_pm = self._pointer_artifact_path(previous, "polymarket_open_markets")
        missing_pm = self._missing_keys(previous_pm, pm_frame, key_column="market_id")
        previous_kx_paths = [
            path
            for name in (
                "kalshi_open_markets",
                "kalshi_unopened_markets",
                "kalshi_paused_markets",
            )
            if (path := self._pointer_artifact_path(previous, name)) is not None
        ]
        previous_kx: Sequence[Path] | None = previous_kx_paths or None
        import pandas as pd

        current_kx_keys = pd.concat(
            [frame[["market_key"]] for frame in conventional_frames.values()],
            ignore_index=True,
        )
        missing_kx = self._missing_keys(
            previous_kx, current_kx_keys, key_column="market_key"
        )
        # Reopenings are added back to the census; terminal rows become explicit
        # operational upserts. Unknown absence is recorded and never fabricated.
        if missing_pm:
            (
                pm_reopened,
                pm_upsert_rows,
                pm_unresolved,
            ) = await self._reconcile_polymarket_absence(polymarket_client, missing_pm)
        else:
            pm_reopened, pm_upsert_rows, pm_unresolved = [], [], []
        if pm_reopened:
            pm_rows = _dedupe_rows(
                [*pm_rows, *pm_reopened],
                venue="polymarket",
                timestamp_keys=(*_PM_UPDATED_KEYS, *_PM_CREATED_KEYS),
            )
            pm_frame = markets_dataframe(pm_rows)
        # The collection clients may have been closed above. Re-open only for
        # bounded absence reads when callers did not inject a reusable fake.
        if missing_kx:
            (
                kx_reopened,
                kx_upsert_rows,
                kx_unresolved,
            ) = await self._reconcile_kalshi_absence(kalshi_client, missing_kx)
        else:
            kx_reopened, kx_upsert_rows, kx_unresolved = (
                {"open": [], "unopened": [], "paused": []},
                [],
                [],
            )
        for status, reopened in kx_reopened.items():
            if reopened:
                conventional[status] = _dedupe_rows(
                    [*conventional[status], *reopened],
                    venue="kalshi",
                    timestamp_keys=(*_KX_UPDATED_KEYS, *_KX_CREATED_KEYS),
                )
                conventional_frames[status] = kalshi_snapshot_dataframe(
                    conventional[status]
                )

        release_id = fs._run_id("market_current")
        staging = self.market_root / ".staging" / release_id
        release = self.releases_root / release_id
        if staging.exists() or release.exists():
            raise FileExistsError(f"refusing to overwrite catalog release {release_id}")
        staging.mkdir(parents=True)
        frames: dict[str, tuple[Any, str, str]] = {
            "polymarket_open_markets": (
                pm_frame,
                "POLYMARKET_OPEN_MARKETS.parquet",
                POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
            ),
            "kalshi_open_markets": (
                conventional_frames["open"],
                "KALSHI_OPEN_MARKETS.parquet",
                KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            ),
            "kalshi_unopened_markets": (
                conventional_frames["unopened"],
                "KALSHI_UNOPENED_MARKETS.parquet",
                KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            ),
            "kalshi_paused_markets": (
                conventional_frames["paused"],
                "KALSHI_PAUSED_MARKETS.parquet",
                KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            ),
            "polymarket_lifecycle_upserts": (
                markets_dataframe(pm_upsert_rows),
                "POLYMARKET_LIFECYCLE_UPSERTS.parquet",
                POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
            ),
            "kalshi_lifecycle_upserts": (
                kalshi_snapshot_dataframe([*kx_upsert_rows, *mve_lifecycle]),
                "KALSHI_LIFECYCLE_UPSERTS.parquet",
                KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            ),
        }
        if mve is not None:
            mve_frame = kalshi_snapshot_dataframe(
                _dedupe_rows(
                    [row for rows in mve.values() for row in rows],
                    venue="kalshi",
                    timestamp_keys=(*_KX_UPDATED_KEYS, *_KX_CREATED_KEYS),
                )
            )
            frames["kalshi_mve_current_markets"] = (
                mve_frame,
                "KALSHI_MVE_CURRENT_MARKETS.parquet",
                KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            )
        artifacts: dict[str, Any] = {}
        as_of = iso_utc(utc_now())
        for name, (frame, filename, schema) in frames.items():
            staged_path = staging / filename
            write_parquet(frame, staged_path, schema=schema, strict=True)
            artifacts[name] = _artifact_from_staged(
                staged_path,
                final_path=release / filename,
                repository_root=self.repository_root,
                rows=len(frame),
                schema=schema,
                as_of_utc=as_of,
            )
            artifacts[name]["source_manifest"] = {
                "path": _stored_path(
                    release / "PUBLISHED_MANIFEST.json",
                    repository_root=self.repository_root,
                )
            }
        if scope == "standard":
            retained_mve = dict(previous["kalshi_mve_current_markets"])
            retained_mve.setdefault("source_manifest", previous.get("manifest"))
            artifacts["kalshi_mve_current_markets"] = retained_mve
        manifest = {
            "schema_version": CURRENT_MANIFEST_SCHEMA,
            "dataset_family": "markets",
            "release_id": release_id,
            "release_kind": "catalog_current",
            "scope": scope,
            "status": "completed",
            "started_at_utc": iso_utc(started),
            "published_at_utc": as_of,
            "predecessor_manifest": self._current_predecessor_reference(previous),
            "collection": {
                "polymarket": pm_collection,
                "kalshi_conventional": kx_collection,
                "kalshi_mve": mve_collection,
                "kalshi_mve_lifecycle": mve_lifecycle_details,
            },
            "lifecycle_reconciliation": {
                "polymarket_missing": len(missing_pm),
                "polymarket_terminal_updates": len(pm_upsert_rows),
                "polymarket_reopenings": len(pm_reopened),
                "polymarket_unresolved_absence": pm_unresolved,
                "kalshi_missing": len(missing_kx),
                "kalshi_terminal_updates": len(kx_upsert_rows),
                "kalshi_reopenings": sum(len(rows) for rows in kx_reopened.values()),
                "kalshi_unresolved_absence": kx_unresolved,
                "historical_rows_deleted": 0,
            },
            "artifacts": artifacts,
            "current_scope_completeness": {
                "polymarket": True,
                "kalshi_conventional": True,
                "kalshi_mve": scope == "all",
                "kalshi_mve_retained_from_predecessor": scope == "standard",
            },
            "network_accessed": True,
            "execution_authority": False,
            "orders_submitted": False,
        }
        manifest_path = staging / "PUBLISHED_MANIFEST.json"
        fs._atomic_json(manifest_path, manifest)
        self.releases_root.mkdir(parents=True, exist_ok=True)
        os.replace(staging, release)
        final_manifest = release / manifest_path.name
        pointer: dict[str, Any] = {
            **previous,
            "dataset_family": "markets",
            "release_id": release_id,
            "release_kind": "catalog_current",
            "updated_at_utc": as_of,
            "manifest": self._manifest_ref(final_manifest),
            "current_scope_completeness": manifest["current_scope_completeness"],
        }
        for name, artifact in artifacts.items():
            pointer[name] = artifact
        fs._atomic_json(self.current_pointer_path, pointer)
        return {
            "published": True,
            "release_id": release_id,
            "manifest": pointer["manifest"],
            "scope": scope,
            "artifacts": artifacts,
            "lifecycle_reconciliation": manifest["lifecycle_reconciliation"],
        }

    def _discovery_artifact_frames(
        self,
        manifests: Sequence[tuple[Path, Mapping[str, Any]]],
        *,
        artifact_name: str,
    ) -> list[tuple[Any, dict[str, Any]]]:
        import pandas as pd

        frames: list[tuple[Any, dict[str, Any]]] = []
        for manifest_path, manifest in manifests:
            artifacts = manifest.get("artifacts")
            artifact = (
                artifacts.get(artifact_name) if isinstance(artifacts, dict) else None
            )
            if not isinstance(artifact, dict) or not int(artifact.get("rows") or 0):
                continue
            path = _resolve_stored_path(
                str(artifact.get("path") or ""), repository_root=self.repository_root
            )
            if not path.is_file() or sha256_file(path) != artifact.get("sha256"):
                raise CatalogError(
                    f"discovery artifact is missing or hash-invalid: {path}"
                )
            frame = pd.read_parquet(path)
            if len(frame) != int(artifact["rows"]):
                raise CatalogError(f"discovery artifact row count drifted: {path}")
            frames.append(
                (
                    frame,
                    {
                        "manifest_path": _stored_path(
                            manifest_path, repository_root=self.repository_root
                        ),
                        "manifest_sha256": sha256_file(manifest_path),
                        "observed_at_utc": manifest.get("observed_at_utc"),
                        "stream": manifest.get("stream"),
                        "native_family": manifest.get("native_family"),
                    },
                )
            )
        return frames

    def promote_history(self) -> dict[str, Any]:
        """Promote newly discovered keys without rewriting known history rows."""
        return history.promote_history(self)

    def compaction_due(self) -> dict[str, Any]:
        _path, manifest = self._history_manifest()
        layers = int(manifest.get("promotion_layer_count") or 0)
        delta_rows = int(manifest.get("uncompacted_delta_rows") or 0)
        base_rows = int(manifest.get("base_row_count") or 0)
        delta_files = int(manifest.get("uncompacted_delta_parquet_file_count") or 0)
        base_files = int(manifest.get("base_parquet_file_count") or 0)
        conditions = {
            "four_weekly_promotion_layers": layers >= 4,
            "delta_rows_exceed_ten_percent": bool(
                base_rows and delta_rows > base_rows * 0.10
            ),
            "delta_files_exceed_twenty_five_percent": bool(
                base_files and delta_files > base_files * 0.25
            ),
        }
        return {
            "due": any(conditions.values()),
            "conditions": conditions,
            "metrics": {
                "promotion_layers": layers,
                "delta_rows": delta_rows,
                "base_rows": base_rows,
                "delta_files": delta_files,
                "base_files": base_files,
            },
        }

    def _operational_compaction_sources(
        self,
        *,
        venue: str,
        after_watermarks: Mapping[str, Any],
        current_manifests: Sequence[tuple[Path, Mapping[str, Any]]],
    ) -> tuple[list[str], dict[str, Any]]:
        columns = (
            POLYMARKET_MARKET_SNAPSHOT_COLUMNS
            if venue == "polymarket"
            else KALSHI_MARKET_SNAPSHOT_COLUMNS
        )
        column_sql = ", ".join(f'"{column}"' for column in columns)
        streams: tuple[DiscoveryStream, ...] = (
            ("polymarket",)
            if venue == "polymarket"
            else ("kalshi-conventional", "kalshi-mve")
        )
        sources: list[str] = []
        resulting: dict[str, Any] = {}
        pointer = self.read_discovery_pointer()
        for stream in streams:
            prior = after_watermarks.get(stream)
            prior_hash = prior.get("sha256") if isinstance(prior, dict) else None
            manifests = self.reachable_discovery_manifests(
                stream, after_hash=str(prior_hash) if prior_hash else None
            )
            current = (pointer.get("streams") or {}).get(stream)
            if isinstance(current, dict):
                resulting[stream] = current
            family = {
                "polymarket": "polymarket",
                "kalshi-conventional": "kalshi_conventional",
                "kalshi-mve": "kalshi_mve",
            }[stream]
            for _path, manifest in manifests:
                artifacts = manifest.get("artifacts")
                if not isinstance(artifacts, dict):
                    continue
                for artifact_name in ("new_markets", "market_upserts"):
                    artifact = artifacts.get(artifact_name)
                    if not isinstance(artifact, dict) or not int(
                        artifact.get("rows") or 0
                    ):
                        continue
                    artifact_path = _resolve_stored_path(
                        str(artifact["path"]), repository_root=self.repository_root
                    )
                    observed = str(manifest.get("observed_at_utc") or "")
                    sources.append(
                        f"SELECT {column_sql}, "
                        f"try_cast({_quote_sql(observed)} AS TIMESTAMPTZ) AS _observed_at, "
                        f"{_quote_sql(family)}::VARCHAR AS _native_family, "
                        f"{_quote_sql(str(manifest.get('release_id')))}::VARCHAR AS _source "
                        f"FROM {_parquet_sql(artifact_path)}"
                    )
        # Current census pointers are replaceable, but lifecycle corrections are
        # append-only through the predecessor chain. Consume every release after
        # the history watermark so an unrelated refresh cannot hide an earlier
        # terminal or reopening correction.
        for _manifest_path, current_manifest in current_manifests:
            lifecycle_name = (
                "polymarket_lifecycle_upserts"
                if venue == "polymarket"
                else "kalshi_lifecycle_upserts"
            )
            artifacts = current_manifest.get("artifacts")
            artifact = (
                artifacts.get(lifecycle_name) if isinstance(artifacts, dict) else None
            )
            if isinstance(artifact, dict) and int(artifact.get("rows") or 0):
                artifact_path = _resolve_stored_path(
                    str(artifact["path"]), repository_root=self.repository_root
                )
                observed = str(
                    artifact.get("as_of_utc")
                    or current_manifest.get("published_at_utc")
                    or ""
                )
                family_sql = (
                    "'polymarket'"
                    if venue == "polymarket"
                    else _kalshi_family_sql("market_key")
                )
                sources.append(
                    f"SELECT {column_sql}, "
                    f"try_cast({_quote_sql(observed)} AS TIMESTAMPTZ) AS _observed_at, "
                    f"{family_sql}::VARCHAR AS _native_family, "
                    f"{_quote_sql('current_lifecycle:' + str(current_manifest.get('release_id') or 'unknown'))}"
                    "::VARCHAR AS _source "
                    f"FROM {_parquet_sql(artifact_path)}"
                )
        return sources, resulting

    async def compact_history(
        self,
        *,
        force: bool = False,
        polymarket_client: Any | None = None,
        kalshi_client: Any | None = None,
    ) -> dict[str, Any]:
        """Rewrite a latest-row base while preserving every parent release."""
        return await history.compact_history(
            self,
            force=force,
            polymarket_client=polymarket_client,
            kalshi_client=kalshi_client,
        )

    def status(self, *, deep: bool = False) -> dict[str, Any]:
        pointer = self.read_discovery_pointer()
        _history_path, _history_manifest, history_integrity = self._history_state(
            deep=deep
        )
        index_state = "absent"
        if self.index_path.is_file():
            try:
                import duckdb

                with duckdb.connect(str(self.index_path), read_only=True) as connection:
                    stored = connection.execute(
                        "SELECT value FROM catalog_meta WHERE key='pointer_fingerprint'"
                    ).fetchone()
                index_state = (
                    "current"
                    if stored and stored[0] == self._index_fingerprint()
                    else "stale"
                )
            except Exception:
                index_state = "invalid"
        return {
            "market_root": str(self.market_root),
            "discovery_pointer": pointer,
            "current_pointer_present": self.current_pointer_path.is_file(),
            "history_pointer_present": self.history_pointer_path.is_file(),
            "history_integrity": history_integrity,
            "known_key_cache": {"path": str(self.index_path), "state": index_state},
            "scheduling_enabled": False,
            "execution_authority": False,
            "orders_submitted": False,
        }
