"""History promotion and compaction workflows for the market catalog."""

from __future__ import annotations

import httpx
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pmkt.data.canonical import (
    KALSHI_MARKET_SNAPSHOT_COLUMNS,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
)
from pmkt.data.normalize import markets_dataframe
from pmkt.data.storage.parquet import write_parquet

from . import fs
from .collect import kalshi_snapshot_dataframe
from .families import _kalshi_family_sql
from .fs import (
    _artifact_from_staged,
    _parquet_sql,
    _quote_sql,
    _stored_path,
    iso_utc,
    parquet_files,
    parse_timestamp,
    sha256_file,
    utc_now,
)
from .types import CatalogError, HISTORY_MANIFEST_SCHEMA

if TYPE_CHECKING:
    from .service import MarketCatalogService


def promote_history(service: MarketCatalogService) -> dict[str, Any]:
    """Promote newly discovered keys without rewriting known history rows."""
    import duckdb
    import pandas as pd

    parent_manifest_path, parent_manifest = service._history_manifest(deep=True)
    parent_manifest_sha256 = sha256_file(parent_manifest_path)
    prior_watermarks = parent_manifest.get("discovery_promotion_watermarks")
    if not isinstance(prior_watermarks, dict):
        prior_watermarks = {}
    manifests_by_stream: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    resulting_watermarks: dict[str, Any] = {}
    pointer = service.read_discovery_pointer()
    for stream in ("polymarket", "kalshi-conventional", "kalshi-mve"):
        prior = prior_watermarks.get(stream)
        prior_hash = prior.get("sha256") if isinstance(prior, dict) else None
        manifests = service.reachable_discovery_manifests(
            stream, after_hash=str(prior_hash) if prior_hash else None
        )
        manifests_by_stream[stream] = manifests
        current = (pointer.get("streams") or {}).get(stream)
        if not isinstance(current, dict):
            raise CatalogError(
                f"history promotion requires a discovery pointer for {stream}"
            )
        resulting_watermarks[stream] = current

    pm_inputs = service._discovery_artifact_frames(
        manifests_by_stream["polymarket"], artifact_name="new_markets"
    )
    kx_inputs = [
        *service._discovery_artifact_frames(
            manifests_by_stream["kalshi-conventional"],
            artifact_name="new_markets",
        ),
        *service._discovery_artifact_frames(
            manifests_by_stream["kalshi-mve"], artifact_name="new_markets"
        ),
    ]
    parent_pm = service._history_artifact_path(parent_manifest, "polymarket")
    parent_kx = service._history_artifact_path(parent_manifest, "kalshi")

    pm_candidates = (
        pd.concat([frame for frame, _lineage in pm_inputs], ignore_index=True)
        if pm_inputs
        else pd.DataFrame(columns=POLYMARKET_MARKET_SNAPSHOT_COLUMNS)
    )
    if not pm_candidates.empty:
        with duckdb.connect(database=":memory:") as connection:
            connection.register("candidates", pm_candidates)
            pm_new = connection.execute(
                f"""
                    SELECT {", ".join(POLYMARKET_MARKET_SNAPSHOT_COLUMNS)}
                    FROM candidates c
                    WHERE NOT EXISTS (
                      SELECT 1 FROM {_parquet_sql(parent_pm)} p
                      WHERE CAST(p.market_id AS VARCHAR) = CAST(c.market_id AS VARCHAR)
                    )
                    QUALIFY row_number() OVER (
                      PARTITION BY market_id
                      ORDER BY try_cast(json_extract_string(raw_json, '$.updatedAt')
                                        AS TIMESTAMPTZ) DESC NULLS LAST,
                               raw_json_sha256 DESC
                    ) = 1
                    """
            ).df()
    else:
        pm_new = pm_candidates

    kx_candidate_frames: list[Any] = []
    for frame, lineage in kx_inputs:
        materialized = frame.copy()
        materialized["_native_family"] = lineage["native_family"]
        kx_candidate_frames.append(materialized)
    kx_candidates = (
        pd.concat(kx_candidate_frames, ignore_index=True)
        if kx_candidate_frames
        else pd.DataFrame(
            columns=[*KALSHI_MARKET_SNAPSHOT_COLUMNS, "_native_family"]
        )
    )
    if not kx_candidates.empty:
        with duckdb.connect(database=":memory:") as connection:
            connection.register("candidates", kx_candidates)
            kx_new = connection.execute(
                f"""
                    SELECT {", ".join(KALSHI_MARKET_SNAPSHOT_COLUMNS)}, _native_family
                    FROM candidates c
                    WHERE NOT EXISTS (
                      SELECT 1 FROM {_parquet_sql(parent_kx)} p
                      WHERE CAST(p.market_key AS VARCHAR) = CAST(c.market_key AS VARCHAR)
                    )
                    QUALIFY row_number() OVER (
                      PARTITION BY market_key
                      ORDER BY try_cast(updated_time AS TIMESTAMPTZ) DESC NULLS LAST,
                               raw_json_sha256 DESC
                    ) = 1
                    """
            ).df()
    else:
        kx_new = kx_candidates

    release_id = fs._run_id("market_history")
    staging = service.history_root / ".staging" / release_id
    release = service.history_root / "releases" / release_id
    if staging.exists() or release.exists():
        raise FileExistsError(f"refusing to overwrite history release {release_id}")
    pm_target = staging / "POLYMARKET_ALL_MARKETS.parquet"
    kx_target = staging / "KALSHI_ALL_MARKETS.parquet"
    promotion_partition = f"promotion_id={release_id}"
    pm_output = (
        pm_target
        / "source=discovery_delta"
        / promotion_partition
        / "native_family=polymarket"
        / "part-000000.parquet"
        if not pm_new.empty
        else None
    )
    kx_outputs: list[tuple[Any, Path]] = []
    if not kx_new.empty:
        with duckdb.connect(database=":memory:") as connection:
            connection.register("new_rows", kx_new)
            bucketed = connection.execute(
                "SELECT *, CAST(hash(market_key) % 128 AS INTEGER) AS _bucket "
                "FROM new_rows"
            ).df()
        for (family, bucket), frame in bucketed.groupby(
            ["_native_family", "_bucket"], sort=True
        ):
            kx_outputs.append(
                (
                    frame.drop(columns=["_native_family", "_bucket"]),
                    kx_target
                    / "source=discovery_delta"
                    / promotion_partition
                    / f"native_family={family}"
                    / f"bucket={int(bucket)}"
                    / "part-000000.parquet",
                )
            )
    linked_pm = fs._hardlink_artifact(parent_pm, pm_target)
    linked_kx = fs._hardlink_artifact(parent_kx, kx_target)
    planned_outputs = [
        *([pm_output] if pm_output is not None else []),
        *(output for _frame, output in kx_outputs),
    ]
    collisions = [path for path in planned_outputs if path.exists()]
    if collisions:
        formatted = ", ".join(str(path) for path in collisions)
        raise CatalogError(
            f"history promotion output collides with linked parent: {formatted}"
        )
    if pm_output is not None:
        write_parquet(
            pm_new,
            pm_output,
            overwrite=False,
            schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
            strict=True,
        )
    for frame, output in kx_outputs:
        write_parquet(
            frame,
            output,
            overwrite=False,
            schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            strict=True,
        )
    pm_profile = fs._profile_history_artifact(pm_target, key_column="market_id")
    kx_profile = fs._profile_history_artifact(kx_target, key_column="market_key")
    for venue, profile in (("polymarket", pm_profile), ("kalshi", kx_profile)):
        if (
            profile["duplicate_key_count"]
            or profile["missing_key_count"]
            or profile["missing_payload_hash_count"]
        ):
            raise CatalogError(
                f"{venue} promoted history is not unique and complete"
            )

    delta_rows = len(pm_new) + len(kx_new)
    base_row_count = int(
        parent_manifest.get("base_row_count")
        or pm_profile["row_count"] + kx_profile["row_count"] - delta_rows
    )
    uncompacted_delta_rows = (
        int(parent_manifest.get("uncompacted_delta_rows") or 0) + delta_rows
    )
    actual_row_count = pm_profile["row_count"] + kx_profile["row_count"]
    expected_row_count = base_row_count + uncompacted_delta_rows
    if actual_row_count != expected_row_count:
        raise CatalogError(
            "promoted history row accounting mismatch: "
            f"actual={actual_row_count}, expected={expected_row_count}"
        )

    promoted_at = iso_utc(utc_now())
    coverage_values = [
        parse_timestamp(item.get("high_watermark_utc"))
        for item in resulting_watermarks.values()
    ]
    coverage_complete = min(
        (value for value in coverage_values if value is not None),
        default=utc_now(),
    )
    current_pointer_ref = (
        {
            "path": _stored_path(
                service.current_pointer_path, repository_root=service.repository_root
            ),
            "sha256": sha256_file(service.current_pointer_path),
        }
        if service.current_pointer_path.is_file()
        else None
    )
    pm_artifact = _artifact_from_staged(
        pm_target,
        final_path=release / pm_target.name,
        repository_root=service.repository_root,
        rows=pm_profile["row_count"],
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    kx_artifact = _artifact_from_staged(
        kx_target,
        final_path=release / kx_target.name,
        repository_root=service.repository_root,
        rows=kx_profile["row_count"],
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    parent_metadata_freshness = parent_manifest.get(
        "metadata_compacted_through_utc"
    ) or parent_manifest.get("published_at_utc")
    manifest_chain = {
        stream: [
            {
                "path": _stored_path(path, repository_root=service.repository_root),
                "sha256": sha256_file(path),
            }
            for path, _manifest in manifests
        ]
        for stream, manifests in manifests_by_stream.items()
    }
    manifest = {
        "schema_version": HISTORY_MANIFEST_SCHEMA,
        "dataset_family": "market_history",
        "release_id": release_id,
        "release_kind": "discovery_key_promotion",
        "status": "completed",
        "grain": "one latest-known row per venue market",
        "published_at_utc": promoted_at,
        "as_of": parent_manifest.get("as_of", {}),
        "coverage_complete_through_utc": iso_utc(coverage_complete),
        "metadata_compacted_through_utc": parent_metadata_freshness,
        "current_catalog_pointer": current_pointer_ref,
        "discovery_promotion_watermarks": resulting_watermarks,
        "metadata_compaction_watermarks": parent_manifest.get(
            "metadata_compaction_watermarks", {}
        ),
        "discovery_manifest_chain": manifest_chain,
        "parent_release": {
            "release_id": parent_manifest.get("release_id"),
            "manifest_path": _stored_path(
                parent_manifest_path, repository_root=service.repository_root
            ),
            "manifest_sha256": sha256_file(parent_manifest_path),
        },
        "promotion_layer_count": int(
            parent_manifest.get("promotion_layer_count") or 0
        )
        + 1,
        "uncompacted_delta_rows": uncompacted_delta_rows,
        "base_row_count": base_row_count,
        "base_parquet_file_count": int(
            parent_manifest.get("base_parquet_file_count") or linked_pm + linked_kx
        ),
        "uncompacted_delta_parquet_file_count": int(
            parent_manifest.get("uncompacted_delta_parquet_file_count") or 0
        )
        + (
            len(parquet_files(pm_target))
            + len(parquet_files(kx_target))
            - linked_pm
            - linked_kx
        ),
        "promoted_rows": {"polymarket": len(pm_new), "kalshi": len(kx_new)},
        "profiles": {"polymarket": pm_profile, "kalshi": kx_profile},
        "provenance": parent_manifest.get("provenance", {}),
        "artifacts": {
            "polymarket_all_markets": pm_artifact,
            "kalshi_all_markets": kx_artifact,
        },
        "limitations": [
            *list(parent_manifest.get("limitations") or []),
            "This is a latest-snapshot dimension, not a change time series.",
            "Known-key metadata and lifecycle upserts remain operational until compaction.",
            "Hard-linked parent Parquet files are immutable shared filesystem references.",
        ],
        "network_accessed": False,
        "research_only": True,
        "execution_authority": False,
        "orders_submitted": False,
    }
    manifest_path = staging / "PUBLISHED_MANIFEST.json"
    fs._atomic_json(manifest_path, manifest)
    current_parent_path, _current_parent = service._history_manifest(deep=True)
    if (
        current_parent_path.resolve() != parent_manifest_path.resolve()
        or sha256_file(current_parent_path) != parent_manifest_sha256
    ):
        raise CatalogError("history parent changed during promotion")
    release.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, release)
    final_manifest = release / manifest_path.name
    pointer_value = {
        "dataset_family": "market_history",
        "grain": manifest["grain"],
        "release_id": release_id,
        "updated_at_utc": promoted_at,
        "coverage_complete_through_utc": manifest["coverage_complete_through_utc"],
        "metadata_compacted_through_utc": parent_metadata_freshness,
        "manifest": service._manifest_ref(final_manifest),
        "polymarket_all_markets": pm_artifact,
        "kalshi_all_markets": kx_artifact,
    }
    fs._atomic_json(service.history_pointer_path, pointer_value)
    return {
        "published": True,
        "release_id": release_id,
        "promoted_rows": manifest["promoted_rows"],
        "manifest": pointer_value["manifest"],
    }


async def compact_history(
    service: MarketCatalogService,
    *,
    force: bool = False,
    polymarket_client: Any | None = None,
    kalshi_client: Any | None = None,
) -> dict[str, Any]:
    """Rewrite a latest-row base while preserving every parent release."""
    import duckdb

    due = service.compaction_due()
    if not force and not due["due"]:
        return {"published": False, "reason": "compaction_not_due", **due}
    parent_manifest_path, parent_manifest = service._history_manifest(deep=True)
    parent_pm = service._history_artifact_path(parent_manifest, "polymarket")
    parent_kx = service._history_artifact_path(parent_manifest, "kalshi")
    prior_watermarks = parent_manifest.get("metadata_compaction_watermarks")
    if not isinstance(prior_watermarks, dict):
        prior_watermarks = {}
    prior_current_watermark = parent_manifest.get("current_lifecycle_watermark")
    prior_current_hash = (
        str(prior_current_watermark.get("sha256") or "")
        if isinstance(prior_current_watermark, dict)
        else ""
    )
    current_manifests = service.reachable_current_manifests(
        after_hash=prior_current_hash or None
    )
    current_lifecycle_watermark: dict[str, Any] | None
    if current_manifests:
        current_lifecycle_watermark = service._manifest_ref(current_manifests[-1][0])
        current_lifecycle_watermark["release_id"] = current_manifests[-1][1].get(
            "release_id"
        )
    elif isinstance(prior_current_watermark, dict):
        current_lifecycle_watermark = dict(prior_current_watermark)
    else:
        current_lifecycle_watermark = None
    parent_observed = str(
        parent_manifest.get("metadata_compacted_through_utc")
        or parent_manifest.get("published_at_utc")
        or iso_utc(utc_now())
    )
    pm_columns = ", ".join(
        f'"{column}"' for column in POLYMARKET_MARKET_SNAPSHOT_COLUMNS
    )
    kx_columns = ", ".join(
        f'"{column}"' for column in KALSHI_MARKET_SNAPSHOT_COLUMNS
    )
    pm_sources = [
        f"SELECT {pm_columns}, try_cast({_quote_sql(parent_observed)} AS TIMESTAMPTZ) "
        "AS _observed_at, 'polymarket'::VARCHAR AS _native_family, "
        "'parent_base'::VARCHAR AS _source "
        f"FROM {_parquet_sql(parent_pm)}"
    ]
    kx_sources = [
        f"SELECT {kx_columns}, try_cast({_quote_sql(parent_observed)} AS TIMESTAMPTZ) "
        f"AS _observed_at, {_kalshi_family_sql('market_key', filename_sql='filename')}"
        "::VARCHAR AS _native_family, "
        "'parent_base'::VARCHAR AS _source "
        f"FROM {_parquet_sql(parent_kx, filename=True)}"
    ]
    pm_operational, pm_watermarks = service._operational_compaction_sources(
        venue="polymarket",
        after_watermarks=prior_watermarks,
        current_manifests=current_manifests,
    )
    kx_operational, kx_watermarks = service._operational_compaction_sources(
        venue="kalshi",
        after_watermarks=prior_watermarks,
        current_manifests=current_manifests,
    )
    pm_sources.extend(pm_operational)
    kx_sources.extend(kx_operational)
    release_id = fs._run_id("market_history_compacted")
    staging = service.history_root / ".staging" / release_id
    release = service.history_root / "releases" / release_id
    if staging.exists() or release.exists():
        raise FileExistsError(f"refusing to overwrite history release {release_id}")
    staging.mkdir(parents=True)
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute(
            "CREATE TEMP VIEW pm_candidates AS "
            + " UNION ALL BY NAME ".join(pm_sources)
        )
        connection.execute(
            "CREATE TEMP VIEW kx_candidates AS "
            + " UNION ALL BY NAME ".join(kx_sources)
        )
        pm_conflicts = [
            str(row[0])
            for row in connection.execute(
                """
                    WITH timed AS (
                      SELECT *, coalesce(
                        try_cast(json_extract_string(raw_json, '$.updatedAt') AS TIMESTAMPTZ),
                        _observed_at
                      ) AS effective_time
                      FROM pm_candidates
                    ), maxima AS (
                      SELECT market_id, max(effective_time) AS effective_time
                      FROM timed GROUP BY market_id
                    )
                    SELECT CAST(t.market_id AS VARCHAR)
                    FROM timed t JOIN maxima m USING (market_id, effective_time)
                    GROUP BY t.market_id
                    HAVING count(DISTINCT raw_json_sha256) > 1
                    """
            ).fetchall()
        ]
        kx_conflicts = [
            str(row[0])
            for row in connection.execute(
                """
                    WITH timed AS (
                      SELECT *, coalesce(try_cast(updated_time AS TIMESTAMPTZ), _observed_at)
                        AS effective_time
                      FROM kx_candidates
                    ), maxima AS (
                      SELECT market_key, max(effective_time) AS effective_time
                      FROM timed GROUP BY market_key
                    )
                    SELECT CAST(t.market_key AS VARCHAR)
                    FROM timed t JOIN maxima m USING (market_key, effective_time)
                    GROUP BY t.market_key
                    HAVING count(DISTINCT raw_json_sha256) > 1
                    """
            ).fetchall()
        ]
        targeted_at = iso_utc(utc_now())
        pm_candidate_view = "pm_candidates"
        kx_candidate_view = "kx_candidates"
        if pm_conflicts:
            if polymarket_client is None:
                raise CatalogError(
                    "Polymarket conflicts require a caller-supplied read-only client"
                )
            resolved = [await polymarket_client.market(key) for key in pm_conflicts]
            frame = markets_dataframe(resolved)
            connection.register("pm_targeted", frame)
            connection.execute(
                f"""
                    CREATE TEMP VIEW pm_candidates_resolved AS
                    SELECT * FROM pm_candidates
                    UNION ALL BY NAME
                    SELECT {pm_columns}, try_cast({_quote_sql(targeted_at)} AS TIMESTAMPTZ),
                           'polymarket', 'targeted_conflict_read'
                    FROM pm_targeted
                    """
            )
            pm_candidate_view = "pm_candidates_resolved"
        if kx_conflicts:
            if kalshi_client is None:
                raise CatalogError(
                    "Kalshi conflicts require a caller-supplied read-only client"
                )
            resolved = []
            for key in kx_conflicts:
                try:
                    resolved.append(await kalshi_client.market(key))
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 404:
                        raise
                    resolved.append(await kalshi_client.historical_market(key))
            frame = kalshi_snapshot_dataframe(resolved)
            connection.register("kx_targeted", frame)
            connection.execute(
                f"""
                    CREATE TEMP VIEW kx_candidates_resolved AS
                    SELECT * FROM kx_candidates
                    UNION ALL BY NAME
                    SELECT {kx_columns}, try_cast({_quote_sql(targeted_at)} AS TIMESTAMPTZ),
                           {_kalshi_family_sql("market_key")},
                           'targeted_conflict_read'
                    FROM kx_targeted
                    """
            )
            kx_candidate_view = "kx_candidates_resolved"
        pm_target = (
            staging
            / "POLYMARKET_ALL_MARKETS.parquet"
            / "source=compacted_base"
            / "native_family=polymarket"
            / "part-000000.parquet"
        )
        pm_target.parent.mkdir(parents=True, exist_ok=True)
        connection.execute(
            f"""
                COPY (
                  SELECT {pm_columns}
                  FROM {pm_candidate_view}
                  QUALIFY row_number() OVER (
                    PARTITION BY market_id
                    ORDER BY try_cast(json_extract_string(raw_json, '$.updatedAt')
                                      AS TIMESTAMPTZ) DESC NULLS LAST,
                             _observed_at DESC, raw_json_sha256 DESC
                  ) = 1
                ) TO {_quote_sql(pm_target.resolve().as_posix())}
                  (FORMAT PARQUET, COMPRESSION ZSTD)
                """
        )
        kx_target = staging / "KALSHI_ALL_MARKETS.parquet"
        kx_target.mkdir(parents=True)
        connection.execute(
            f"""
                COPY (
                  SELECT {kx_columns}, _native_family AS native_family,
                         CAST(hash(market_key) % 128 AS INTEGER) AS bucket
                  FROM {kx_candidate_view}
                  QUALIFY row_number() OVER (
                    PARTITION BY market_key
                    ORDER BY try_cast(updated_time AS TIMESTAMPTZ) DESC NULLS LAST,
                             _observed_at DESC, raw_json_sha256 DESC
                  ) = 1
                ) TO {_quote_sql(kx_target.resolve().as_posix())}
                  (FORMAT PARQUET, COMPRESSION ZSTD,
                   PARTITION_BY(native_family, bucket))
                """
        )
    finally:
        connection.close()
    pm_profile = fs._profile_history_artifact(
        staging / "POLYMARKET_ALL_MARKETS.parquet", key_column="market_id"
    )
    kx_profile = fs._profile_history_artifact(
        staging / "KALSHI_ALL_MARKETS.parquet", key_column="market_key"
    )
    if pm_profile["duplicate_key_count"] or kx_profile["duplicate_key_count"]:
        raise CatalogError("compacted history contains duplicate keys")
    compacted_at = iso_utc(utc_now())
    pm_artifact = _artifact_from_staged(
        staging / "POLYMARKET_ALL_MARKETS.parquet",
        final_path=release / "POLYMARKET_ALL_MARKETS.parquet",
        repository_root=service.repository_root,
        rows=pm_profile["row_count"],
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    kx_artifact = _artifact_from_staged(
        staging / "KALSHI_ALL_MARKETS.parquet",
        final_path=release / "KALSHI_ALL_MARKETS.parquet",
        repository_root=service.repository_root,
        rows=kx_profile["row_count"],
        schema=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    watermarks = {**pm_watermarks, **kx_watermarks}
    manifest = {
        "schema_version": HISTORY_MANIFEST_SCHEMA,
        "dataset_family": "market_history",
        "release_id": release_id,
        "release_kind": "compacted_latest_snapshot_base",
        "status": "completed",
        "grain": "one latest-known row per venue market",
        "published_at_utc": compacted_at,
        "as_of": parent_manifest.get("as_of", {}),
        "coverage_complete_through_utc": parent_manifest.get(
            "coverage_complete_through_utc", compacted_at
        ),
        "metadata_compacted_through_utc": compacted_at,
        "discovery_promotion_watermarks": parent_manifest.get(
            "discovery_promotion_watermarks", {}
        ),
        "metadata_compaction_watermarks": watermarks,
        "current_lifecycle_watermark": current_lifecycle_watermark,
        "parent_release": {
            "release_id": parent_manifest.get("release_id"),
            "manifest_path": _stored_path(
                parent_manifest_path, repository_root=service.repository_root
            ),
            "manifest_sha256": sha256_file(parent_manifest_path),
        },
        "parent_manifests": [
            *list(parent_manifest.get("parent_manifests") or []),
            {
                "path": _stored_path(
                    parent_manifest_path, repository_root=service.repository_root
                ),
                "sha256": sha256_file(parent_manifest_path),
            },
        ],
        "provenance": parent_manifest.get("provenance", {}),
        "limitations": parent_manifest.get("limitations", []),
        "targeted_conflict_reads": {
            "polymarket": pm_conflicts,
            "kalshi": kx_conflicts,
        },
        "promotion_layer_count": 0,
        "uncompacted_delta_rows": 0,
        "uncompacted_delta_parquet_file_count": 0,
        "base_row_count": pm_profile["row_count"] + kx_profile["row_count"],
        "base_parquet_file_count": pm_artifact["parquet_file_count"]
        + kx_artifact["parquet_file_count"],
        "profiles": {"polymarket": pm_profile, "kalshi": kx_profile},
        "artifacts": {
            "polymarket_all_markets": pm_artifact,
            "kalshi_all_markets": kx_artifact,
        },
        "network_accessed": bool(pm_conflicts or kx_conflicts),
        "research_only": True,
        "execution_authority": False,
        "orders_submitted": False,
    }
    manifest_path = staging / "PUBLISHED_MANIFEST.json"
    fs._atomic_json(manifest_path, manifest)
    release.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, release)
    final_manifest = release / manifest_path.name
    pointer = {
        "dataset_family": "market_history",
        "grain": manifest["grain"],
        "release_id": release_id,
        "updated_at_utc": compacted_at,
        "coverage_complete_through_utc": manifest["coverage_complete_through_utc"],
        "metadata_compacted_through_utc": compacted_at,
        "current_lifecycle_watermark": current_lifecycle_watermark,
        "manifest": service._manifest_ref(final_manifest),
        "polymarket_all_markets": pm_artifact,
        "kalshi_all_markets": kx_artifact,
    }
    fs._atomic_json(service.history_pointer_path, pointer)
    return {
        "published": True,
        "release_id": release_id,
        "manifest": pointer["manifest"],
        "profiles": manifest["profiles"],
        "due": due,
    }
