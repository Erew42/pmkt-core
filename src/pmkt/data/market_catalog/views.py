"""DuckDB view registration for immutable catalog history."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pmkt.data.canonical import (
    KALSHI_MARKET_SNAPSHOT_COLUMNS,
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS,
)

from .families import (
    _kalshi_family_provenance_sql,
    _kalshi_family_sql,
    _polymarket_operational_family_sql,
)
from .fs import (
    _parquet_sql,
    _quote_sql,
    parquet_files,
)
from .reader import ResolvedCatalogArtifact
from .service import MarketCatalogService
from .types import FAMILY_CLASSIFIER_VERSION


CATALOG_VIEW_CONTRACT_VERSION = "pmkt.catalog_views.v1"


def register_catalog_views(
    connection: Any, market_root: str | Path = "data/markets"
) -> None:
    """Register latest immutable history with native and derived family columns."""
    service = MarketCatalogService(market_root)
    _manifest_path, manifest = service._history_manifest()
    pm_path = service._history_artifact_path(manifest, "polymarket")
    kx_path = service._history_artifact_path(manifest, "kalshi")
    pm_source = _parquet_sql(pm_path)
    kx_files = parquet_files(kx_path)
    kx_literals = ", ".join(_quote_sql(path.resolve().as_posix()) for path in kx_files)
    _register_views(
        connection,
        pm_source=pm_source,
        kx_files_sql=kx_literals,
        kx_relative_path_sql="filename",
        polymarket_family_provenance="partition_provenance",
    )


def register_resolved_catalog_views(
    connection: Any,
    artifacts: tuple[ResolvedCatalogArtifact, ...],
    *,
    view_contract_version: str = CATALOG_VIEW_CONTRACT_VERSION,
) -> None:
    """Register one already validated artifact set without following a pointer."""
    if view_contract_version != CATALOG_VIEW_CONTRACT_VERSION:
        raise ValueError(
            f"unsupported catalog view contract {view_contract_version!r}"
        )
    by_venue = {artifact.venue: artifact for artifact in artifacts}
    if set(by_venue) != {"polymarket", "kalshi"}:
        raise ValueError("catalog artifacts must contain polymarket and kalshi")
    pm_artifact = by_venue["polymarket"]
    kx_artifact = by_venue["kalshi"]
    pm_source = _paths_sql(pm_artifact.files)
    kx_files_sql = ", ".join(
        _quote_sql(path.resolve().as_posix()) for path in kx_artifact.files
    )
    kx_relative_path_sql = _relative_path_case(kx_artifact)
    _register_views(
        connection,
        pm_source=pm_source,
        kx_files_sql=kx_files_sql,
        kx_relative_path_sql=kx_relative_path_sql,
        polymarket_family_provenance="venue_identity",
    )


def _paths_sql(paths: tuple[Path, ...]) -> str:
    literals = ", ".join(_quote_sql(path.resolve().as_posix()) for path in paths)
    return f"read_parquet([{literals}], union_by_name=true, hive_partitioning=false)"


def _relative_path_case(artifact: ResolvedCatalogArtifact) -> str:
    normalized_filename = "replace(CAST(filename AS VARCHAR), chr(92), '/')"
    cases = " ".join(
        f"WHEN {normalized_filename} = {_quote_sql(path.resolve().as_posix())} "
        f"THEN {_quote_sql('/' + relative.casefold())}"
        for path, relative in zip(artifact.files, artifact.relative_files)
    )
    return f"CASE {cases} ELSE NULL END"


def _register_views(
    connection: Any,
    *,
    pm_source: str,
    kx_files_sql: str,
    kx_relative_path_sql: str,
    polymarket_family_provenance: str,
) -> None:
    pm_columns = ", ".join(
        f'p."{column}"' for column in POLYMARKET_MARKET_SNAPSHOT_COLUMNS
    )
    kx_columns = ", ".join(f'k."{column}"' for column in KALSHI_MARKET_SNAPSHOT_COLUMNS)
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW market_catalog_polymarket AS
        SELECT {pm_columns},
               'polymarket'::VARCHAR AS native_family,
               {_quote_sql(polymarket_family_provenance)}::VARCHAR
                   AS family_provenance,
               {_polymarket_operational_family_sql("slug")}::VARCHAR
                   AS operational_family,
               {_quote_sql(FAMILY_CLASSIFIER_VERSION)}::VARCHAR
                   AS family_classifier_version
        FROM {pm_source} AS p
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW market_catalog_kalshi AS
        WITH source_rows AS (
          SELECT * FROM read_parquet(
            [{kx_files_sql}], union_by_name=true, hive_partitioning=false, filename=true
          )
        ), classified_rows AS (
          SELECT *, {kx_relative_path_sql} AS _pmkt_relative_path
          FROM source_rows
        )
        SELECT {kx_columns},
               {_kalshi_family_sql("market_key", filename_sql="_pmkt_relative_path")}::VARCHAR
                   AS native_family,
               {_kalshi_family_provenance_sql("market_key", filename_sql="_pmkt_relative_path")}
                   ::VARCHAR AS family_provenance,
               {_kalshi_family_sql("market_key", filename_sql="_pmkt_relative_path")}::VARCHAR
                   AS operational_family,
               {_quote_sql(FAMILY_CLASSIFIER_VERSION)}::VARCHAR
                   AS family_classifier_version
        FROM classified_rows AS k
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE VIEW market_catalog AS
        SELECT 'polymarket'::VARCHAR AS venue,
               CAST(market_id AS VARCHAR) AS market_key,
               question, close_time, CAST(closed AS BOOLEAN) AS closed,
               CAST(NULL AS VARCHAR) AS status, raw_json, raw_json_sha256,
               native_family, family_provenance, operational_family,
               family_classifier_version
        FROM market_catalog_polymarket
        UNION ALL BY NAME
        SELECT 'kalshi'::VARCHAR AS venue,
               CAST(market_key AS VARCHAR) AS market_key,
               question, close_time, CAST(closed AS BOOLEAN) AS closed,
               status, raw_json, raw_json_sha256,
               native_family, family_provenance, operational_family,
               family_classifier_version
        FROM market_catalog_kalshi
        """
    )


__all__ = [
    "CATALOG_VIEW_CONTRACT_VERSION",
    "register_catalog_views",
    "register_resolved_catalog_views",
]
