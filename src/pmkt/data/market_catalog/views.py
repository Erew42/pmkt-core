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
from .service import MarketCatalogService
from .types import FAMILY_CLASSIFIER_VERSION


def register_catalog_views(
    connection: Any, market_root: str | Path = "data/markets"
) -> None:
    """Register latest immutable history with native and derived family columns."""
    service = MarketCatalogService(market_root)
    _manifest_path, manifest = service._history_manifest()
    pm_path = service._history_artifact_path(manifest, "polymarket")
    kx_path = service._history_artifact_path(manifest, "kalshi")
    pm_source = _parquet_sql(pm_path)
    pm_columns = ", ".join(
        f'p."{column}"' for column in POLYMARKET_MARKET_SNAPSHOT_COLUMNS
    )
    kx_columns = ", ".join(f'k."{column}"' for column in KALSHI_MARKET_SNAPSHOT_COLUMNS)
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW market_catalog_polymarket AS
        SELECT {pm_columns},
               'polymarket'::VARCHAR AS native_family,
               'partition_provenance'::VARCHAR AS family_provenance,
               {_polymarket_operational_family_sql("slug")}::VARCHAR
                   AS operational_family,
               {_quote_sql(FAMILY_CLASSIFIER_VERSION)}::VARCHAR
                   AS family_classifier_version
        FROM {pm_source} AS p
        """
    )
    # Filename carries immutable acquisition provenance for new partitions.
    kx_files = parquet_files(kx_path)
    kx_literals = ", ".join(_quote_sql(path.resolve().as_posix()) for path in kx_files)
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW market_catalog_kalshi AS
        WITH source_rows AS (
          SELECT * FROM read_parquet(
            [{kx_literals}], union_by_name=true, hive_partitioning=false, filename=true
          )
        )
        SELECT {kx_columns},
               {_kalshi_family_sql("market_key", filename_sql="filename")}::VARCHAR
                   AS native_family,
               {_kalshi_family_provenance_sql("market_key", filename_sql="filename")}
                   ::VARCHAR AS family_provenance,
               {_kalshi_family_sql("market_key", filename_sql="filename")}::VARCHAR
                   AS operational_family,
               {_quote_sql(FAMILY_CLASSIFIER_VERSION)}::VARCHAR
                   AS family_classifier_version
        FROM source_rows AS k
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
