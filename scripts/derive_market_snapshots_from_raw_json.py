from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from pmkt.data.normalize import markets_dataframe
from pmkt.data.contract_evidence import contract_evidence_dataframe
from pmkt.data.contract_evidence_manifest import (
    CONTRACT_EVIDENCE_MANIFEST_VERSION,
    contract_evidence_manifest_path,
    write_contract_evidence_manifest,
)
from pmkt.data.registry import (
    CONTRACT_EVIDENCE_COLUMNS,
    CONTRACT_EVIDENCE_SCHEMA_VERSION,
    KALSHI_MARKET_SNAPSHOT_COLUMNS,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
)
from pmkt.data.storage.parquet import write_parquet
from pmkt.data.validation import coerce_frame, validate_frame
from pmkt.exchanges.kalshi.client import kalshi_markets_dataframe

RAW_COLUMNS = ("raw_json", "raw_json_sha256")


@dataclass(frozen=True)
class VenueDerivationResult:
    exchange: str
    source_path: Path
    snapshot_path: Path
    projection_path: Path
    contract_evidence_path: Path
    contract_evidence_manifest_path: Path
    schema_version: str
    derivation_mode: str
    row_count: int
    batch_count: int

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "source_path": str(self.source_path),
            "snapshot_path": str(self.snapshot_path),
            "projection_path": str(self.projection_path),
            "contract_evidence_path": str(self.contract_evidence_path),
            "contract_evidence_manifest_path": str(
                self.contract_evidence_manifest_path
            ),
            "schema_version": self.schema_version,
            "derivation_mode": self.derivation_mode,
            "row_count": self.row_count,
            "batch_count": self.batch_count,
        }


@dataclass(frozen=True)
class DerivationResult:
    output_dir: Path
    polymarket: VenueDerivationResult
    kalshi: VenueDerivationResult
    summary_path: Path
    manifest_path: Path

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "output_dir": str(self.output_dir),
            "polymarket": self.polymarket.to_jsonable(),
            "kalshi": self.kalshi.to_jsonable(),
            "summary_path": str(self.summary_path),
            "manifest_path": str(self.manifest_path),
        }


def derive_market_snapshots_from_raw_json(
    *,
    polymarket_source: Path,
    kalshi_source: Path,
    output_dir: Path,
    batch_size: int = 50_000,
    overwrite: bool = False,
    observed_at_utc: str | None = None,
) -> DerivationResult:
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    _prepare_output_dir(output_dir, overwrite=overwrite)

    polymarket = _derive_venue(
        exchange="polymarket",
        source_path=polymarket_source,
        output_dir=output_dir,
        batch_size=batch_size,
        schema_version=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        snapshot_columns=tuple(POLYMARKET_MARKET_SNAPSHOT_COLUMNS),
        key_column="market_id",
        normalizer=markets_dataframe,
        observed_at_utc=observed_at_utc,
    )
    kalshi = _derive_venue(
        exchange="kalshi",
        source_path=kalshi_source,
        output_dir=output_dir,
        batch_size=batch_size,
        schema_version=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        snapshot_columns=tuple(KALSHI_MARKET_SNAPSHOT_COLUMNS),
        key_column="market_key",
        normalizer=kalshi_markets_dataframe,
        observed_at_utc=observed_at_utc,
    )
    summary_path = output_dir / "summary.json"
    manifest_path = output_dir / "derived_manifest.json"
    result = DerivationResult(
        output_dir=output_dir,
        polymarket=polymarket,
        kalshi=kalshi,
        summary_path=summary_path,
        manifest_path=manifest_path,
    )
    _write_json(summary_path, result.to_jsonable())
    _write_json(manifest_path, _manifest_payload(result))
    return result


def _derive_venue(
    *,
    exchange: str,
    source_path: Path,
    output_dir: Path,
    batch_size: int,
    schema_version: str,
    snapshot_columns: tuple[str, ...],
    key_column: str,
    normalizer: Callable[[list[dict[str, Any]]], pd.DataFrame],
    observed_at_utc: str | None,
) -> VenueDerivationResult:
    if not source_path.exists():
        raise FileNotFoundError(f"{exchange} source path not found: {source_path}")

    parts_dir = output_dir / "_parts" / exchange
    parts_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / f"{exchange}_markets.parquet"
    projection_path = output_dir / f"{exchange}_matching_projection.parquet"
    contract_evidence_path = output_dir / f"{exchange}_contract_evidence.parquet"
    row_count = 0
    batch_count = 0
    derivation_mode: str | None = None

    for batch_count, raw_batch in enumerate(
        _iter_source_batches(source_path, batch_size=batch_size),
        start=1,
    ):
        normalized, batch_mode = _derive_batch(
            exchange=exchange,
            source_batch=raw_batch,
            source_path=source_path,
            batch_index=batch_count,
            schema_version=schema_version,
            normalizer=normalizer,
        )
        if derivation_mode is None:
            derivation_mode = batch_mode
        elif derivation_mode != batch_mode:
            raise ValueError(
                f"{exchange} source mixes derivation modes: "
                f"{derivation_mode} then {batch_mode}"
            )
        row_count += int(len(normalized))
        write_parquet(
            normalized,
            parts_dir / f"part-{batch_count:06d}.parquet",
            overwrite=True,
            schema=schema_version,
            strict=True,
        )
        print(
            f"[derive-snapshots] exchange={exchange} "
            f"batch={batch_count} rows={len(normalized)} total_rows={row_count}",
            flush=True,
        )

    _coalesce_parts(
        parts_dir=parts_dir,
        output_path=snapshot_path,
        columns=snapshot_columns,
        schema_version=schema_version,
        empty_normalizer=normalizer,
    )
    row_count = _validate_unique_keys(
        path=snapshot_path,
        exchange=exchange,
        key_column=key_column,
    )
    _write_projection(
        source_path=snapshot_path,
        output_path=projection_path,
        columns=tuple(column for column in snapshot_columns if column != "raw_json"),
    )
    projection_count = _count_parquet_rows(projection_path)
    if projection_count != row_count:
        raise ValueError(
            f"{exchange} projection row count mismatch: "
            f"{projection_count} projection rows vs {row_count} snapshot rows"
        )
    evidence_count, observation_time_source = _write_contract_evidence_sidecar(
        exchange=exchange,
        evidence_source_path=source_path,
        output_path=contract_evidence_path,
        output_dir=output_dir,
        batch_size=batch_size,
        source_endpoint=(
            "snapshot:raw_json"
            if derivation_mode == "raw_json"
            else "snapshot:normalized"
        ),
        observed_at_utc=observed_at_utc,
    )
    if evidence_count != row_count:
        raise ValueError(
            f"{exchange} contract evidence row count mismatch: "
            f"{evidence_count} evidence rows vs {row_count} snapshot rows"
        )

    evidence_manifest_path = write_contract_evidence_manifest(
        pd.read_parquet(contract_evidence_path),
        artifact_path=contract_evidence_path,
        venue=exchange,
        source_endpoint=(
            "snapshot:raw_json"
            if derivation_mode == "raw_json"
            else "snapshot:normalized"
        ),
        payload_scope="snapshot",
        observation_time_source=observation_time_source,
    )

    return VenueDerivationResult(
        exchange=exchange,
        source_path=source_path,
        snapshot_path=snapshot_path,
        projection_path=projection_path,
        contract_evidence_path=contract_evidence_path,
        contract_evidence_manifest_path=evidence_manifest_path,
        schema_version=schema_version,
        derivation_mode=derivation_mode or "empty",
        row_count=row_count,
        batch_count=batch_count,
    )


def _write_contract_evidence_sidecar(
    *,
    exchange: str,
    evidence_source_path: Path,
    output_path: Path,
    output_dir: Path,
    batch_size: int,
    source_endpoint: str,
    observed_at_utc: str | None,
) -> tuple[int, str]:
    evidence_parts = output_dir / "_parts" / f"{exchange}_contract_evidence"
    evidence_parts.mkdir(parents=True, exist_ok=True)
    derived_at = datetime.now(timezone.utc).isoformat()
    row_count = 0
    observation_sources: set[str] = set()
    for batch_index, batch in enumerate(
        _iter_source_batches(evidence_source_path, batch_size=batch_size),
        start=1,
    ):
        records = batch.to_dict(orient="records")
        for record in records:
            if _is_missing(record.get("observed_at_utc")):
                if observed_at_utc is not None:
                    record["observed_at_utc"] = observed_at_utc
                    observation_sources.add("cli_override")
            else:
                observation_sources.add("source_row")
        evidence = contract_evidence_dataframe(
            records,
            venue=exchange,  # type: ignore[arg-type]
            source_endpoint=source_endpoint,
            payload_scope="snapshot",
            derived_at_utc=derived_at,
        )
        write_parquet(
            evidence,
            evidence_parts / f"part-{batch_index:06d}.parquet",
            overwrite=True,
            schema=CONTRACT_EVIDENCE_SCHEMA_VERSION,
            strict=True,
        )
        row_count += int(len(evidence))
    _coalesce_parts(
        parts_dir=evidence_parts,
        output_path=output_path,
        columns=tuple(CONTRACT_EVIDENCE_COLUMNS),
        schema_version=CONTRACT_EVIDENCE_SCHEMA_VERSION,
        empty_normalizer=lambda _: pd.DataFrame(columns=CONTRACT_EVIDENCE_COLUMNS),
    )
    if not observation_sources:
        observation_source = "unavailable"
    elif len(observation_sources) == 1:
        observation_source = next(iter(observation_sources))
    else:
        observation_source = "mixed_source_row_cli_override"
    return _count_parquet_rows(output_path), observation_source


def _derive_batch(
    *,
    exchange: str,
    source_batch: pd.DataFrame,
    source_path: Path,
    batch_index: int,
    schema_version: str,
    normalizer: Callable[[list[dict[str, Any]]], pd.DataFrame],
) -> tuple[pd.DataFrame, str]:
    raw_present = source_batch["raw_json"].notna()
    if bool(raw_present.any()):
        if not bool(raw_present.all()):
            raise ValueError(
                f"{exchange} source {source_path} batch {batch_index} mixes "
                "null and non-null raw_json values"
            )
        return (
            _normalize_raw_batch(
                exchange=exchange,
                raw_batch=source_batch,
                source_path=source_path,
                batch_index=batch_index,
                schema_version=schema_version,
                normalizer=normalizer,
            ),
            "raw_json",
        )
    return (
        _coerce_normalized_batch(
            exchange=exchange,
            source_batch=source_batch,
            source_path=source_path,
            batch_index=batch_index,
            schema_version=schema_version,
        ),
        "normalized_snapshot_fallback",
    )


def _normalize_raw_batch(
    *,
    exchange: str,
    raw_batch: pd.DataFrame,
    source_path: Path,
    batch_index: int,
    schema_version: str,
    normalizer: Callable[[list[dict[str, Any]]], pd.DataFrame],
) -> pd.DataFrame:
    payloads: list[dict[str, Any]] = []
    source_hashes: list[str] = []
    source_raw_json: list[str] = []
    for row_index, row in enumerate(raw_batch.to_dict("records")):
        raw_json = row.get("raw_json")
        source_hash = row.get("raw_json_sha256")
        if _is_missing(raw_json):
            raise ValueError(
                _row_error(
                    exchange, source_path, batch_index, row_index, "missing raw_json"
                )
            )
        if _is_missing(source_hash):
            raise ValueError(
                _row_error(
                    exchange,
                    source_path,
                    batch_index,
                    row_index,
                    "missing raw_json_sha256",
                )
            )
        raw_text = str(raw_json)
        source_hash_text = str(source_hash)
        actual_hash = _sha256_text(raw_text)
        if actual_hash != source_hash_text:
            raise ValueError(
                _row_error(
                    exchange,
                    source_path,
                    batch_index,
                    row_index,
                    f"raw_json_sha256 mismatch: expected {source_hash_text}, got {actual_hash}",
                )
            )
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                _row_error(
                    exchange,
                    source_path,
                    batch_index,
                    row_index,
                    f"invalid raw_json: {exc}",
                )
            ) from exc
        if not isinstance(payload, dict):
            raise ValueError(
                _row_error(
                    exchange,
                    source_path,
                    batch_index,
                    row_index,
                    "raw_json is not an object",
                )
            )
        payloads.append(payload)
        source_hashes.append(source_hash_text)
        source_raw_json.append(raw_text)

    normalized = normalizer(payloads)
    if len(normalized) != len(payloads):
        raise ValueError(
            f"{exchange} normalizer dropped rows for {source_path} batch {batch_index}: "
            f"{len(payloads)} payloads became {len(normalized)} rows"
        )
    normalized = coerce_frame(normalized, schema_version)
    normalized["raw_json"] = source_raw_json
    normalized["raw_json_sha256"] = source_hashes
    report = validate_frame(normalized, schema_version, strict=True)
    if not report.ok:
        raise ValueError(
            f"{exchange} normalized batch {batch_index} failed {schema_version}: "
            + "; ".join(report.errors)
        )
    return normalized


def _coerce_normalized_batch(
    *,
    exchange: str,
    source_batch: pd.DataFrame,
    source_path: Path,
    batch_index: int,
    schema_version: str,
) -> pd.DataFrame:
    if "raw_json_sha256" not in source_batch.columns:
        raise ValueError(f"{source_path} is missing required column: raw_json_sha256")
    missing_hash = source_batch["raw_json_sha256"].isna()
    if bool(missing_hash.any()):
        first = int(missing_hash[missing_hash].index[0])
        raise ValueError(
            _row_error(
                exchange,
                source_path,
                batch_index,
                first,
                "missing raw_json_sha256",
            )
        )
    normalized = coerce_frame(source_batch, schema_version)
    report = validate_frame(normalized, schema_version, strict=True)
    if not report.ok:
        raise ValueError(
            f"{exchange} normalized fallback batch {batch_index} failed {schema_version}: "
            + "; ".join(report.errors)
        )
    return normalized


def _iter_source_batches(path: Path, *, batch_size: int):
    import pyarrow.dataset as ds

    dataset = ds.dataset(path, format="parquet")
    missing = sorted(set(RAW_COLUMNS) - set(dataset.schema.names))
    if missing:
        raise ValueError(f"{path} is missing required columns: {', '.join(missing)}")
    scanner = dataset.scanner(batch_size=batch_size)
    for batch in scanner.to_batches():
        frame = batch.to_pandas()
        if not frame.empty:
            yield frame


def _coalesce_parts(
    *,
    parts_dir: Path,
    output_path: Path,
    columns: tuple[str, ...],
    schema_version: str,
    empty_normalizer: Callable[[list[dict[str, Any]]], pd.DataFrame],
) -> None:
    part_files = sorted(parts_dir.glob("*.parquet"))
    if not part_files:
        empty = coerce_frame(empty_normalizer([]), schema_version)
        write_parquet(
            empty, output_path, overwrite=True, schema=schema_version, strict=True
        )
        return

    import duckdb

    output_path.parent.mkdir(parents=True, exist_ok=True)
    column_sql = ", ".join(_quote_ident(column) for column in columns)
    source_glob = _sql_literal((parts_dir.resolve() / "*.parquet").as_posix())
    output_literal = _sql_literal(output_path.resolve().as_posix())
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("SET preserve_insertion_order=false")
        con.execute(
            f"""
            COPY (
                SELECT {column_sql}
                FROM read_parquet({source_glob}, union_by_name=true)
            )
            TO {output_literal} (FORMAT PARQUET)
            """
        )
    finally:
        con.close()


def _write_projection(
    *,
    source_path: Path,
    output_path: Path,
    columns: tuple[str, ...],
) -> None:
    import duckdb

    column_sql = ", ".join(_quote_ident(column) for column in columns)
    source_literal = _sql_literal(source_path.resolve().as_posix())
    output_literal = _sql_literal(output_path.resolve().as_posix())
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(
            f"""
            COPY (
                SELECT {column_sql}
                FROM read_parquet({source_literal})
            )
            TO {output_literal} (FORMAT PARQUET)
            """
        )
    finally:
        con.close()


def _validate_unique_keys(*, path: Path, exchange: str, key_column: str) -> int:
    import duckdb

    source_literal = _sql_literal(path.resolve().as_posix())
    key_sql = _quote_ident(key_column)
    con = duckdb.connect(database=":memory:")
    try:
        row_count = int(
            con.execute(
                f"SELECT count(*) FROM read_parquet({source_literal})"
            ).fetchone()[0]
        )
        null_count = int(
            con.execute(
                f"SELECT count(*) FROM read_parquet({source_literal}) WHERE {key_sql} IS NULL"
            ).fetchone()[0]
        )
        if null_count:
            raise ValueError(
                f"{exchange} snapshot has {null_count} rows with null {key_column}"
            )
        duplicates = con.execute(
            f"""
            SELECT {key_sql}, count(*) AS row_count
            FROM read_parquet({source_literal})
            GROUP BY {key_sql}
            HAVING count(*) > 1
            ORDER BY row_count DESC, {key_sql}
            LIMIT 10
            """
        ).fetchall()
    finally:
        con.close()
    if duplicates:
        sample = ", ".join(f"{key} ({count})" for key, count in duplicates)
        raise ValueError(f"{exchange} duplicate {key_column} values: {sample}")
    return row_count


def _count_parquet_rows(path: Path) -> int:
    import duckdb

    source_literal = _sql_literal(path.resolve().as_posix())
    con = duckdb.connect(database=":memory:")
    try:
        return int(
            con.execute(
                f"SELECT count(*) FROM read_parquet({source_literal})"
            ).fetchone()[0]
        )
    finally:
        con.close()


def _prepare_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and not overwrite and any(output_dir.iterdir()):
        raise FileExistsError(
            f"output directory already exists and is not empty: {output_dir}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    if not overwrite:
        return
    for path in (
        output_dir / "polymarket_markets.parquet",
        output_dir / "polymarket_matching_projection.parquet",
        output_dir / "polymarket_contract_evidence.parquet",
        contract_evidence_manifest_path(
            output_dir / "polymarket_contract_evidence.parquet"
        ),
        output_dir / "kalshi_markets.parquet",
        output_dir / "kalshi_matching_projection.parquet",
        output_dir / "kalshi_contract_evidence.parquet",
        contract_evidence_manifest_path(
            output_dir / "kalshi_contract_evidence.parquet"
        ),
        output_dir / "summary.json",
        output_dir / "derived_manifest.json",
    ):
        if path.exists():
            path.unlink()
    parts_dir = output_dir / "_parts"
    if parts_dir.exists():
        shutil.rmtree(parts_dir)


def _manifest_payload(result: DerivationResult) -> dict[str, Any]:
    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "derive_market_snapshots_from_raw_json",
        "output_dir": str(result.output_dir),
        "source_paths": {
            "polymarket": str(result.polymarket.source_path),
            "kalshi": str(result.kalshi.source_path),
        },
        "dataset_paths": {
            "polymarket_markets": str(result.polymarket.snapshot_path),
            "polymarket_matching_projection": str(result.polymarket.projection_path),
            "polymarket_contract_evidence": str(
                result.polymarket.contract_evidence_path
            ),
            "polymarket_contract_evidence_manifest": str(
                result.polymarket.contract_evidence_manifest_path
            ),
            "kalshi_markets": str(result.kalshi.snapshot_path),
            "kalshi_matching_projection": str(result.kalshi.projection_path),
            "kalshi_contract_evidence": str(result.kalshi.contract_evidence_path),
            "kalshi_contract_evidence_manifest": str(
                result.kalshi.contract_evidence_manifest_path
            ),
        },
        "schema_versions": {
            "polymarket_markets": result.polymarket.schema_version,
            "polymarket_matching_projection": result.polymarket.schema_version,
            "polymarket_contract_evidence": CONTRACT_EVIDENCE_SCHEMA_VERSION,
            "polymarket_contract_evidence_manifest": (
                CONTRACT_EVIDENCE_MANIFEST_VERSION
            ),
            "kalshi_markets": result.kalshi.schema_version,
            "kalshi_matching_projection": result.kalshi.schema_version,
            "kalshi_contract_evidence": CONTRACT_EVIDENCE_SCHEMA_VERSION,
            "kalshi_contract_evidence_manifest": CONTRACT_EVIDENCE_MANIFEST_VERSION,
        },
        "row_counts": {
            "polymarket_markets": result.polymarket.row_count,
            "polymarket_matching_projection": result.polymarket.row_count,
            "polymarket_contract_evidence": result.polymarket.row_count,
            "kalshi_markets": result.kalshi.row_count,
            "kalshi_matching_projection": result.kalshi.row_count,
            "kalshi_contract_evidence": result.kalshi.row_count,
        },
    }


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _row_error(
    exchange: str,
    source_path: Path,
    batch_index: int,
    row_index: int,
    message: str,
) -> str:
    return (
        f"{exchange} source {source_path} batch {batch_index} "
        f"row {row_index}: {message}"
    )


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(missing) if isinstance(missing, bool) else False


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive current market snapshot schemas from stored raw_json payloads."
    )
    parser.add_argument("--polymarket-source", type=Path, required=True)
    parser.add_argument("--kalshi-source", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--observed-at-utc",
        default=None,
        help=(
            "Explicit UTC fallback for snapshots without row-level observed_at_utc; "
            "required when source rows do not carry observation time."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    result = derive_market_snapshots_from_raw_json(
        polymarket_source=args.polymarket_source,
        kalshi_source=args.kalshi_source,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        overwrite=args.overwrite,
        observed_at_utc=args.observed_at_utc,
    )
    print(json.dumps(result.to_jsonable(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
