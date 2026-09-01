from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Mapping
from typing import Annotated, Any

import httpx
import typer

from pmkt.exchanges.polymarket.gamma import GammaClient
from pmkt.exchanges.kalshi.client import (
    AsyncKalshiClient,
    kalshi_markets_dataframe,
)
from pmkt.data.canonical import (
    CONTRACT_EVIDENCE_SCHEMA_VERSION,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
)
from pmkt.data.normalize import markets_dataframe
from pmkt.data.contract_evidence import contract_evidence_dataframe
from pmkt.data.contract_evidence_manifest import (
    CONTRACT_EVIDENCE_MANIFEST_VERSION,
    build_contract_evidence_manifest_from_summary,
    contract_evidence_bundle_path,
    file_sha256,
    write_contract_evidence_bundle,
)
from pmkt.data.parquet_atomic import AtomicParquetWriter
from pmkt.data.storage.parquet import write_parquet
from pmkt.pagination import normalize_polymarket_cursor, polymarket_cursor_stop_reason


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )


class _StreamingContractEvidenceBundle:
    """Bounded-memory builder for the existing immutable evidence bundle."""

    def __init__(
        self,
        *,
        out: Path,
        venue: str,
        source_endpoint: str,
        payload_scope: str,
        observed_at_utc: str,
    ) -> None:
        self.out = out
        self.venue = venue
        self.source_endpoint = source_endpoint
        self.payload_scope = payload_scope
        self.observed_at_utc = observed_at_utc
        self.derived_at_utc = _utc_now_iso()
        self.destination = contract_evidence_bundle_path(out)
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        if self.destination.exists():
            raise FileExistsError(
                f"contract evidence bundle destination is immutable: {self.destination}"
            )
        self.staging = Path(
            tempfile.mkdtemp(
                prefix=f".{self.destination.name}.staging-",
                dir=self.destination.parent,
            )
        )
        self.artifact_path = self.staging / out.name
        self.writer = AtomicParquetWriter(
            self.artifact_path,
            schema_version=CONTRACT_EVIDENCE_SCHEMA_VERSION,
        )
        self.evidence_ids: set[str] = set()
        self.payload_hashes: set[str] = set()
        self.observation_min_utc: str | None = None
        self.observation_max_utc: str | None = None
        self.payload_authority_valid = True

    def append(self, markets: list[dict[str, object]]) -> None:
        evidence = contract_evidence_dataframe(
            markets,
            venue=self.venue,  # type: ignore[arg-type]
            source_endpoint=self.source_endpoint,
            payload_scope=self.payload_scope,
            observed_at_utc=self.observed_at_utc,
            derived_at_utc=self.derived_at_utc,
            source_payload_kind="venue_api_response",
        )
        page_ids = {str(value).strip() for value in evidence["evidence_id"].tolist()}
        duplicates = page_ids & self.evidence_ids
        if duplicates:
            raise ValueError(
                "duplicate contract evidence identities across pages: "
                + ", ".join(sorted(duplicates)[:5])
            )
        self.evidence_ids.update(page_ids)
        self.payload_hashes.update(
            str(value).strip()
            for value in evidence["raw_payload_hash"].tolist()
            if str(value).strip()
        )
        observations = [
            str(value).strip() for value in evidence["observed_at_utc"].tolist()
        ]
        if observations:
            page_min = min(observations)
            page_max = max(observations)
            self.observation_min_utc = min(
                value for value in (self.observation_min_utc, page_min) if value
            )
            self.observation_max_utc = max(
                value for value in (self.observation_max_utc, page_max) if value
            )
        for provenance in evidence["field_provenance_json"].tolist():
            source_payload = (
                provenance.get("_source_payload")
                if isinstance(provenance, Mapping)
                else None
            )
            if not isinstance(source_payload, Mapping) or (
                source_payload.get("authoritative") is not True
                or str(source_payload.get("kind") or "") != "venue_api_response"
            ):
                self.payload_authority_valid = False
        self.writer.append(evidence)

    def finish(
        self,
        *,
        collection_complete: bool,
        stop_reason: str,
        continuation_cursor: str | None,
        collection_errors: tuple[str, ...],
        page_count: int,
    ) -> tuple[Path, int, Path]:
        try:
            artifact = self.writer.finish()
            normalized_errors = sorted(set(collection_errors))
            normalized_cursor = (
                str(continuation_cursor).strip() if continuation_cursor else None
            ) or None
            source_manifest = {
                "venue": self.venue,
                "source_endpoint": self.source_endpoint,
                "payload_scope": self.payload_scope,
                "observed_at_utc": self.observed_at_utc,
                "page_count": int(page_count),
                "row_count": int(self.writer.row_count),
                "collection_complete": bool(collection_complete),
                "stop_reason": stop_reason,
                "continuation_cursor": normalized_cursor,
                "collection_errors": normalized_errors,
            }
            source_path = self.staging / "source_collection_manifest.json"
            source_path.write_text(
                json.dumps(source_manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            manifest = build_contract_evidence_manifest_from_summary(
                artifact_path=artifact,
                venue=self.venue,
                source_endpoint=self.source_endpoint,
                payload_scope=self.payload_scope,
                observation_time_source="capture_clock",
                source_payload_kind="venue_api_response",
                collection_complete=collection_complete,
                stop_reason=stop_reason,
                continuation_cursor=normalized_cursor,
                collection_errors=tuple(normalized_errors),
                source_manifest_sha256=file_sha256(source_path),
                row_count=self.writer.row_count,
                observation_min_utc=self.observation_min_utc,
                observation_max_utc=self.observation_max_utc,
                evidence_ids=self.evidence_ids,
                payload_hashes=self.payload_hashes,
                payload_authority_valid=self.payload_authority_valid,
            )
            manifest_path = self.staging / "manifest.json"
            _write_json(manifest_path, manifest)
            self.staging.replace(self.destination)
            published_artifact = self.destination / self.out.name
            published_manifest = self.destination / manifest_path.name
            print(
                f"Wrote {self.writer.row_count} contract evidence rows to "
                f"{published_artifact} with manifest {published_manifest}"
            )
            return published_artifact, self.writer.row_count, published_manifest
        finally:
            if self.staging.exists():
                shutil.rmtree(self.staging, ignore_errors=True)

    def abort(self) -> None:
        self.writer.abort()
        shutil.rmtree(self.staging, ignore_errors=True)


def _market_payload(value: Any) -> dict[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return model_dump(by_alias=True, exclude_unset=True, mode="json")
        except TypeError:
            return model_dump()
    raise TypeError(f"Expected market mapping or model, got {type(value)}")


async def _gamma_markets_payload_page(
    gamma: Any, *, limit: int, offset: int, closed: bool | None
) -> list[dict[str, object]]:
    raw_method = getattr(gamma, "markets_raw_page", None)
    if callable(raw_method):
        rows = await raw_method(limit=limit, offset=offset, closed=closed)
    else:
        rows = await gamma.markets_page(limit=limit, offset=offset, closed=closed)
    return [_market_payload(row) for row in rows]


async def _gamma_keyset_payload_page(
    gamma: Any, *, limit: int, after_cursor: str | None, closed: bool | None
) -> dict[str, Any]:
    raw_method = getattr(gamma, "markets_keyset_raw_page", None)
    if callable(raw_method):
        page = await raw_method(limit=limit, after_cursor=after_cursor, closed=closed)
    else:
        page = await gamma.markets_keyset_page(
            limit=limit, after_cursor=after_cursor, closed=closed
        )
    rows = page.get("markets")
    if not isinstance(rows, list):
        raise TypeError("Expected keyset response field 'markets' to be a list")
    return {**page, "markets": [_market_payload(row) for row in rows]}


def _closed_option_to_bool(closed: str) -> bool | None:
    if closed == "open":
        return False
    if closed == "closed":
        return True
    return None


async def _ingest_markets_async(
    out: Path,
    pages: int,
    limit: int,
    closed: bool | None,
    contract_evidence_out: Path | None = None,
) -> None:
    observed_at = _utc_now_iso()
    markets: list[dict[str, object]] = []
    offset = 0
    page_count = 0
    stop_reason = "page_limit_reached"
    async with GammaClient() as gamma:
        for _ in range(pages):
            try:
                page = await _gamma_markets_payload_page(
                    gamma, limit=limit, offset=offset, closed=closed
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 422 and offset > 0:
                    stop_reason = "endpoint_offset_limit"
                    break
                raise
            page_count += 1
            if not page:
                stop_reason = "empty_page"
                break
            markets.extend(page)
            if len(page) < limit:
                stop_reason = "short_page"
                break
            offset += limit

    df = markets_dataframe(markets)
    path = write_parquet(
        df,
        out,
        overwrite=True,
        schema=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    )
    if contract_evidence_out is not None:
        _write_contract_evidence(
            markets,
            venue="polymarket",
            source_endpoint="gamma:/markets",
            payload_scope="list",
            observed_at_utc=observed_at,
            collection_complete=False,
            stop_reason=stop_reason,
            continuation_cursor=str(offset)
            if stop_reason == "page_limit_reached"
            else None,
            collection_errors=(),
            page_count=page_count,
            out=contract_evidence_out,
        )
    print(f"Wrote {len(df)} markets to {path}")


def ingest_markets(
    out: Annotated[Path, typer.Option(help="Output parquet path.")],
    pages: Annotated[int, typer.Option(help="Number of pages to fetch.")] = 2,
    limit: Annotated[int, typer.Option(help="Markets per page.")] = 100,
    closed: Annotated[
        str, typer.Option(help="Filter by market status: 'open', 'closed', or 'any'.")
    ] = "any",
    contract_evidence_out: Annotated[
        Path | None,
        typer.Option(
            "--contract-evidence-out",
            help="Atomic contract_evidence.v1 bundle target (published as PATH.bundle).",
        ),
    ] = None,
):
    """Fetch a bounded Gamma /markets offset sample and write parquet."""
    closed_val = _closed_option_to_bool(closed)

    asyncio.run(
        _ingest_markets_async(
            out,
            pages,
            limit,
            closed_val,
            contract_evidence_out=contract_evidence_out,
        )
    )


async def _ingest_markets_keyset_async(
    out: Path,
    *,
    limit: int,
    closed: bool | None,
    max_pages: int | None = None,
    manifest_out: Path | None = None,
    contract_evidence_out: Path | None = None,
) -> None:
    if limit < 1 or limit > 100:
        raise ValueError("Gamma keyset limit must be between 1 and 100")
    if max_pages is not None and max_pages < 1:
        raise ValueError("max_pages must be >= 1 when provided")
    started_at = _utc_now_iso()
    writer = AtomicParquetWriter(
        out,
        schema_version=POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    try:
        evidence_writer = (
            _StreamingContractEvidenceBundle(
                out=contract_evidence_out,
                venue="polymarket",
                source_endpoint="gamma:/markets/keyset",
                payload_scope="keyset_list",
                observed_at_utc=started_at,
            )
            if contract_evidence_out is not None
            else None
        )
    except BaseException:
        writer.abort()
        raise
    market_ids: set[str] = set()
    page_count = 0
    cursor: str | None = None
    final_cursor: str | None = None
    stop_reason = "not_started"
    errors: list[str] = []
    try:
        async with GammaClient() as gamma:
            while True:
                if max_pages is not None and page_count >= max_pages:
                    stop_reason = "max_pages_reached"
                    break
                page = await _gamma_keyset_payload_page(
                    gamma,
                    limit=limit,
                    after_cursor=cursor,
                    closed=closed,
                )
                page_count += 1
                page_markets = page.get("markets")
                if not isinstance(page_markets, list) or not page_markets:
                    stop_reason = "empty_page"
                    final_cursor = page.get("next_cursor") or None
                    break
                frame = markets_dataframe(page_markets)
                page_ids = {
                    str(value).strip() for value in frame["market_id"].tolist()
                }
                duplicates = page_ids & market_ids
                if duplicates:
                    raise ValueError(
                        "duplicate Polymarket market IDs across pages: "
                        + ", ".join(sorted(duplicates)[:5])
                    )
                market_ids.update(page_ids)
                writer.append(frame)
                if evidence_writer is not None:
                    evidence_writer.append(page_markets)
                next_cursor = normalize_polymarket_cursor(page.get("next_cursor"))
                final_cursor = next_cursor
                cursor_stop_reason = polymarket_cursor_stop_reason(
                    next_cursor,
                    previous_cursor=cursor,
                )
                if cursor_stop_reason is not None:
                    stop_reason = cursor_stop_reason
                    break
                cursor = next_cursor
    except Exception as exc:
        stop_reason = "error"
        errors.append(f"{type(exc).__name__}: {exc}")
        raise
    finally:
        finished_at = _utc_now_iso()
        evidence_path: Path | None = None
        evidence_manifest_path: Path | None = None
        evidence_count = 0
        if writer.row_count or stop_reason != "not_started":
            try:
                path = writer.finish()
            except BaseException:
                if evidence_writer is not None:
                    evidence_writer.abort()
                raise
            if evidence_writer is not None:
                evidence_path, evidence_count, evidence_manifest_path = (
                    evidence_writer.finish(
                        collection_complete=stop_reason
                        in {"empty_page", "cursor_exhausted", "terminal_cursor_lte"},
                        stop_reason=stop_reason,
                        continuation_cursor=final_cursor,
                        collection_errors=tuple(errors),
                        page_count=page_count,
                    )
                )
            if manifest_out is not None:
                dataset_paths = {"output": str(path)}
                schema_versions = {
                    "output": POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
                }
                row_counts = {"output": writer.row_count}
                if evidence_path is not None and evidence_manifest_path is not None:
                    dataset_paths["contract_evidence"] = str(evidence_path)
                    dataset_paths["contract_evidence_manifest"] = str(
                        evidence_manifest_path
                    )
                    schema_versions["contract_evidence"] = (
                        CONTRACT_EVIDENCE_SCHEMA_VERSION
                    )
                    schema_versions["contract_evidence_manifest"] = (
                        CONTRACT_EVIDENCE_MANIFEST_VERSION
                    )
                    row_counts["contract_evidence"] = evidence_count
                _write_json(
                    manifest_out,
                    {
                        "endpoint": "/markets/keyset",
                        "mode": "gamma_markets_keyset",
                        "requested_closed_filter": closed,
                        "started_at_utc": started_at,
                        "finished_at_utc": finished_at,
                        "page_count": page_count,
                        "row_count": writer.row_count,
                        "unique_market_count": len(market_ids),
                        "final_cursor": final_cursor,
                        "stop_reason": stop_reason,
                        "errors": errors,
                        "output_path": str(path),
                        "dataset_paths": dataset_paths,
                        "schema_versions": schema_versions,
                        "row_counts": row_counts,
                    },
                )
        else:
            writer.abort()
            if evidence_writer is not None:
                evidence_writer.abort()
    print(f"Wrote {writer.row_count} Polymarket keyset markets to {out}")


def ingest_markets_keyset(
    out: Annotated[Path, typer.Option(help="Output parquet path.")],
    limit: Annotated[
        int, typer.Option(help="Markets per keyset page, from 1 through 100.")
    ] = 100,
    closed: Annotated[
        str, typer.Option(help="Filter by market status: 'open', 'closed', or 'any'.")
    ] = "any",
    max_pages: Annotated[
        int | None,
        typer.Option("--max-pages", help="Optional page cap for bounded smoke runs."),
    ] = None,
    manifest_out: Annotated[
        Path | None,
        typer.Option("--manifest-out", help="Optional collection manifest JSON path."),
    ] = None,
    contract_evidence_out: Annotated[
        Path | None,
        typer.Option(
            "--contract-evidence-out",
            help="Atomic contract_evidence.v1 bundle target (published as PATH.bundle).",
        ),
    ] = None,
) -> None:
    """Fetch complete Gamma /markets/keyset pages and write parquet."""
    if limit < 1 or limit > 100:
        raise typer.BadParameter(
            "must be between 1 and 100 for Gamma keyset pagination",
            param_hint="--limit",
        )
    asyncio.run(
        _ingest_markets_keyset_async(
            out,
            limit=limit,
            closed=_closed_option_to_bool(closed),
            max_pages=max_pages,
            manifest_out=manifest_out,
            contract_evidence_out=contract_evidence_out,
        )
    )


async def _ingest_kalshi_markets_async(
    out: Path,
    *,
    limit: int,
    max_pages: int,
    status: str | None,
    complete: bool = False,
    manifest_out: Path | None = None,
    contract_evidence_out: Path | None = None,
) -> None:
    started_at = _utc_now_iso()
    writer = AtomicParquetWriter(
        out,
        schema_version=KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    try:
        evidence_writer = (
            _StreamingContractEvidenceBundle(
                out=contract_evidence_out,
                venue="kalshi",
                source_endpoint="kalshi:/markets",
                payload_scope="cursor_list",
                observed_at_utc=started_at,
            )
            if contract_evidence_out is not None
            else None
        )
    except BaseException:
        writer.abort()
        raise
    market_keys: set[str] = set()
    page_count = 0
    cursor: str | None = None
    final_cursor: str | None = None
    stop_reason = "not_started"
    errors: list[str] = []
    page_limit: int | None = None if complete else max_pages
    try:
        async with AsyncKalshiClient() as kalshi:
            while True:
                if page_limit is not None and page_count >= page_limit:
                    stop_reason = "max_pages_reached"
                    break
                page = await kalshi.markets_page(
                    limit=limit,
                    cursor=cursor,
                    status=status,
                )
                page_count += 1
                page_markets = page.get("markets")
                if not isinstance(page_markets, list) or not page_markets:
                    stop_reason = "empty_page"
                    final_cursor = page.get("cursor") or None
                    break
                accepted_page: list[dict[str, object]] = []
                for market in page_markets:
                    if not isinstance(market, dict):
                        continue
                    accepted_page.append(market)
                if accepted_page:
                    frame = kalshi_markets_dataframe(accepted_page)
                    page_keys = {
                        str(value).strip() for value in frame["market_key"].tolist()
                    }
                    duplicates = page_keys & market_keys
                    if duplicates:
                        raise ValueError(
                            "duplicate Kalshi market keys across pages: "
                            + ", ".join(sorted(duplicates)[:5])
                        )
                    market_keys.update(page_keys)
                    writer.append(frame)
                    if evidence_writer is not None:
                        evidence_writer.append(accepted_page)
                next_cursor = page.get("cursor") or None
                final_cursor = next_cursor
                if not next_cursor:
                    stop_reason = "cursor_exhausted"
                    break
                cursor = str(next_cursor)
    except Exception as exc:
        stop_reason = "error"
        errors.append(f"{type(exc).__name__}: {exc}")
        writer.abort()
        if evidence_writer is not None:
            evidence_writer.abort()
        raise

    try:
        path = writer.finish()
    except BaseException:
        if evidence_writer is not None:
            evidence_writer.abort()
        raise
    evidence_path: Path | None = None
    evidence_manifest_path: Path | None = None
    evidence_count = 0
    if evidence_writer is not None:
        evidence_path, evidence_count, evidence_manifest_path = (
            evidence_writer.finish(
                collection_complete=stop_reason in {"empty_page", "cursor_exhausted"},
                stop_reason=stop_reason,
                continuation_cursor=final_cursor,
                collection_errors=tuple(errors),
                page_count=page_count,
            )
        )
    if manifest_out is not None:
        dataset_paths = {"output": str(path)}
        schema_versions = {"output": KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION}
        row_counts = {"output": writer.row_count}
        if evidence_path is not None and evidence_manifest_path is not None:
            dataset_paths["contract_evidence"] = str(evidence_path)
            dataset_paths["contract_evidence_manifest"] = str(evidence_manifest_path)
            schema_versions["contract_evidence"] = CONTRACT_EVIDENCE_SCHEMA_VERSION
            schema_versions["contract_evidence_manifest"] = (
                CONTRACT_EVIDENCE_MANIFEST_VERSION
            )
            row_counts["contract_evidence"] = evidence_count
        _write_json(
            manifest_out,
            {
                "endpoint": "/markets",
                "mode": (
                    "kalshi_markets_cursor_complete"
                    if complete
                    else "kalshi_markets_cursor_bounded"
                ),
                "status_filter": status,
                "started_at_utc": started_at,
                "finished_at_utc": _utc_now_iso(),
                "page_count": page_count,
                "row_count": writer.row_count,
                "unique_market_count": len(market_keys),
                "final_cursor": final_cursor,
                "stop_reason": stop_reason,
                "errors": errors,
                "output_path": str(path),
                "dataset_paths": dataset_paths,
                "schema_versions": schema_versions,
                "row_counts": row_counts,
            },
        )
    message = f"Wrote {writer.row_count} Kalshi markets to {path}"
    print(message)


def _write_contract_evidence(
    markets: list[dict[str, object]],
    *,
    venue: str,
    source_endpoint: str,
    payload_scope: str,
    observed_at_utc: str,
    collection_complete: bool,
    stop_reason: str,
    continuation_cursor: str | None,
    collection_errors: tuple[str, ...],
    page_count: int,
    out: Path,
) -> tuple[Path, int, Path]:
    evidence = contract_evidence_dataframe(
        markets,
        venue=venue,  # type: ignore[arg-type]
        source_endpoint=source_endpoint,
        payload_scope=payload_scope,
        observed_at_utc=observed_at_utc,
        derived_at_utc=_utc_now_iso(),
        source_payload_kind="venue_api_response",
    )
    source_collection_manifest = {
        "venue": venue,
        "source_endpoint": source_endpoint,
        "payload_scope": payload_scope,
        "observed_at_utc": observed_at_utc,
        "page_count": int(page_count),
        "row_count": int(len(markets)),
        "collection_complete": bool(collection_complete),
        "stop_reason": stop_reason,
        "continuation_cursor": continuation_cursor,
        "collection_errors": list(collection_errors),
    }
    path, manifest_path = write_contract_evidence_bundle(
        evidence,
        artifact_path=out,
        venue=venue,
        source_endpoint=source_endpoint,
        payload_scope=payload_scope,
        observation_time_source="capture_clock",
        source_payload_kind="venue_api_response",
        collection_complete=collection_complete,
        stop_reason=stop_reason,
        continuation_cursor=continuation_cursor,
        collection_errors=collection_errors,
        source_collection_manifest=source_collection_manifest,
    )
    print(
        f"Wrote {len(evidence)} contract evidence rows to {path} "
        f"with manifest {manifest_path}"
    )
    return path, int(len(evidence)), manifest_path


def ingest_kalshi_markets(
    out: Annotated[Path, typer.Option(help="Output parquet path.")],
    limit: Annotated[int, typer.Option(help="Markets per page, up to 1000.")] = 100,
    max_pages: Annotated[int, typer.Option(help="Maximum cursor pages to fetch.")] = 1,
    status: Annotated[
        str,
        typer.Option(
            help="Kalshi market status: unopened, open, paused, closed, settled, or any."
        ),
    ] = "open",
    complete: Annotated[
        bool,
        typer.Option(
            "--complete",
            help="Iterate Kalshi cursor pages until exhaustion instead of stopping at --max-pages.",
        ),
    ] = False,
    manifest_out: Annotated[
        Path | None,
        typer.Option("--manifest-out", help="Optional collection manifest JSON path."),
    ] = None,
    contract_evidence_out: Annotated[
        Path | None,
        typer.Option(
            "--contract-evidence-out",
            help="Atomic contract_evidence.v1 bundle target (published as PATH.bundle).",
        ),
    ] = None,
) -> None:
    """Fetch Kalshi markets and write normalized parquet."""
    status_value = None if status == "any" else status
    asyncio.run(
        _ingest_kalshi_markets_async(
            out,
            limit=limit,
            max_pages=max_pages,
            status=status_value,
            complete=complete,
            manifest_out=manifest_out,
            contract_evidence_out=contract_evidence_out,
        )
    )
