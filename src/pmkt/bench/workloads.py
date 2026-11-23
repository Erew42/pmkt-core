"""Read-plane measurements. Prepare replay inputs once outside timed calls."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from decimal import Decimal
from functools import partial
from itertools import islice
import json
from pathlib import Path
import random
import subprocess
import sys
from tempfile import TemporaryDirectory
from typing import Any

from pmkt import _native
from pmkt.exchanges._ws_book import decode_decimal_json, encode_message
from .datasets import Capture, open_dataset
from .runner import Case

# Bound Python object preparation independently of full native capture workloads.
WIRE_SAMPLE_FRAMES = 1024
PARQUET_BATCH_ROWS = 65536


def _cold_process_import() -> None:
    subprocess.run([sys.executable, "-c", "import pmkt"], check=True)


def _book_updates(
    capture: Capture, name: str, selected: Callable[[str], bool]
) -> list[Case]:
    update_name = f"native.update_{'market' if capture.venue == 'polymarket' else 'kalshi'}_book.{name}"
    shard_names = {n: f"native.shard_feed.{name}.{n}" for n in (1, 256, 512)}
    do_update = selected(update_name)
    # Replay the same Polymarket frame on a warm shard with one active token.
    sizes = [
        n
        for n, case in shard_names.items()
        if selected(case) and capture.venue == "polymarket"
    ]
    if not (do_update or sizes):
        return []
    poly: list[tuple[str, str, dict[str, Any], dict[str, Any] | None]] = []
    kalshi: list[tuple[str, str, dict[str, Any], int | None, int | None]] = []
    shard_frame: tuple[str, bytes] | None = None
    for row in islice(capture.envelopes(), WIRE_SAMPLE_FRAMES):
        if row["direction"] != "received" or row["frame_type"] != "text":
            continue
        try:
            msg = decode_decimal_json(row["payload_raw"])
        except json.JSONDecodeError:
            continue  # e.g. a recorded PONG
        for event in msg if isinstance(msg, list) else [msg]:
            if not isinstance(event, dict):
                continue
            kind = event.get("event_type", event.get("type"))
            if capture.venue == "polymarket":
                if kind == "book" and event.get("asset_id"):
                    if shard_frame is None:
                        shard_frame = (
                            str(event["asset_id"]),
                            encode_message(event).encode(),
                        )
                    poly.append((str(event["asset_id"]), kind, event, None))
                elif kind == "price_change":
                    for change in event.get("price_changes", []):
                        if change.get("asset_id"):
                            poly.append((str(change["asset_id"]), kind, event, change))
            elif kind in ("orderbook_snapshot", "orderbook_delta"):
                body = event["msg"]
                if body.get("market_ticker"):
                    kalshi.append(
                        (
                            body["market_ticker"],
                            kind,
                            body,
                            event.get("sid"),
                            event.get("seq"),
                        )
                    )

    kalshi = _seed_kalshi_books(kalshi)
    cases = []
    if do_update and poly:
        from pmkt.exchanges.polymarket.ws import MarketBookState

        def update_poly() -> None:
            states: dict[str, MarketBookState] = {}
            for instrument, kind, event, change in poly:
                if instrument not in states:
                    states[instrument] = MarketBookState(instrument)
                _native.update_market_book(states[instrument], kind, event, change)

        cases.append(Case(update_name, update_poly, len(poly), "ops"))
    if do_update and kalshi:
        from pmkt.exchanges.kalshi.ws import KalshiOrderBookState

        def update_kalshi() -> None:
            states: dict[str, KalshiOrderBookState] = {}
            for instrument, kind, body, sid, seq in kalshi:
                if instrument not in states:
                    states[instrument] = KalshiOrderBookState(instrument)
                _native.update_kalshi_book(states[instrument], kind, body, sid, seq)

        cases.append(Case(update_name, update_kalshi, len(kalshi), "ops"))
    if shard_frame is not None:
        active, wire = shard_frame
        for n in sizes:
            feed = _native.ShardFeed(
                "polymarket", "bench", [active, *(f"quiet-{i}" for i in range(n - 1))]
            )
            feed.connected(1, 0)
            warm = feed.ingest("bench", 1, wire, 1, detach=False)
            if active not in warm:
                raise ValueError(
                    f"snapshot did not update active instrument in {capture.label}"
                )
            cases.append(
                Case(
                    shard_names[n],
                    partial(feed.ingest, "bench", 1, wire, 2, detach=False),
                    1,
                    "frames",
                )
            )
    return cases


def _seed_kalshi_books(
    events: list[tuple[str, str, dict[str, Any], int | None, int | None]],
) -> list[tuple[str, str, dict[str, Any], int | None, int | None]]:
    """Prepend a synthetic snapshot for each ticker whose deltas precede any snapshot.

    A short sample (such as the repository fixture) may start mid-stream, and
    deltas applied to an empty book do not measure ordinary updates. Each such
    book starts with every level its deltas touch, deep enough that no delta
    empties it, plus one level on an untouched side. Because every touched level
    stays live, a seeded book may cross. Tickers whose stream begins with a real
    snapshot are left unchanged.
    """
    depth: dict[str, dict[str, dict[str, Decimal]]] = {}
    first: dict[str, tuple[int | None, int | None]] = {}
    snapshotted: set[str] = set()
    for ticker, kind, body, sid, seq in events:
        if kind == "orderbook_snapshot":
            snapshotted.add(ticker)
        if ticker in snapshotted:
            continue
        first.setdefault(ticker, (sid, seq))
        price = body.get("price_dollars", body.get("price"))
        change = body.get("delta_fp", body.get("delta", body.get("size_delta")))
        side = body.get("side")
        if price is None or change is None or side not in ("yes", "no"):
            continue
        levels = depth.setdefault(ticker, {"yes": {}, "no": {}})[side]
        removed = max(-Decimal(str(change)), Decimal(0))
        levels[str(price)] = levels.get(str(price), Decimal(1)) + removed
    seeds = []
    for ticker, (sid, seq) in first.items():
        sides = depth.get(ticker, {"yes": {}, "no": {}})
        # Quote an untouched side one cent away from crossing the other side.
        for side, other in (("yes", "no"), ("no", "yes")):
            best = max(map(Decimal, sides[other]), default=None)
            if not sides[side] and best is not None and best < Decimal("0.98"):
                sides[side][f"{Decimal('0.99') - best:.4f}"] = Decimal(1)
        snapshot: dict[str, Any] = {"market_ticker": ticker} | {
            f"{side}_dollars_fp": [[p, f"{q:.2f}"] for p, q in sorted(levels.items())]
            for side, levels in sides.items()
        }
        seeds.append(
            (ticker, "orderbook_snapshot", snapshot, sid, seq - 1 if seq else None)
        )
    return [*seeds, *events]


def _materialize(table: Any, pandas: bool) -> object:
    import pyarrow as pa

    # A native C stream is one-shot. Re-export the same already reconstructed
    # buffers via a fresh capsule per call, to isolate the consumer cost.
    capsule = table.__arrow_c_stream__()
    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
    result = reader.read_all()
    return result.to_pandas() if pandas else result


def _materialization_cases(
    name: str, table: Any, selected: Callable[[str], bool]
) -> list[Case]:
    cases = []
    # Importing a C stream is zero-copy: its cost does not scale with rows, so it
    # is one op per call. Only the pandas conversion copies every row.
    for pandas, prefix in [(False, "arrow_c_stream"), (True, "pandas")]:
        case = f"research.{prefix}.{name}"
        if selected(case) and table.num_rows:
            work, unit = (table.num_rows, "rows") if pandas else (1, "ops")
            cases.append(Case(case, partial(_materialize, table, pandas), work, unit))
    return cases


def _wants_table(name: str, selected: Callable[[str], bool]) -> bool:
    return selected(f"research.arrow_c_stream.{name}") or selected(
        f"research.pandas.{name}"
    )


def _parquet_cases(
    path: Path, name: str, selected: Callable[[str], bool], stack: ExitStack
) -> list[Case]:
    query_name = f"research.duckdb.{name}"
    want_table = _wants_table(name, selected)
    if not (want_table or selected(query_name)):
        return []
    import pyarrow as pa
    import pyarrow.parquet as pq

    cases = []
    with pq.ParquetFile(path) as parquet:
        if want_table:
            batch = next(parquet.iter_batches(batch_size=PARQUET_BATCH_ROWS), None)
            if batch is not None:
                cases.extend(
                    _materialization_cases(
                        name, pa.Table.from_batches([batch]), selected
                    )
                )
        if selected(query_name) and parquet.metadata.num_rows:
            import duckdb

            connection = stack.enter_context(duckdb.connect(":memory:"))
            # Aggregate actual column values. COUNT(*) alone may only read the footer.
            column = parquet.schema_arrow.names[0].replace('"', '""')
            query = f'SELECT bit_xor(hash("{column}")) FROM read_parquet(?, hive_partitioning=false)'

            def scan() -> object:
                return connection.execute(query, [str(path)]).fetchone()

            cases.append(Case(query_name, scan, parquet.metadata.num_rows, "rows"))
    return cases


def _fixture_catalog(root: Path) -> Path:
    from pmkt.data.market_catalog.fs import _artifact
    from pmkt.data.market_catalog.types import HISTORY_MANIFEST_SCHEMA
    from pmkt.data.normalize import markets_dataframe
    from pmkt.data.registry import (
        KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    )
    from pmkt.data.storage.parquet import write_parquet
    from pmkt.exchanges.kalshi.client import kalshi_markets_dataframe

    artifacts = {}
    for venue, schema, frame in [
        (
            "polymarket",
            POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
            markets_dataframe(
                [{"id": f"pm-{n}", "question": f"Question {n}?"} for n in range(128)]
            ),
        ),
        (
            "kalshi",
            KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            kalshi_markets_dataframe(
                [{"ticker": f"KX-{n}", "title": f"Question {n}?"} for n in range(128)]
            ),
        ),
    ]:
        path = root / f"{venue}.parquet"
        write_parquet(frame, path, schema=schema, strict=True)
        artifacts[f"{venue}_all_markets"] = _artifact(
            path, repository_root=root, rows=len(frame), schema=schema
        )
    manifest = root / "PUBLISHED_MANIFEST.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": HISTORY_MANIFEST_SCHEMA,
                "dataset_family": "market_history",
                "release_id": "fixture",
                "status": "completed",
                "artifacts": artifacts,
            }
        ),
        encoding="utf-8",
    )
    return manifest


def _catalog_case(path: Path, root: Path, name: str, stack: ExitStack) -> Case:
    from pmkt.catalog import CatalogSnapshot

    catalog = stack.enter_context(CatalogSnapshot.open_manifest(path, path_base=root))
    query = "SELECT venue, count(DISTINCT market_key) AS markets FROM market_catalog GROUP BY venue ORDER BY venue"
    # Bind/initialize the connection once. Each sample still executes the full query.
    catalog.query(query)
    return Case(name, partial(catalog.query, query))


def _merge_case(name: str, copies: tuple[Capture, ...]) -> Case:
    sessions = [(str(i), "bench", c.session) for i, c in enumerate(copies)]
    fn = partial(_native.merge_raw_captures, copies[0].venue, sessions)
    return Case(name, fn, sum(c.frames for c in copies), "frames")


@contextmanager
def workloads(
    data_dir: Path | None, seed: int, selected: Callable[[str], bool]
) -> Iterator[tuple[dict[str, Any], list[Case]]]:
    with ExitStack() as stack:
        dataset = stack.enter_context(open_dataset(data_dir))
        cases = []
        if selected("python.cold_process_import"):
            cases.append(Case("python.cold_process_import", _cold_process_import))
        totals = Counter(c.venue for c in dataset.captures)
        seen: Counter[str] = Counter()
        for capture in dataset.captures:
            seen[capture.venue] += 1
            name = (
                capture.venue
                if totals[capture.venue] == 1
                else f"{capture.venue}.{seen[capture.venue]}"
            )
            for operation, function in (
                ("inspect", _native.inspect_raw_capture),
                ("reconstruct", _native.reconstruct_raw_capture),
            ):
                case = f"native.{operation}.{name}"
                if selected(case):
                    cases.append(
                        Case(
                            case,
                            partial(function, capture.session),
                            capture.frames,
                            "frames",
                        )
                    )
            replicate = f"native.merge_replicated.{name}"
            if selected(replicate):
                cases.append(_merge_case(replicate, (capture, capture)))
            cases.extend(_book_updates(capture, name, selected))
            if _wants_table(name, selected):
                import pyarrow as pa

                tables = _native.reconstruct_raw_capture(capture.session)
                table = pa.table(tables["topbook"])
                cases.extend(_materialization_cases(name, table, selected))
        for i, group in enumerate(dataset.merge_groups, 1):
            name = f"native.merge_copies.{group[0].venue}.{i}"
            if selected(name):
                cases.append(_merge_case(name, group))
        for i, path in enumerate(dataset.parquet, 1):
            cases.extend(_parquet_cases(path, f"parquet.{i}", selected, stack))
        if data_dir is None and selected("research.duckdb.fixture"):
            import pyarrow as pa
            import pyarrow.parquet as pq

            temp = Path(
                stack.enter_context(TemporaryDirectory(prefix="pmkt-bench-parquet-"))
            )
            path = temp / "research.parquet"
            rng = random.Random(seed)
            pq.write_table(
                pa.table({"price_ticks": [rng.randrange(1, 100) for _ in range(4096)]}),
                path,
            )
            cases.extend(
                _parquet_cases(
                    path, "fixture", lambda n: n == "research.duckdb.fixture", stack
                )
            )
        for i, manifest in enumerate(dataset.catalogs, 1):
            query_name = f"research.catalog.{i}"
            if selected(query_name) and data_dir is not None:
                cases.append(
                    _catalog_case(manifest, data_dir.resolve(), query_name, stack)
                )
        if data_dir is None and selected("research.catalog.fixture"):
            temp = Path(
                stack.enter_context(TemporaryDirectory(prefix="pmkt-bench-catalog-"))
            )
            cases.append(
                _catalog_case(
                    _fixture_catalog(temp), temp, "research.catalog.fixture", stack
                )
            )
        yield dataset.provenance, cases
