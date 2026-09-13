"""Offline storage experiment; outputs are NOT a full@3 capture or replay contract.

Run with --help. Input must be one ordered, uninterrupted capture segment. The
benchmark uses recorded observation time, never execution speed or venue time.
It deliberately excludes sockets, supervisor recovery, health and eligibility.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class CausalGrid:
    """Keep the last observation per book until its next completed grid point.

    Advance BEFORE mutating books. Values may refer to caller-owned state only
    until advance returns; consume returned samples before applying new input.
    Equal-time updates are batched: advance(t) seals points strictly before t;
    advance(t, inclusive=True) also seals t and disallows further observations t.
    """

    def __init__(self, origin_ns: int, interval_ns: int) -> None:
        if interval_ns <= 0:
            raise ValueError("interval_ns must be positive")
        self.origin_ns = origin_ns
        self.interval_ns = interval_ns
        self.next_ns = origin_ns
        self.minimum_ns = origin_ns
        self.pending: dict[str, tuple[int, Any]] = {}

    def advance(self, now_ns: int, *, inclusive: bool = False):
        if now_ns < self.minimum_ns:
            raise ValueError("observation time moved backwards or crossed a sealed point")
        self.minimum_ns = now_ns + int(inclusive)
        limit = now_ns if inclusive else now_ns - 1
        if self.next_ns > limit:
            return []
        samples = [
            (key, self.next_ns, observed_ns, value)
            for key, (observed_ns, value) in sorted(self.pending.items())
        ]
        assert all(observed <= point for _, point, observed, _ in samples)
        self.pending.clear()
        self.next_ns = self.origin_ns + (
            (limit - self.origin_ns) // self.interval_ns + 1
        ) * self.interval_ns
        return samples

    def observe(self, key: str, observed_ns: int, value: Any) -> None:
        if not self.minimum_ns <= observed_ns <= self.next_ns:
            raise ValueError("advance the grid before observing an update")
        self.minimum_ns = observed_ns
        self.pending[key] = (observed_ns, value)


def timestamp_ns(text: str) -> int:
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("observation timestamps must be timezone aware")
    delta = value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 10**9 + delta.microseconds * 1000


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    from pmkt.data.normalize_books import (
        kalshi_ws_snapshot_to_topbook, polymarket_ws_snapshot_to_topbook,
    )
    from pmkt.exchanges.kalshi import order_book_stream as kx
    from pmkt.exchanges.kalshi.ws import (
        KalshiOrderBookState, apply_kalshi_orderbook_message,
    )
    from pmkt.exchanges.polymarket import order_book_stream as pm
    from pmkt.exchanges.polymarket.ws import MarketBookState, apply_market_message
    from pmkt.streaming.datasets import merge_profile_dataset_specs
    from pmkt.streaming.durability import DurableCaptureCoordinator
    from pmkt.streaming.durability_settings import CaptureDurabilitySettings
    from pmkt.streaming.profiles import (
        add_book_integrity, integrity_dataset_specs, select_storage_profile,
    )
    from pmkt.streaming.recovery_contracts import RunStateV1
    from pmkt.streaming.tape import canonical_utc
    from pmkt.streaming.tape_producers import KalshiTapeProducer, PolymarketTapeProducer
    from pmkt.streaming.topbook_emission import TopbookEmissionTracker

    if not math.isfinite(args.interval) or args.interval < 0:
        raise ValueError("interval must be finite and nonnegative (0 = dense)")
    interval_ns = round(args.interval * 10**9)
    if args.interval > 0 and interval_ns == 0:
        raise ValueError("interval must be at least one nanosecond")
    selection_data = json.loads(args.selection.read_text())
    ids = selection_data[
        "polymarket_token_ids" if args.venue == "pm" else "kalshi_tickers"
    ]
    venue = "polymarket" if args.venue == "pm" else "kalshi"
    module = pm if args.venue == "pm" else kx
    selected = set(ids)
    records = []
    origin = None
    previous = None
    # Loading and selection are excluded from storage timing, equally for all modes.
    with args.input.open() as handle:
        for sequence, line in enumerate(handle, 1):
            record = json.loads(line)
            utc = record.get("observed_at_utc") or record["received_at_utc"]
            ns = timestamp_ns(utc)
            if origin is None:
                origin = timestamp_ns(args.origin_utc) if args.origin_utc else ns
            if ns < origin or (previous is not None and ns < previous):
                raise ValueError("input must be ordered and at/after segment origin")
            previous = ns
            if args.seconds is not None and ns - origin > args.seconds * 10**9:
                break
            records.append((sequence, ns, canonical_utc(utc), record["message"]))
    if not records or origin is None:
        raise ValueError("empty input window")
    horizon = origin + round(args.seconds * 10**9) if args.seconds is not None else records[-1][1]
    if previous is None or horizon > previous:
        raise ValueError("input does not cover the requested end time")
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    run_dir = out / "replay"
    profile = select_storage_profile("full", profile_version="3")
    retained = {"topbook_main", "depth_main"}
    keep_tape = args.retention not in {"states", "raw-archive"}
    keep_legacy = args.retention in {"all", "no-sidecar"}
    keep_raw = args.retention == "all"
    if keep_tape:
        retained.update({"tape_event", "tape_level", "tape_control"})
    if keep_legacy:
        retained.update({"parsed_event", "legacy_snapshot", "legacy_level"})
    specs = {
        str(spec.role): spec for spec in integrity_dataset_specs(
            profile, merge_profile_dataset_specs(module.STREAM_DATASETS)
        ) if str(spec.role) in retained
    }
    assert set(specs) == retained
    paths = {role: role for role in retained}
    schemas = {role: spec.schema_version for role, spec in specs.items()}
    if keep_raw:
        paths["raw_jsonl"] = "raw_events.jsonl"
        schemas["raw_jsonl"] = "legacy.raw_jsonl.v1"
    settings = CaptureDurabilitySettings.resolve(
        requested_segment_rows=args.segment_rows, requested_segment_seconds=5.0,
    )
    states = {
        key: MarketBookState(asset_id=key) if args.venue == "pm"
        else KalshiOrderBookState(market_ticker=key) for key in ids
    }
    producer = (
        PolymarketTapeProducer(collector_run_id="replay", shard_id="0", integrity_evidence=True)
        if args.venue == "pm" else KalshiTapeProducer(
            collector_run_id="replay", shard_id="0", use_yes_price=True, integrity_evidence=True,
        )
    )
    adapter_settings = {} if args.venue == "pm" else producer._adapter_settings()
    coordinator = DurableCaptureCoordinator(
        run_dir=run_dir,
        run_state=RunStateV1(
            run_id="replay", profile_name="book-grid-storage-experiment", profile_version="1",
            expected_role_paths=paths, shard_plan={"0": ids},
            started_at_utc=canonical_utc(args.origin_utc) if args.origin_utc else records[0][2],
            adapter_settings_by_venue={venue: adapter_settings},
        ),
        role_schema_versions=schemas, role_schemas={r: s.schema for r, s in specs.items()},
        external_file_roles=["raw_jsonl"] if keep_raw else [],
        segment_row_limit=settings.effective_segment_rows,
        commit_interval_seconds=settings.effective_segment_seconds, durability_settings=settings,
    )
    coordinator.checkpoint_coalesce_seconds = settings.barrier_coalesce_seconds
    stage_times: dict[str, dict[str, float]] = {}
    for name in ("_validate_group", "_write_role_segment", "_publish_artifacts"):
        original = getattr(coordinator, name)

        def timed(*values, _name=name, _original=original, **kwargs):
            start = time.perf_counter()
            try:
                return _original(*values, **kwargs)
            finally:
                label = _name + (":" + str(values[0]) if _name == "_write_role_segment" else "")
                metric = stage_times.setdefault(label, {"calls": 0, "seconds": 0., "max_seconds": 0.})
                elapsed = time.perf_counter() - start
                metric["calls"] += 1
                metric["seconds"] += elapsed
                metric["max_seconds"] = max(metric["max_seconds"], elapsed)

        setattr(coordinator, name, timed)
    # Hash logical rows before commit batching; fixed run/coordinates permit exact
    # parity checks for every retained role across independent benchmark directories.
    digests = {role: hashlib.sha256() for role in retained}
    original_add = coordinator.add

    def add(role, row):
        digests[role].update(json.dumps(row, sort_keys=True, default=str).encode() + b"\n")
        original_add(role, row)

    coordinator.add = add
    grid = CausalGrid(origin, interval_ns) if interval_ns else None
    topbooks = TopbookEmissionTracker()
    index = (out / "sampling-index.jsonl").open("w")
    raw = (run_dir / "raw_events.jsonl").open("w") if keep_raw else None
    archive = gzip.open(out / "raw_events.jsonl.gz", "wt", compresslevel=1) if args.retention == "raw-archive" else None
    raw_digest = hashlib.sha256()
    counts = Counter()
    initialized = set()
    grid_observations = []
    source_observations = []

    def emit(key, point_ns, observed_ns, value):
        sequence, utc, snapshot = value
        state = states[key]
        assert observed_ns <= point_ns
        top_kwargs = dict(
            collector_run_id="replay", received_at_utc=utc,
            received_at_monotonic_ns=observed_ns - origin, local_sequence=sequence,
        )
        if args.venue == "pm":
            rows = [polymarket_ws_snapshot_to_topbook(snapshot.as_dict(), **top_kwargs)]
        else:
            rows = kalshi_ws_snapshot_to_topbook(snapshot.as_dict(), **top_kwargs)
        depth_utc = utc
        for row in rows:
            stamped = add_book_integrity(
                row, integrity=snapshot.book_integrity_valid, selection=profile,
            )
            emission = topbooks.observe(stamped, now_monotonic_ns=observed_ns - origin, force_main=True)
            assert emission is not None
            coordinator.add("topbook_main", emission.row)
            depth_utc = str(emission.row["received_at_utc"])
        if module._should_emit_canonical_depth(snapshot):
            for row in module._canonical_depth_rows_from_state(
                run_id="replay", sequence=sequence, received_at_utc=depth_utc,
                snapshot=snapshot, state=state,
            ):
                coordinator.add("depth_main", add_book_integrity(
                    row, integrity=snapshot.book_integrity_valid, selection=profile,
                ))
        index.write(json.dumps({
            "book_id": key, "grid_relative_ns": point_ns - origin,
            "observed_relative_ns": observed_ns - origin, "local_sequence": sequence,
            "book_integrity_valid": snapshot.book_integrity_valid,
        }) + "\n")
        counts["persisted_states"] += 1
        counts["persisted_invalid_states"] += int(not snapshot.book_integrity_valid)
        grid_observations.append((key, sequence, point_ns, observed_ns))

    start = time.perf_counter()
    cpu_start = time.process_time()
    try:
        for sequence, ns, utc, message in records:
            if grid:
                for sample in grid.advance(ns):
                    emit(*sample)
            event = str(message.get("event_type") or message.get("type") or "")
            payload = message.get("msg", message)
            if args.venue == "kx" and payload.get("market_ticker") not in selected:
                continue
            if args.venue == "pm":
                assets = {str(message.get("asset_id") or "")}
                assets.update(str(change.get("asset_id") or "") for change in message.get("price_changes", []))
                if not assets.intersection(selected):
                    continue
            if args.venue == "pm":
                snapshots = apply_market_message(states, message, allowed_asset_ids=selected)
            else:
                snapshots = apply_kalshi_orderbook_message(states, message)
            if raw or archive:
                raw_line = json.dumps({"sequence": sequence, "received_at_utc": utc, "message": message}) + "\n"
                (raw or archive).write(raw_line)
                raw_digest.update(raw_line.encode())
            if keep_legacy:
                coordinator.add("parsed_event", module._event_row(sequence, ns / 10**9, utc, message))
                for row in module._level_rows(sequence, ns / 10**9, utc, message):
                    if args.venue == "kx" or row.get("asset_id") in selected:
                        coordinator.add("legacy_level", row)
            if keep_tape:
                producer.observe(
                    message=message, states=states, received_at_utc=utc,
                    received_at_monotonic_ns=ns - origin, local_sequence=sequence,
                ).write_to(coordinator)
            for snapshot in snapshots:
                key = snapshot.asset_id if args.venue == "pm" else snapshot.market_ticker
                if snapshot.initial_snapshot_received:
                    initialized.add(key)
                counts["observed_states"] += 1
                source_observations.append((key, sequence, ns))
                counts["observed_invalid_states"] += int(not snapshot.book_integrity_valid)
                if keep_legacy:
                    coordinator.add("legacy_snapshot", module._snapshot_row(
                        sequence, ns / 10**9, utc, snapshot.as_dict(),
                    ))
                value = (sequence, utc, snapshot)
                if grid:
                    grid.observe(key, ns, value)
                else:
                    emit(key, ns, ns, value)
            counts["input_messages"] += 1
            counts["book_messages"] += int(event in {
                "book", "price_change", "orderbook_snapshot", "orderbook_delta",
            })
            if coordinator.barrier_due():
                coordinator.commit()
        if grid:
            for sample in grid.advance(horizon, inclusive=True):
                emit(*sample)
        if raw:
            raw.flush()
            os.fsync(raw.fileno())
            raw.close()
        if archive:
            archive.close()
            with (out / "raw_events.jsonl.gz").open("rb+") as handle:
                os.fsync(handle.fileno())
        coordinator.finalize()
    finally:
        index.close()
        if raw and not raw.closed:
            raw.close()
        if archive and not archive.closed:
            archive.close()
    elapsed = time.perf_counter() - start
    cpu_elapsed = time.process_time() - cpu_start
    # Independent reference: group each observation by its first grid point at
    # or after it, keeping the last input in each bucket. Never include a tail
    # bucket beyond the recorded horizon. Validation is outside the timed region.
    if interval_ns:
        expected = {}
        for key, sequence, ns in source_observations:
            point = origin + ((ns - origin + interval_ns - 1) // interval_ns) * interval_ns
            if point <= horizon:
                expected[(point, key)] = (key, sequence, point, ns)
        assert grid_observations == [expected[key] for key in sorted(expected)]
    summary = {
        "format": "pmkt.book-grid-storage-experiment.v1",
        "venue": venue, "interval_seconds": args.interval, "retention": args.retention,
        "source": str(args.input), "source_sha256": file_hash(args.input),
        "selection_sha256": file_hash(args.selection), "instruments": len(ids),
        "initialized_instruments": len(initialized), "origin_ns": origin,
        "horizon_relative_ns": horizon - origin, "source_window_seconds": (horizon - origin) / 10**9,
        "counts": dict(counts), "row_counts": coordinator.row_counts,
        "elapsed_seconds": elapsed, "cpu_seconds": cpu_elapsed,
        "stages": stage_times, "durability": coordinator.durability_manifest(),
        "logical_sha256": {role: digest.hexdigest() for role, digest in digests.items()},
        "bytes_by_role": {
            role: sum(p.stat().st_size for p in (run_dir / role).glob("*.parquet"))
            for role in retained
        },
        "raw_bytes": (run_dir / "raw_events.jsonl").stat().st_size if keep_raw else 0,
        "raw_archive_bytes": (out / "raw_events.jsonl.gz").stat().st_size if archive else 0,
        "raw_logical_sha256": raw_digest.hexdigest() if raw or archive else None,
        "pending_uncompleted_grid_states": len(grid.pending) if grid else 0,
        "strict_commit_validation": "passed",
        "causal_selection_oracle": "passed" if interval_ns else "dense",
        "limitations": [
            "Storage component replay, not a live transport or capture acceptance test.",
            "Health, recovery decisions, eligibility and periodic capture controls excluded.",
            "States-only output cannot reconstruct intervening events or integrity transitions.",
            "Raw archive preserves selected messages for offline normalization; it is not a committed canonical tape.",
            "Book row timestamps remain source observations; grid coordinates are in sampling-index.jsonl.",
            "Origin defaults to first recorded observation, not an inferred socket connection time.",
        ],
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--venue", choices=["pm", "kx"], required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=0.25, help="Seconds; 0 means dense")
    parser.add_argument("--retention", choices=["all", "no-sidecar", "tape", "states", "raw-archive"], default="all")
    parser.add_argument("--seconds", type=float, default=None, help="Replay the first N recorded seconds")
    parser.add_argument("--origin-utc", default=None, help="Explicit recorded segment origin, at/before first input")
    parser.add_argument("--segment-rows", type=int, default=100000)
    args = parser.parse_args()
    if args.seconds is not None and (not math.isfinite(args.seconds) or args.seconds <= 0):
        parser.error("seconds must be finite and positive")
    summary = benchmark(args)
    summary["commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    summary["harness_sha256"] = file_hash(Path(__file__))
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps({key: summary[key] for key in (
        "venue", "interval_seconds", "retention", "elapsed_seconds", "row_counts",
    )}), flush=True)


if __name__ == "__main__":
    main()
