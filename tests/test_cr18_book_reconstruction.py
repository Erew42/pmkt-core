from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from pmkt.cli import reconstruction as reconstruction_cli
import pmkt.data.book_reconstruction_streaming as reconstruction_streaming
from pmkt.cli.app import app
from pmkt.data import book_reconstruction as reconstruction_data
from pmkt.data.book_reconstruction import (
    BookTapeReconstructionError,
    BookTapeReconstructionResult,
    _compare_depths,
    _compare_topbooks,
    _frame_semantic_hash,
    _reset_venue_order_for_reconnect,
    _validate_venue_order,
    reconstruct_book_tape,
)
from pmkt.data.book_reconstruction_streaming import (
    MAX_RECONSTRUCTION_MATERIALIZED_ROWS,
    _ReconstructionEngine,
    stream_reconstruct_book_tape,
)
from pmkt.data.kalshi_quotes import KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
from pmkt.data.normalize_books import kalshi_ws_snapshot_to_topbook
from pmkt.data.registry import DEPTH_SCHEMA_VERSION, TOPBOOK_SCHEMA_VERSION
from pmkt.data.validation import validate_frame
from pmkt.exchanges.kalshi.order_book_stream import stream_kalshi_order_book_data
from pmkt.exchanges.kalshi.ws import apply_kalshi_orderbook_message
from pmkt.exchanges.polymarket.order_book_stream import stream_order_book_data
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.streaming.durability import (
    COMMIT_JOURNAL_V1_NAME,
    COMMIT_JOURNAL_V2_NAME,
    RUN_STATE_NAME,
    file_sha256,
)
from pmkt.streaming.instrument_evidence import CAPTURE_INSTRUMENT_EVIDENCE_ROLE
from pmkt.streaming.profiles import select_storage_profile
from pmkt.streaming.recovery_contracts import (
    CaptureCommitRecordV1,
    CaptureCommitRecordV2,
    RunStateV1,
)
from pmkt.streaming.storage_backends import CaptureStorageBackend
from pmkt.streaming.tape import NativeBookLevel, post_book_hash


class FakeReadAuth:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.calls: list[str] = []

    def headers_for_get(self, path: str) -> dict[str, str]:
        self.calls.append(path)
        return {"KALSHI-ACCESS-KEY": "key-id"}

def _import_depth_parity_bucket_count(
    value: str | None,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    if value is None:
        environment.pop("PMKT_DEPTH_PARITY_BUCKET_COUNT", None)
    else:
        environment["PMKT_DEPTH_PARITY_BUCKET_COUNT"] = value
    return subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from pmkt.data.book_reconstruction_streaming import "
                "_DEPTH_PARITY_BUCKET_COUNT; "
                "print(_DEPTH_PARITY_BUCKET_COUNT)"
            ),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
def _import_duckdb_parity_memory_limit(
    value: str | None,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    if value is None:
        environment.pop("PMKT_DUCKDB_PARITY_MEMORY_LIMIT", None)
    else:
        environment["PMKT_DUCKDB_PARITY_MEMORY_LIMIT"] = value
    return subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from pmkt.data.book_reconstruction_streaming import "
                "DUCKDB_PARITY_MEMORY_LIMIT; "
                "print(DUCKDB_PARITY_MEMORY_LIMIT)"
            ),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_duckdb_parity_memory_limit_defaults_to_384mb() -> None:
    result = _import_duckdb_parity_memory_limit(None)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "384MB"



def test_depth_parity_bucket_count_defaults_to_64() -> None:
    result = _import_depth_parity_bucket_count(None)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "64"


def test_depth_parity_bucket_count_accepts_positive_environment_override() -> None:
    result = _import_depth_parity_bucket_count("256")

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "256"


@pytest.mark.parametrize("value", ["0", "-1", "not-an-integer"])
def test_depth_parity_bucket_count_rejects_invalid_environment_override(
    value: str,
) -> None:
    result = _import_depth_parity_bucket_count(value)

    assert result.returncode != 0
    assert "PMKT_DEPTH_PARITY_BUCKET_COUNT must be" in result.stderr


def _eligibility_evidence(*instrument_ids: str) -> dict[str, dict[str, str]]:
    observed_at = datetime.now(tz=timezone.utc).isoformat()
    return {
        instrument_id: {
            "status": "eligible",
            "reason": "source_active",
            "source_identity": "synthetic-reconstruction-fixture.v1",
            "source_reference": "tests/test_cr18_book_reconstruction.py",
            "source_sha256": "a" * 64,
            "observed_at_utc": observed_at,
        }
        for instrument_id in instrument_ids
    }


def _downgrade_capture_journal_to_v1(manifest_path: Path) -> None:
    run_dir = manifest_path.parent
    v2_path = run_dir / COMMIT_JOURNAL_V2_NAME
    legacy_records = []
    for line in v2_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        current = CaptureCommitRecordV2.from_mapping(json.loads(line))
        legacy_records.append(
            CaptureCommitRecordV1.create(
                group_id=current.group_id,
                committed_at_utc=current.committed_at_utc,
                artifacts=current.artifacts,
            )
        )
    (run_dir / COMMIT_JOURNAL_V1_NAME).write_text(
        "".join(
            json.dumps(record.to_mapping(), sort_keys=True) + "\n"
            for record in legacy_records
        ),
        encoding="utf-8",
    )
    v2_path.unlink()

    state_path = run_dir / RUN_STATE_NAME
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.pop("capture_durability", None)
    state_path.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["capture_commit_journal"] = COMMIT_JOURNAL_V1_NAME
    manifest.pop("capture_durability", None)
    for artifact in manifest["dataset_artifacts"].values():
        segment_path = run_dir / artifact["segment_manifest_path"]
        segment = json.loads(segment_path.read_text(encoding="utf-8"))
        segment["journal_path"] = COMMIT_JOURNAL_V1_NAME
        segment_path.write_text(
            json.dumps(segment, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        artifact["segment_manifest_hash"] = file_sha256(segment_path)
    evidence = manifest["dataset_artifacts"].get(CAPTURE_INSTRUMENT_EVIDENCE_ROLE)
    if evidence is not None:
        manifest["capture_completeness"]["evidence_artifact_hash"] = evidence[
            "segment_manifest_hash"
        ]
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True) + "\n",
        encoding="utf-8",
    )


class _FakeWebSocket:
    def __init__(
        self,
        messages: list[Any],
        *,
        idle_after_messages_s: float | None = None,
    ) -> None:
        self.messages = deque(messages)
        self.sent: list[str] = []
        self.idle_after_messages_s = idle_after_messages_s
        self._idled = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.messages:
            return self.messages.popleft()
        if self.idle_after_messages_s is not None and not self._idled:
            self._idled = True
            await asyncio.sleep(self.idle_after_messages_s)
        raise StopAsyncIteration


def test_reconstruction_semantic_hash_treats_decimal_nan_as_null() -> None:
    missing = pd.DataFrame([{"nullable_decimal": None}])
    decimal_nan = pd.DataFrame([{"nullable_decimal": Decimal("NaN")}])

    assert _frame_semantic_hash(decimal_nan) == _frame_semantic_hash(missing)


def test_streamed_publication_cleans_staging_when_initialization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "initialization-failure"

    def fail_stream_initialization(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise BookTapeReconstructionError("injected authority failure")

    monkeypatch.setattr(
        reconstruction_cli,
        "stream_reconstruct_book_tape",
        fail_stream_initialization,
    )

    with pytest.raises(BookTapeReconstructionError, match="authority failure"):
        reconstruction_cli.publish_streamed_book_tape_reconstruction(
            tmp_path / "manifest.json",
            destination,
        )

    assert not destination.exists()
    assert not list(tmp_path.glob(".initialization-failure.staging-*"))


def test_streamed_publication_persists_parity_mismatch_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "parity-failure"
    mismatch_report_path = tmp_path / "parity-failure.parity-mismatch.json"

    class MismatchStream:
        report = {
            "schema_version": "book_tape_reconstruction_report.v1",
            "status": "mismatch",
            "topbook_comparison": {"discrepancy_count": 2},
            "depth_comparison": {"discrepancy_count": 0},
        }
        closed = False

        def __iter__(self):
            return iter(())

        def close(self) -> None:
            self.closed = True

    stream = MismatchStream()
    monkeypatch.setattr(
        reconstruction_cli,
        "stream_reconstruct_book_tape",
        lambda *args, **kwargs: stream,
    )

    with pytest.raises(ValueError, match="diagnostic report"):
        reconstruction_cli.publish_streamed_book_tape_reconstruction(
            tmp_path / "manifest.json",
            destination,
            batch_rows=7,
        )

    assert stream.closed is True
    assert not destination.exists()
    assert not list(tmp_path.glob(".parity-failure.staging-*"))
    assert mismatch_report_path.is_file()
    report = json.loads(mismatch_report_path.read_text(encoding="utf-8"))
    assert report["status"] == "mismatch"
    assert report["publication"] == {
        "atomic_directory_publication": False,
        "destination": str(destination.resolve()),
        "incremental_parquet_writer": True,
        "batch_rows": 7,
        "reason": "parity_mismatch",
    }
    assert report["outputs"] == {}


@pytest.mark.asyncio
async def test_promoted_sqlite_capture_reconstructs_through_both_readers(
    tmp_path,
) -> None:
    fake = _FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "market-1",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            json.dumps(
                {
                    "event_type": "price_change",
                    "asset_id": "token-1",
                    "market": "market-1",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "side": "SELL",
                            "price": "0.55",
                            "size": "7",
                        },
                        {
                            "asset_id": "token-1",
                            "side": "SELL",
                            "price": "0.60",
                            "size": "0",
                        },
                    ],
                }
            ),
        ]
    )

    async def connect_factory(_: str) -> _FakeWebSocket:
        return fake

    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="sqlite-poly-reconstruct",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        instrument_eligibility_evidence=_eligibility_evidence("token-1"),
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("book-tape"),
        capture_storage_backend=CaptureStorageBackend.SQLITE_WAL,
    )
    manifest = tmp_path / "sqlite-poly-reconstruct" / "manifest.json"

    materialized = reconstruct_book_tape(manifest)
    streamed = stream_reconstruct_book_tape(manifest, batch_rows=1)
    streamed_batches = list(streamed)

    assert materialized.report["status"] == "success"
    assert materialized.report["journal_coverage_complete"] is True
    assert materialized.topbooks.iloc[-1]["best_ask_dollars"] == 0.55
    assert streamed.report["status"] == "success"
    assert streamed_batches


@pytest.mark.asyncio
async def test_reconstructs_polymarket_checkpoint_and_absolute_delta_deterministically(
    tmp_path,
    monkeypatch,
) -> None:
    fake = _FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "market-1",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            json.dumps(
                {
                    "event_type": "price_change",
                    "asset_id": "token-1",
                    "market": "market-1",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "side": "SELL",
                            "price": "0.55",
                            "size": "7",
                        },
                        {
                            "asset_id": "token-1",
                            "side": "SELL",
                            "price": "0.60",
                            "size": "0",
                        },
                    ],
                }
            ),
        ]
    )

    async def connect_factory(_: str) -> _FakeWebSocket:
        return fake

    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="poly-reconstruct",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        instrument_eligibility_evidence=_eligibility_evidence("token-1"),
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("book-tape"),
    )
    manifest = tmp_path / "poly-reconstruct" / "manifest.json"
    first = reconstruct_book_tape(manifest)
    second = reconstruct_book_tape(manifest)
    pd.testing.assert_frame_equal(first.topbooks, second.topbooks)
    pd.testing.assert_frame_equal(first.depths, second.depths)
    assert first.topbooks.iloc[-1]["best_ask_dollars"] == 0.55
    assert first.depths.iloc[-1]["size_contracts"] == 7.0
    # The price-change source message contains two level mutations. Both apply,
    # but reconstruction publishes one state for that exact source message.
    assert first.report["applied_event_count"] == 2
    assert first.report["topbook_row_count"] == 2
    assert first.report["depth_row_count"] == 4
    assert first.report["status"] == "success"
    assert first.report["research_audit_only"] is True
    assert first.report["runtime_authority"] is False
    assert first.report["journal_coverage_complete"] is True
    assert first.report["committed_role_coverage_complete"] is True
    assert first.report["topbook_comparison"]["status"] == "match"
    assert first.report["topbook_comparison"]["mismatch_count"] == 0
    assert first.report["topbook_comparison"]["discrepancy_count"] == 0
    assert first.report["depth_comparison"]["status"] == "not_available"
    assert (
        first.report["output_semantic_hashes"]
        == second.report["output_semantic_hashes"]
    )
    assert validate_frame(first.topbooks, TOPBOOK_SCHEMA_VERSION, strict=True).ok
    assert validate_frame(first.depths, DEPTH_SCHEMA_VERSION, strict=True).ok

    stream = stream_reconstruct_book_tape(manifest, batch_rows=1)
    streamed = list(stream)
    assert streamed
    assert all(
        len(batch.topbooks) <= 1 and len(batch.depths) <= 1 for batch in streamed
    )
    streamed_topbooks = pa.Table.from_batches(
        [batch.topbooks for batch in streamed]
    ).to_pandas()
    streamed_depths = pa.Table.from_batches(
        [batch.depths for batch in streamed]
    ).to_pandas()
    pd.testing.assert_frame_equal(
        first.topbooks,
        streamed_topbooks,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(first.depths, streamed_depths, check_dtype=False)
    assert stream.report["status"] == "success"
    assert stream.report["streaming"]["arrow_batch_rows"] == 1
    assert MAX_RECONSTRUCTION_MATERIALIZED_ROWS == 250_000

    payload = json.loads(manifest.read_text(encoding="utf-8"))
    original_profile_version = payload["storage_profile"]["profile_version"]
    assert {item["role"] for item in first.report["source_artifact_provenance"]} == set(
        payload["dataset_artifacts"]
    )

    output_dir = tmp_path / "published-reconstruction"
    outputs = reconstruction_cli.publish_book_tape_reconstruction(
        first,
        output_dir,
    )
    assert set(outputs) == {"topbook", "depth", "report"}
    assert all(output_dir in path.parents for path in map(Path, outputs.values()))
    assert all(Path(path).is_file() for path in outputs.values())
    published_report = json.loads(Path(outputs["report"]).read_text(encoding="utf-8"))
    assert published_report["publication"] == {
        "atomic_directory_publication": True,
        "destination": str(output_dir.resolve()),
    }

    cli_output_dir = tmp_path / "cli-reconstruction"
    cli_result = CliRunner().invoke(
        app,
        [
            "reconstruct-book-tape",
            "--manifest",
            str(manifest),
            "--out-dir",
            str(cli_output_dir),
        ],
    )
    assert cli_result.exit_code == 0, cli_result.output
    cli_outputs = json.loads(cli_result.stdout)
    assert set(cli_outputs) == {"topbook", "depth", "report"}
    assert all(Path(path).is_file() for path in cli_outputs.values())
    with pytest.raises(FileExistsError, match="already exists"):
        reconstruction_cli.publish_book_tape_reconstruction(first, output_dir)

    mismatch = BookTapeReconstructionResult(
        first.topbooks,
        first.depths,
        {**first.report, "status": "mismatch"},
    )
    rejected_dir = tmp_path / "rejected-reconstruction"
    with pytest.raises(ValueError, match="parity discrepancies"):
        reconstruction_cli.publish_book_tape_reconstruction(
            mismatch,
            rejected_dir,
        )
    assert not rejected_dir.exists()

    real_write_parquet = reconstruction_cli.write_parquet
    call_count = 0

    def fail_second_write(*args: Any, **kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise OSError("injected depth write failure")
        return real_write_parquet(*args, **kwargs)

    failed_dir = tmp_path / "failed-reconstruction"
    with monkeypatch.context() as context:
        context.setattr(
            reconstruction_cli,
            "write_parquet",
            fail_second_write,
        )
        with pytest.raises(OSError, match="injected depth write failure"):
            reconstruction_cli.publish_book_tape_reconstruction(
                first,
                failed_dir,
            )
    assert not failed_dir.exists()
    assert not list(tmp_path.glob(".failed-reconstruction.staging-*"))

    read_committed_rows = reconstruction_data.read_committed_capture_rows

    def read_with_mismatched_tape_encoding(
        run_dir: Any,
        artifacts: Any,
    ) -> Any:
        rows_by_role = read_committed_rows(run_dir, artifacts)
        event_rows = rows_by_role.get("tape_event")
        if event_rows:
            return {
                **rows_by_role,
                "tape_event": [
                    {**row, "encoding_version": "book-tape.v2"} for row in event_rows
                ],
            }
        return rows_by_role

    with monkeypatch.context() as context:
        context.setattr(
            reconstruction_data,
            "read_committed_capture_rows",
            read_with_mismatched_tape_encoding,
        )
        with pytest.raises(BookTapeReconstructionError, match="encoding_version"):
            reconstruction_data._load_committed_run_evidence(manifest)

    evidence = reconstruction_data._load_committed_run_evidence(manifest)
    original_events = evidence.frames["tape_event"]
    original_levels = evidence.frames["tape_level"]
    delta = original_events[original_events["event_kind"] == "delta"].iloc[0]
    delta_levels = original_levels[
        original_levels["event_id"].astype(str).eq(str(delta["event_id"]))
    ].sort_values("level_ordinal")
    assert len(delta_levels) == 2
    first_delta = delta.copy()
    first_delta["event_id"] = "same-message-delta-0"
    first_delta["subsequence"] = int(delta["subsequence"])
    first_delta["expected_level_row_count"] = 1
    first_delta["post_book_hash"] = post_book_hash(
        venue="polymarket",
        venue_book_id="token-1",
        levels=[
            NativeBookLevel("bid", "0.4", 10.0),
            NativeBookLevel("ask", "0.55", 7.0),
            NativeBookLevel("ask", "0.6", 5.0),
        ],
    )
    second_delta = delta.copy()
    second_delta["event_id"] = "same-message-delta-1"
    second_delta["subsequence"] = int(delta["subsequence"]) + 1
    second_delta["expected_level_row_count"] = 1
    first_level = delta_levels.iloc[[0]].copy()
    first_level.loc[:, "event_id"] = first_delta["event_id"]
    first_level.loc[:, "level_ordinal"] = 0
    second_level = delta_levels.iloc[[1]].copy()
    second_level.loc[:, "event_id"] = second_delta["event_id"]
    second_level.loc[:, "level_ordinal"] = 0
    split_events = pd.concat(
        [
            original_events[original_events["event_kind"] != "delta"],
            pd.DataFrame([first_delta, second_delta]),
        ],
        ignore_index=True,
    )
    split_levels = pd.concat(
        [
            original_levels[
                ~original_levels["event_id"].astype(str).eq(str(delta["event_id"]))
            ],
            first_level,
            second_level,
        ],
        ignore_index=True,
    )
    engine = _ReconstructionEngine(
        shard_by_book=evidence.shard_by_book,
        adapter_settings_by_venue=evidence.adapter_settings_by_venue,
    )
    grouped_topbooks, grouped_depths = engine.process(
        split_events,
        split_levels,
        evidence.frames["tape_control"],
    )
    final_topbooks, final_depths = engine.finish()
    grouped_topbooks.extend(final_topbooks)
    grouped_depths.extend(final_depths)
    assert engine.applied_event_count == 3
    assert engine.emitted_message_count == 2
    assert len(grouped_topbooks) == 2
    assert grouped_topbooks[-1]["best_ask_dollars"] == 0.55
    assert len(grouped_depths) == 4

    duplicate_events = pd.concat(
        [
            evidence.frames["tape_event"],
            evidence.frames["tape_event"].iloc[[0]],
        ],
        ignore_index=True,
    )
    duplicate_evidence = replace(
        evidence,
        frames={**evidence.frames, "tape_event": duplicate_events},
    )
    with monkeypatch.context() as context:
        context.setattr(
            reconstruction_data,
            "_load_committed_run_evidence",
            lambda _, **__: duplicate_evidence,
        )
        with pytest.raises(
            BookTapeReconstructionError,
            match="non-continuous event coordinate",
        ):
            reconstruction_data._reconstruct_book_tape_legacy(manifest)

    outside_events = evidence.frames["tape_event"].copy()
    delta_index = outside_events.index[outside_events["event_kind"] == "delta"][0]
    outside_events.loc[delta_index, "epoch_id"] = "f" * 64
    outside_evidence = replace(
        evidence,
        frames={**evidence.frames, "tape_event": outside_events},
    )
    with monkeypatch.context() as context:
        context.setattr(
            reconstruction_data,
            "_load_committed_run_evidence",
            lambda _, **__: outside_evidence,
        )
        with pytest.raises(
            BookTapeReconstructionError,
            match="no matching open epoch",
        ):
            reconstruction_data._reconstruct_book_tape_legacy(manifest)

    unmapped_row = evidence.frames["tape_event"].iloc[[0]].copy()
    unmapped_row.loc[:, "event_id"] = "unmapped-event"
    unmapped_row.loc[:, "venue_book_id"] = "unmapped-token"
    unmapped_row.loc[:, "venue_market_id"] = "unmapped-market"
    unmapped_evidence = replace(
        evidence,
        frames={
            **evidence.frames,
            "tape_event": pd.concat(
                [evidence.frames["tape_event"], unmapped_row],
                ignore_index=True,
            ),
        },
    )
    with monkeypatch.context() as context:
        context.setattr(
            reconstruction_data,
            "_load_committed_run_evidence",
            lambda _, **__: unmapped_evidence,
        )
        with pytest.raises(
            BookTapeReconstructionError,
            match="no exact shard mapping",
        ):
            reconstruction_data._reconstruct_book_tape_legacy(
                manifest,
                venue_book_id="token-1",
            )
    journal_path = manifest.parent / payload["capture_commit_journal"]
    journal_bytes = journal_path.read_bytes()
    real_validate_journal = reconstruction_data.validate_commit_journal

    def mutate_journal_after_validation(run_dir: Any) -> Any:
        records = real_validate_journal(run_dir)
        journal_path.write_bytes(journal_bytes + b"\n")
        return records

    with monkeypatch.context() as context:
        context.setattr(
            reconstruction_data,
            "validate_commit_journal",
            mutate_journal_after_validation,
        )
        with pytest.raises(
            BookTapeReconstructionError,
            match="source commit journal changed",
        ):
            reconstruction_data._reconstruct_book_tape_legacy(manifest)
    journal_path.write_bytes(journal_bytes)

    payload["storage_profile"]["tape_encoding_version"] = "unknown"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(BookTapeReconstructionError, match="tape_encoding_version"):
        reconstruct_book_tape(manifest)

    payload["storage_profile"]["tape_encoding_version"] = "book-tape.v1"
    payload["storage_profile"]["profile_version"] = "999"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(BookTapeReconstructionError, match="profile"):
        reconstruct_book_tape(manifest)

    payload["storage_profile"]["profile_version"] = original_profile_version
    payload["sequence_gap_count"] = 1
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(BookTapeReconstructionError, match="sequence gap"):
        reconstruct_book_tape(manifest)

    payload["sequence_gap_count"] = 0
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    event_entry = payload["dataset_artifacts"]["tape_event"]
    segment_manifest = json.loads(
        (manifest.parent / event_entry["segment_manifest_path"]).read_text(
            encoding="utf-8"
        )
    )
    segment_path = (
        manifest.parent
        / event_entry["path"]
        / segment_manifest["completed_segments"][0]["path"]
    )
    orphan_path = manifest.parent / "unjournaled.parquet"
    shutil.copyfile(segment_path, orphan_path)
    with pytest.raises(BookTapeReconstructionError, match="uncommitted artifacts"):
        reconstruct_book_tape(manifest)
    orphan_path.unlink()

    original_segment = segment_path.read_bytes()
    altered_segment = pq.read_table(segment_path)
    field = altered_segment.schema.field("received_at_monotonic_ns")
    field_index = altered_segment.schema.get_field_index(field.name)
    values = altered_segment.column(field_index).to_pylist()
    values[0] += 1
    altered_segment = altered_segment.set_column(
        field_index, field, pa.array(values, type=field.type)
    )
    pq.write_table(altered_segment, segment_path)
    with pytest.raises(BookTapeReconstructionError, match="artifact hash mismatch"):
        reconstruct_book_tape(manifest)
    segment_path.write_bytes(original_segment)

    real_scoped_recover = reconstruction_streaming.recover_stream_run

    def mutate_segment_after_scoped_recovery(*args: Any, **kwargs: Any) -> Any:
        report = real_scoped_recover(*args, **kwargs)
        expected = report.validated_artifact_fingerprints[
            segment_path.relative_to(manifest.parent).as_posix()
        ]
        stat = segment_path.stat()
        os.utime(
            segment_path,
            ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000),
        )
        assert (
            reconstruction_streaming._artifact_stat_fingerprint(segment_path)
            != expected
        )
        return report

    with monkeypatch.context() as context:
        context.setattr(
            reconstruction_streaming,
            "recover_stream_run",
            mutate_segment_after_scoped_recovery,
        )
        with pytest.raises(BookTapeReconstructionError, match="changed after recovery"):
            reconstruct_book_tape(manifest)

    def mutate_segment_after_validation(run_dir: Any) -> Any:
        records = real_validate_journal(run_dir)
        segment_path.write_bytes(original_segment + b"\n")
        return records

    with monkeypatch.context() as context:
        context.setattr(
            reconstruction_data,
            "validate_commit_journal",
            mutate_segment_after_validation,
        )
        with pytest.raises(
            BookTapeReconstructionError,
            match="artifact hash changed while loading",
        ):
            reconstruction_data._reconstruct_book_tape_legacy(manifest)
    segment_path.write_bytes(original_segment)
    lines = journal_path.read_text(encoding="utf-8").splitlines()
    journal_path.write_text("\n".join([*lines, lines[0]]) + "\n", encoding="utf-8")
    with pytest.raises(BookTapeReconstructionError, match="duplicate group_id"):
        reconstruct_book_tape(manifest)


@pytest.mark.asyncio
async def test_reconstruction_reads_legacy_v1_journal_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "market-1",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> _FakeWebSocket:
        return fake

    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="poly-v1-reconstruct",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        instrument_eligibility_evidence=_eligibility_evidence("token-1"),
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("book-tape"),
    )
    manifest = tmp_path / "poly-v1-reconstruct" / "manifest.json"
    _downgrade_capture_journal_to_v1(manifest)

    recovery_calls = 0
    real_recover = reconstruction_streaming.recover_stream_run

    def counted_recover(*args: Any, **kwargs: Any) -> Any:
        nonlocal recovery_calls
        recovery_calls += 1
        return real_recover(*args, **kwargs)

    monkeypatch.setattr(
        reconstruction_streaming,
        "recover_stream_run",
        counted_recover,
    )
    result = reconstruct_book_tape(manifest)

    assert result.report["status"] == "success"
    assert result.report["source_journal"].endswith(COMMIT_JOURNAL_V1_NAME)
    assert len(result.topbooks) == 1
    assert recovery_calls == 1


@pytest.mark.asyncio
async def test_ignores_nonreconstructible_checkpoint_outside_epoch(
    tmp_path,
) -> None:
    fake = _FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "invalid-token",
                    "market": "invalid-market",
                    "bids": [],
                    "asks": [],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> _FakeWebSocket:
        return fake

    await stream_order_book_data(
        ["invalid-token"],
        output_root=tmp_path,
        run_name="nonreconstructible-checkpoint",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("book-tape", profile_version="1"),
    )

    result = reconstruct_book_tape(
        tmp_path / "nonreconstructible-checkpoint" / "manifest.json"
    )

    assert result.report["status"] == "success"
    assert result.report["topbook_comparison"]["excluded_invalid_source_row_count"] == 3
    assert result.topbooks.empty
    assert result.depths.empty
    assert [item["reason"] for item in result.report["ignored_events"]] == [
        "non_reconstructible_checkpoint_outside_epoch"
    ]


@pytest.mark.asyncio
async def test_reconstructs_periodic_polymarket_checkpoint_with_full_depth_parity(
    tmp_path,
) -> None:
    fake = _FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "periodic-token",
                    "market": "periodic-market",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ],
        # Keep the transport open beyond the capture deadline. A clean remote
        # close is intentionally a stream failure, not a successful boundary.
        idle_after_messages_s=2.0,
    )

    async def connect_factory(_: str) -> _FakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="polymarket-0",
                subscribed_instruments=("periodic-token",),
            )
        ],
        max_message_age_ms=50,
        max_valid_book_age_ms=1_000,
    )

    manifest = await stream_order_book_data(
        ["periodic-token"],
        output_root=tmp_path,
        run_name="poly-periodic-reconstruct",
        duration_s=1,
        max_messages=2,
        capture_intent="smoke",
        max_reconnects=0,
        instrument_eligibility_evidence=_eligibility_evidence("periodic-token"),
        heartbeat_interval=None,
        connect_factory=connect_factory,
        feed_supervisor=supervisor,
        storage_profile=select_storage_profile(
            "full",
            book_checkpoint_interval_seconds=0.01,
        ),
    )

    assert manifest["capture_completeness"]["terminal_reason"] == "deadline_reached"

    result = reconstruct_book_tape(
        tmp_path / "poly-periodic-reconstruct" / "manifest.json"
    )

    periodic_epochs = [
        epoch
        for epoch in result.report["epoch_coverage"]
        if epoch["checkpoint_reason"] == "periodic"
    ]
    assert periodic_epochs
    assert any(
        epoch["closed_by_checkpoint_event_id"] is not None
        for epoch in result.report["epoch_coverage"]
    )
    assert result.report["status"] == "success"
    assert result.report["topbook_comparison"]["status"] == "match"
    depth = result.report["depth_comparison"]
    assert depth["status"] == "match"
    assert depth["periodic_checkpoint_row_count"] > 0
    assert (
        depth["periodic_checkpoint_compared_row_count"]
        == depth["periodic_checkpoint_row_count"]
    )


@pytest.mark.asyncio
async def test_reconstructs_kalshi_native_yes_no_sides(tmp_path) -> None:
    fake = _FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10"]],
                        "no_dollars_fp": [["0.65", "5"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 2,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "side": "yes",
                        "price_dollars": "0.40",
                        "delta_fp": "2",
                    },
                }
            ),
        ],
        idle_after_messages_s=0.12,
    )

    async def connect_factory(_: str, __: dict[str, str]) -> _FakeWebSocket:
        return fake

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="kalshi-reconstruct",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        instrument_eligibility_evidence=_eligibility_evidence("KXTEST"),
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        storage_profile=select_storage_profile(
            "full",
            book_checkpoint_interval_seconds=0.01,
        ),
    )
    assert (
        manifest["request"]["quote_normalization_policy"]
        == KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
    )
    run_state = json.loads(
        (tmp_path / "kalshi-reconstruct" / RUN_STATE_NAME).read_text(
            encoding="utf-8"
        )
    )
    assert run_state["adapter_settings_by_venue"]["kalshi"] == {
        "quote_normalization_policy": KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
        "use_yes_price": True,
    }
    result = reconstruct_book_tape(tmp_path / "kalshi-reconstruct" / "manifest.json")
    assert set(result.topbooks["instrument_id"]) == {"KXTEST:YES", "KXTEST:NO"}
    yes_depth = result.depths[result.depths["instrument_id"] == "KXTEST:YES"]
    final_yes = yes_depth[
        yes_depth["local_sequence"] == yes_depth["local_sequence"].max()
    ]
    assert final_yes.iloc[0]["venue_sequence"] == 2
    assert final_yes.iloc[0]["size_contracts"] == 12.0
    assert set(result.depths["side"]) == {"yes", "no"}
    source_pairs = {
        (row.best_bid_source, row.best_ask_source)
        for row in result.topbooks.itertuples()
        if row.outcome == "NO"
    }
    assert source_pairs == {("complement_derived", "complement_derived")}
    assert result.report["topbook_comparison"]["status"] == "match"
    assert result.report["depth_comparison"]["status"] == "match"
    depth = result.report["depth_comparison"]
    assert depth["periodic_checkpoint_row_count"] > 0
    assert (
        depth["periodic_checkpoint_compared_row_count"]
        == depth["periodic_checkpoint_row_count"]
    )
    assert any(
        epoch["checkpoint_reason"] == "periodic"
        for epoch in result.report["epoch_coverage"]
    )


@pytest.mark.parametrize(
    "fixture_name",
    ["kalshi_orderbook_yes_price.json", "kalshi_orderbook_no_price.json"],
)
def test_kalshi_reconstruction_matches_each_transcript_wire_mode(
    fixture_name: str,
) -> None:
    fixture = json.loads(
        (Path(__file__).with_name("fixtures") / fixture_name).read_text(
            encoding="utf-8"
        )
    )
    states = {}
    snapshots = []
    for message in fixture["messages"]:
        snapshots.extend(
            apply_kalshi_orderbook_message(
                states,
                message,
                use_yes_price=fixture["use_yes_price"],
            )
        )
    state = states["KXTRANSCRIPT"]
    native_book = {
        **{("yes", str(price)): size for price, size in state.yes_bids.items()},
        **{("no", str(price)): size for price, size in state.no_bids.items()},
    }
    event = {
        "venue": "kalshi",
        "collector_run_id": "fixture-run",
        "venue_book_id": "KXTRANSCRIPT",
        "venue_market_id": "market-transcript",
        "received_at_utc": "2026-01-01T00:00:00+00:00",
        "received_at_monotonic_ns": 1,
        "local_sequence": 2,
        "quality_flags_json": "[]",
        "valid_state": True,
        "venue_sid": snapshots[-1].sid,
        "venue_sequence": snapshots[-1].seq,
        "event_id": "fixture-event",
    }
    reconstructed, _ = reconstruction_data._normalize_native_book(
        event,
        native_book,
        use_yes_price=fixture["use_yes_price"],
        quote_normalization_policy=KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
    )
    live = kalshi_ws_snapshot_to_topbook(
        snapshots[-1].as_dict(),
        collector_run_id="fixture-run",
        received_at_utc="2026-01-01T00:00:00+00:00",
        received_at_monotonic_ns=1,
        local_sequence=2,
    )
    fields = (
        "outcome",
        "best_bid_dollars",
        "best_ask_dollars",
        "bid_size_contracts",
        "ask_size_contracts",
        "best_bid_source",
        "best_ask_source",
    )
    assert [[row[field] for field in fields] for row in reconstructed] == [
        [row[field] for field in fields] for row in live
    ]


@pytest.mark.parametrize(
    "settings,expected",
    [
        pytest.param({}, True, id="historical-missing"),
        pytest.param({"use_yes_price": True}, True, id="explicit-true"),
        pytest.param({"use_yes_price": False}, False, id="explicit-false"),
    ],
)
def test_dense_and_streaming_reconstruction_resolve_exact_kalshi_wire_mode(
    settings: dict[str, Any],
    expected: bool,
) -> None:
    assert reconstruction_data._kalshi_use_yes_price(settings) is expected
    engine = _ReconstructionEngine(
        shard_by_book={},
        adapter_settings_by_venue={"kalshi": settings},
    )
    assert engine.use_yes_price is expected


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="null"),
        pytest.param("true", id="true-text"),
        pytest.param("false", id="false-text"),
        pytest.param(1, id="one"),
        pytest.param(0, id="zero"),
    ],
)
def test_dense_and_streaming_reconstruction_reject_non_boolean_kalshi_wire_mode(
    value: object,
) -> None:
    settings = {"use_yes_price": value}
    with pytest.raises(BookTapeReconstructionError, match="must be a boolean"):
        reconstruction_data._kalshi_use_yes_price(settings)
    with pytest.raises(BookTapeReconstructionError, match="must be a boolean"):
        _ReconstructionEngine(
            shard_by_book={},
            adapter_settings_by_venue={"kalshi": settings},
        )


def test_reconstruction_rejects_duplicate_kalshi_sequence() -> None:
    last_sequence: dict[tuple[str, str, str], int] = {}
    last_book_sequence: dict[tuple[str, str, str], int] = {}
    last_sid: dict[tuple[str, str, str], str] = {}
    key = ("run-1", "kalshi", "KXTEST")
    _validate_venue_order(
        {
            "venue_sequence": 1,
            "venue_sid": "7",
            "event_kind": "checkpoint",
            "checkpoint_reason": "startup",
        },
        key,
        "kalshi-000",
        last_sequence,
        last_book_sequence,
        last_sid,
    )
    _validate_venue_order(
        {
            "venue_sequence": 1,
            "venue_sid": "7",
            "event_kind": "checkpoint",
            "checkpoint_reason": "periodic",
        },
        key,
        "kalshi-000",
        last_sequence,
        last_book_sequence,
        last_sid,
    )
    assert last_sequence == {("run-1", "kalshi-000", "7"): 1}
    assert last_book_sequence == {key: 1}

    with pytest.raises(
        BookTapeReconstructionError,
        match="duplicate or non-increasing",
    ):
        _validate_venue_order(
            {"venue_sequence": 1, "venue_sid": "7", "event_kind": "delta"},
            key,
            "kalshi-000",
            last_sequence,
            last_book_sequence,
            last_sid,
        )


def test_reconstruction_resets_kalshi_sequence_at_reconnect_boundary() -> None:
    key = ("run-1", "kalshi", "KXTEST")
    last_sequence = {("run-1", "kalshi-000", "7"): 198}
    last_book_sequence = {key: 198}
    last_sid = {key: "7"}

    _reset_venue_order_for_reconnect(
        key,
        "kalshi-000",
        last_sequence,
        last_book_sequence,
        last_sid,
    )
    _validate_venue_order(
        {
            "venue_sequence": 1,
            "venue_sid": "7",
            "event_kind": "checkpoint",
            "checkpoint_reason": "resync",
        },
        key,
        "kalshi-000",
        last_sequence,
        last_book_sequence,
        last_sid,
    )

    assert last_sequence == {("run-1", "kalshi-000", "7"): 1}
    assert last_book_sequence == {key: 1}
    assert last_sid == {key: "7"}


def test_reconstruction_scopes_kalshi_sequence_by_exact_shard() -> None:
    last_sequence: dict[tuple[str, str, str], int] = {}
    last_book_sequence: dict[tuple[str, str, str], int] = {}
    last_sid: dict[tuple[str, str, str], str] = {}
    event = {
        "venue_sequence": 1,
        "venue_sid": "7",
        "event_kind": "checkpoint",
        "checkpoint_reason": "startup",
    }

    _validate_venue_order(
        event,
        ("run-1", "kalshi", "KXALPHA"),
        "kalshi-000",
        last_sequence,
        last_book_sequence,
        last_sid,
    )
    _validate_venue_order(
        event,
        ("run-1", "kalshi", "KXBETA"),
        "kalshi-001",
        last_sequence,
        last_book_sequence,
        last_sid,
    )

    assert last_sequence == {
        ("run-1", "kalshi-000", "7"): 1,
        ("run-1", "kalshi-001", "7"): 1,
    }


def test_reconstruction_binds_manifest_shards_to_finalized_run_state() -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"tape_event": "tape-event"},
        shard_plan={
            "kalshi-000": ["KXALPHA"],
            "kalshi-001": ["KXBETA"],
        },
        started_at_utc="2026-01-01T00:00:00Z",
        status="finalized",
    )
    payload = {
        "feed_shards": [
            {
                "venue": "kalshi",
                "shard_id": "kalshi-000",
                "subscribed_instruments": ["KXALPHA"],
                "instrument_count": 1,
            },
            {
                "venue": "kalshi",
                "shard_id": "kalshi-001",
                "subscribed_instruments": ["KXBETA"],
                "instrument_count": 1,
            },
        ]
    }

    assert reconstruction_data._shard_by_book(payload, state) == {
        ("kalshi", "KXALPHA"): "kalshi-000",
        ("kalshi", "KXBETA"): "kalshi-001",
    }

    swapped = deepcopy(payload)
    swapped["feed_shards"][0]["subscribed_instruments"] = ["KXBETA"]
    swapped["feed_shards"][1]["subscribed_instruments"] = ["KXALPHA"]
    with pytest.raises(BookTapeReconstructionError, match="run-state shard_plan"):
        reconstruction_data._shard_by_book(swapped, state)

    collapsed = deepcopy(payload)
    collapsed["feed_shards"][0]["subscribed_instruments"] = [
        "KXALPHA",
        "KXBETA",
    ]
    collapsed["feed_shards"][0]["instrument_count"] = 2
    collapsed["feed_shards"].pop()
    with pytest.raises(BookTapeReconstructionError, match="run-state shard_plan"):
        reconstruction_data._shard_by_book(collapsed, state)


def test_streaming_topbook_parity_ignores_invalid_reconstructed_transitions(
    tmp_path: Path,
) -> None:
    def topbook_row(
        local_sequence: int,
        *,
        valid_state: bool,
        best_bid: float,
        best_ask: float,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": "topbook.v1",
            "collector_run_id": "run-1",
            "exchange": "kalshi",
            "venue_market_id": "market-1",
            "instrument_id": "book-1:YES",
            "outcome": "YES",
            "source": "reconstructed_book_tape",
            "received_at_utc": f"2026-01-01T00:00:{local_sequence:02d}.000000Z",
            "received_at_monotonic_ns": local_sequence,
            "exchange_ts_utc": None,
            "local_sequence": local_sequence,
            "venue_sequence": local_sequence,
            "venue_sid": 1,
            "book_hash": None,
            "best_bid_dollars": best_bid,
            "best_ask_dollars": best_ask,
            "mid_dollars": (best_bid + best_ask) / 2,
            "spread_dollars": best_ask - best_bid,
            "spread_bps": None,
            "bid_size_contracts": 1.0,
            "ask_size_contracts": 1.0,
            "best_bid_source": "direct",
            "best_ask_source": "direct",
            "tick_size_dollars": 0.01,
            "min_order_size_contracts": 1.0,
            "quote_age_ms": 0,
            "valid_state": valid_state,
            "quality_flags": [] if valid_state else ["crossed_book"],
            "raw_event_ref": None,
            "_reconstruction_event_id": event_id,
            "_source_role": "topbook_main",
            "_source_schema_version": "topbook.v1",
            "_source_artifact_path": "topbook/part-1.parquet",
        }

    reconstructed_path = tmp_path / "reconstructed.parquet"
    source_path = tmp_path / "source.parquet"
    reconstructed_rows = [
        topbook_row(1, valid_state=True, best_bid=0.4, best_ask=0.6, event_id="e1"),
        topbook_row(2, valid_state=False, best_bid=0.6, best_ask=0.6, event_id="e2"),
        topbook_row(3, valid_state=True, best_bid=0.4, best_ask=0.6, event_id="e3"),
    ]
    source_rows = [
        topbook_row(1, valid_state=True, best_bid=0.4, best_ask=0.6),
    ]
    pq.write_table(
        reconstruction_streaming._rows_to_table(
            reconstructed_rows,
            reconstruction_streaming._parity_schema("topbook.v1", source=False),
        ),
        reconstructed_path,
    )
    pq.write_table(
        reconstruction_streaming._rows_to_table(
            source_rows,
            reconstruction_streaming._parity_schema("topbook.v1", source=True),
        ),
        source_path,
    )

    comparison = reconstruction_streaming._duckdb_topbook_parity(
        reconstructed_path,
        source_path,
    )

    assert comparison["status"] == "match"
    assert comparison["required_state_change_count"] == 1
    assert comparison["missing_source_change_count"] == 0
    assert comparison["discrepancy_count"] == 0


def test_topbook_comparison_reports_coordinates_and_provenance() -> None:
    common = {
        "collector_run_id": "run-1",
        "exchange": "polymarket",
        "venue_market_id": "market-1",
        "instrument_id": "token-1",
        "outcome": None,
        "received_at_utc": "2026-01-01T00:00:00.000000Z",
        "received_at_monotonic_ns": 1,
        "local_sequence": 1,
        "subsequence": 0,
        "best_bid_dollars": 0.4,
        "best_ask_dollars": 0.6,
        "bid_size_contracts": 1.0,
        "ask_size_contracts": 1.0,
        "best_bid_source": "direct",
        "best_ask_source": "direct",
        "valid_state": True,
        "quality_flags": [],
    }
    reconstructed = pd.DataFrame(
        [
            {
                **common,
                "book_hash": "a" * 64,
                "_reconstruction_event_id": "event-1",
            }
        ]
    )
    source = pd.DataFrame(
        [
            {
                **common,
                "book_hash": "b" * 40,
                "best_ask_dollars": 0.7,
                "_source_role": "topbook_main",
                "_source_schema_version": "topbook.v1",
            }
        ]
    )

    comparison = _compare_topbooks(reconstructed, source)

    assert comparison["status"] == "mismatch"
    assert comparison["mismatch_count"] == 1
    assert comparison["missing_source_change_count"] == 1
    mismatch = comparison["mismatches"][0]
    assert mismatch["coordinate"]["local_sequence"] == 1
    assert mismatch["source_provenance"]["role"] == "topbook_main"
    assert set(mismatch["fields"]) == {"best_ask_dollars"}
    assert comparison["excluded_fields"] == ["book_hash"]


def test_topbook_comparison_rejects_source_only_tick_and_min_order_change() -> None:
    common = {
        "collector_run_id": "run-1",
        "exchange": "polymarket",
        "venue_market_id": "market-1",
        "instrument_id": "token-1",
        "outcome": None,
        "received_at_utc": "2026-01-01T00:00:00.000000Z",
        "received_at_monotonic_ns": 1,
        "local_sequence": 1,
        "subsequence": 0,
        "best_bid_dollars": 0.4,
        "best_ask_dollars": 0.6,
        "bid_size_contracts": 1.0,
        "ask_size_contracts": 1.0,
        "best_bid_source": "direct",
        "best_ask_source": "direct",
        "tick_size_dollars": 0.01,
        "min_order_size_contracts": 1.0,
        "valid_state": True,
        "quality_flags": [],
        "book_hash": None,
    }
    reconstructed = pd.DataFrame([{**common, "_reconstruction_event_id": "event-1"}])
    source = pd.DataFrame(
        [
            {
                **common,
                "_source_role": "topbook_main",
                "_source_schema_version": "topbook.v1",
            },
            {
                **common,
                "received_at_utc": "2026-01-01T00:00:01.000000Z",
                "received_at_monotonic_ns": 2,
                "local_sequence": 2,
                "tick_size_dollars": 0.02,
                "min_order_size_contracts": 2.0,
                "_source_role": "topbook_main",
                "_source_schema_version": "topbook.v1",
            },
        ]
    )

    comparison = _compare_topbooks(reconstructed, source)

    assert comparison["status"] == "mismatch"
    assert comparison["discrepancy_count"] == 1
    assert set(comparison["mismatches"][0]["fields"]) == {
        "tick_size_dollars",
        "min_order_size_contracts",
    }


def test_depth_comparison_is_separate_and_reports_source_coordinates() -> None:
    common = {
        "collector_run_id": "run-1",
        "exchange": "kalshi",
        "venue_market_id": "market-1",
        "instrument_id": "KXTEST:YES",
        "outcome": "YES",
        "received_at_utc": "2026-01-01T00:00:00.000000Z",
        "received_at_monotonic_ns": 1,
        "local_sequence": 1,
        "venue_sequence": 1,
        "venue_sid": "7",
        "side": "yes",
        "level_index": 0,
        "book_hash": "b" * 64,
        "price_dollars": 0.4,
        "size_contracts": 10.0,
        "cumulative_size_contracts": 10.0,
        "is_delta": False,
        "valid_state": True,
        "quality_flags": [],
    }
    reconstructed = pd.DataFrame(
        [
            {**common, "book_hash": None},
            {
                **common,
                "received_at_utc": "2026-01-01T00:00:01.000000Z",
                "received_at_monotonic_ns": 2,
                "local_sequence": 2,
            },
        ]
    )
    source = pd.DataFrame(
        [
            {
                **common,
                "size_contracts": 11.0,
                "_source_role": "depth_main",
                "_source_schema_version": "depth.v1",
            }
        ]
    )

    comparison = _compare_depths(
        reconstructed,
        source,
        available=True,
    )

    assert comparison["status"] == "mismatch"
    assert comparison["mismatch_count"] == 1
    assert comparison["missing_source_row_count"] == 1
    assert comparison["discrepancy_count"] == 2
    mismatch = comparison["mismatches"][0]
    assert mismatch["coordinate"]["venue_sequence"] == 1
    assert mismatch["source_provenance"]["role"] == "depth_main"
    assert set(mismatch["fields"]) == {"size_contracts"}
    assert comparison["excluded_fields"] == ["book_hash"]
