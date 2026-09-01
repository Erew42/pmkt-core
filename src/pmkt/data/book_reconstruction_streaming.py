from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pmkt.data.book_reconstruction import (
    _COMPARISON_SCHEMAS,
    _PROVENANCE_COLUMNS,
    _TAPE_SCHEMAS,
    RECONSTRUCTION_REPORT_VERSION,
    BookTapeReconstructionError,
    _apply_absolute_delta,
    _bool,
    _causal_items,
    _checkpoint_book,
    _filter_committed_role_rows,
    _kalshi_quote_policy,
    _kalshi_use_yes_price,
    _manifest_run_dir,
    _normalize_native_book,
    _nullable_text,
    _read_object,
    _reset_venue_order_for_reconnect,
    _schema_frame,
    _shard_by_book,
    _source_message_ownership,
    _source_provenance,
    _text,
    _validate_profile_compatibility,
    _validate_reconstructed_post_book_hash,
    _validate_venue_order,
)
from pmkt.data.manifests import (
    _journal_artifact_run_binding,
    validate_run_manifest,
)
from pmkt.data.registry import (
    DEPTH_COLUMNS,
    DEPTH_SCHEMA_VERSION,
    TOPBOOK_COLUMNS,
    TOPBOOK_SCHEMA_VERSION,
    arrow_schema,
    get_table_spec,
)
from pmkt.data.validation import (
    validate_book_control_evidence,
    validate_book_tape_bundle,
)
from pmkt.streaming.durability import (
    RUN_STATE_NAME,
    normalize_capture_value,
)
from pmkt.streaming.profiles import DatasetRole
from pmkt.streaming.recovery import (
    ArtifactStatFingerprint,
    _artifact_stat_fingerprint,
    _normalize_profile_v1_capture_flags,
    recover_stream_run,
    resolve_commit_journal_path,
)
from pmkt.streaming.recovery_contracts import CaptureCommitRecord, RunStateV1

DEFAULT_RECONSTRUCTION_BATCH_ROWS = 50_000
MAX_RECONSTRUCTION_MATERIALIZED_ROWS = 250_000
DUCKDB_PARITY_MEMORY_LIMIT = os.environ.get(
    "PMKT_DUCKDB_PARITY_MEMORY_LIMIT", "384MB"
)
MAX_PARITY_MISMATCH_DETAILS = 100


def _positive_int_environment(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be a positive integer; got {raw_value!r}"
        ) from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero; got {raw_value!r}")
    return value


_DEPTH_PARITY_BUCKET_COUNT = _positive_int_environment(
    "PMKT_DEPTH_PARITY_BUCKET_COUNT",
    64,
)
_PARITY_METADATA_COLUMNS = (
    "_reconstruction_event_id",
    "_reconstruction_event_kind",
    "_reconstruction_checkpoint_reason",
    *_PROVENANCE_COLUMNS,
)


@dataclass(frozen=True)
class BookTapeArrowBatch:
    """One bounded pair of normalized reconstruction record batches."""

    topbooks: pa.RecordBatch
    depths: pa.RecordBatch


@dataclass(frozen=True)
class _StreamingRunAuthority:
    manifest_path: Path
    manifest_sha256: str
    manifest_bytes: bytes
    payload: Mapping[str, Any]
    profile: Mapping[str, Any]
    run_dir: Path
    state_path: Path
    state_bytes: bytes
    journal_path: Path
    journal_bytes: bytes
    records: tuple[CaptureCommitRecord, ...]
    selected_schemas: Mapping[str, str]
    artifact_fingerprints: Mapping[
        str, ArtifactStatFingerprint
    ]
    artifact_provenance: tuple[Mapping[str, Any], ...]
    adapter_settings_by_venue: Mapping[str, Mapping[str, Any]]
    shard_by_book: Mapping[tuple[str, str], str]
    journal_sha256: str
    tape_encoding_version: str


class BookTapeReconstructionStream(Iterator[BookTapeArrowBatch]):
    """Yield bounded Arrow batches and expose the report after exhaustion."""

    def __init__(
        self,
        manifest_path: str | Path,
        *,
        venue_book_id: str | None = None,
        batch_rows: int = DEFAULT_RECONSTRUCTION_BATCH_ROWS,
    ) -> None:
        if batch_rows <= 0:
            raise ValueError("batch_rows must be positive")
        requested_book = None
        if venue_book_id is not None:
            requested_book = venue_book_id.strip()
            if not requested_book:
                raise BookTapeReconstructionError("venue_book_id must be non-empty")
        self._batch_rows = int(batch_rows)
        self._requested_book = requested_book
        self._started_at_perf = time.perf_counter()
        self._evidence = _load_streaming_run_authority(Path(manifest_path).resolve())
        self._authority_loaded_at_perf = time.perf_counter()
        self._engine = _ReconstructionEngine(
            shard_by_book=self._evidence.shard_by_book,
            adapter_settings_by_venue=self._evidence.adapter_settings_by_venue,
        )
        self._record_index = 0
        self._group_output_iter: Iterator[
            tuple[list[dict[str, Any]], list[dict[str, Any]]]
        ] | None = None
        self._selected_event_seen = False
        self._topbook_rows: list[dict[str, Any]] = []
        self._depth_rows: list[dict[str, Any]] = []
        self._source_topbook_count = 0
        self._excluded_invalid_topbook_count = 0
        self._topbook_count = 0
        self._depth_count = 0
        self._depth_comparison_available = (
            "depth_main" in self._evidence.payload["dataset_artifacts"]
        )
        self._journal_group_ids = tuple(
            record.group_id for record in self._evidence.records
        )
        self._source_depth_groups = tuple(
            (
                record.group_id,
                self._evidence.run_dir.joinpath(*Path(artifact.path).parts),
            )
            for record in self._evidence.records
            for artifact in record.artifacts
            if artifact.role == "depth_main"
        )
        self._topbook_hash = hashlib.sha256()
        self._depth_hash = hashlib.sha256()
        self._report: Mapping[str, Any] | None = None
        self._finished = False
        self._stage = tempfile.TemporaryDirectory(prefix="pmkt-book-reconstruction-")
        root = Path(self._stage.name)
        self._paths = {
            "reconstructed_topbook": root / "reconstructed_topbook.parquet",
            "reconstructed_depth": root / "reconstructed_depth.parquet",
            "source_topbook": root / "source_topbook.parquet",
        }
        self._writers: dict[str, pq.ParquetWriter] = {}

    @property
    def report(self) -> Mapping[str, Any]:
        if self._report is None:
            raise RuntimeError(
                "reconstruction report is available only after stream exhaustion"
            )
        return self._report

    def __iter__(self) -> BookTapeReconstructionStream:
        return self

    def __next__(self) -> BookTapeArrowBatch:
        while not self._topbook_rows and not self._depth_rows:
            if self._group_output_iter is not None:
                try:
                    topbooks, depths = next(self._group_output_iter)
                except StopIteration:
                    self._group_output_iter = None
                    continue
                self._topbook_rows.extend(topbooks)
                self._depth_rows.extend(depths)
                continue
            if self._record_index < len(self._evidence.records):
                self._start_next_group()
                continue
            if not self._finished:
                if self._requested_book is not None and not self._selected_event_seen:
                    raise BookTapeReconstructionError(
                        "no committed tape events for venue book "
                        f"{self._requested_book!r}"
                    )
                topbooks, depths = self._engine.finish()
                self._topbook_rows.extend(topbooks)
                self._depth_rows.extend(depths)
                self._finished = True
                if self._topbook_rows or self._depth_rows:
                    break
                self._complete()
            raise StopIteration

        topbook_rows = self._topbook_rows[: self._batch_rows]
        depth_rows = self._depth_rows[: self._batch_rows]
        del self._topbook_rows[: len(topbook_rows)]
        del self._depth_rows[: len(depth_rows)]
        topbook_stage = self._stage_reconstructed_rows(
            "reconstructed_topbook",
            topbook_rows,
        )
        depth_stage = (
            self._stage_reconstructed_rows(
                "reconstructed_depth",
                depth_rows,
            )
            if self._depth_comparison_available
            else None
        )
        self._topbook_count += len(topbook_rows)
        self._depth_count += len(depth_rows)
        topbook_batch = _public_record_batch_from_stage(
            topbook_stage,
            TOPBOOK_SCHEMA_VERSION,
        )
        depth_batch = (
            _public_record_batch_from_stage(
                depth_stage,
                DEPTH_SCHEMA_VERSION,
            )
            if depth_stage is not None
            else _public_record_batch(depth_rows, DEPTH_SCHEMA_VERSION)
        )
        _update_batch_hash(self._topbook_hash, topbook_batch)
        _update_batch_hash(self._depth_hash, depth_batch)
        if self._finished and not self._topbook_rows and not self._depth_rows:
            # Finalize parity before the caller's next iteration so report is
            # immediately available after a conventional for-loop.
            self._complete()
        return BookTapeArrowBatch(topbook_batch, depth_batch)

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()
        self._stage.cleanup()

    def _start_next_group(self) -> None:
        record = self._evidence.records[self._record_index]
        self._record_index += 1
        frames = _load_streaming_record_frames(
            self._evidence, record, venue_book_id=self._requested_book
        )
        self._selected_event_seen = (
            self._selected_event_seen or not frames["tape_event"].empty
        )
        topbook_frames = [
            frame
            for frame in (
                frames["topbook_main"],
                frames["topbook_checkpoint"],
            )
            if not frame.empty
        ]
        source_topbooks = (
            pd.concat(topbook_frames, ignore_index=True)
            if topbook_frames
            else frames["topbook_main"].iloc[0:0].copy()
        )
        self._source_topbook_count += len(source_topbooks)
        valid_topbooks = source_topbooks[
            source_topbooks["valid_state"].fillna(False).astype(bool)
        ]
        self._excluded_invalid_topbook_count += len(source_topbooks) - len(
            valid_topbooks
        )
        self._stage_source_frame("source_topbook", valid_topbooks)
        self._group_output_iter = self._engine.process_batches(
            frames["tape_event"],
            frames["tape_level"],
            frames["tape_control"],
            max_output_rows=self._batch_rows,
        )

    def _stage_source_frame(self, name: str, frame: pd.DataFrame) -> None:
        schema_version = (
            TOPBOOK_SCHEMA_VERSION if name.endswith("topbook") else DEPTH_SCHEMA_VERSION
        )
        rows = [
            {
                column: normalize_capture_value(row.get(column))
                for column in (
                    *(TOPBOOK_COLUMNS if name.endswith("topbook") else DEPTH_COLUMNS),
                    *_PROVENANCE_COLUMNS,
                )
            }
            for row in frame.to_dict("records")
        ]
        self._write_stage_rows(name, rows, schema_version, source=True)

    def _stage_reconstructed_rows(
        self,
        name: str,
        rows: list[dict[str, Any]],
    ) -> pa.Table:
        schema_version = (
            TOPBOOK_SCHEMA_VERSION if name.endswith("topbook") else DEPTH_SCHEMA_VERSION
        )
        table = _rows_to_table(
            rows,
            _parity_schema(schema_version, source=False),
        )
        self._write_stage_table(name, table)
        return table

    def _write_stage_rows(
        self,
        name: str,
        rows: list[dict[str, Any]],
        schema_version: str,
        *,
        source: bool,
    ) -> None:
        schema = _parity_schema(schema_version, source=source)
        if not rows:
            return
        table = _rows_to_table(rows, schema)
        self._write_stage_table(name, table)

    def _write_stage_table(
        self,
        name: str,
        table: pa.Table,
    ) -> None:
        if not len(table):
            return
        writer = self._writers.get(name)
        if writer is None:
            writer = pq.ParquetWriter(self._paths[name], table.schema)
            self._writers[name] = writer
        writer.write_table(table)

    def _complete(self) -> None:
        if self._report is not None:
            return
        complete_started_at = time.perf_counter()
        _verify_streaming_authority(self._evidence, verify_artifacts=True)
        for name, writer in tuple(self._writers.items()):
            writer.close()
            self._writers.pop(name)
        for name, path in self._paths.items():
            if path.exists():
                continue
            schema_version = (
                TOPBOOK_SCHEMA_VERSION
                if name.endswith("topbook")
                else DEPTH_SCHEMA_VERSION
            )
            source = name.startswith("source_")
            pq.write_table(
                _rows_to_table([], _parity_schema(schema_version, source=source)),
                path,
            )
        topbook_parity_started_at = time.perf_counter()
        topbook_comparison = _duckdb_topbook_parity(
            self._paths["reconstructed_topbook"],
            self._paths["source_topbook"],
        )
        topbook_parity_completed_at = time.perf_counter()
        depth_comparison = _duckdb_depth_parity(
            self._paths["reconstructed_depth"],
            self._source_depth_groups,
            journal_group_ids=self._journal_group_ids,
            available=self._depth_comparison_available,
            reconstructed_count=self._depth_count,
            venue_book_id=self._requested_book,
        )
        depth_parity_completed_at = time.perf_counter()
        _verify_streaming_authority(self._evidence, verify_artifacts=True)
        post_parity_verify_completed_at = time.perf_counter()
        topbook_comparison["excluded_invalid_source_row_count"] = (
            self._excluded_invalid_topbook_count
        )
        discrepancy_count = int(topbook_comparison["discrepancy_count"]) + int(
            depth_comparison["discrepancy_count"]
        )
        evidence = self._evidence
        profile = evidence.profile
        source_artifact_hashes: dict[str, list[str]] = {}
        for artifact_record in evidence.artifact_provenance:
            source_artifact_hashes.setdefault(
                _text(artifact_record.get("role")),
                [],
            ).append(_text(artifact_record.get("artifact_sha256")))
        self._report = {
            "schema_version": RECONSTRUCTION_REPORT_VERSION,
            "status": "success" if discrepancy_count == 0 else "mismatch",
            "research_audit_only": True,
            "runtime_authority": False,
            "source_manifest": str(evidence.manifest_path),
            "source_manifest_sha256": evidence.manifest_sha256,
            "source_journal": str(evidence.journal_path),
            "source_journal_sha256": evidence.journal_sha256,
            "source_run_id": _text(evidence.payload.get("run_id")),
            "selection": {
                "venue_book_id": self._requested_book,
                "applied_during_validated_segment_scan": True,
            },
            "source_profile": {
                "name": _text(profile.get("name")),
                "profile_version": _text(profile.get("profile_version")),
                "terminal_completeness": _text(profile.get("terminal_completeness")),
            },
            "source_artifact_hashes": {
                role: sorted(hashes)
                for role, hashes in sorted(source_artifact_hashes.items())
            },
            "source_artifact_provenance": [
                dict(item) for item in evidence.artifact_provenance
            ],
            "journaled_group_count": len(evidence.records),
            "journal_coverage_complete": True,
            "committed_role_coverage_complete": True,
            "causal_sequence_validation": {
                "status": "complete",
                "event_control_coordinate_count": (
                    self._engine.event_control_coordinate_count
                ),
                "terminal_book_count": len(self._engine.terminal_books),
            },
            "event_count": self._engine.event_count,
            "selected_event_count": self._engine.event_count,
            "applied_event_count": self._engine.applied_event_count,
            "emitted_source_message_count": self._engine.emitted_message_count,
            "ignored_orphan_rows": [],
            "ignored_events": list(self._engine.ignored),
            "invalid_events": [],
            "epoch_coverage": [
                self._engine.epoch_reports[key]
                for key in sorted(self._engine.epoch_reports)
            ],
            "topbook_comparison": topbook_comparison,
            "depth_comparison": depth_comparison,
            "topbook_row_count": self._topbook_count,
            "depth_row_count": self._depth_count,
            "output_semantic_hashes": {
                "policy": "arrow-record-batch-ipc-sha256.v1",
                "topbook_rows": self._topbook_hash.hexdigest(),
                "depth_rows": self._depth_hash.hexdigest(),
            },
            "streaming": {
                "arrow_batch_rows": self._batch_rows,
                "bounded_output_materialization": True,
                "parity_engine": "duckdb",
                "duckdb_memory_limit": DUCKDB_PARITY_MEMORY_LIMIT,
                "maximum_mismatch_details": MAX_PARITY_MISMATCH_DETAILS,
                "phase_seconds": {
                    "authority_load": (
                        self._authority_loaded_at_perf - self._started_at_perf
                    ),
                    "causal_reconstruction_and_stage_writes": (
                        complete_started_at - self._authority_loaded_at_perf
                    ),
                    "pre_parity_finalize": (
                        topbook_parity_started_at - complete_started_at
                    ),
                    "topbook_parity": (
                        topbook_parity_completed_at - topbook_parity_started_at
                    ),
                    "depth_parity": (
                        depth_parity_completed_at - topbook_parity_completed_at
                    ),
                    "post_parity_authority_verify": (
                        post_parity_verify_completed_at - depth_parity_completed_at
                    ),
                    "stream_to_verified_report": (
                        post_parity_verify_completed_at - self._started_at_perf
                    ),
                },
                "artifact_integrity": {
                    "initial_sha256_and_schema_validation": True,
                    "scoped_roles": sorted(evidence.selected_schemas),
                    "validated_artifact_count": len(
                        evidence.artifact_fingerprints
                    ),
                    "post_validation_fingerprint_policy": (
                        "size-mtime-ctime-inode-device.v1"
                    ),
                },
            },
        }
        self._stage.cleanup()


class _ReconstructionEngine:
    def __init__(
        self,
        *,
        shard_by_book: Mapping[tuple[str, str], str],
        adapter_settings_by_venue: Mapping[str, Mapping[str, Any]],
    ) -> None:
        self.shard_by_book = shard_by_book
        self.adapter_settings_by_venue = adapter_settings_by_venue
        self.use_yes_price = _kalshi_use_yes_price(
            adapter_settings_by_venue.get("kalshi", {})
        )
        self.kalshi_quote_policy = _kalshi_quote_policy(
            adapter_settings_by_venue.get("kalshi", {})
        )
        self.ignored: list[dict[str, Any]] = []
        self.open_epochs: dict[tuple[str, str, str], str] = {}
        self.pending_recovery: dict[tuple[str, str, str], str] = {}
        self.terminal_books: set[tuple[str, str, str]] = set()
        self.event_books: set[tuple[str, str, str]] = set()
        self.native_books: dict[
            tuple[str, str, str],
            dict[tuple[str, str], float],
        ] = {}
        self.last_venue_sequence: dict[tuple[str, str, str], int] = {}
        self.last_book_venue_sequence: dict[tuple[str, str, str], int] = {}
        self.last_venue_sid: dict[tuple[str, str, str], str] = {}
        self.last_book_coordinate: dict[
            tuple[str, str, str],
            tuple[pd.Timestamp, int, int, int, str],
        ] = {}
        self.epoch_reports: dict[
            tuple[str, str, str, str],
            dict[str, Any],
        ] = {}
        self.event_count = 0
        self.applied_event_count = 0
        self.emitted_message_count = 0
        self.event_control_coordinate_count = 0
        self._pending_owner: tuple[Any, ...] | None = None
        self._pending_row: dict[str, Any] | None = None
        self._pending_book: dict[tuple[str, str], float] | None = None
        self._pending_coordinate: tuple[pd.Timestamp, int, int, int, str] | None = None

    def process(
        self,
        events: pd.DataFrame,
        levels: pd.DataFrame,
        controls: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        output_topbooks: list[dict[str, Any]] = []
        output_depths: list[dict[str, Any]] = []
        for topbooks, depths in self.process_batches(
            events,
            levels,
            controls,
            max_output_rows=None,
        ):
            output_topbooks.extend(topbooks)
            output_depths.extend(depths)
        return output_topbooks, output_depths

    def process_batches(
        self,
        events: pd.DataFrame,
        levels: pd.DataFrame,
        controls: pd.DataFrame,
        *,
        max_output_rows: int | None,
    ) -> Iterator[tuple[list[dict[str, Any]], list[dict[str, Any]]]]:
        if max_output_rows is not None and max_output_rows <= 0:
            raise ValueError("max_output_rows must be positive when provided")
        output_topbooks: list[dict[str, Any]] = []
        output_depths: list[dict[str, Any]] = []
        levels_by_event = {
            (str(run_id), str(event_id)): group.sort_values(
                "level_ordinal",
                kind="mergesort",
            )
            for (run_id, event_id), group in levels.groupby(
                ["collector_run_id", "event_id"],
                sort=False,
            )
        }
        causal_items = _causal_items(events, controls)
        self.event_control_coordinate_count += len(causal_items)
        self.event_count += len(events)
        for family, row, coordinate in causal_items:
            owner = _source_message_ownership(row) if family == "event" else None
            if self._pending_owner is not None and owner != self._pending_owner:
                topbooks, depths = self._emit_pending()
                output_topbooks.extend(topbooks)
                output_depths.extend(depths)
                if max_output_rows is not None and (
                    len(output_topbooks) >= max_output_rows
                    or len(output_depths) >= max_output_rows
                ):
                    yield output_topbooks, output_depths
                    output_topbooks = []
                    output_depths = []
            key = (
                _text(row.get("collector_run_id")),
                _text(row.get("venue")),
                _text(row.get("venue_book_id")),
            )
            if not all(key):
                raise BookTapeReconstructionError(
                    f"{family} evidence lacks exact run/venue/book ownership"
                )
            if (key[1], key[2]) not in self.shard_by_book:
                raise BookTapeReconstructionError(
                    f"no exact shard mapping for {key[1]}:{key[2]}"
                )
            previous = self.last_book_coordinate.get(key)
            if previous is not None and coordinate <= previous:
                raise BookTapeReconstructionError(
                    f"non-continuous {family} coordinate for {key[1]}:{key[2]}"
                )
            if key in self.terminal_books:
                raise BookTapeReconstructionError(
                    f"{family} evidence occurs after terminal boundary for "
                    f"{key[1]}:{key[2]}"
                )
            self.last_book_coordinate[key] = coordinate
            if family == "control":
                self._apply_control(row, key)
                continue
            self.event_books.add(key)
            event_id = _text(row.get("event_id"))
            event_levels = levels_by_event.get(
                (key[0], event_id),
                levels.iloc[0:0],
            )
            _validate_venue_order(
                row,
                key,
                self.shard_by_book[(key[1], key[2])],
                self.last_venue_sequence,
                self.last_book_venue_sequence,
                self.last_venue_sid,
            )
            kind = _text(row.get("event_kind"))
            reconstructible = _bool(row.get("reconstructible"))
            epoch = _text(row.get("epoch_id"))
            if kind == "checkpoint":
                if not reconstructible:
                    if key in self.pending_recovery or key in self.open_epochs:
                        raise BookTapeReconstructionError(
                            f"non-reconstructible checkpoint {event_id} overlaps "
                            "an open epoch"
                        )
                    self.ignored.append(
                        {
                            "event_id": event_id,
                            "venue": key[1],
                            "venue_book_id": key[2],
                            "reason": ("non_reconstructible_checkpoint_outside_epoch"),
                            "source_provenance": _source_provenance(row),
                        }
                    )
                    continue
                if not epoch:
                    raise BookTapeReconstructionError(
                        f"checkpoint {event_id} must open a reconstructible epoch"
                    )
                if key in self.pending_recovery:
                    raise BookTapeReconstructionError(
                        f"checkpoint {event_id} precedes recovery of the prior "
                        "checkpoint"
                    )
                prior_epoch = self.open_epochs.get(key)
                if prior_epoch == epoch:
                    raise BookTapeReconstructionError(
                        f"checkpoint {event_id} reuses the open epoch"
                    )
                if prior_epoch is not None:
                    prior_report = self.epoch_reports[
                        (key[0], key[1], key[2], prior_epoch)
                    ]
                    prior_report["closed_by_checkpoint_event_id"] = event_id
                    prior_report["closed_at_local_sequence"] = int(
                        row.get("local_sequence") or 0
                    )
                self.native_books[key] = _checkpoint_book(row, event_levels)
                self.open_epochs[key] = epoch
                self.pending_recovery[key] = event_id
                self.epoch_reports[(key[0], key[1], key[2], epoch)] = {
                    "collector_run_id": key[0],
                    "venue": key[1],
                    "venue_book_id": key[2],
                    "epoch_id": epoch,
                    "checkpoint_event_id": event_id,
                    "checkpoint_reason": _nullable_text(row.get("checkpoint_reason")),
                    "opened_by_control_id": None,
                    "closed_by_control_id": None,
                    "closed_by_checkpoint_event_id": None,
                    "closed_at_local_sequence": None,
                    "first_local_sequence": int(row.get("local_sequence") or 0),
                    "last_local_sequence": int(row.get("local_sequence") or 0),
                    "applied_event_count": 0,
                }
            elif not reconstructible:
                if epoch or key in self.open_epochs:
                    raise BookTapeReconstructionError(
                        f"non-reconstructible event {event_id} overlaps an open epoch"
                    )
                self.ignored.append(
                    {
                        "event_id": event_id,
                        "venue": key[1],
                        "venue_book_id": key[2],
                        "reason": "non_reconstructible_outside_epoch",
                        "source_provenance": _source_provenance(row),
                    }
                )
                continue
            else:
                if key in self.pending_recovery:
                    raise BookTapeReconstructionError(
                        f"delta {event_id} precedes its recovery control"
                    )
                if (
                    not epoch
                    or self.open_epochs.get(key) != epoch
                    or key not in self.native_books
                ):
                    raise BookTapeReconstructionError(
                        f"reconstructible delta {event_id} has no matching open epoch"
                    )
                _apply_absolute_delta(self.native_books[key], event_levels)
            _validate_reconstructed_post_book_hash(
                row,
                self.native_books[key],
                adapter_settings=self.adapter_settings_by_venue.get(
                    key[1],
                    {},
                ),
            )
            report = self.epoch_reports[(key[0], key[1], key[2], epoch)]
            report["last_local_sequence"] = int(row.get("local_sequence") or 0)
            report["applied_event_count"] = int(report["applied_event_count"]) + 1
            self.applied_event_count += 1
            self._pending_owner = owner
            self._pending_row = dict(row)
            self._pending_book = dict(self.native_books[key])
            self._pending_coordinate = coordinate
        if output_topbooks or output_depths:
            yield output_topbooks, output_depths

    def finish(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        output = self._emit_pending()
        if self.pending_recovery:
            raise BookTapeReconstructionError(
                "reconstruction ended with unconfirmed checkpoint epochs"
            )
        if self.open_epochs:
            raise BookTapeReconstructionError("reconstruction ended with open epochs")
        missing_terminal = sorted(self.event_books - self.terminal_books)
        if missing_terminal:
            raise BookTapeReconstructionError(
                f"reconstruction lacks terminal coverage for {missing_terminal}"
            )
        return output

    def _apply_control(
        self,
        row: Mapping[str, Any],
        key: tuple[str, str, str],
    ) -> None:
        control_type = _text(row.get("control_type"))
        if (
            control_type == "book_invalidated"
            and _text(row.get("reason")) == "reconnect"
        ):
            _reset_venue_order_for_reconnect(
                key,
                self.shard_by_book[(key[1], key[2])],
                self.last_venue_sequence,
                self.last_book_venue_sequence,
                self.last_venue_sid,
            )
        if control_type == "book_recovered":
            expected_event = self.pending_recovery.get(key)
            if expected_event is None:
                raise BookTapeReconstructionError(
                    "book_recovered control has no pending checkpoint for "
                    f"{key[1]}:{key[2]}"
                )
            if _text(row.get("evidence_id")) != expected_event:
                raise BookTapeReconstructionError(
                    "book_recovered control does not reference the pending checkpoint"
                )
            epoch = _text(row.get("epoch_id"))
            if not epoch or self.open_epochs.get(key) != epoch:
                raise BookTapeReconstructionError(
                    "book_recovered control epoch does not match checkpoint"
                )
            self.pending_recovery.pop(key)
            self.epoch_reports[(key[0], key[1], key[2], epoch)][
                "opened_by_control_id"
            ] = _text(row.get("control_id"))
            return
        if control_type not in {"book_invalidated", "stream_ended"}:
            return
        if key in self.pending_recovery:
            raise BookTapeReconstructionError(
                f"{control_type} closes an unconfirmed checkpoint epoch"
            )
        closed = self.open_epochs.pop(key, None)
        declared_epoch = _nullable_text(row.get("epoch_id"))
        if closed is not None and declared_epoch not in {None, closed}:
            raise BookTapeReconstructionError(
                f"{control_type} epoch does not match the open epoch"
            )
        if closed is not None:
            report = self.epoch_reports[(key[0], key[1], key[2], closed)]
            report["closed_by_control_id"] = _text(row.get("control_id"))
            report["last_local_sequence"] = int(row.get("local_sequence") or 0)
        self.native_books.pop(key, None)
        if control_type == "stream_ended":
            self.terminal_books.add(key)

    def _emit_pending(
        self,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if (
            self._pending_row is None
            or self._pending_book is None
            or self._pending_coordinate is None
        ):
            return [], []
        row = self._pending_row
        topbooks, depths = _normalize_native_book(
            row,
            self._pending_book,
            use_yes_price=self.use_yes_price,
            quote_normalization_policy=self.kalshi_quote_policy,
        )
        provenance = {column: row.get(column) for column in _PROVENANCE_COLUMNS}
        metadata = {
            "_reconstruction_event_id": _text(row.get("event_id")),
            "_reconstruction_event_kind": _text(row.get("event_kind")),
            "_reconstruction_checkpoint_reason": _nullable_text(
                row.get("checkpoint_reason")
            ),
            **provenance,
        }
        for item in topbooks:
            item.update(metadata)
        for item in depths:
            item.update(metadata)
        self._pending_owner = None
        self._pending_row = None
        self._pending_book = None
        self._pending_coordinate = None
        self.emitted_message_count += 1
        return topbooks, depths


def _load_streaming_run_authority(path: Path) -> _StreamingRunAuthority:
    manifest_bytes = path.read_bytes()
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    validation = validate_run_manifest(
        path,
        exact_artifact_validation="structure",
    )
    if not validation.ok:
        raise BookTapeReconstructionError(
            f"invalid source manifest: {'; '.join(validation.all_errors)}"
        )
    payload = _read_object(path)
    artifacts = payload.get("dataset_artifacts")
    profile = payload.get("storage_profile")
    if not isinstance(artifacts, Mapping) or not isinstance(profile, Mapping):
        raise BookTapeReconstructionError(
            "reconstruction requires exact dataset_artifacts and storage_profile"
        )
    definition = _validate_profile_compatibility(profile)
    if payload.get("status") != "success":
        raise BookTapeReconstructionError(
            "reconstruction requires a successful clean capture manifest"
        )
    if profile.get("terminal_completeness") != "complete":
        raise BookTapeReconstructionError(
            "reconstruction requires complete terminal profile evidence"
        )
    run_id = _text(payload.get("run_id"))
    if not run_id:
        raise BookTapeReconstructionError("source manifest run_id is required")
    run_dir = _manifest_run_dir(payload, path)
    if run_dir != path.parent:
        raise BookTapeReconstructionError(
            "source manifest must be located in its exact run_dir"
        )
    state_path = run_dir / RUN_STATE_NAME
    try:
        state_bytes = state_path.read_bytes()
        state_payload = json.loads(state_bytes)
        if not isinstance(state_payload, Mapping):
            raise ValueError("run state must be a JSON object")
        state = RunStateV1.from_mapping(state_payload)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise BookTapeReconstructionError(
            f"invalid reconstruction run state: {exc}"
        ) from exc
    if (
        state.status != "finalized"
        or state.run_id != run_id
        or state.profile_name != _text(profile.get("name"))
        or state.profile_version != _text(profile.get("profile_version"))
    ):
        raise BookTapeReconstructionError(
            "run state does not exactly match finalized manifest authority"
        )
    shard_by_book = _shard_by_book(payload, state)
    selected_schemas = {
        **_TAPE_SCHEMAS,
        **{
            role: schema
            for role, schema in _COMPARISON_SCHEMAS.items()
            if role in artifacts
        },
    }
    if "topbook_main" not in selected_schemas:
        raise BookTapeReconstructionError(
            "reconstruction requires committed topbook_main evidence"
        )
    journal_path = resolve_commit_journal_path(run_dir)
    try:
        journal_bytes = journal_path.read_bytes()
        recovery = recover_stream_run(
            run_dir,
            payload_validation="integrity",
            artifact_roles=selected_schemas,
        )
        records = recovery.validated_records
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise BookTapeReconstructionError(
            f"invalid committed reconstruction evidence: {exc}"
        ) from exc
    if recovery.state_status != "finalized":
        raise BookTapeReconstructionError("reconstruction run state is not finalized")
    if recovery.journal_errors:
        raise BookTapeReconstructionError(
            "reconstruction commit journal is invalid: "
            + "; ".join(recovery.journal_errors)
        )
    if recovery.orphan_paths:
        raise BookTapeReconstructionError(
            "reconstruction run contains uncommitted artifacts: "
            + ", ".join(recovery.orphan_paths)
        )
    if recovery.valid_group_count != len(records):
        raise BookTapeReconstructionError(
            "recovery and journal authority disagree on committed groups"
        )
    _, journal_binding_errors = _journal_artifact_run_binding(
        payload,
        artifacts,
        run_dir=run_dir,
        run_id=run_id,
        validated_state=state,
        validated_records=records,
    )
    if journal_binding_errors:
        raise BookTapeReconstructionError(
            "manifest and recovered journal authority disagree: "
            + "; ".join(journal_binding_errors)
        )
    successfully_committed = profile.get("successfully_committed_roles")
    if not isinstance(successfully_committed, list) or not all(
        isinstance(role, str) and role.strip() for role in successfully_committed
    ):
        raise BookTapeReconstructionError(
            "storage profile lacks exact successfully_committed_roles"
        )
    committed_roles = set(successfully_committed)
    artifact_roles = {str(role) for role in artifacts}
    journal_roles = set(recovery.committed_role_counts)
    if committed_roles != artifact_roles or committed_roles != journal_roles:
        raise BookTapeReconstructionError(
            "manifest, profile, and journal committed-role coverage disagree"
        )
    required_tape_roles = set(_TAPE_SCHEMAS)
    if not required_tape_roles <= committed_roles:
        missing = sorted(required_tape_roles - committed_roles)
        raise BookTapeReconstructionError(
            f"reconstruction is missing committed tape roles: {missing}"
        )
    required_definition_roles = {role.value for role in definition.mandatory_roles}
    if not required_definition_roles <= committed_roles:
        missing = sorted(required_definition_roles - committed_roles)
        raise BookTapeReconstructionError(
            f"reconstruction profile is missing committed roles: {missing}"
        )
    journal_by_role: dict[
        str,
        list[tuple[CaptureCommitRecord, Any]],
    ] = {}
    for record in records:
        for artifact in record.artifacts:
            journal_by_role.setdefault(artifact.role, []).append((record, artifact))
    selected_artifact_paths = {
        artifact.path
        for role in selected_schemas
        for _, artifact in journal_by_role.get(role, [])
    }
    validated_artifact_paths = set(recovery.validated_artifact_fingerprints)
    if validated_artifact_paths != selected_artifact_paths:
        missing = sorted(selected_artifact_paths - validated_artifact_paths)
        unexpected = sorted(validated_artifact_paths - selected_artifact_paths)
        raise BookTapeReconstructionError(
            "scoped recovery fingerprint coverage disagrees with reconstruction "
            f"roles: missing={missing[:3]}, unexpected={unexpected[:3]}"
        )
    provenance = tuple(
        {
            "role": role,
            "schema_version": _text(artifacts[role].get("schema_version")),
            "journal_group_id": record.group_id,
            "journal_committed_at_utc": record.committed_at_utc,
            "artifact_path": artifact.path,
            "artifact_sha256": artifact.sha256,
            "row_count": artifact.row_count,
            "first_local_sequence": artifact.first_local_sequence,
            "last_local_sequence": artifact.last_local_sequence,
        }
        for role in sorted(journal_by_role)
        for record, artifact in journal_by_role[role]
    )
    for role, expected_schema in selected_schemas.items():
        entry = artifacts.get(role)
        if not isinstance(entry, Mapping):
            raise BookTapeReconstructionError(
                f"dataset_artifacts.{role} must be an object"
            )
        schema_version = _text(entry.get("schema_version"))
        if schema_version != expected_schema:
            raise BookTapeReconstructionError(
                f"dataset_artifacts.{role} must use {expected_schema}"
            )
        try:
            allowed = definition.role_schema_versions[DatasetRole(role)]
        except (KeyError, ValueError) as exc:
            raise BookTapeReconstructionError(
                f"profile does not declare reconstruction role {role}"
            ) from exc
        if schema_version not in allowed:
            raise BookTapeReconstructionError(
                f"profile does not authorize {role}@{schema_version}"
            )
        role_artifacts = journal_by_role.get(role, [])
        if not role_artifacts:
            raise BookTapeReconstructionError(f"commit journal is missing role {role}")
        committed_count = sum(artifact.row_count for _, artifact in role_artifacts)
        if entry.get("row_count") != committed_count:
            raise BookTapeReconstructionError(
                f"dataset_artifacts.{role} row_count disagrees with journal"
            )
    if int(payload.get("sequence_gap_count") or 0) != 0:
        raise BookTapeReconstructionError(
            "source manifest reports a venue sequence gap"
        )
    authority = _StreamingRunAuthority(
        manifest_path=path,
        manifest_sha256=manifest_sha256,
        manifest_bytes=manifest_bytes,
        payload=payload,
        profile=profile,
        run_dir=run_dir,
        state_path=state_path,
        state_bytes=state_bytes,
        journal_path=journal_path,
        journal_bytes=journal_bytes,
        records=records,
        selected_schemas=selected_schemas,
        artifact_fingerprints=dict(recovery.validated_artifact_fingerprints),
        artifact_provenance=provenance,
        adapter_settings_by_venue={
            str(venue): dict(settings)
            for venue, settings in (state.adapter_settings_by_venue or {}).items()
        },
        shard_by_book=dict(shard_by_book),
        journal_sha256=hashlib.sha256(journal_bytes).hexdigest(),
        tape_encoding_version=definition.tape_encoding_version,
    )
    _verify_streaming_authority(authority, verify_artifacts=False)
    return authority


def _load_streaming_record_frames(
    authority: _StreamingRunAuthority,
    record: CaptureCommitRecord,
    *,
    venue_book_id: str | None,
) -> dict[str, pd.DataFrame]:
    rows_by_role: dict[str, list[dict[str, Any]]] = {
        role: [] for role in authority.selected_schemas
    }
    for artifact in record.artifacts:
        role = artifact.role
        schema_version = authority.selected_schemas.get(role)
        if schema_version is None:
            continue
        # Full-profile depth parity reads the already integrity-validated committed
        # Parquet segments directly in DuckDB. Avoid converting millions of source
        # depth rows through Python merely to write an identical staging copy.
        if role == "depth_main":
            continue
        persisted = _read_validated_artifact_rows(authority, artifact)
        if authority.profile.get("profile_version") == "1":
            _normalize_profile_v1_capture_flags(
                persisted,
                schema_version=schema_version,
            )
        allowed_columns = set(get_table_spec(schema_version).columns)
        for row in persisted:
            unknown_columns = sorted(set(row) - allowed_columns)
            if unknown_columns:
                raise BookTapeReconstructionError(
                    f"committed {role} artifact contains unknown columns: "
                    + ", ".join(unknown_columns)
                )
        if len(persisted) != artifact.row_count:
            raise BookTapeReconstructionError(
                f"committed row count changed while loading {artifact.path}"
            )
        for ordinal, row in enumerate(persisted):
            selected = _filter_committed_role_rows(
                role,
                [row],
                venue_book_id=venue_book_id,
            )
            if not selected:
                continue
            rows_by_role[role].append(
                {
                    **row,
                    "_source_role": role,
                    "_source_schema_version": schema_version,
                    "_source_journal_group_id": record.group_id,
                    "_source_journal_committed_at_utc": (record.committed_at_utc),
                    "_source_artifact_path": artifact.path,
                    "_source_artifact_sha256": artifact.sha256,
                    "_source_artifact_row_count": artifact.row_count,
                    "_source_artifact_first_local_sequence": (
                        artifact.first_local_sequence
                    ),
                    "_source_artifact_last_local_sequence": (
                        artifact.last_local_sequence
                    ),
                    "_source_artifact_row_ordinal": ordinal,
                }
            )
    frames = {
        role: pd.DataFrame(
            rows_by_role[role],
            columns=[
                *get_table_spec(schema_version).columns,
                *_PROVENANCE_COLUMNS,
            ],
        )
        for role, schema_version in authority.selected_schemas.items()
    }
    for role, schema_version in _COMPARISON_SCHEMAS.items():
        if role not in frames:
            frames[role] = pd.DataFrame(
                columns=[
                    *get_table_spec(schema_version).columns,
                    *_PROVENANCE_COLUMNS,
                ]
            )
    events = _schema_frame(frames["tape_event"], _TAPE_SCHEMAS["tape_event"])
    levels = _schema_frame(frames["tape_level"], _TAPE_SCHEMAS["tape_level"])
    controls = _schema_frame(
        frames["tape_control"],
        _TAPE_SCHEMAS["tape_control"],
    )
    bundle_report = validate_book_tape_bundle(
        events,
        levels,
        controls,
        expected_encoding_version=authority.tape_encoding_version,
    )
    if not bundle_report.ok:
        raise BookTapeReconstructionError(
            "invalid committed tape bundle: " + "; ".join(bundle_report.errors)
        )
    control_report = validate_book_control_evidence(
        controls,
        tape_events=events,
        topbook_main=_schema_frame(
            frames["topbook_main"],
            TOPBOOK_SCHEMA_VERSION,
        ),
        topbook_checkpoint=_schema_frame(
            frames["topbook_checkpoint"],
            TOPBOOK_SCHEMA_VERSION,
        ),
    )
    if not control_report.ok:
        raise BookTapeReconstructionError(
            "invalid committed control evidence: " + "; ".join(control_report.errors)
        )
    health = frames["health"]
    if (
        "sequence_gap_count" in health
        and pd.to_numeric(
            health["sequence_gap_count"],
            errors="coerce",
        )
        .fillna(0)
        .gt(0)
        .any()
    ):
        raise BookTapeReconstructionError(
            "reconstruction health evidence reports a venue sequence gap"
        )
    return frames


def _validated_artifact_path(
    authority: _StreamingRunAuthority,
    artifact: Any,
) -> Path:
    relative = Path(artifact.path)
    if relative.is_absolute() or ".." in relative.parts:
        raise BookTapeReconstructionError(
            f"validated artifact path is no longer canonical: {artifact.path}"
        )
    return authority.run_dir.joinpath(*relative.parts)


def _read_validated_artifact_rows(
    authority: _StreamingRunAuthority,
    artifact: Any,
) -> list[dict[str, Any]]:
    path = _validated_artifact_path(authority, artifact)
    if path.suffix.lower() != ".parquet":
        raise BookTapeReconstructionError(
            f"reconstruction artifact is not Parquet: {artifact.path}"
        )
    try:
        frame = pd.read_parquet(path)
    except (OSError, ValueError) as exc:
        raise BookTapeReconstructionError(
            f"artifact became unreadable while loading: {artifact.path}"
        ) from exc
    rows = [
        {
            str(key): normalize_capture_value(value)
            for key, value in row.items()
        }
        for row in frame.to_dict(orient="records")
    ]
    _require_validated_artifact_unchanged(authority, artifact)
    return rows


def _require_validated_artifact_unchanged(
    authority: _StreamingRunAuthority,
    artifact: Any,
) -> None:
    expected = authority.artifact_fingerprints.get(artifact.path)
    if expected is None:
        raise BookTapeReconstructionError(
            f"artifact lacks scoped recovery fingerprint: {artifact.path}"
        )
    path = _validated_artifact_path(authority, artifact)
    try:
        actual = _artifact_stat_fingerprint(path)
    except OSError as exc:
        raise BookTapeReconstructionError(
            f"artifact became unreadable after recovery validation: {artifact.path}"
        ) from exc
    if actual != expected:
        raise BookTapeReconstructionError(
            f"artifact changed after recovery validation: {artifact.path}"
        )


def _verify_streaming_authority(
    authority: _StreamingRunAuthority,
    *,
    verify_artifacts: bool,
) -> None:
    for authority_path, expected_bytes, label in (
        (
            authority.manifest_path,
            authority.manifest_bytes,
            "source manifest",
        ),
        (
            authority.state_path,
            authority.state_bytes,
            "source run state",
        ),
        (
            authority.journal_path,
            authority.journal_bytes,
            "source commit journal",
        ),
    ):
        try:
            current_bytes = authority_path.read_bytes()
        except OSError as exc:
            raise BookTapeReconstructionError(
                f"{label} became unreadable while evidence was loaded: {exc}"
            ) from exc
        if current_bytes != expected_bytes:
            raise BookTapeReconstructionError(
                f"{label} changed while reconstruction evidence was loaded"
            )
    if verify_artifacts:
        for record in authority.records:
            for artifact in record.artifacts:
                if artifact.role not in authority.selected_schemas:
                    continue
                _require_validated_artifact_unchanged(authority, artifact)


def stream_reconstruct_book_tape(
    manifest_path: str | Path,
    *,
    venue_book_id: str | None = None,
    batch_rows: int = DEFAULT_RECONSTRUCTION_BATCH_ROWS,
) -> BookTapeReconstructionStream:
    return BookTapeReconstructionStream(
        manifest_path,
        venue_book_id=venue_book_id,
        batch_rows=batch_rows,
    )


def _journal_group_frame(
    frame: pd.DataFrame,
    group_id: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return frame[frame["_source_journal_group_id"].astype(str).eq(group_id)].copy()


def _public_record_batch(
    rows: list[dict[str, Any]],
    schema_version: str,
) -> pa.RecordBatch:
    schema = arrow_schema(get_table_spec(schema_version))
    table = _rows_to_table(rows, schema)
    batches = table.to_batches(max_chunksize=max(1, len(rows)))
    if batches:
        return batches[0]
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in schema],
        schema=schema,
    )


def _public_record_batch_from_stage(
    table: pa.Table,
    schema_version: str,
) -> pa.RecordBatch:
    schema = arrow_schema(get_table_spec(schema_version))
    selected = table.select(schema.names)
    batches = selected.to_batches(max_chunksize=max(1, len(selected)))
    if batches:
        batch = batches[0]
        if batch.schema == schema:
            return batch
        return pa.RecordBatch.from_arrays(
            [batch.column(index) for index in range(batch.num_columns)],
            schema=schema,
        )
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in schema],
        schema=schema,
    )


def _parity_schema(schema_version: str, *, source: bool) -> pa.Schema:
    schema = arrow_schema(get_table_spec(schema_version))
    metadata = _PROVENANCE_COLUMNS if source else _PARITY_METADATA_COLUMNS
    string_fields = {
        "_source_role",
        "_source_schema_version",
        "_source_journal_group_id",
        "_source_journal_committed_at_utc",
        "_source_artifact_path",
        "_source_artifact_sha256",
        "_reconstruction_event_id",
        "_reconstruction_event_kind",
        "_reconstruction_checkpoint_reason",
    }
    fields = list(schema)
    existing = set(schema.names)
    for name in metadata:
        if name in existing:
            continue
        dtype = pa.string() if name in string_fields else pa.int64()
        fields.append(pa.field(name, dtype, nullable=True))
    return pa.schema(fields)


def _rows_to_table(
    rows: list[dict[str, Any]],
    schema: pa.Schema,
) -> pa.Table:
    # Reconstruction and staged-source rows have already crossed the explicit
    # Python normalization boundary. Let Arrow consume the row batch directly
    # instead of walking every cell once in Python and then again in Arrow.
    return pa.Table.from_pylist(rows, schema=schema)


def _update_batch_hash(
    digest: Any,
    batch: pa.RecordBatch,
) -> None:
    serialized = batch.serialize().to_pybytes()
    digest.update(len(batch).to_bytes(8, byteorder="big", signed=False))
    digest.update(len(serialized).to_bytes(8, byteorder="big", signed=False))
    digest.update(serialized)


def _duckdb_scalar_int(cursor: duckdb.DuckDBPyConnection) -> int:
    row = cursor.fetchone()
    if row is None:
        raise BookTapeReconstructionError("DuckDB parity query returned no scalar row")
    return int(row[0])


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _duckdb_topbook_parity(
    reconstructed_path: Path,
    source_path: Path,
) -> dict[str, Any]:
    fields = (
        "venue_market_id",
        "outcome",
        "best_bid_dollars",
        "best_ask_dollars",
        "bid_size_contracts",
        "ask_size_contracts",
        "best_bid_source",
        "best_ask_source",
        "tick_size_dollars",
        "min_order_size_contracts",
        "valid_state",
        "quality_flags",
    )
    connection = duckdb.connect()
    try:
        # Topbook parity runs first and previously set only the memory limit, so
        # it inherited every-core parallelism, insertion-order buffering, and a
        # CWD-relative spill path. Depth parity already bounds all three; keep
        # both stages on the same connection contract.
        spill_root = source_path.parent / "topbook-spill"
        spill_root.mkdir(exist_ok=True)
        connection.execute(f"SET memory_limit = '{DUCKDB_PARITY_MEMORY_LIMIT}'")
        connection.execute("SET threads = 2")
        connection.execute("SET preserve_insertion_order = false")
        connection.execute(f"SET temp_directory = {sql_literal(spill_root.as_posix())}")
        connection.from_parquet(str(reconstructed_path)).create_view("reconstructed")
        connection.from_parquet(str(source_path)).create_view("source")
        duplicate_count = _duckdb_scalar_int(
            connection.execute(
                """
                SELECT count(*) FROM (
                    SELECT collector_run_id, exchange, instrument_id,
                           received_at_utc
                    FROM source
                    GROUP BY ALL
                    HAVING count(*) > 1
                )
                """
            )
        )
        if duplicate_count:
            raise BookTapeReconstructionError(
                "topbook source comparison keys are not unique"
            )
        reconstructed_duplicate_count = _duckdb_scalar_int(
            connection.execute(
                """
                SELECT count(*) FROM (
                    SELECT collector_run_id, exchange, instrument_id,
                           local_sequence
                    FROM reconstructed
                    GROUP BY ALL
                    HAVING count(*) > 1
                )
                """
            )
        )
        if reconstructed_duplicate_count:
            raise BookTapeReconstructionError(
                "reconstructed topbook causal keys are not unique"
            )
        differences = " OR ".join(
            _duckdb_distinct_expression(f"r.{field}", f"s.{field}", field)
            for field in fields
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE selected_topbook AS
                SELECT
                    s.*,
                    r._reconstruction_event_id,
                    r._source_artifact_path AS reconstruction_artifact_path,
                    r.local_sequence AS reconstructed_local_sequence,
                    ({differences}) AS values_differ
                FROM source s
                ASOF LEFT JOIN reconstructed r
                  ON r.collector_run_id = s.collector_run_id
                 AND r.exchange = s.exchange
                 AND r.instrument_id = s.instrument_id
                 AND r.local_sequence <= s.local_sequence
            """
        )
        missing = _duckdb_scalar_int(
            connection.execute(
                """
                SELECT count(*) FROM selected_topbook
                WHERE reconstructed_local_sequence IS NULL
                """
            )
        )
        mismatched = _duckdb_scalar_int(
            connection.execute(
                """
                SELECT count(*) FROM selected_topbook
                WHERE reconstructed_local_sequence IS NOT NULL
                  AND values_differ
                """
            )
        )
        compared = _duckdb_scalar_int(
            connection.execute(
                """
                SELECT count(*) FROM selected_topbook
                WHERE reconstructed_local_sequence IS NOT NULL
                """
            )
        )
        represented_query = f"""
            WITH ordered AS (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY collector_run_id, exchange, instrument_id
                        ORDER BY local_sequence, received_at_monotonic_ns,
                                 received_at_utc, _reconstruction_event_id
                    ) AS state_index,
                    lag(struct_pack({
            ", ".join(f"{field} := {field}" for field in fields)
        }))
                    OVER (
                        PARTITION BY collector_run_id, exchange, instrument_id
                        ORDER BY local_sequence, received_at_monotonic_ns,
                                 received_at_utc, _reconstruction_event_id
                    ) AS prior_state,
                    lead(local_sequence) OVER (
                        PARTITION BY collector_run_id, exchange, instrument_id
                        ORDER BY local_sequence, received_at_monotonic_ns,
                                 received_at_utc, _reconstruction_event_id
                    ) AS next_sequence
                FROM reconstructed
                WHERE coalesce(valid_state, false)
            ),
            changes AS (
                SELECT * FROM ordered
                WHERE state_index = 1
                   OR prior_state IS DISTINCT FROM
                      struct_pack({
            ", ".join(f"{field} := {field}" for field in fields)
        })
            )
            , represented AS (
            SELECT
                c.*,
                (
                    s.local_sequence IS NOT NULL
                    AND (
                        c.next_sequence IS NULL
                        OR s.local_sequence < c.next_sequence
                    )
                    AND NOT ({
            " OR ".join(
                _duckdb_distinct_expression(f"c.{field}", f"s.{field}", field)
                for field in fields
            )
        })
                ) AS is_represented
            FROM changes c
            ASOF LEFT JOIN source s
              ON s.collector_run_id = c.collector_run_id
             AND s.exchange = c.exchange
             AND s.instrument_id = c.instrument_id
             AND s.local_sequence >= c.local_sequence
            )
            """
        represented_counts = connection.execute(
            represented_query
            + """
            SELECT
                count(*) AS required_changes,
                count(*) FILTER (WHERE NOT is_represented) AS missing_changes
            FROM represented
            """
        ).fetchone()
        if represented_counts is None:
            raise BookTapeReconstructionError(
                "DuckDB topbook change query returned no aggregate row"
            )
        required_changes = int(represented_counts[0])
        missing_changes = int(represented_counts[1])
        mismatch_query = f"""
            SELECT
                CASE
                    WHEN reconstructed_local_sequence IS NULL
                    THEN 'missing_reconstructed_topbook'
                    ELSE 'topbook_value_mismatch'
                END AS kind,
                collector_run_id,
                exchange,
                instrument_id,
                received_at_utc,
                local_sequence,
                _source_artifact_path AS source_artifact_path,
                reconstruction_artifact_path
            FROM selected_topbook
            WHERE reconstructed_local_sequence IS NULL OR values_differ
            LIMIT {MAX_PARITY_MISMATCH_DETAILS}
            """
        mismatch_rows = connection.execute(mismatch_query).fetchdf().to_dict("records")
        if len(mismatch_rows) < MAX_PARITY_MISMATCH_DETAILS and missing_changes:
            remaining = MAX_PARITY_MISMATCH_DETAILS - len(mismatch_rows)
            missing_change_rows = (
                connection.execute(
                    represented_query
                    + f"""
                SELECT
                    'missing_source_topbook_change' AS kind,
                    collector_run_id,
                    exchange,
                    instrument_id,
                    received_at_utc,
                    local_sequence,
                    _reconstruction_event_id AS reconstruction_event_id,
                    _source_artifact_path AS reconstruction_artifact_path
                FROM represented
                WHERE NOT is_represented
                LIMIT {remaining}
                """
                )
                .fetchdf()
                .to_dict("records")
            )
            mismatch_rows.extend(missing_change_rows)
        discrepancy_count = missing + mismatched + missing_changes
        source_count = _duckdb_scalar_int(
            connection.execute("SELECT count(*) FROM source")
        )
        reconstructed_count = _duckdb_scalar_int(
            connection.execute("SELECT count(*) FROM reconstructed")
        )
        return {
            "available": True,
            "status": "match" if discrepancy_count == 0 else "mismatch",
            "excluded_fields": ["book_hash"],
            "source_row_count": source_count,
            "reconstructed_row_count": reconstructed_count,
            "required_state_change_count": required_changes,
            "compared_row_count": compared,
            "mismatch_count": mismatched,
            "missing_reconstructed_row_count": missing,
            "missing_source_change_count": missing_changes,
            "discrepancy_count": discrepancy_count,
            "mismatches": [_normalize_mismatch_row(row) for row in mismatch_rows],
            "mismatch_details_truncated": (discrepancy_count > len(mismatch_rows)),
        }
    finally:
        connection.close()


def _duckdb_depth_parity(
    reconstructed_path: Path,
    source_groups: tuple[tuple[str, Path], ...],
    *,
    journal_group_ids: tuple[str, ...],
    available: bool,
    reconstructed_count: int,
    venue_book_id: str | None,
) -> dict[str, Any]:
    comparison_policy = "state-key-hash-bucketed-duckdb.v1"
    if not available:
        return {
            "available": False,
            "status": "not_available",
            "comparison_policy": comparison_policy,
            "bucket_count": _DEPTH_PARITY_BUCKET_COUNT,

            "excluded_fields": ["book_hash"],
            "source_row_count": 0,
            "reconstructed_row_count": reconstructed_count,
            "compared_row_count": 0,
            "periodic_checkpoint_row_count": 0,
            "periodic_checkpoint_compared_row_count": 0,
            "mismatch_count": 0,
            "missing_reconstructed_row_count": 0,
            "missing_source_row_count": 0,
            "discrepancy_count": 0,
            "mismatches": [],
            "mismatch_details_truncated": False,
            "excluded_invalid_source_row_count": 0,
        }
    if not source_groups:
        raise BookTapeReconstructionError(
            "depth comparison is declared available but has no committed artifacts"
        )
    if len(set(journal_group_ids)) != len(journal_group_ids):
        raise BookTapeReconstructionError(
            "journal group identifiers are not unique for depth parity"
        )
    group_positions = {
        group_id: ordinal for ordinal, group_id in enumerate(journal_group_ids)
    }
    source_map_rows: list[dict[str, Any]] = []
    for artifact_ordinal, (group_id, path) in enumerate(source_groups):
        group_ordinal = group_positions.get(group_id)
        if group_ordinal is None:
            raise BookTapeReconstructionError(
                f"depth artifact references unknown journal group {group_id!r}"
            )
        source_map_rows.append(
            {
                "group_id": group_id,
                "source_artifact_path": str(path),
                "source_artifact_ordinal": artifact_ordinal,
                "group_ordinal": group_ordinal,
                "group_bucket": (
                    group_ordinal // _DEPTH_PARITY_BUCKET_COUNT
                ),
            }
        )
    if len({row["source_artifact_path"] for row in source_map_rows}) != len(
        source_map_rows
    ):
        raise BookTapeReconstructionError(
            "depth parity source artifact paths are not unique"
        )
    journal_map_rows = [
        {
            "group_id": group_id,
            "group_ordinal": ordinal,
            "group_bucket": ordinal // _DEPTH_PARITY_BUCKET_COUNT,
        }
        for ordinal, group_id in enumerate(journal_group_ids)
    ]
    keys = (
        "collector_run_id",
        "exchange",
        "instrument_id",
        "local_sequence",
        "venue_sequence",
        "venue_sid",
        "side",
        "level_index",
    )
    fields = (
        "venue_market_id",
        "outcome",
        "price_dollars",
        "size_contracts",
        "cumulative_size_contracts",
        "is_delta",
        "valid_state",
        "quality_flags",
    )

    parity_bucket_fields = (
        "collector_run_id",
        "exchange",
        "instrument_id",
        "side",
        "level_index",
    )
    parity_bucket_expression = (
        "(hash("
        + ", ".join(parity_bucket_fields)
        + f") % {_DEPTH_PARITY_BUCKET_COUNT})::BIGINT"
    )

    parity_started_at = time.perf_counter()
    parity_stage = tempfile.TemporaryDirectory(prefix="pmkt-depth-parity-")
    stage_root = Path(parity_stage.name)
    connection = duckdb.connect()
    try:
        spill_root = stage_root / "spill"
        spill_root.mkdir()
        connection.execute(f"SET memory_limit = '{DUCKDB_PARITY_MEMORY_LIMIT}'")
        connection.execute("SET threads = 2")
        connection.execute("SET preserve_insertion_order = false")
        connection.execute("SET partitioned_write_max_open_files = 4")
        connection.execute(
            f"SET temp_directory = {sql_literal(spill_root.as_posix())}"
        )
        connection.register(
            "_journal_group_map_arrow",
            pa.Table.from_pylist(journal_map_rows),
        )
        connection.execute(
            "CREATE TEMP TABLE journal_group_map AS "
            "SELECT * FROM _journal_group_map_arrow"
        )
        connection.unregister("_journal_group_map_arrow")
        connection.register(
            "_depth_source_map_arrow",
            pa.Table.from_pylist(source_map_rows),
        )
        connection.execute(
            "CREATE TEMP TABLE depth_source_map AS "
            "SELECT * FROM _depth_source_map_arrow"
        )
        connection.unregister("_depth_source_map_arrow")
        connection.from_parquet(
            str(reconstructed_path),
            filename=True,
            file_row_number=True,
        ).create_view("reconstructed_artifacts")
        source_paths = [str(path) for _, path in source_groups]
        connection.from_parquet(
            source_paths,
            filename=True,
            file_row_number=True,
        ).create_view("source_artifacts")
        selection = "true"
        if venue_book_id is not None:
            literal = sql_literal(venue_book_id)
            selection = (
                f"(s.instrument_id = {literal} OR "
                f"starts_with(s.instrument_id, {literal} || ':'))"
            )
        connection.execute(
            f"""
            CREATE TEMP VIEW selected_source_artifacts AS
                SELECT
                    s.* EXCLUDE (filename, file_row_number),
                    s.filename AS _source_artifact_path,
                    s.file_row_number AS _source_file_row_number,
                    m.group_id AS _source_journal_group_id,
                    m.source_artifact_ordinal,
                    m.group_ordinal,
                    m.group_bucket
                FROM source_artifacts s
                INNER JOIN depth_source_map m
                  ON s.filename = m.source_artifact_path
                WHERE {selection}
            """
        )
        connection.execute(
            """
            CREATE TEMP VIEW valid_source AS
                SELECT * FROM selected_source_artifacts
                WHERE coalesce(valid_state, false)
            """
        )
        connection.execute(
            f"""
            CREATE TEMP VIEW source AS
                SELECT
                    * EXCLUDE (_source_file_row_number, source_artifact_ordinal),
                    {parity_bucket_expression} AS _parity_bucket
                FROM valid_source
            """
        )
        unmapped_reconstructed = _duckdb_scalar_int(
            connection.execute(
                """
                SELECT count(*)
                FROM reconstructed_artifacts r
                LEFT JOIN journal_group_map m
                  ON r._source_journal_group_id = m.group_id
                WHERE m.group_id IS NULL
                """
            )
        )
        if unmapped_reconstructed:
            raise BookTapeReconstructionError(
                "reconstructed depth rows reference an unknown journal group"
            )
        connection.execute(
            f"""
            CREATE TEMP VIEW reconstructed_exact AS
                SELECT
                    r.* EXCLUDE (filename, file_row_number),
                    m.group_ordinal,
                    m.group_bucket,
                    {parity_bucket_expression} AS _parity_bucket
                FROM reconstructed_artifacts r
                INNER JOIN journal_group_map m
                  ON r._source_journal_group_id = m.group_id
                WHERE NOT (
                    r._reconstruction_event_kind = 'checkpoint'
                    AND r._reconstruction_checkpoint_reason = 'periodic'
                )
            """
        )
        connection.execute(
            f"""
            CREATE TEMP VIEW reconstructed_periodic AS
                SELECT
                    r.* EXCLUDE (filename, file_row_number),
                    {parity_bucket_expression} AS _parity_bucket
                FROM reconstructed_artifacts r
                WHERE r._reconstruction_event_kind = 'checkpoint'
                  AND r._reconstruction_checkpoint_reason = 'periodic'
            """
        )
        source_counts = connection.execute(
            """
            SELECT
                count(*) AS selected_rows,
                count(*) FILTER (WHERE coalesce(valid_state, false)) AS valid_rows
            FROM selected_source_artifacts
            """
        ).fetchone()
        if source_counts is None:
            raise BookTapeReconstructionError(
                "DuckDB source depth count query returned no aggregate row"
            )
        selected_source_count = int(source_counts[0])
        source_count = int(source_counts[1])
        excluded_invalid_source_count = selected_source_count - source_count

        source_stage_columns = (
            "_parity_bucket",
            "_source_journal_group_id",
            *keys,
            *fields,
            "_source_artifact_path",
        )
        reconstructed_stage_columns = source_stage_columns
        source_stage_root = stage_root / "source_exact"
        reconstructed_stage_root = stage_root / "reconstructed_exact"
        periodic_stage_root = stage_root / "reconstructed_periodic"
        staging_started_at = time.perf_counter()
        connection.execute(
            f"""
            COPY (
                SELECT {", ".join(source_stage_columns)} FROM source
            ) TO {sql_literal(source_stage_root.as_posix())} (
                FORMAT PARQUET,
                PARTITION_BY (_parity_bucket),
                COMPRESSION SNAPPY,
                ROW_GROUP_SIZE 100000
            )
            """
        )
        connection.execute(
            f"""
            COPY (
                SELECT {", ".join(reconstructed_stage_columns)}
                FROM reconstructed_exact
            ) TO {sql_literal(reconstructed_stage_root.as_posix())} (
                FORMAT PARQUET,
                PARTITION_BY (_parity_bucket),
                COMPRESSION SNAPPY,
                ROW_GROUP_SIZE 100000
            )
            """
        )
        connection.execute(
            f"""
            COPY (
                SELECT {", ".join(reconstructed_stage_columns)}
                FROM reconstructed_periodic
            ) TO {sql_literal(periodic_stage_root.as_posix())} (
                FORMAT PARQUET,
                PARTITION_BY (_parity_bucket),
                COMPRESSION SNAPPY,
                ROW_GROUP_SIZE 100000
            )
            """
        )
        staging_completed_at = time.perf_counter()

        join_keys = keys
        join_condition = " AND ".join(
            f"r.{key} IS NOT DISTINCT FROM s.{key}" for key in join_keys
        )
        differences = " OR ".join(
            _duckdb_distinct_expression(f"r.{field}", f"s.{field}", field)
            for field in fields
        )
        periodic_join_condition = " AND ".join(
            (
                "s.collector_run_id = p.collector_run_id",
                "s.exchange = p.exchange",
                "s.instrument_id = p.instrument_id",
                "s.side = p.side",
                "s.level_index = p.level_index",
                "s.local_sequence <= p.local_sequence",
            )
        )
        periodic_differences = " OR ".join(
            _duckdb_distinct_expression(f"p.{field}", f"s.{field}", field)
            for field in ("venue_sequence", "venue_sid", *fields)
        )
        source_projection = ", ".join(source_stage_columns[1:])
        reconstructed_projection = ", ".join(reconstructed_stage_columns[1:])
        missing_reconstructed = 0
        mismatched = 0
        unmatched_reconstructed = 0
        exact_compared = 0
        periodic_count = 0
        periodic_missing = 0
        periodic_mismatch = 0
        mismatch_rows: list[dict[str, Any]] = []
        bucket_count = _DEPTH_PARITY_BUCKET_COUNT
        bucket_comparison_started_at = time.perf_counter()
        for bucket in range(bucket_count):
            source_bucket = source_stage_root / f"_parity_bucket={bucket}"
            reconstructed_bucket = (
                reconstructed_stage_root / f"_parity_bucket={bucket}"
            )
            periodic_bucket = periodic_stage_root / f"_parity_bucket={bucket}"
            source_relation = (
                f"read_parquet({sql_literal((source_bucket / '*.parquet').as_posix())})"
                if source_bucket.exists()
                else f"(SELECT {source_projection} FROM source WHERE false)"
            )
            reconstructed_relation = (
                "read_parquet("
                + sql_literal((reconstructed_bucket / "*.parquet").as_posix())
                + ")"
                if reconstructed_bucket.exists()
                else (
                    f"(SELECT {reconstructed_projection} "
                    "FROM reconstructed_exact WHERE false)"
                )
            )
            periodic_relation = (
                "read_parquet("
                + sql_literal((periodic_bucket / "*.parquet").as_posix())
                + ")"
                if periodic_bucket.exists()
                else (
                    f"(SELECT {reconstructed_projection} "
                    "FROM reconstructed_periodic WHERE false)"
                )
            )
            if (
                not source_bucket.exists()
                and not reconstructed_bucket.exists()
                and not periodic_bucket.exists()
            ):
                continue
            for relation, label in (
                (source_relation, "source depth"),
                (reconstructed_relation, "reconstructed depth"),
            ):
                duplicate_count = _duckdb_scalar_int(
                    connection.execute(
                        f"""
                        SELECT count(*) FROM (
                            SELECT {", ".join(join_keys)}
                            FROM {relation}
                            GROUP BY ALL
                            HAVING count(*) > 1
                        )
                        """
                    )
                )
                if duplicate_count:
                    raise BookTapeReconstructionError(
                        f"{label} comparison keys are not unique"
                    )
            exact = f"""
                WITH exact AS (
                    SELECT
                        s.collector_run_id AS source_run_id,
                        r.collector_run_id AS reconstructed_run_id,
                        s._source_journal_group_id AS source_journal_group_id,
                        r._source_journal_group_id
                            AS reconstruction_journal_group_id,
                        coalesce(s.collector_run_id, r.collector_run_id)
                            AS collector_run_id,
                        coalesce(s.exchange, r.exchange) AS exchange,
                        coalesce(s.instrument_id, r.instrument_id) AS instrument_id,
                        coalesce(s.local_sequence, r.local_sequence)
                            AS local_sequence,
                        coalesce(s.venue_sequence, r.venue_sequence)
                            AS venue_sequence,
                        coalesce(s.side, r.side) AS side,
                        coalesce(s.level_index, r.level_index) AS level_index,

                        s._source_artifact_path AS source_artifact_path,
                        r._source_artifact_path AS reconstruction_artifact_path,
                        ({differences}) AS values_differ
                    FROM {source_relation} s
                    FULL OUTER JOIN {reconstructed_relation} r
                      ON {join_condition}
                )
            """
            exact_counts = connection.execute(
                exact
                + """
                SELECT
                    count(*) FILTER (WHERE reconstructed_run_id IS NULL),
                    count(*) FILTER (
                        WHERE source_run_id IS NOT NULL
                          AND reconstructed_run_id IS NOT NULL
                          AND values_differ
                    ),
                    count(*) FILTER (
                        WHERE source_run_id IS NULL
                          AND reconstructed_run_id IS NOT NULL
                    ),
                    count(*) FILTER (
                        WHERE source_run_id IS NOT NULL
                          AND reconstructed_run_id IS NOT NULL
                    )
                FROM exact
                """
            ).fetchone()
            if exact_counts is None:
                raise BookTapeReconstructionError(
                    "DuckDB exact depth query returned no aggregate row"
                )
            bucket_missing_reconstructed = int(exact_counts[0])
            bucket_mismatched = int(exact_counts[1])
            bucket_unmatched_reconstructed = int(exact_counts[2])
            missing_reconstructed += bucket_missing_reconstructed
            mismatched += bucket_mismatched
            unmatched_reconstructed += bucket_unmatched_reconstructed
            exact_compared += int(exact_counts[3])
            bucket_discrepancies = (
                bucket_missing_reconstructed
                + bucket_mismatched
                + bucket_unmatched_reconstructed
            )
            remaining = MAX_PARITY_MISMATCH_DETAILS - len(mismatch_rows)
            if bucket_discrepancies and remaining:
                mismatch_rows.extend(
                    connection.execute(
                        exact
                        + f"""
                        SELECT
                            CASE
                                WHEN reconstructed_run_id IS NULL
                                THEN 'missing_reconstructed_depth'
                                WHEN source_run_id IS NULL
                                THEN 'missing_source_depth'
                                ELSE 'depth_value_mismatch'
                            END AS kind,
                            source_journal_group_id,
                            reconstruction_journal_group_id,
                            collector_run_id,
                            exchange,
                            instrument_id,
                            local_sequence,
                            venue_sequence,
                            side,
                            level_index,

                            source_artifact_path,
                            reconstruction_artifact_path
                        FROM exact
                        WHERE reconstructed_run_id IS NULL
                           OR source_run_id IS NULL
                           OR values_differ
                        ORDER BY
                            local_sequence,
                            instrument_id,
                            side,
                            level_index
                        LIMIT {remaining}
                        """
                    )
                    .fetchdf()
                    .to_dict("records")
                )

            periodic_query = f"""
                WITH selected AS (
                    SELECT
                        p.*,
                        s.collector_run_id AS source_run_id,
                        s.local_sequence AS source_local_sequence,
                        s._source_artifact_path AS source_artifact_path,
                        ({periodic_differences}) AS values_differ
                    FROM {periodic_relation} p
                    ASOF LEFT JOIN {source_relation} s
                      ON {periodic_join_condition}
                )
            """
            periodic_counts = connection.execute(
                periodic_query
                + """
                SELECT
                    count(*),
                    count(*) FILTER (WHERE source_run_id IS NULL),
                    count(*) FILTER (
                        WHERE source_run_id IS NOT NULL AND values_differ
                    )
                FROM selected
                """
            ).fetchone()
            if periodic_counts is None:
                raise BookTapeReconstructionError(
                    "DuckDB periodic depth query returned no aggregate row"
                )
            bucket_periodic_count = int(periodic_counts[0])
            bucket_periodic_missing = int(periodic_counts[1])
            bucket_periodic_mismatch = int(periodic_counts[2])
            periodic_count += bucket_periodic_count
            periodic_missing += bucket_periodic_missing
            periodic_mismatch += bucket_periodic_mismatch
            remaining = MAX_PARITY_MISMATCH_DETAILS - len(mismatch_rows)
            if remaining and (
                bucket_periodic_missing or bucket_periodic_mismatch
            ):
                mismatch_rows.extend(
                    connection.execute(
                        periodic_query
                        + f"""
                        SELECT
                            CASE
                                WHEN source_run_id IS NULL
                                THEN 'missing_periodic_source_depth'
                                ELSE 'periodic_depth_value_mismatch'
                            END AS kind,
                            collector_run_id,
                            exchange,
                            instrument_id,
                            local_sequence,
                            side,
                            level_index,
                            source_artifact_path,
                            _source_artifact_path
                                AS reconstruction_artifact_path
                        FROM selected
                        WHERE source_run_id IS NULL OR values_differ
                        ORDER BY local_sequence, instrument_id, side, level_index
                        LIMIT {remaining}
                        """
                    )
                    .fetchdf()
                    .to_dict("records")
                )

        bucket_comparison_completed_at = time.perf_counter()
        periodic_compared = periodic_count - periodic_missing
        compared = exact_compared + periodic_compared
        discrepancy_count = (
            missing_reconstructed
            + mismatched
            + unmatched_reconstructed
            + periodic_missing
            + periodic_mismatch
        )
        return {
            "available": True,
            "status": "match" if discrepancy_count == 0 else "mismatch",
            "comparison_policy": comparison_policy,
            "bucket_count": _DEPTH_PARITY_BUCKET_COUNT,
            "phase_seconds": {
                "setup": staging_started_at - parity_started_at,
                "staging": staging_completed_at - staging_started_at,
                "bucketed_comparison": (
                    bucket_comparison_completed_at - bucket_comparison_started_at
                ),
                "total": time.perf_counter() - parity_started_at,
            },
            "excluded_fields": ["book_hash"],
            "source_row_count": source_count,
            "reconstructed_row_count": reconstructed_count,
            "compared_row_count": compared,
            "periodic_checkpoint_row_count": periodic_count,
            "periodic_checkpoint_compared_row_count": periodic_compared,
            "mismatch_count": mismatched + periodic_mismatch,
            "missing_reconstructed_row_count": missing_reconstructed,
            "missing_source_row_count": unmatched_reconstructed + periodic_missing,
            "discrepancy_count": discrepancy_count,
            "mismatches": [_normalize_mismatch_row(row) for row in mismatch_rows],
            "mismatch_details_truncated": discrepancy_count > len(mismatch_rows),
            "excluded_invalid_source_row_count": excluded_invalid_source_count,
        }
    finally:
        connection.close()
        parity_stage.cleanup()

def _duckdb_distinct_expression(
    left: str,
    right: str,
    field: str,
) -> str:
    if field in {
        "best_bid_dollars",
        "best_ask_dollars",
        "bid_size_contracts",
        "ask_size_contracts",
        "tick_size_dollars",
        "min_order_size_contracts",
        "price_dollars",
        "size_contracts",
        "cumulative_size_contracts",
    }:
        return (
            f"(({left} IS NULL) <> ({right} IS NULL) OR "
            f"({left} IS NOT NULL AND {right} IS NOT NULL "
            f"AND abs({left} - {right}) > 1e-12))"
        )
    return f"({left} IS DISTINCT FROM {right})"


def _normalize_mismatch_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): normalize_capture_value(value) for key, value in row.items()}


__all__ = [
    "BookTapeArrowBatch",
    "BookTapeReconstructionStream",
    "DEFAULT_RECONSTRUCTION_BATCH_ROWS",
    "MAX_RECONSTRUCTION_MATERIALIZED_ROWS",
    "stream_reconstruct_book_tape",
]
