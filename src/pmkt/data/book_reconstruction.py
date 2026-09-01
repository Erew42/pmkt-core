from __future__ import annotations

import hashlib
import json
import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from pmkt.data.manifests import validate_run_manifest
from pmkt.data.kalshi_quotes import (
    project_kalshi_quotes,
    resolve_kalshi_quote_normalization_policy,
    resolve_kalshi_use_yes_price,
)
from pmkt.data.normalize_books import (
    kalshi_ws_snapshot_to_topbook,
    polymarket_ws_snapshot_to_topbook,
)
from pmkt.data.registry import DEPTH_COLUMNS, TOPBOOK_COLUMNS, get_table_spec
from pmkt.data.schemas import depth_row
from pmkt.data.validation import (
    validate_book_control_evidence,
    validate_book_tape_bundle,
)

from pmkt.streaming.durability import (
    RUN_STATE_NAME,
    file_sha256,
    read_committed_capture_rows,
)
from pmkt.streaming.profiles import (
    DatasetRole,
    StorageProfileDefinition,
    get_storage_profile_definition,
)
from pmkt.streaming.recovery import (
    recover_stream_run,
    resolve_commit_journal_path,
    validate_commit_journal,
)
from pmkt.streaming.recovery_contracts import (
    CaptureCommitArtifactV1,
    CaptureCommitRecord,
    RunStateV1,
    resolve_run_relative_path,
)
from pmkt.streaming.tape import (
    NativeBookLevel,
    canonical_decimal,
    post_book_hash,
    semantic_hash,
)
from pmkt.streaming.topbook_emission import topbook_state_fingerprint

RECONSTRUCTION_REPORT_VERSION = "book_tape_reconstruction_report.v1"
_TAPE_SCHEMAS = {
    "tape_event": "book_tape_event.v1",
    "tape_level": "book_tape_level.v1",
    "tape_control": "book_tape_control.v1",
}
_COMPARISON_SCHEMAS = {
    "topbook_main": "topbook.v1",
    "topbook_checkpoint": "topbook.v1",
    "depth_main": "depth.v1",
    "health": "feed_health.v1",
}
_PROVENANCE_COLUMNS = (
    "_source_role",
    "_source_schema_version",
    "_source_journal_group_id",
    "_source_journal_committed_at_utc",
    "_source_artifact_path",
    "_source_artifact_sha256",
    "_source_artifact_row_count",
    "_source_artifact_first_local_sequence",
    "_source_artifact_last_local_sequence",
    "_source_artifact_row_ordinal",
)
_RECONSTRUCTION_COLUMNS = (
    "_reconstruction_event_id",
    "_reconstruction_event_kind",
    "_reconstruction_checkpoint_reason",
    "_reconstruction_causal_key",
    *_PROVENANCE_COLUMNS,
)


class BookTapeReconstructionError(ValueError):
    pass


def _kalshi_quote_policy(adapter_settings: Mapping[str, Any]) -> str:
    try:
        return resolve_kalshi_quote_normalization_policy(
            adapter_settings.get("quote_normalization_policy")
        )
    except ValueError as exc:
        raise BookTapeReconstructionError(str(exc)) from exc


def _kalshi_use_yes_price(adapter_settings: Mapping[str, Any]) -> bool:
    try:
        return resolve_kalshi_use_yes_price(adapter_settings)
    except ValueError as exc:
        raise BookTapeReconstructionError(str(exc)) from exc


@dataclass(frozen=True)
class BookTapeReconstructionResult:
    topbooks: pd.DataFrame
    depths: pd.DataFrame
    report: Mapping[str, Any]


@dataclass(frozen=True)
class _CommittedRunEvidence:
    manifest_path: Path
    manifest_sha256: str
    payload: Mapping[str, Any]
    profile: Mapping[str, Any]
    run_dir: Path
    journal_path: Path
    records: tuple[CaptureCommitRecord, ...]
    frames: Mapping[str, pd.DataFrame]
    artifact_provenance: tuple[Mapping[str, Any], ...]
    adapter_settings_by_venue: Mapping[str, Mapping[str, Any]]
    shard_by_book: Mapping[tuple[str, str], str]
    journal_sha256: str


def reconstruct_book_tape(
    manifest_path: str | Path,
    *,
    venue_book_id: str | None = None,
) -> BookTapeReconstructionResult:
    """Materialize a bounded reconstruction for tests and small callers.

    Larger captures must use ``stream_reconstruct_book_tape`` so output rows
    are never accumulated in one process-wide DataFrame.
    """
    import pyarrow as pa

    from pmkt.data.book_reconstruction_streaming import (
        MAX_RECONSTRUCTION_MATERIALIZED_ROWS,
        stream_reconstruct_book_tape,
    )

    stream = stream_reconstruct_book_tape(
        manifest_path,
        venue_book_id=venue_book_id,
    )
    topbook_batches: list[pa.RecordBatch] = []
    depth_batches: list[pa.RecordBatch] = []
    materialized_rows = 0
    try:
        for batch in stream:
            materialized_rows += len(batch.topbooks) + len(batch.depths)
            if materialized_rows > MAX_RECONSTRUCTION_MATERIALIZED_ROWS:
                raise BookTapeReconstructionError(
                    "reconstruction exceeds the 250,000-row materialization "
                    "limit; use stream_reconstruct_book_tape"
                )
            if len(batch.topbooks):
                topbook_batches.append(batch.topbooks)
            if len(batch.depths):
                depth_batches.append(batch.depths)
        topbooks = (
            pa.Table.from_batches(topbook_batches).to_pandas()
            if topbook_batches
            else pd.DataFrame(columns=TOPBOOK_COLUMNS)
        )
        depths = (
            pa.Table.from_batches(depth_batches).to_pandas()
            if depth_batches
            else pd.DataFrame(columns=DEPTH_COLUMNS)
        )
        return BookTapeReconstructionResult(
            topbooks.loc[:, TOPBOOK_COLUMNS].reset_index(drop=True),
            depths.loc[:, DEPTH_COLUMNS].reset_index(drop=True),
            stream.report,
        )
    finally:
        stream.close()


def _reconstruct_book_tape_legacy(
    manifest_path: str | Path,
    *,
    venue_book_id: str | None = None,
) -> BookTapeReconstructionResult:
    requested_book: str | None = None
    if venue_book_id is not None:
        requested_book = venue_book_id.strip()
        if not requested_book:
            raise BookTapeReconstructionError("venue_book_id must be non-empty")
    evidence = _load_committed_run_evidence(
        Path(manifest_path).resolve(),
        venue_book_id=requested_book,
    )
    payload = evidence.payload
    events = evidence.frames["tape_event"].copy()
    levels = evidence.frames["tape_level"].copy()
    controls = evidence.frames["tape_control"].copy()
    source_topbook_frames = [
        frame
        for frame in (
            evidence.frames["topbook_main"],
            evidence.frames["topbook_checkpoint"],
        )
        if not frame.empty
    ]
    source_topbooks = (
        pd.concat(source_topbook_frames, ignore_index=True)
        if source_topbook_frames
        else evidence.frames["topbook_main"].iloc[0:0].copy()
    )
    source_depths = evidence.frames["depth_main"].copy()
    if requested_book is not None:
        if not events["venue_book_id"].astype(str).eq(requested_book).any():
            raise BookTapeReconstructionError(
                f"no committed tape events for venue book {requested_book!r}"
            )

    shard_by_book = evidence.shard_by_book
    kalshi_adapter_settings = evidence.adapter_settings_by_venue.get("kalshi", {})
    use_yes_price = _kalshi_use_yes_price(kalshi_adapter_settings)
    kalshi_quote_policy = _kalshi_quote_policy(kalshi_adapter_settings)
    reconstructed_topbooks: list[dict[str, Any]] = []
    reconstructed_depths: list[dict[str, Any]] = []
    ignored: list[dict[str, Any]] = []
    open_epochs: dict[tuple[str, str, str], str] = {}
    pending_recovery: dict[tuple[str, str, str], str] = {}
    terminal_books: set[tuple[str, str, str]] = set()
    native_books: dict[
        tuple[str, str, str],
        dict[tuple[str, str], float],
    ] = {}
    last_venue_sequence: dict[tuple[str, str, str], int] = {}
    last_book_venue_sequence: dict[tuple[str, str, str], int] = {}
    last_venue_sid: dict[tuple[str, str, str], str] = {}
    last_book_coordinate: dict[
        tuple[str, str, str],
        tuple[pd.Timestamp, int, int, int, str],
    ] = {}
    epoch_reports: dict[tuple[str, str, str, str], dict[str, Any]] = {}
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
    for item_index, (family, row, coordinate) in enumerate(causal_items):
        key = (
            _text(row.get("collector_run_id")),
            _text(row.get("venue")),
            _text(row.get("venue_book_id")),
        )
        if not all(key):
            raise BookTapeReconstructionError(
                f"{family} evidence lacks exact run/venue/book ownership"
            )
        if (key[1], key[2]) not in shard_by_book:
            raise BookTapeReconstructionError(
                f"no exact shard mapping for {key[1]}:{key[2]}"
            )
        previous = last_book_coordinate.get(key)
        if previous is not None and coordinate <= previous:
            raise BookTapeReconstructionError(
                f"non-continuous {family} coordinate for {key[1]}:{key[2]}"
            )
        if key in terminal_books:
            raise BookTapeReconstructionError(
                f"{family} evidence occurs after terminal boundary for "
                f"{key[1]}:{key[2]}"
            )
        last_book_coordinate[key] = coordinate

        if family == "control":
            control_type = _text(row.get("control_type"))
            if (
                control_type == "book_invalidated"
                and _text(row.get("reason")) == "reconnect"
            ):
                _reset_venue_order_for_reconnect(
                    key,
                    shard_by_book[(key[1], key[2])],
                    last_venue_sequence,
                    last_book_venue_sequence,
                    last_venue_sid,
                )
            if control_type == "book_recovered":
                expected_event = pending_recovery.get(key)
                if expected_event is None:
                    raise BookTapeReconstructionError(
                        f"book_recovered control has no pending checkpoint for "
                        f"{key[1]}:{key[2]}"
                    )
                if _text(row.get("evidence_id")) != expected_event:
                    raise BookTapeReconstructionError(
                        "book_recovered control does not reference the pending "
                        "checkpoint"
                    )
                epoch = _text(row.get("epoch_id"))
                if not epoch or open_epochs.get(key) != epoch:
                    raise BookTapeReconstructionError(
                        "book_recovered control epoch does not match checkpoint"
                    )
                pending_recovery.pop(key)
                epoch_reports[(key[0], key[1], key[2], epoch)][
                    "opened_by_control_id"
                ] = _text(row.get("control_id"))
                continue
            if control_type not in {"book_invalidated", "stream_ended"}:
                continue
            if key in pending_recovery:
                raise BookTapeReconstructionError(
                    f"{control_type} closes an unconfirmed checkpoint epoch"
                )
            closed = open_epochs.pop(key, None)
            declared_epoch = _nullable_text(row.get("epoch_id"))
            if closed is not None and declared_epoch not in {None, closed}:
                raise BookTapeReconstructionError(
                    f"{control_type} epoch does not match the open epoch"
                )
            if closed is not None:
                epoch_report = epoch_reports[(key[0], key[1], key[2], closed)]
                epoch_report["closed_by_control_id"] = _text(row.get("control_id"))
                epoch_report["last_local_sequence"] = int(
                    row.get("local_sequence") or 0
                )
            native_books.pop(key, None)
            if control_type == "stream_ended":
                terminal_books.add(key)
            continue

        event_id = _text(row.get("event_id"))
        event_levels = levels_by_event.get(
            (key[0], event_id),
            levels.iloc[0:0],
        )
        _validate_venue_order(
            row,
            key,
            shard_by_book[(key[1], key[2])],
            last_venue_sequence,
            last_book_venue_sequence,
            last_venue_sid,
        )
        kind = _text(row.get("event_kind"))
        reconstructible = _bool(row.get("reconstructible"))
        epoch = _text(row.get("epoch_id"))
        if kind == "checkpoint":
            if not reconstructible:
                if key in pending_recovery or key in open_epochs:
                    raise BookTapeReconstructionError(
                        f"non-reconstructible checkpoint {event_id} overlaps an "
                        "open epoch"
                    )
                ignored.append(
                    {
                        "event_id": event_id,
                        "venue": key[1],
                        "venue_book_id": key[2],
                        "reason": "non_reconstructible_checkpoint_outside_epoch",
                        "source_provenance": _source_provenance(row),
                    }
                )
                continue
            if not epoch:
                raise BookTapeReconstructionError(
                    f"checkpoint {event_id} must open a reconstructible epoch"
                )
            if key in pending_recovery:
                raise BookTapeReconstructionError(
                    f"checkpoint {event_id} precedes recovery of the prior checkpoint"
                )
            prior_epoch = open_epochs.get(key)
            if prior_epoch == epoch:
                raise BookTapeReconstructionError(
                    f"checkpoint {event_id} reuses the open epoch"
                )
            if prior_epoch is not None:
                prior_report = epoch_reports[(key[0], key[1], key[2], prior_epoch)]
                prior_report["closed_by_checkpoint_event_id"] = event_id
                prior_report["closed_at_local_sequence"] = int(
                    row.get("local_sequence") or 0
                )
            book = _checkpoint_book(row, event_levels)
            native_books[key] = book
            open_epochs[key] = epoch
            pending_recovery[key] = event_id
            epoch_reports[(key[0], key[1], key[2], epoch)] = {
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
            if epoch or key in open_epochs:
                raise BookTapeReconstructionError(
                    f"non-reconstructible event {event_id} overlaps an open epoch"
                )
            ignored.append(
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
            if key in pending_recovery:
                raise BookTapeReconstructionError(
                    f"delta {event_id} precedes its recovery control"
                )
            if not epoch or open_epochs.get(key) != epoch or key not in native_books:
                raise BookTapeReconstructionError(
                    f"reconstructible delta {event_id} has no matching open epoch"
                )
            _apply_absolute_delta(native_books[key], event_levels)

        _validate_reconstructed_post_book_hash(
            row,
            native_books[key],
            adapter_settings=evidence.adapter_settings_by_venue.get(
                key[1],
                {},
            ),
        )
        epoch_report = epoch_reports[(key[0], key[1], key[2], epoch)]
        epoch_report["last_local_sequence"] = int(row.get("local_sequence") or 0)
        epoch_report["applied_event_count"] = (
            int(epoch_report["applied_event_count"]) + 1
        )
        next_item = (
            causal_items[item_index + 1] if item_index + 1 < len(causal_items) else None
        )
        if (
            next_item is not None
            and next_item[0] == "event"
            and _source_message_ownership(row)
            == _source_message_ownership(next_item[1])
        ):
            # Apply every same-message subevent and validate each post-state,
            # but publish exactly one normalized state for the source message.
            continue
        topbook_rows, depth_rows = _normalize_native_book(
            row,
            native_books[key],
            use_yes_price=use_yes_price,
            quote_normalization_policy=kalshi_quote_policy,
        )
        provenance = {column: row.get(column) for column in _PROVENANCE_COLUMNS}
        reconstruction_coordinate = list(coordinate[:4])
        for item in topbook_rows:
            item.update(
                {
                    "_reconstruction_event_id": event_id,
                    "_reconstruction_event_kind": kind,
                    "_reconstruction_checkpoint_reason": _nullable_text(
                        row.get("checkpoint_reason")
                    ),
                    "_reconstruction_causal_key": reconstruction_coordinate,
                    **provenance,
                }
            )
        for item in depth_rows:
            item.update(
                {
                    "_reconstruction_event_id": event_id,
                    "_reconstruction_event_kind": kind,
                    "_reconstruction_checkpoint_reason": _nullable_text(
                        row.get("checkpoint_reason")
                    ),
                    "_reconstruction_causal_key": reconstruction_coordinate,
                    **provenance,
                }
            )
        reconstructed_topbooks.extend(topbook_rows)
        reconstructed_depths.extend(depth_rows)

    if pending_recovery:
        raise BookTapeReconstructionError(
            "reconstruction ended with unconfirmed checkpoint epochs"
        )
    if open_epochs:
        raise BookTapeReconstructionError("reconstruction ended with open epochs")
    event_books = {
        (
            _text(row.get("collector_run_id")),
            _text(row.get("venue")),
            _text(row.get("venue_book_id")),
        )
        for row in events.to_dict("records")
    }
    missing_terminal = sorted(event_books - terminal_books)
    if missing_terminal:
        raise BookTapeReconstructionError(
            f"reconstruction lacks terminal coverage for {missing_terminal}"
        )

    internal_topbooks = pd.DataFrame(
        reconstructed_topbooks,
        columns=[*TOPBOOK_COLUMNS, *_RECONSTRUCTION_COLUMNS],
    )
    internal_depths = pd.DataFrame(
        reconstructed_depths,
        columns=[*DEPTH_COLUMNS, *_RECONSTRUCTION_COLUMNS],
    )
    selected_event_count = int(len(events))
    if requested_book is not None:
        internal_topbooks = _filter_instrument_frame(
            internal_topbooks,
            requested_book,
        )
        internal_depths = _filter_instrument_frame(
            internal_depths,
            requested_book,
        )
        source_topbooks = _filter_instrument_frame(
            source_topbooks,
            requested_book,
        )
        source_depths = _filter_instrument_frame(
            source_depths,
            requested_book,
        )
        selected_event_count = int(
            events["venue_book_id"].astype(str).eq(requested_book).sum()
        )

    parity_source_topbooks = source_topbooks[
        source_topbooks["valid_state"].fillna(False).astype(bool)
    ].reset_index(drop=True)
    parity_source_depths = source_depths[
        source_depths["valid_state"].fillna(False).astype(bool)
    ].reset_index(drop=True)
    topbook_comparison = {
        **_compare_topbooks(
            internal_topbooks,
            parity_source_topbooks,
        ),
        "excluded_invalid_source_row_count": int(
            len(source_topbooks) - len(parity_source_topbooks)
        ),
    }
    depth_comparison = {
        **_compare_depths(
            internal_depths,
            parity_source_depths,
            available="depth_main" in evidence.payload["dataset_artifacts"],
        ),
        "excluded_invalid_source_row_count": int(
            len(source_depths) - len(parity_source_depths)
        ),
    }
    topbooks = internal_topbooks.loc[:, TOPBOOK_COLUMNS].reset_index(drop=True)
    depths = internal_depths.loc[:, DEPTH_COLUMNS].reset_index(drop=True)
    output_hashes = {
        "topbook_rows": _frame_semantic_hash(topbooks),
        "depth_rows": _frame_semantic_hash(depths),
    }
    discrepancy_count = int(topbook_comparison["discrepancy_count"]) + int(
        depth_comparison["discrepancy_count"]
    )
    profile = evidence.profile
    source_artifact_hashes: dict[str, list[str]] = {}
    for artifact_record in evidence.artifact_provenance:
        source_artifact_hashes.setdefault(
            _text(artifact_record.get("role")),
            [],
        ).append(_text(artifact_record.get("artifact_sha256")))
    report: dict[str, Any] = {
        "schema_version": RECONSTRUCTION_REPORT_VERSION,
        "status": "success" if discrepancy_count == 0 else "mismatch",
        "research_audit_only": True,
        "runtime_authority": False,
        "source_manifest": str(evidence.manifest_path),
        "source_manifest_sha256": evidence.manifest_sha256,
        "source_journal": str(evidence.journal_path),
        "source_journal_sha256": evidence.journal_sha256,
        "source_run_id": _text(payload.get("run_id")),
        "selection": {
            "venue_book_id": requested_book,
            "applied_after_complete_run_validation": True,
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
            "event_control_coordinate_count": len(causal_items),
            "terminal_book_count": len(terminal_books),
        },
        "event_count": int(len(events)),
        "selected_event_count": selected_event_count,
        "applied_event_count": sum(
            int(item["applied_event_count"]) for item in epoch_reports.values()
        ),
        "ignored_orphan_rows": [],
        "ignored_events": ignored,
        "invalid_events": [],
        "epoch_coverage": [epoch_reports[key] for key in sorted(epoch_reports)],
        "topbook_comparison": topbook_comparison,
        "depth_comparison": depth_comparison,
        "topbook_row_count": int(len(topbooks)),
        "depth_row_count": int(len(depths)),
        "output_semantic_hashes": output_hashes,
    }
    return BookTapeReconstructionResult(topbooks, depths, report)


def _load_committed_run_evidence(
    path: Path,
    *,
    venue_book_id: str | None = None,
) -> _CommittedRunEvidence:
    manifest_bytes = path.read_bytes()
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    validation = validate_run_manifest(path)
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
    journal_path = resolve_commit_journal_path(run_dir)
    try:
        journal_bytes = journal_path.read_bytes()
        recovery = recover_stream_run(run_dir)
        records = validate_commit_journal(run_dir)
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
    journal_by_role: dict[
        str,
        list[tuple[CaptureCommitRecord, Any]],
    ] = {}
    for record in records:
        for artifact in record.artifacts:
            journal_by_role.setdefault(artifact.role, []).append((record, artifact))

    frames: dict[str, pd.DataFrame] = {}
    provenance = [
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
    ]
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
        rows: list[dict[str, Any]] = []
        for record, artifact in role_artifacts:
            _require_committed_artifact_hash(run_dir, artifact)
            persisted = read_committed_capture_rows(
                run_dir,
                (artifact,),
            ).get(role, [])
            _require_committed_artifact_hash(run_dir, artifact)
            if len(persisted) != artifact.row_count:
                raise BookTapeReconstructionError(
                    f"committed row count changed while loading {artifact.path}"
                )
            persisted = _filter_committed_role_rows(
                role, persisted, venue_book_id=venue_book_id
            )
            for ordinal, row in enumerate(persisted):
                rows.append(
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
        frames[role] = pd.DataFrame(
            rows,
            columns=[
                *get_table_spec(schema_version).columns,
                *_PROVENANCE_COLUMNS,
            ],
        )

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
        expected_encoding_version=definition.tape_encoding_version,
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
            "topbook.v1",
        ),
        topbook_checkpoint=_schema_frame(
            frames["topbook_checkpoint"],
            "topbook.v1",
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
    if int(payload.get("sequence_gap_count") or 0) != 0:
        raise BookTapeReconstructionError(
            "source manifest reports a venue sequence gap"
        )
    for record in records:
        for artifact in record.artifacts:
            _require_committed_artifact_hash(run_dir, artifact)

    authority_snapshots = (
        (path, manifest_bytes, "source manifest"),
        (state_path, state_bytes, "source run state"),
        (journal_path, journal_bytes, "source commit journal"),
    )
    for authority_path, expected_bytes, label in authority_snapshots:
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
    return _CommittedRunEvidence(
        manifest_path=path,
        manifest_sha256=manifest_sha256,
        payload=payload,
        profile=profile,
        run_dir=run_dir,
        journal_path=journal_path,
        records=records,
        frames=frames,
        artifact_provenance=tuple(provenance),
        adapter_settings_by_venue={
            str(venue): dict(settings)
            for venue, settings in (state.adapter_settings_by_venue or {}).items()
        },
        shard_by_book=dict(shard_by_book),
        journal_sha256=hashlib.sha256(journal_bytes).hexdigest(),
    )


def _schema_frame(frame: pd.DataFrame, schema_version: str) -> pd.DataFrame:
    return frame.loc[:, list(get_table_spec(schema_version).columns)].copy()


def _require_committed_artifact_hash(
    run_dir: Path,
    artifact: CaptureCommitArtifactV1,
) -> None:
    try:
        artifact_path = resolve_run_relative_path(
            run_dir,
            artifact.path,
            key="commit artifact path",
        )
        actual = file_sha256(artifact_path)
    except (OSError, ValueError) as exc:
        raise BookTapeReconstructionError(
            f"committed artifact became unreadable: {artifact.path}: {exc}"
        ) from exc
    if actual != artifact.sha256:
        raise BookTapeReconstructionError(
            f"committed artifact hash changed while loading {artifact.path}"
        )


def _checkpoint_book(
    event: Mapping[str, Any], levels: pd.DataFrame
) -> dict[tuple[str, str], float]:
    counts = json.loads(_text(event.get("side_counts_json")))
    actual = levels["source_side"].astype(str).value_counts().to_dict()
    if {str(key): int(value) for key, value in counts.items()} != {
        side: int(actual.get(side, 0)) for side in counts
    }:
        raise BookTapeReconstructionError(
            f"checkpoint {event.get('event_id')} side-count mismatch"
        )
    book: dict[tuple[str, str], float] = {}
    _apply_absolute_delta(book, levels)
    return book


def _apply_absolute_delta(
    book: dict[tuple[str, str], float], levels: pd.DataFrame
) -> None:
    selected = levels.loc[
        :,
        ["source_side", "price_key", "size_after_contracts"],
    ]
    for source_side, price_key, size_after in selected.itertuples(
        index=False, name=None
    ):
        side = _text(source_side)
        price = canonical_decimal(price_key)
        size = float(size_after)
        if not math.isfinite(size) or size < 0:
            raise BookTapeReconstructionError("negative or non-finite tape size")
        key = (side, price)
        if size == 0:
            book.pop(key, None)
        else:
            book[key] = size


def _validate_reconstructed_post_book_hash(
    event: Mapping[str, Any],
    book: Mapping[tuple[str, str], float],
    *,
    adapter_settings: Mapping[str, Any],
) -> None:
    actual = post_book_hash(
        venue=_text(event.get("venue")),
        venue_book_id=_text(event.get("venue_book_id")),
        levels=[
            NativeBookLevel(side, price, size) for (side, price), size in book.items()
        ],
        adapter_settings=adapter_settings or None,
    )
    if actual != _text(event.get("post_book_hash")):
        raise BookTapeReconstructionError(
            f"reconstructed event {event.get('event_id')} post-book hash mismatch"
        )


def _normalize_native_book(
    event: Mapping[str, Any],
    book: Mapping[tuple[str, str], float],
    *,
    use_yes_price: bool,
    quote_normalization_policy: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    venue = _text(event.get("venue"))
    run_id = _text(event.get("collector_run_id"))
    book_id = _text(event.get("venue_book_id"))
    market_id = _text(event.get("venue_market_id"))
    received = _text(event.get("received_at_utc"))
    monotonic_ns = int(event.get("received_at_monotonic_ns") or 0)
    sequence = int(event.get("local_sequence") or 0)
    quality_flags = [
        str(flag) for flag in json.loads(_text(event.get("quality_flags_json")))
    ]
    flags = set(quality_flags)
    valid = _bool(event.get("valid_state"))
    event_timestamp = _nullable_text(event.get("exchange_at_utc")) or received
    if venue == "polymarket":
        bids = _side_map(book, "bid")
        asks = _side_map(book, "ask")
        best_bid = max(bids) if bids else None
        best_ask = min(asks) if asks else None
        poly_snapshot = {
            "asset_id": book_id,
            "market": market_id,
            "event_type": "reconstructed_tape",
            "timestamp": event_timestamp,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "best_bid_size": bids.get(best_bid) if best_bid is not None else None,
            "best_ask_size": asks.get(best_ask) if best_ask is not None else None,
            "valid_state": valid,
            "quality_flags": sorted(flags),
            "initial_snapshot_received": True,
            "last_book_hash": None,
        }
        topbooks = [
            polymarket_ws_snapshot_to_topbook(
                poly_snapshot,
                collector_run_id=run_id,
                source="reconstructed_book_tape",
                received_at_utc=received,
                received_at_monotonic_ns=monotonic_ns,
                local_sequence=sequence,
                raw_event_ref=_text(event.get("event_id")),
            )
        ]
        depths = _polymarket_depth(
            event,
            bids=bids,
            asks=asks,
            quality_flags=quality_flags,
        )
        return topbooks, depths
    if venue != "kalshi":
        raise BookTapeReconstructionError(f"unsupported tape venue {venue!r}")
    yes_bids = _side_map(book, "yes")
    no_bids = _side_map(book, "no")
    yes_bid = max(yes_bids) if yes_bids else None
    no_price = (
        min(no_bids) if use_yes_price and no_bids else max(no_bids) if no_bids else None
    )
    quotes = project_kalshi_quotes(
        yes_bid=yes_bid,
        no_ladder_price=no_price,
        yes_bid_size=yes_bids.get(yes_bid) if yes_bid is not None else None,
        no_ladder_size=no_bids.get(no_price) if no_price is not None else None,
        use_yes_price=use_yes_price,
        policy_version=quote_normalization_policy,
    )
    kalshi_snapshot = {
        "market_ticker": book_id,
        "market_id": market_id,
        "event_type": "reconstructed_tape",
        "sid": _int_or_none(event.get("venue_sid")),
        "seq": _int_or_none(event.get("venue_sequence")),
        "timestamp": event_timestamp,
        **quotes.as_dict(),
        "valid_state": valid,
        "quality_flags": sorted(flags),
        "initial_snapshot_received": True,
    }
    topbooks = kalshi_ws_snapshot_to_topbook(
        kalshi_snapshot,
        collector_run_id=run_id,
        source="reconstructed_book_tape",
        received_at_utc=received,
        received_at_monotonic_ns=monotonic_ns,
        local_sequence=sequence,
        raw_event_ref=_text(event.get("event_id")),
    )
    return topbooks, _kalshi_depth(
        event,
        market_id=market_id,
        book_id=book_id,
        yes_bids=yes_bids,
        no_bids=no_bids,
        quality_flags=quality_flags,
    )


def _polymarket_depth(
    event: Mapping[str, Any],
    *,
    bids: Mapping[float, float],
    asks: Mapping[float, float],
    quality_flags: list[str],
) -> list[dict[str, Any]]:
    base = depth_row(
        collector_run_id=_text(event.get("collector_run_id")),
        exchange="polymarket",
        venue_market_id=_text(event.get("venue_market_id")),
        instrument_id=_text(event.get("venue_book_id")),
        source="reconstructed_book_tape",
        received_at_utc=_text(event.get("received_at_utc")),
        exchange_ts_utc=_nullable_text(event.get("exchange_at_utc")),
        local_sequence=int(event.get("local_sequence") or 0),
        book_hash=None,
        is_delta=False,
        valid_state=_bool(event.get("valid_state")),
        quality_flags=quality_flags,
    )
    rows: list[dict[str, Any]] = []
    for side, values in (
        ("bid", sorted(bids.items(), reverse=True)),
        ("ask", sorted(asks.items())),
    ):
        cumulative = 0.0
        for index, (price, size) in enumerate(values):
            cumulative += size
            row = base.copy()
            row.update(
                side=side,
                level_index=index,
                price_dollars=price,
                size_contracts=size,
                cumulative_size_contracts=cumulative,
            )
            rows.append(row)
    return rows


def _kalshi_depth(
    event: Mapping[str, Any],
    *,
    market_id: str,
    book_id: str,
    yes_bids: Mapping[float, float],
    no_bids: Mapping[float, float],
    quality_flags: list[str],
) -> list[dict[str, Any]]:
    base = depth_row(
        collector_run_id=_text(event.get("collector_run_id")),
        exchange="kalshi",
        venue_market_id=market_id or book_id,
        source="reconstructed_book_tape",
        received_at_utc=_text(event.get("received_at_utc")),
        exchange_ts_utc=_nullable_text(event.get("exchange_at_utc")),
        local_sequence=int(event.get("local_sequence") or 0),
        venue_sequence=_int_or_none(event.get("venue_sequence")),
        venue_sid=_int_or_none(event.get("venue_sid")),
        is_delta=False,
        valid_state=_bool(event.get("valid_state")),
        quality_flags=quality_flags,
    )
    rows: list[dict[str, Any]] = []
    for outcome, side, values in (
        ("YES", "yes", sorted(yes_bids.items(), reverse=True)),
        ("NO", "no", sorted(no_bids.items(), reverse=True)),
    ):
        side_base = base.copy()
        side_base.update(
            instrument_id=f"{book_id}:{outcome}",
            outcome=outcome,
            side=side,
        )
        cumulative = 0.0
        for index, (price, size) in enumerate(values):
            cumulative += size
            row = side_base.copy()
            row.update(
                level_index=index,
                price_dollars=price,
                size_contracts=size,
                cumulative_size_contracts=cumulative,
            )
            rows.append(row)
    return rows

def _reset_venue_order_for_reconnect(
    key: tuple[str, str, str],
    shard_id: str,
    last_sequence: dict[tuple[str, str, str], int],
    last_book_sequence: dict[tuple[str, str, str], int],
    last_sid: dict[tuple[str, str, str], str],
) -> None:
    if key[1] != "kalshi":
        return
    exact_shard_id = shard_id.strip()
    last_book_sequence.pop(key, None)
    last_sid.pop(key, None)
    for sequence_key in tuple(last_sequence):
        if sequence_key[:2] == (key[0], exact_shard_id):
            last_sequence.pop(sequence_key)


def _validate_venue_order(
    event: Mapping[str, Any],
    key: tuple[str, str, str],
    shard_id: str,
    last_sequence: dict[tuple[str, str, str], int],
    last_book_sequence: dict[tuple[str, str, str], int],
    last_sid: dict[tuple[str, str, str], str],
) -> None:
    if key[1] != "kalshi":
        return
    sequence = _int_or_none(event.get("venue_sequence"))
    sid = _nullable_text(event.get("venue_sid"))
    if sequence is None or sid is None:
        raise BookTapeReconstructionError(
            "Kalshi tape event lacks venue sequence or sid"
        )
    exact_shard_id = shard_id.strip()
    if not exact_shard_id:
        raise BookTapeReconstructionError(
            "Kalshi tape event lacks exact shard ownership"
        )
    kind = _text(event.get("event_kind"))
    checkpoint_reason = _nullable_text(event.get("checkpoint_reason"))
    prior_sid = last_sid.get(key)
    previous_book_sequence = last_book_sequence.get(key)
    if kind == "checkpoint" and checkpoint_reason == "periodic":
        if (
            prior_sid != sid
            or previous_book_sequence is None
            or sequence != previous_book_sequence
        ):
            raise BookTapeReconstructionError(
                "Kalshi periodic checkpoint does not restate the current "
                "book venue sequence and sid"
            )
        return

    shard_key = (key[0], exact_shard_id, sid)
    previous = last_sequence.get(shard_key)
    if previous is not None and sequence <= previous:
        raise BookTapeReconstructionError(
            "Kalshi venue sequence is duplicate or non-increasing: "
            f"previous {previous}, got {sequence}"
        )
    if prior_sid is not None and sid != prior_sid and kind != "checkpoint":
        raise BookTapeReconstructionError("Kalshi sid changed outside a checkpoint")
    last_sequence[shard_key] = sequence
    last_book_sequence[key] = sequence
    last_sid[key] = sid


def _causal_items(
    events: pd.DataFrame,
    controls: pd.DataFrame,
) -> list[
    tuple[
        str,
        dict[str, Any],
        tuple[pd.Timestamp, int, int, int, str],
    ]
]:
    items = [
        ("event", row, _row_causal_key(row, family="event"))
        for row in events.to_dict("records")
    ]
    items.extend(
        ("control", row, _row_causal_key(row, family="control"))
        for row in controls.to_dict("records")
    )

    def sort_key(
        item: tuple[
            str,
            dict[str, Any],
            tuple[pd.Timestamp, int, int, int, str],
        ],
    ) -> tuple[Any, ...]:
        family, row, coordinate = item
        priority = (
            0
            if family == "control"
            and _text(row.get("control_type")) in {"book_invalidated", "stream_ended"}
            else 1
            if family == "event"
            else 2
        )
        return (*coordinate[:4], priority, coordinate[4])

    return sorted(items, key=sort_key)


def _source_message_ownership(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Exact source-message owner used to collapse consecutive subevents."""
    return (
        _text(row.get("collector_run_id")),
        _text(row.get("venue")),
        _text(row.get("venue_book_id")),
        _nullable_text(row.get("epoch_id")),
        _text(row.get("received_at_utc")),
        int(row.get("received_at_monotonic_ns") or 0),
        int(row.get("local_sequence") or 0),
        _nullable_text(row.get("raw_event_hash")),
    )


def _instrument_matches(instrument_id: str, venue_book_id: str) -> bool:
    return instrument_id == venue_book_id or instrument_id.startswith(
        f"{venue_book_id}:"
    )


def _filter_committed_role_rows(
    role: str,
    rows: list[dict[str, Any]],
    *,
    venue_book_id: str | None,
) -> list[dict[str, Any]]:
    if venue_book_id is None:
        return rows
    if role in _TAPE_SCHEMAS:
        return [row for row in rows if _text(row.get("venue_book_id")) == venue_book_id]
    if role in {"topbook_main", "topbook_checkpoint", "depth_main"}:
        return [
            row
            for row in rows
            if _instrument_matches(_text(row.get("instrument_id")), venue_book_id)
        ]
    return rows


def _shard_by_book(
    payload: Mapping[str, Any],
    state: RunStateV1,
) -> dict[tuple[str, str], str]:
    raw = payload.get("feed_shards")
    if not isinstance(raw, list) or not raw:
        raise BookTapeReconstructionError("reconstruction requires exact feed_shards")
    mapping: dict[tuple[str, str], str] = {}
    manifest_plan: dict[str, list[str]] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            raise BookTapeReconstructionError("feed_shards entries must be objects")
        venue = _text(item.get("venue")).lower()
        shard = _text(item.get("shard_id"))
        instruments = item.get("subscribed_instruments")
        if (
            not venue
            or not shard
            or not isinstance(instruments, list)
            or not instruments
        ):
            raise BookTapeReconstructionError(
                "feed shard requires venue, shard_id, and instruments"
            )
        if shard in manifest_plan:
            raise BookTapeReconstructionError(
                f"feed shard ID is duplicated within the run: {shard}"
            )
        if any(
            not isinstance(instrument, str) or not instrument.strip()
            for instrument in instruments
        ):
            raise BookTapeReconstructionError(
                "feed shard instruments must be non-empty text"
            )
        normalized = [instrument.strip() for instrument in instruments]
        if len(normalized) != len(set(normalized)):
            raise BookTapeReconstructionError(
                f"feed shard {venue}:{shard} repeats an instrument"
            )
        if type(item.get("instrument_count")) is not int or item.get(
            "instrument_count"
        ) != len(normalized):
            raise BookTapeReconstructionError(
                f"feed shard {venue}:{shard} instrument_count is inconsistent"
            )
        manifest_plan[shard] = normalized
        for instrument in normalized:
            book = instrument
            if venue == "kalshi":
                book = book.split(":", 1)[0]
            key = (venue, book)
            prior = mapping.get(key)
            if prior is not None:
                raise BookTapeReconstructionError(
                    f"ambiguous shard ownership for {venue}:{book}"
                )
            mapping[key] = shard
    if dict(state.shard_plan) != manifest_plan:
        raise BookTapeReconstructionError(
            "feed_shards must exactly match finalized run-state shard_plan"
        )
    return mapping


def _filter_instrument_frame(
    frame: pd.DataFrame,
    venue_book_id: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    instruments = frame["instrument_id"].astype(str)
    return frame[
        instruments.map(
            lambda instrument: _instrument_matches(instrument, venue_book_id)
        )
    ].copy()


def _compare_topbooks(
    reconstructed: pd.DataFrame,
    source: pd.DataFrame,
) -> dict[str, Any]:
    source_keys = [
        "collector_run_id",
        "exchange",
        "instrument_id",
        "received_at_utc",
    ]
    if source.duplicated(source_keys).any():
        raise BookTapeReconstructionError(
            "topbook source comparison keys are not unique"
        )
    fields = [
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
    ]
    histories: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    history_positions: dict[
        tuple[str, str, str],
        list[tuple[int, int, pd.Timestamp]],
    ] = {}
    event_rows: dict[
        tuple[str, str, str],
        dict[str, dict[str, Any]],
    ] = {}
    required_changes: dict[
        tuple[str, str, str],
        list[dict[str, Any]],
    ] = {}
    for key, group in reconstructed.groupby(
        ["collector_run_id", "exchange", "instrument_id"],
        sort=False,
    ):
        normalized_key = _instrument_group_key(key)
        rows = sorted(
            group.to_dict("records"),
            key=lambda row: _comparison_position(
                row,
                family="topbook",
            ),
        )
        histories[normalized_key] = rows
        history_positions[normalized_key] = [
            _comparison_position(row, family="topbook") for row in rows
        ]
        event_rows[normalized_key] = {
            _text(row.get("_reconstruction_event_id")): row
            for row in rows
            if _text(row.get("_reconstruction_event_id"))
        }
        changes: list[dict[str, Any]] = []
        prior_fingerprint: str | None = None
        for row in rows:
            fingerprint = topbook_state_fingerprint(row)
            if fingerprint != prior_fingerprint:
                changes.append(row)
                prior_fingerprint = fingerprint
        required_changes[normalized_key] = changes

    source_histories: dict[
        tuple[str, str, str],
        list[dict[str, Any]],
    ] = {}
    source_positions: dict[
        tuple[str, str, str],
        list[tuple[int, int, pd.Timestamp]],
    ] = {}
    source_fingerprints: dict[tuple[str, str, str], list[str]] = {}
    for key, group in source.groupby(
        ["collector_run_id", "exchange", "instrument_id"],
        sort=False,
    ):
        normalized_key = _instrument_group_key(key)
        rows = sorted(
            group.to_dict("records"),
            key=lambda row: _comparison_position(
                row,
                family="topbook_source",
            ),
        )
        source_histories[normalized_key] = rows
        source_positions[normalized_key] = [
            _comparison_position(row, family="topbook_source") for row in rows
        ]
        source_fingerprints[normalized_key] = [
            topbook_state_fingerprint(row) for row in rows
        ]

    mismatches: list[dict[str, Any]] = []
    compared = 0
    for key in sorted(source_histories):
        history = histories.get(key, [])
        positions = history_positions.get(key, [])
        by_event_id = event_rows.get(key, {})
        for source_row in source_histories[key]:
            source_coordinate = _comparison_position(
                source_row,
                family="topbook_source",
            )
            candidate_index = bisect_right(positions, source_coordinate) - 1
            reconstructed_row = (
                history[candidate_index] if candidate_index >= 0 else None
            )
            raw_event_ref = _text(source_row.get("raw_event_ref"))
            exact = by_event_id.get(raw_event_ref)
            if (
                exact is not None
                and _comparison_position(exact, family="topbook") <= source_coordinate
            ):
                reconstructed_row = exact
            if reconstructed_row is None:
                mismatches.append(
                    {
                        "kind": "missing_reconstructed_topbook",
                        "coordinate": _comparison_coordinate(source_row),
                        "source_provenance": _source_provenance(source_row),
                    }
                )
                continue
            compared += 1
            differing = {
                field: {
                    "reconstructed": _comparison_value(reconstructed_row.get(field)),
                    "source": _comparison_value(source_row.get(field)),
                }
                for field in fields
                if (field != "book_hash" or _present(source_row.get(field)))
                and not _equal(
                    reconstructed_row.get(field),
                    source_row.get(field),
                )
            }
            if differing:
                mismatches.append(
                    {
                        "kind": "topbook_value_mismatch",
                        "coordinate": _comparison_coordinate(source_row),
                        "reconstruction_event_id": _text(
                            reconstructed_row.get("_reconstruction_event_id")
                        ),
                        "fields": differing,
                        "source_provenance": _source_provenance(source_row),
                        "reconstruction_provenance": _source_provenance(
                            reconstructed_row
                        ),
                    }
                )

    missing_source_changes: list[dict[str, Any]] = []
    for key, changes in required_changes.items():
        positions = source_positions.get(key, [])
        fingerprints = source_fingerprints.get(key, [])
        for index, row in enumerate(changes):
            lower = _comparison_position(row, family="topbook")
            upper = (
                _comparison_position(
                    changes[index + 1],
                    family="topbook",
                )
                if index + 1 < len(changes)
                else None
            )
            start = bisect_left(positions, lower)
            stop = (
                bisect_left(positions, upper) if upper is not None else len(positions)
            )
            expected_fingerprint = topbook_state_fingerprint(row)
            if expected_fingerprint in fingerprints[start:stop]:
                continue
            missing_source_changes.append(row)
            mismatches.append(
                {
                    "kind": "missing_source_topbook_change",
                    "coordinate": _comparison_coordinate(row),
                    "reconstruction_event_id": _text(
                        row.get("_reconstruction_event_id")
                    ),
                    "reconstruction_provenance": _source_provenance(row),
                }
            )

    discrepancy_count = len(mismatches)
    return {
        "available": True,
        "status": "match" if discrepancy_count == 0 else "mismatch",
        "excluded_fields": ["book_hash"],
        "source_row_count": int(len(source)),
        "reconstructed_row_count": int(len(reconstructed)),
        "required_state_change_count": sum(
            len(changes) for changes in required_changes.values()
        ),
        "compared_row_count": compared,
        "mismatch_count": sum(
            item["kind"] == "topbook_value_mismatch" for item in mismatches
        ),
        "missing_reconstructed_row_count": sum(
            item["kind"] == "missing_reconstructed_topbook" for item in mismatches
        ),
        "missing_source_change_count": len(missing_source_changes),
        "discrepancy_count": discrepancy_count,
        "mismatches": mismatches,
    }


def _compare_depths(
    reconstructed: pd.DataFrame,
    source: pd.DataFrame,
    *,
    available: bool,
) -> dict[str, Any]:
    if not available:
        return {
            "available": False,
            "status": "not_available",
            "excluded_fields": ["book_hash"],
            "source_row_count": 0,
            "reconstructed_row_count": int(len(reconstructed)),
            "compared_row_count": 0,
            "periodic_checkpoint_row_count": 0,
            "periodic_checkpoint_compared_row_count": 0,
            "mismatch_count": 0,
            "missing_reconstructed_row_count": 0,
            "missing_source_row_count": 0,
            "discrepancy_count": 0,
            "mismatches": [],
        }
    keys = [
        "collector_run_id",
        "exchange",
        "instrument_id",
        "local_sequence",
        "venue_sequence",
        "venue_sid",
        "side",
        "level_index",
    ]
    fields = [
        "venue_market_id",
        "outcome",
        "price_dollars",
        "size_contracts",
        "cumulative_size_contracts",
        "is_delta",
        "valid_state",
        "quality_flags",
    ]
    reconstructed_rows = reconstructed.to_dict("records")
    source_rows = source.to_dict("records")
    reconstructed_by_key = _unique_parity_rows(
        reconstructed_rows,
        keys=keys,
        label="reconstructed depth",
    )
    source_by_key = _unique_parity_rows(
        source_rows,
        keys=keys,
        label="source depth",
    )
    mismatches: list[dict[str, Any]] = []
    compared = 0
    for key, source_row in source_by_key.items():
        reconstructed_row = reconstructed_by_key.get(key)
        if reconstructed_row is None:
            mismatches.append(
                {
                    "kind": "missing_reconstructed_depth",
                    "coordinate": _comparison_coordinate(source_row),
                    "side": _text(source_row.get("side")),
                    "level_index": int(source_row.get("level_index") or 0),
                    "source_provenance": _source_provenance(source_row),
                }
            )
            continue
        compared += 1
        differing = _depth_differences(
            reconstructed_row,
            source_row,
            fields=fields,
        )
        if differing:
            mismatches.append(
                {
                    "kind": "depth_value_mismatch",
                    "coordinate": _comparison_coordinate(source_row),
                    "side": _text(source_row.get("side")),
                    "level_index": int(source_row.get("level_index") or 0),
                    "fields": differing,
                    "source_provenance": _source_provenance(source_row),
                    "reconstruction_provenance": _source_provenance(reconstructed_row),
                }
            )

    missing_source_keys = sorted(
        set(reconstructed_by_key) - set(source_by_key),
        key=repr,
    )
    periodic_groups: dict[
        tuple[str, str, str, str],
        list[dict[str, Any]],
    ] = {}
    for key in missing_source_keys:
        reconstructed_row = reconstructed_by_key[key]
        if (
            _text(reconstructed_row.get("_reconstruction_event_kind")) == "checkpoint"
            and _text(reconstructed_row.get("_reconstruction_checkpoint_reason"))
            == "periodic"
        ):
            group_key = (
                _text(reconstructed_row.get("_reconstruction_event_id")),
                _text(reconstructed_row.get("collector_run_id")),
                _text(reconstructed_row.get("exchange")),
                _text(reconstructed_row.get("instrument_id")),
            )
            periodic_groups.setdefault(group_key, []).append(reconstructed_row)
            continue
        mismatches.append(
            {
                "kind": "missing_source_depth",
                "coordinate": _comparison_coordinate(reconstructed_row),
                "side": _text(reconstructed_row.get("side")),
                "level_index": int(reconstructed_row.get("level_index") or 0),
                "reconstruction_provenance": _source_provenance(reconstructed_row),
            }
        )

    source_snapshots: dict[
        tuple[str, str, str],
        dict[tuple[int, pd.Timestamp], list[dict[str, Any]]],
    ] = {}
    for source_row in source_rows:
        instrument_key = (
            _text(source_row.get("collector_run_id")),
            _text(source_row.get("exchange")),
            _text(source_row.get("instrument_id")),
        )
        position = _depth_comparison_position(source_row, family="depth_source")
        source_snapshots.setdefault(instrument_key, {}).setdefault(
            position,
            [],
        ).append(source_row)
    source_positions = {
        key: sorted(snapshots) for key, snapshots in source_snapshots.items()
    }

    periodic_compared = 0
    periodic_fields = ["venue_sequence", "venue_sid", *fields]
    for group_key, periodic_rows in sorted(periodic_groups.items()):
        event_id, run_id, exchange, instrument_id = group_key
        positions = {
            _depth_comparison_position(row, family="periodic reconstructed depth")
            for row in periodic_rows
        }
        if len(positions) != 1:
            raise BookTapeReconstructionError(
                f"periodic checkpoint {event_id} depth rows have mixed coordinates"
            )
        position = next(iter(positions))
        instrument_key = (run_id, exchange, instrument_id)
        candidates = source_positions.get(instrument_key, [])
        baseline_index = bisect_right(candidates, position) - 1
        if baseline_index < 0:
            representative = periodic_rows[0]
            mismatches.append(
                {
                    "kind": "missing_source_depth_baseline",
                    "coordinate": _comparison_coordinate(representative),
                    "reconstruction_event_id": event_id,
                    "reconstruction_provenance": _source_provenance(representative),
                }
            )
            continue
        baseline_position = candidates[baseline_index]
        baseline_rows = source_snapshots[instrument_key][baseline_position]
        periodic_by_level = _unique_parity_rows(
            periodic_rows,
            keys=["side", "level_index"],
            label=f"periodic checkpoint {event_id} depth",
        )
        baseline_by_level = _unique_parity_rows(
            baseline_rows,
            keys=["side", "level_index"],
            label=f"periodic checkpoint {event_id} source depth baseline",
        )
        for level_key in sorted(
            set(periodic_by_level) | set(baseline_by_level),
            key=repr,
        ):
            periodic_row = periodic_by_level.get(level_key)
            baseline_row = baseline_by_level.get(level_key)
            if periodic_row is None:
                assert baseline_row is not None
                mismatches.append(
                    {
                        "kind": "missing_periodic_reconstructed_depth",
                        "coordinate": _comparison_coordinate(baseline_row),
                        "reconstruction_event_id": event_id,
                        "side": _text(baseline_row.get("side")),
                        "level_index": int(baseline_row.get("level_index") or 0),
                        "source_provenance": _source_provenance(baseline_row),
                    }
                )
                continue
            if baseline_row is None:
                mismatches.append(
                    {
                        "kind": "missing_periodic_source_depth",
                        "coordinate": _comparison_coordinate(periodic_row),
                        "reconstruction_event_id": event_id,
                        "side": _text(periodic_row.get("side")),
                        "level_index": int(periodic_row.get("level_index") or 0),
                        "reconstruction_provenance": _source_provenance(periodic_row),
                    }
                )
                continue
            periodic_compared += 1
            compared += 1
            differing = _depth_differences(
                periodic_row,
                baseline_row,
                fields=periodic_fields,
            )
            if differing:
                mismatches.append(
                    {
                        "kind": "periodic_depth_value_mismatch",
                        "coordinate": _comparison_coordinate(periodic_row),
                        "source_coordinate": _comparison_coordinate(baseline_row),
                        "reconstruction_event_id": event_id,
                        "side": _text(periodic_row.get("side")),
                        "level_index": int(periodic_row.get("level_index") or 0),
                        "fields": differing,
                        "source_provenance": _source_provenance(baseline_row),
                        "reconstruction_provenance": _source_provenance(periodic_row),
                    }
                )

    discrepancy_count = len(mismatches)
    return {
        "available": True,
        "status": "match" if discrepancy_count == 0 else "mismatch",
        "excluded_fields": ["book_hash"],
        "source_row_count": int(len(source)),
        "reconstructed_row_count": int(len(reconstructed)),
        "compared_row_count": compared,
        "periodic_checkpoint_row_count": sum(
            len(rows) for rows in periodic_groups.values()
        ),
        "periodic_checkpoint_compared_row_count": periodic_compared,
        "mismatch_count": sum(
            item["kind"] in {"depth_value_mismatch", "periodic_depth_value_mismatch"}
            for item in mismatches
        ),
        "missing_reconstructed_row_count": sum(
            item["kind"]
            in {"missing_reconstructed_depth", "missing_periodic_reconstructed_depth"}
            for item in mismatches
        ),
        "missing_source_row_count": sum(
            item["kind"]
            in {
                "missing_source_depth",
                "missing_source_depth_baseline",
                "missing_periodic_source_depth",
            }
            for item in mismatches
        ),
        "discrepancy_count": discrepancy_count,
        "mismatches": mismatches,
    }


def _depth_differences(
    reconstructed: Mapping[str, Any],
    source: Mapping[str, Any],
    *,
    fields: list[str],
) -> dict[str, dict[str, Any]]:
    return {
        field: {
            "reconstructed": _comparison_value(reconstructed.get(field)),
            "source": _comparison_value(source.get(field)),
        }
        for field in fields
        if (field != "book_hash" or _present(source.get(field)))
        and not _equal(
            reconstructed.get(field),
            source.get(field),
        )
    }


def _validate_profile_compatibility(
    profile: Mapping[str, Any],
) -> StorageProfileDefinition:
    name = _text(profile.get("name"))
    version = _text(profile.get("profile_version"))
    if not name or not version:
        raise BookTapeReconstructionError(
            "storage profile name and profile_version are required"
        )
    try:
        definition = get_storage_profile_definition(name, version)
    except ValueError as exc:
        raise BookTapeReconstructionError(str(exc)) from exc
    if not definition.committed_full_book_tape_required:
        raise BookTapeReconstructionError(
            "storage profile does not promise reconstructable full-book tape"
        )
    expected = {
        "profile_version": definition.profile_version,
        "tape_encoding_version": definition.tape_encoding_version,
    }
    for field, value in expected.items():
        actual = _text(profile.get(field))
        if actual != value:
            raise BookTapeReconstructionError(
                f"unsupported reconstruction {field}: "
                f"expected {value!r}, got {actual!r}"
            )
    return definition


def _row_causal_key(
    row: Mapping[str, Any],
    *,
    family: str,
) -> tuple[pd.Timestamp, int, int, int, str]:
    timestamp = pd.to_datetime(
        row.get("received_at_utc"),
        utc=True,
        errors="coerce",
    )
    if pd.isna(timestamp):
        raise BookTapeReconstructionError(
            f"{family} evidence has an invalid causal timestamp"
        )
    monotonic_ns = _int_or_none(row.get("received_at_monotonic_ns"))
    local_sequence = _int_or_none(row.get("local_sequence"))
    subsequence = _int_or_none(row.get("subsequence")) or 0
    if (
        monotonic_ns is None
        or monotonic_ns < 0
        or local_sequence is None
        or local_sequence < 0
        or subsequence < 0
    ):
        raise BookTapeReconstructionError(
            f"{family} evidence has an invalid causal coordinate"
        )
    stable_id = _text(
        row.get("event_id")
        or row.get("control_id")
        or row.get("raw_event_ref")
        or row.get("instrument_id")
    )
    return (
        timestamp,
        monotonic_ns,
        local_sequence,
        subsequence,
        stable_id,
    )


def _comparison_position(
    row: Mapping[str, Any],
    *,
    family: str,
) -> tuple[int, int, pd.Timestamp]:
    timestamp, monotonic_ns, local_sequence, _, _ = _row_causal_key(
        row,
        family=family,
    )
    return local_sequence, monotonic_ns, timestamp


def _depth_comparison_position(
    row: Mapping[str, Any],
    *,
    family: str,
) -> tuple[int, pd.Timestamp]:
    timestamp = pd.to_datetime(
        row.get("received_at_utc"),
        utc=True,
        errors="coerce",
    )
    local_sequence = _int_or_none(row.get("local_sequence"))
    if pd.isna(timestamp) or local_sequence is None or local_sequence < 0:
        raise BookTapeReconstructionError(
            f"{family} evidence has an invalid depth coordinate"
        )
    return local_sequence, timestamp


def _instrument_group_key(value: Any) -> tuple[str, str, str]:
    normalized = tuple(str(item) for item in value)
    if len(normalized) != 3:
        raise BookTapeReconstructionError(
            "instrument comparison group must have three keys"
        )
    return normalized[0], normalized[1], normalized[2]


def _unique_parity_rows(
    rows: list[dict[str, Any]],
    *,
    keys: list[str],
    label: str,
) -> dict[tuple[Any, ...], dict[str, Any]]:
    result: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(_comparison_value(row.get(column)) for column in keys)
        if key in result:
            raise BookTapeReconstructionError(f"{label} comparison keys are not unique")
        result[key] = row
    return result


def _source_provenance(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "role": _text(row.get("_source_role")),
        "schema_version": _text(row.get("_source_schema_version")),
        "journal_group_id": _text(row.get("_source_journal_group_id")),
        "journal_committed_at_utc": _text(row.get("_source_journal_committed_at_utc")),
        "artifact_path": _text(row.get("_source_artifact_path")),
        "artifact_sha256": _text(row.get("_source_artifact_sha256")),
        "artifact_row_count": _int_or_none(row.get("_source_artifact_row_count")),
        "artifact_first_local_sequence": _int_or_none(
            row.get("_source_artifact_first_local_sequence")
        ),
        "artifact_last_local_sequence": _int_or_none(
            row.get("_source_artifact_last_local_sequence")
        ),
        "artifact_row_ordinal": _int_or_none(row.get("_source_artifact_row_ordinal")),
    }


def _comparison_coordinate(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "collector_run_id": _text(row.get("collector_run_id")),
        "venue": _text(row.get("exchange") or row.get("venue")),
        "venue_market_id": _text(row.get("venue_market_id")),
        "instrument_id": _text(row.get("instrument_id") or row.get("venue_book_id")),
        "received_at_utc": _text(row.get("received_at_utc")),
        "received_at_monotonic_ns": _int_or_none(row.get("received_at_monotonic_ns")),
        "local_sequence": _int_or_none(row.get("local_sequence")),
        "venue_sequence": _int_or_none(row.get("venue_sequence")),
        "venue_sid": _int_or_none(row.get("venue_sid")),
    }


def _comparison_value(value: Any) -> Any:
    if not _present(value):
        return None
    if isinstance(value, Decimal) and not value.is_finite():
        return None
    if isinstance(value, Mapping):
        return {
            str(key): _comparison_value(item)
            for key, item in sorted(
                value.items(),
                key=lambda item: str(item[0]),
            )
        }
    if isinstance(value, (list, tuple, set)):
        return [_comparison_value(item) for item in value]
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, Decimal) and not value.is_finite():
        return None
    return value


def _frame_semantic_hash(frame: pd.DataFrame) -> str:
    return semantic_hash(
        {
            "columns": list(frame.columns),
            "rows": [
                {column: _comparison_value(row.get(column)) for column in frame.columns}
                for row in frame.to_dict("records")
            ],
        }
    )


def _equal(left: Any, right: Any) -> bool:
    normalized_left = _comparison_value(left)
    normalized_right = _comparison_value(right)
    if normalized_left is None and normalized_right is None:
        return True
    if (
        isinstance(normalized_left, (int, float))
        and not isinstance(normalized_left, bool)
        and isinstance(normalized_right, (int, float))
        and not isinstance(normalized_right, bool)
    ):
        return math.isclose(
            float(normalized_left),
            float(normalized_right),
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    return normalized_left == normalized_right


def _side_map(book: Mapping[tuple[str, str], float], side: str) -> dict[float, float]:
    return {
        float(price): size
        for (candidate, price), size in book.items()
        if candidate == side
    }


def _manifest_run_dir(payload: Mapping[str, Any], manifest_path: Path) -> Path:
    raw = Path(_text(payload.get("run_dir")) or str(manifest_path.parent))
    return (
        raw.resolve() if raw.is_absolute() else (manifest_path.parent / raw).resolve()
    )


def _read_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise BookTapeReconstructionError(f"expected JSON object: {path}")
    return payload


def _text(value: Any) -> str:
    return "" if not _present(value) else str(value).strip()


def _nullable_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _present(value: Any) -> bool:
    if value is None:
        return False
    missing = pd.isna(value)
    if isinstance(missing, bool) or not hasattr(missing, "__len__"):
        return not bool(missing)
    return True


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _int_or_none(value: Any) -> int | None:
    if not _present(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _native(value: Any) -> Any:
    if not _present(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


__all__ = [
    "BookTapeReconstructionError",
    "BookTapeReconstructionResult",
    "RECONSTRUCTION_REPORT_VERSION",
    "reconstruct_book_tape",
]
