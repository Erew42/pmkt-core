from __future__ import annotations

import asyncio
import multiprocessing
import queue as queue_module
import re
import traceback
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from pmkt.exchanges.polymarket.order_book_stream import stream_order_book_data
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.streaming.connection_partitions import ConnectionPartition
from pmkt.streaming.durability import file_sha256, write_json_atomic_fsync
from pmkt.streaming.profiles import (
    StorageProfileOverrides,
    StorageProfileSelection,
    select_storage_profile,
)


def _safe_run_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip(".-")
    return cleaned or "shard"


def _storage_profile_process_payload(
    selection: StorageProfileSelection,
) -> dict[str, Any]:
    """Return a spawn-safe description of a storage-profile selection."""
    return {
        "name": selection.definition.name,
        "profile_version": selection.definition.profile_version,
        "experimental_profile_acknowledged": (
            selection.experimental_profile_acknowledged
        ),
        "overrides": selection.overrides.to_mapping(),
        "feed_health_interval_seconds": (
            selection.definition.feed_health_interval_seconds
        ),
        "topbook_checkpoint_interval_seconds": (
            selection.definition.topbook_checkpoint_interval_seconds
        ),
        "book_checkpoint_interval_seconds": (
            selection.definition.book_checkpoint_interval_seconds
        ),
        "expected_manifest": selection.to_manifest_mapping(),
    }


def _storage_profile_from_process_payload(
    payload: Mapping[str, Any],
) -> StorageProfileSelection:
    overrides = payload.get("overrides")
    expected_manifest = payload.get("expected_manifest")
    if not isinstance(overrides, Mapping) or not isinstance(
        expected_manifest, Mapping
    ):
        raise ValueError("invalid spawned storage-profile payload")
    selection = select_storage_profile(
        str(payload["name"]),
        profile_version=str(payload["profile_version"]),
        overrides=StorageProfileOverrides(
            keep_raw_jsonl=bool(overrides.get("keep_raw_jsonl")),
            topbook_emission_per_event=bool(
                overrides.get("topbook_emission_per_event")
            ),
            emit_full_depth=bool(overrides.get("emit_full_depth")),
            emit_legacy_book_artifacts=bool(
                overrides.get("emit_legacy_book_artifacts")
            ),
        ),
        experimental_profile_acknowledged=bool(
            payload.get("experimental_profile_acknowledged")
        ),
        feed_health_interval_seconds=float(payload["feed_health_interval_seconds"]),
        topbook_checkpoint_interval_seconds=float(
            payload["topbook_checkpoint_interval_seconds"]
        ),
        book_checkpoint_interval_seconds=(
            None
            if payload.get("book_checkpoint_interval_seconds") is None
            else float(payload["book_checkpoint_interval_seconds"])
        ),
    )
    if selection.to_manifest_mapping() != dict(expected_manifest):
        raise ValueError("spawned storage-profile reconstruction drifted")
    return selection


def _polymarket_partition_process_entry(
    worker_index: int,
    requests: Sequence[Mapping[str, Any]],
    result_queue: Any,
) -> None:
    async def run_worker() -> list[tuple[int, dict[str, Any]]]:
        async def run_request(
            request: Mapping[str, Any],
        ) -> tuple[int, dict[str, Any]]:
            delay = float(request["start_delay_seconds"])
            if delay:
                await asyncio.sleep(delay)
            shard = FeedShardHealth(
                venue="polymarket",
                shard_id=str(request["shard_id"]),
                subscribed_instruments=tuple(request["instruments"]),
                relation_ids=tuple(request["relation_ids"]),
            )
            supervisor = LiveFeedSupervisor(
                [shard],
                preflight_report=request["preflight_report"],
                max_message_age_ms=int(request["max_message_age_ms"]),
                max_valid_book_age_ms=int(request["max_valid_book_age_ms"]),
            )
            collector_kwargs = dict(request["collector_kwargs"])
            storage_profile_payload = request.get("storage_profile")
            if storage_profile_payload is not None:
                if not isinstance(storage_profile_payload, Mapping):
                    raise ValueError("invalid spawned storage-profile payload")
                collector_kwargs["storage_profile"] = (
                    _storage_profile_from_process_payload(storage_profile_payload)
                )
            manifest = await stream_order_book_data(
                list(request["instruments"]),
                output_root=Path(request["output_root"]),
                run_name=str(request["run_name"]),
                feed_supervisor=supervisor,
                **collector_kwargs,
            )
            return int(request["partition_index"]), manifest

        return list(await asyncio.gather(*(run_request(item) for item in requests)))

    try:
        result_queue.put((worker_index, True, asyncio.run(run_worker())))
    except BaseException as exc:
        result_queue.put(
            (
                worker_index,
                False,
                {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )
        )


async def run_polymarket_partition_processes(
    *,
    partitions: Sequence[ConnectionPartition],
    group_dir: Path,
    group_name: str,
    process_count: int,
    start_stagger_seconds: float,
    collector_kwargs: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if process_count <= 0:
        raise ValueError("process_count must be positive")
    effective_process_count = min(process_count, len(partitions))
    requests_by_worker: list[list[dict[str, Any]]] = [
        [] for _ in range(effective_process_count)
    ]
    process_collector_kwargs = dict(collector_kwargs)
    storage_profile = process_collector_kwargs.pop("storage_profile", None)
    if storage_profile is not None and not isinstance(
        storage_profile, StorageProfileSelection
    ):
        raise TypeError(
            "multi-process Polymarket capture requires a storage-profile selection"
        )
    storage_profile_payload = (
        _storage_profile_process_payload(storage_profile)
        if isinstance(storage_profile, StorageProfileSelection)
        else None
    )
    for index, partition in enumerate(partitions):
        requests_by_worker[index % effective_process_count].append(
            {
                "partition_index": index,
                "shard_id": partition.shard_id,
                "instruments": partition.instruments,
                "relation_ids": partition.relation_ids,
                "preflight_report": partition.supervisor.preflight_report,
                "max_message_age_ms": partition.supervisor.max_message_age_ms,
                "max_valid_book_age_ms": partition.supervisor.max_valid_book_age_ms,
                "output_root": str(group_dir),
                "run_name": (
                    f"{group_name}__{index:03d}__"
                    f"{_safe_run_component(partition.shard_id)}"
                ),
                "start_delay_seconds": index * start_stagger_seconds,
                "collector_kwargs": process_collector_kwargs,
                **(
                    {"storage_profile": storage_profile_payload}
                    if storage_profile_payload is not None
                    else {}
                ),
            }
        )

    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_polymarket_partition_process_entry,
            args=(worker_index, tuple(requests), result_queue),
            name=f"pmkt-polymarket-capture-{worker_index}",
        )
        for worker_index, requests in enumerate(requests_by_worker)
    ]
    started_processes = processes[:0]
    worker_results: dict[int, list[tuple[int, dict[str, Any]]]] = {}
    try:
        for process in processes:
            process.start()
            started_processes.append(process)
        while len(worker_results) < len(processes):
            try:
                worker_index, ok, payload = await asyncio.to_thread(
                    result_queue.get, True, 0.5
                )
            except queue_module.Empty:
                exited_without_result = [
                    process
                    for worker_index, process in enumerate(processes)
                    if worker_index not in worker_results
                    and process.exitcode is not None
                ]
                if exited_without_result:
                    details = ", ".join(
                        f"{process.name}={process.exitcode}"
                        for process in exited_without_result
                    )
                    raise RuntimeError(
                        f"Polymarket capture worker exited without a result: {details}"
                    )
                continue
            if not ok:
                raise RuntimeError(
                    "Polymarket capture worker failed: "
                    f"{payload['type']}: {payload['message']}\n{payload['traceback']}"
                )
            worker_results[int(worker_index)] = list(payload)
    except BaseException:
        for process in started_processes:
            if process.is_alive():
                process.terminate()
        raise
    finally:
        for process in started_processes:
            await asyncio.to_thread(process.join, 5.0)
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join)
        result_queue.close()
        result_queue.join_thread()

    indexed = [item for values in worker_results.values() for item in values]
    return [manifest for _, manifest in sorted(indexed)]


async def run_connection_partition_group(
    *,
    venue: str,
    partitions: Sequence[ConnectionPartition],
    collector: Any,
    output_dir: Path,
    run_name: str | None,
    start_stagger_seconds: float,
    collector_kwargs: Mapping[str, Any],
    process_count: int = 1,
) -> dict[str, Any]:
    if not partitions:
        raise ValueError("capture requires at least one connection partition")
    if len(partitions) == 1:
        partition = partitions[0]
        return await collector(
            list(partition.instruments),
            output_root=output_dir,
            run_name=run_name,
            feed_supervisor=partition.supervisor,
            **collector_kwargs,
        )

    group_name = run_name or (
        f"{venue}_connection_group_"
        f"{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    group_dir = output_dir / group_name
    group_dir.mkdir(parents=True, exist_ok=True)

    async def run_partition(
        index: int, partition: ConnectionPartition
    ) -> dict[str, Any]:
        if start_stagger_seconds:
            await asyncio.sleep(index * start_stagger_seconds)
        child_name = (
            f"{group_name}__{index:03d}__"
            f"{_safe_run_component(partition.shard_id)}"
        )
        return await collector(
            list(partition.instruments),
            output_root=group_dir,
            run_name=child_name,
            feed_supervisor=partition.supervisor,
            **collector_kwargs,
        )

    if process_count > 1:
        if venue != "polymarket":
            raise ValueError("multi-process connection groups currently support Polymarket")
        if collector is not stream_order_book_data:
            raise ValueError(
                "multi-process Polymarket capture requires the real collector"
            )
        manifests = await run_polymarket_partition_processes(
            partitions=partitions,
            group_dir=group_dir,
            group_name=group_name,
            process_count=process_count,
            start_stagger_seconds=start_stagger_seconds,
            collector_kwargs=collector_kwargs,
        )
    else:
        tasks = [
            asyncio.create_task(run_partition(index, partition))
            for index, partition in enumerate(partitions)
        ]
        try:
            manifests = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    children: list[dict[str, Any]] = []
    total_counts: dict[str, int] = {}
    total_row_counts: dict[str, int] = {}
    total_requested = 0
    total_initial_snapshots = 0
    total_missing_snapshots = 0
    total_reconnects = 0
    total_recoveries = 0
    statuses: list[str] = []
    for partition, manifest in zip(partitions, manifests, strict=True):
        manifest_path = Path(str(manifest["run_dir"])) / "manifest.json"
        counts = manifest.get("counts") or {}
        for key, value in counts.items():
            total_counts[str(key)] = total_counts.get(str(key), 0) + int(value or 0)
        row_counts = manifest.get("row_counts") or {}
        for key, value in row_counts.items():
            total_row_counts[str(key)] = total_row_counts.get(str(key), 0) + int(
                value or 0
            )
        completeness = manifest.get("capture_completeness") or {}
        requested = int(completeness.get("requested_instrument_count") or 0)
        initial_snapshots = int(completeness.get("initial_snapshot_count") or 0)
        missing_snapshots = int(
            completeness.get("unexplained_missing_instrument_count") or 0
        )
        reconnects = int(manifest.get("reconnect_count") or 0)
        recoveries = int(manifest.get("socket_recovery_count") or 0)
        total_requested += requested
        total_initial_snapshots += initial_snapshots
        total_missing_snapshots += missing_snapshots
        total_reconnects += reconnects
        total_recoveries += recoveries
        status = str(manifest.get("status") or "unknown")
        statuses.append(status)
        children.append(
            {
                "shard_id": partition.shard_id,
                "instrument_count": len(partition.instruments),
                "relation_count": len(partition.relation_ids),
                "run_dir": str(manifest["run_dir"]),
                "manifest_path": str(manifest_path),
                "manifest_sha256": file_sha256(manifest_path),
                "status": status,
                "counts": {str(key): int(value or 0) for key, value in counts.items()},
                "row_counts": {
                    str(key): int(value or 0) for key, value in row_counts.items()
                },
                "capture_summary": {
                    "requested_instrument_count": requested,
                    "initial_snapshot_count": initial_snapshots,
                    "missing_initial_snapshot_count": missing_snapshots,
                    "reconnect_count": reconnects,
                    "socket_recovery_count": recoveries,
                },
            }
        )
    group_status = (
        "success"
        if all(status == "success" for status in statuses)
        else "partial"
        if all(status in {"success", "partial"} for status in statuses)
        else "error"
    )
    group_manifest = {
        "schema_version": "capture_connection_group.v1",
        "run_id": group_name,
        "run_dir": str(group_dir.resolve()),
        "venue": venue,
        "status": group_status,
        "connection_count": len(partitions),
        "worker_process_count": min(process_count, len(partitions)),
        "connection_start_stagger_seconds": start_stagger_seconds,
        "counts": total_counts,
        "row_counts": total_row_counts,
        "capture_summary": {
            "requested_instrument_count": total_requested,
            "initial_snapshot_count": total_initial_snapshots,
            "missing_initial_snapshot_count": total_missing_snapshots,
            "initial_snapshot_ratio": (
                total_initial_snapshots / total_requested if total_requested else None
            ),
            "reconnect_count": total_reconnects,
            "socket_recovery_count": total_recoveries,
        },
        "children": children,
    }
    group_manifest_path = group_dir / "capture_connection_group.v1.json"
    write_json_atomic_fsync(group_manifest_path, group_manifest)
    return {**group_manifest, "group_manifest_path": str(group_manifest_path)}
