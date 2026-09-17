"""Small synthetic historical tape for reader tests; no live legacy collector."""
from pathlib import Path

from pmkt.data.manifests import build_run_manifest, write_manifest
from pmkt.data.normalize_books import polymarket_ws_snapshot_to_topbook
from pmkt.exchanges.polymarket.ws import apply_market_message
from pmkt.streaming.legacy.datasets import CANONICAL_PROFILE_DATASETS
from pmkt.streaming.legacy.durability import COMMIT_JOURNAL_NAME
from pmkt.streaming.legacy.durability_settings import CaptureDurabilitySettings
from legacy_profile_fixture import create_profile_runtime
from pmkt.streaming.legacy.profiles import resolve_dataset_specs, select_storage_profile
from pmkt.streaming.legacy.tape_producers import PolymarketTapeProducer


def historical_polymarket_tape(tmp_path: Path) -> Path:
    root = tmp_path / "poly-reconstruct"
    root.mkdir()
    selection = select_storage_profile("book-tape", profile_version="1")
    runtime = create_profile_runtime(
        run_dir=root, selection=selection,
        specs=resolve_dataset_specs(selection, CANONICAL_PROFILE_DATASETS),
        shard_plan={"polymarket-0": ["token-1"]}, adapter_settings_by_venue={"polymarket": {}},
        started_at_utc="2026-07-19T10:00:00+00:00",
        durability_settings=CaptureDurabilitySettings.resolve(requested_segment_rows=100, requested_segment_seconds=30),
    )
    producer = PolymarketTapeProducer(collector_run_id=root.name, shard_id="polymarket-0")
    states = {}
    messages = [
        {"event_type": "book", "asset_id": "token-1", "market": "market-1",
         "bids": [{"price": "0.40", "size": "10"}], "asks": [{"price": "0.60", "size": "5"}]},
        {"event_type": "price_change", "asset_id": "token-1", "market": "market-1",
         "price_changes": [{"asset_id": "token-1", "side": "SELL", "price": "0.55", "size": "7"},
                           {"asset_id": "token-1", "side": "SELL", "price": "0.60", "size": "0"}]},
    ]
    for sequence, message in enumerate(messages, 1):
        stamp = f"2026-07-19T10:00:0{sequence}+00:00"
        snapshots = apply_market_message(states, message)
        producer.observe(message=message, states=states, received_at_utc=stamp,
            received_at_monotonic_ns=sequence * 1_000_000_000, local_sequence=sequence).write_to(runtime.coordinator)
        for snapshot in snapshots:
            row = polymarket_ws_snapshot_to_topbook(snapshot.as_dict(), collector_run_id=root.name,
                received_at_utc=stamp, received_at_monotonic_ns=sequence * 1_000_000_000, local_sequence=sequence)
            runtime.coordinator.add("topbook_main", row)
    end = "2026-07-19T10:00:03+00:00"
    producer.ended(states=states, received_at_utc=end, received_at_monotonic_ns=3_000_000_000,
        local_sequence=3, reason="completed").write_to(runtime.coordinator)
    runtime.force_finalize()
    artifacts = runtime.coordinator.dataset_artifacts()
    manifest = build_run_manifest(
        run_id=root.name, run_dir=root, started_at_utc="2026-07-19T10:00:00+00:00", ended_at_utc=end,
        status="success", command="synthetic historical reader fixture", dataset_paths={}, schema_versions={}, row_counts={},
        extra={"dataset_artifacts": artifacts, "storage_profile": runtime.manifest_profile(terminal_completeness="complete"),
               "feed_shards": [{"venue": "polymarket", "shard_id": "polymarket-0", "subscribed_instruments": ["token-1"], "instrument_count": 1}],
               "capture_commit_journal": COMMIT_JOURNAL_NAME,
               "capture_durability": runtime.coordinator.durability_manifest(),
               "capture_storage": runtime.coordinator.storage_manifest()},
    )
    result = write_manifest(root / "manifest.json", manifest)
    runtime.mark_finalized()
    return result
