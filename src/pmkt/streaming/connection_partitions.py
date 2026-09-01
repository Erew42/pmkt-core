from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Iterable, Mapping, Sequence

from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor


@dataclass(frozen=True)
class ConnectionPartition:
    """One exact feed shard owned by one WebSocket collector connection."""

    venue: str
    shard_id: str
    instruments: tuple[str, ...]
    relation_ids: tuple[str, ...]
    supervisor: LiveFeedSupervisor


def build_connection_partitions(
    supervisor: LiveFeedSupervisor,
    *,
    venue: str,
    instruments: Sequence[str],
    max_instruments: int | None = None,
    relation_ids_by_instrument: Mapping[str, Iterable[str]] | None = None,
    affinity_key_by_instrument: Mapping[str, str] | None = None,
) -> tuple[ConnectionPartition, ...]:
    """Bind logical feed shards to bounded, non-overlapping connections.

    Existing plan-shard boundaries are never merged.  A boundary may be split into
    deterministic child shard IDs when ``max_instruments`` is supplied.  When an
    affinity mapping is supplied, instruments sharing a non-empty key remain on
    the same connection. Exact relation ownership is required when a
    relation-bearing shard is split; copying the parent's relation list onto every
    child would make health provenance ambiguous.
    """

    if venue not in {"polymarket", "kalshi"}:
        raise ValueError(f"unsupported venue {venue!r}")
    if max_instruments is not None and (
        type(max_instruments) is not int or max_instruments <= 0
    ):
        raise ValueError("max_instruments must be a positive integer when provided")

    ordered = tuple(dict.fromkeys(str(value).strip() for value in instruments))
    if not ordered or any(not value for value in ordered):
        raise ValueError("instruments must contain non-empty unique values")
    selected = set(ordered)
    by_base_shard: dict[str, list[str]] = {
        shard.shard_id: [] for shard in supervisor.venue_shards(venue)
    }
    if not by_base_shard:
        raise ValueError(f"feed supervisor has no {venue} shards")
    for instrument in ordered:
        try:
            shard = supervisor.shard_for_instrument(venue, instrument)
        except KeyError as exc:
            raise ValueError(
                f"{venue} instrument is outside the feed supervisor: {instrument}"
            ) from exc
        by_base_shard[shard.shard_id].append(instrument)

    supervisor_instruments = {
        instrument
        for shard in supervisor.venue_shards(venue)
        for instrument in shard.subscribed_instruments
    }
    if selected != supervisor_instruments:
        extra = sorted(supervisor_instruments - selected)
        raise ValueError(
            "connection partition selection must exactly match supervisor instruments"
            + (f": unselected {', '.join(extra[:5])}" if extra else "")
        )

    relation_map = {
        str(instrument): tuple(
            sorted(
                {
                    str(relation_id).strip()
                    for relation_id in relation_ids
                    if str(relation_id).strip()
                }
            )
        )
        for instrument, relation_ids in (relation_ids_by_instrument or {}).items()
    }
    affinity_map: dict[str, str] = {}
    for instrument, affinity_key in (affinity_key_by_instrument or {}).items():
        instrument_text = str(instrument).strip()
        affinity_text = str(affinity_key).strip() if affinity_key is not None else ""
        if instrument_text and affinity_text:
            affinity_map[instrument_text] = affinity_text
    partitions: list[ConnectionPartition] = []
    for base_shard in supervisor.venue_shards(venue):
        shard_instruments = by_base_shard[base_shard.shard_id]
        limit = max_instruments or len(shard_instruments)
        if affinity_map and max_instruments is not None:
            chunks = _affinity_chunks(
                shard_instruments,
                limit=limit,
                affinity_map=affinity_map,
            )
        else:
            partition_count = ceil(len(shard_instruments) / limit)
            # Stripe the stable plan order instead of taking contiguous slices.
            # Plan generation commonly clusters related or similarly active
            # markets; a contiguous tail can therefore become an almost silent
            # connection and trigger repeated freshness recovery. Stripes remain
            # deterministic and keep every partition at or below the requested
            # bound.
            chunks = tuple(
                tuple(shard_instruments[index::partition_count])
                for index in range(partition_count)
            )
        if len(chunks) > 1 and base_shard.relation_ids and not relation_map:
            raise ValueError(
                "relation_ids_by_instrument is required when splitting a "
                "relation-bearing feed shard"
            )
        for index, chunk in enumerate(chunks):
            shard_id = (
                base_shard.shard_id
                if len(chunks) == 1
                else f"{base_shard.shard_id}-c{index:03d}"
            )
            relation_ids = (
                tuple(
                    sorted(
                        {
                            relation_id
                            for instrument in chunk
                            for relation_id in relation_map.get(instrument, ())
                        }
                    )
                )
                if relation_map
                else base_shard.relation_ids
            )
            child_shard = FeedShardHealth(
                venue=venue,
                shard_id=shard_id,
                subscribed_instruments=chunk,
                relation_ids=relation_ids,
            )
            child_supervisor = LiveFeedSupervisor(
                [child_shard],
                preflight_report=supervisor.preflight_report,
                max_message_age_ms=supervisor.max_message_age_ms,
                max_valid_book_age_ms=supervisor.max_valid_book_age_ms,
            )
            partitions.append(
                ConnectionPartition(
                    venue=venue,
                    shard_id=shard_id,
                    instruments=chunk,
                    relation_ids=relation_ids,
                    supervisor=child_supervisor,
                )
            )
    if not partitions:
        raise ValueError("connection partitioning produced no shards")
    return tuple(partitions)


def _affinity_chunks(
    instruments: Sequence[str],
    *,
    limit: int,
    affinity_map: Mapping[str, str],
) -> tuple[tuple[str, ...], ...]:
    """Greedily balance indivisible affinity groups under a hard size cap."""

    grouped: dict[tuple[str, str], list[str]] = {}
    for instrument in instruments:
        affinity_key = affinity_map.get(instrument)
        group_key = (
            ("affinity", affinity_key)
            if affinity_key is not None
            else ("instrument", instrument)
        )
        grouped.setdefault(group_key, []).append(instrument)
    groups = tuple(grouped.values())
    for group_key, group in zip(grouped, groups, strict=True):
        if len(group) > limit:
            raise ValueError(
                "connection affinity group exceeds max_instruments: "
                f"{group_key[1]!r} has {len(group)}, cap is {limit}"
            )

    initial_count = ceil(len(instruments) / limit)
    bins: list[list[str]] = [[] for _ in range(initial_count)]
    loads = [0 for _ in range(initial_count)]
    for group in groups:
        candidates = [
            index
            for index, load in enumerate(loads)
            if load + len(group) <= limit
        ]
        if candidates:
            selected = min(candidates, key=lambda index: (loads[index], index))
        else:
            selected = len(bins)
            bins.append([])
            loads.append(0)
        bins[selected].extend(group)
        loads[selected] += len(group)
    return tuple(tuple(chunk) for chunk in bins if chunk)


def subscription_plan_relation_ids_by_instrument(
    plan: Mapping[str, object], *, venue: str
) -> dict[str, tuple[str, ...]]:
    if venue == "polymarket":
        section = "polymarket_assets"
        id_key = "asset_id"
    elif venue == "kalshi":
        section = "kalshi_market_tickers"
        id_key = "market_ticker"
    else:
        raise ValueError(f"unsupported venue {venue!r}")
    values = plan.get(section)
    if not isinstance(values, list):
        return {}
    result: dict[str, tuple[str, ...]] = {}
    for item in values:
        if not isinstance(item, Mapping):
            continue
        instrument = str(item.get(id_key) or "").strip()
        match_ids = item.get("match_ids")
        if not instrument or not isinstance(match_ids, list):
            continue
        result[instrument] = tuple(
            sorted(
                {
                    str(match_id).strip()
                    for match_id in match_ids
                    if str(match_id).strip()
                }
            )
        )
    return result


def subscription_plan_affinity_keys_by_instrument(
    plan: Mapping[str, object], *, venue: str
) -> dict[str, str]:
    """Return exact market affinity declared by a subscription plan."""

    if venue == "polymarket":
        section = "polymarket_assets"
        id_key = "asset_id"
        affinity_keys = ("market_key", "condition_id")
    elif venue == "kalshi":
        section = "kalshi_market_tickers"
        id_key = "market_ticker"
        affinity_keys = ("market_key", "event_ticker")
    else:
        raise ValueError(f"unsupported venue {venue!r}")
    values = plan.get(section)
    if not isinstance(values, list):
        return {}
    result: dict[str, str] = {}
    for item in values:
        if not isinstance(item, Mapping):
            continue
        instrument = str(item.get(id_key) or "").strip()
        affinity = next(
            (
                text
                for key in affinity_keys
                if (text := str(item.get(key) or "").strip())
            ),
            "",
        )
        if not instrument or not affinity:
            continue
        previous = result.get(instrument)
        if previous is not None and previous != affinity:
            raise ValueError(
                "subscription plan maps an instrument to multiple affinity keys: "
                f"{venue}/{instrument} -> {previous!r}, {affinity!r}"
            )
        result[instrument] = affinity
    return result


__all__ = [
    "ConnectionPartition",
    "build_connection_partitions",
    "subscription_plan_affinity_keys_by_instrument",
    "subscription_plan_relation_ids_by_instrument",
]
