from __future__ import annotations

import pytest

from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.streaming.connection_partitions import (
    build_connection_partitions,
    subscription_plan_affinity_keys_by_instrument,
    subscription_plan_relation_ids_by_instrument,
)


def test_connection_partitions_preserve_existing_plan_shards() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-a",
                subscribed_instruments=("a", "b"),
                relation_ids=("r-a",),
            ),
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-b",
                subscribed_instruments=("c",),
                relation_ids=("r-b",),
            ),
        ],
        max_message_age_ms=123,
        max_valid_book_age_ms=456,
    )

    partitions = build_connection_partitions(
        supervisor,
        venue="polymarket",
        instruments=("a", "b", "c"),
    )

    assert [(item.shard_id, item.instruments) for item in partitions] == [
        ("pm-a", ("a", "b")),
        ("pm-b", ("c",)),
    ]
    assert partitions[0].relation_ids == ("r-a",)
    assert partitions[1].relation_ids == ("r-b",)
    assert partitions[0].supervisor.max_message_age_ms == 123
    assert partitions[0].supervisor.max_valid_book_age_ms == 456


def test_connection_partitions_split_with_exact_relation_ownership() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-main",
                subscribed_instruments=("a", "b", "c", "d", "e"),
                relation_ids=("r1", "r2", "r3"),
            )
        ]
    )

    partitions = build_connection_partitions(
        supervisor,
        venue="kalshi",
        instruments=("a", "b", "c", "d", "e"),
        max_instruments=2,
        relation_ids_by_instrument={
            "a": ("r1",),
            "b": ("r1", "r2"),
            "c": ("r2",),
            "d": (),
            "e": ("r3",),
        },
    )

    assert [(item.shard_id, item.instruments) for item in partitions] == [
        ("kx-main-c000", ("a", "d")),
        ("kx-main-c001", ("b", "e")),
        ("kx-main-c002", ("c",)),
    ]
    assert [item.relation_ids for item in partitions] == [
        ("r1",),
        ("r1", "r2", "r3"),
        ("r2",),
    ]
    assert [item.supervisor.shard_metadata() for item in partitions] == [
        [
            {
                "venue": "kalshi",
                "shard_id": "kx-main-c000",
                "instrument_count": 2,
                "relation_count": 1,
                "subscribed_instruments": ["a", "d"],
                "relation_ids": ["r1"],
            }
        ],
        [
            {
                "venue": "kalshi",
                "shard_id": "kx-main-c001",
                "instrument_count": 2,
                "relation_count": 3,
                "subscribed_instruments": ["b", "e"],
                "relation_ids": ["r1", "r2", "r3"],
            }
        ],
        [
            {
                "venue": "kalshi",
                "shard_id": "kx-main-c002",
                "instrument_count": 1,
                "relation_count": 1,
                "subscribed_instruments": ["c"],
                "relation_ids": ["r2"],
            }
        ],
    ]


def test_connection_partitions_reject_ambiguous_split_relations() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-main",
                subscribed_instruments=("a", "b"),
                relation_ids=("r1",),
            )
        ]
    )

    with pytest.raises(ValueError, match="relation_ids_by_instrument"):
        build_connection_partitions(
            supervisor,
            venue="polymarket",
            instruments=("a", "b"),
            max_instruments=1,
        )


def test_connection_partitions_require_exact_supervisor_selection() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-main",
                subscribed_instruments=("a", "b"),
            )
        ]
    )

    with pytest.raises(ValueError, match="exactly match"):
        build_connection_partitions(
            supervisor,
            venue="polymarket",
            instruments=("a",),
        )


def test_connection_partitions_keep_affinity_groups_indivisible() -> None:
    instruments = ("a-yes", "a-no", "b-yes", "b-no", "c-yes", "c-no")
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-main",
                subscribed_instruments=instruments,
            )
        ]
    )

    partitions = build_connection_partitions(
        supervisor,
        venue="polymarket",
        instruments=instruments,
        max_instruments=3,
        affinity_key_by_instrument={
            "a-yes": "market-a",
            "a-no": "market-a",
            "b-yes": "market-b",
            "b-no": "market-b",
            "c-yes": "market-c",
            "c-no": "market-c",
        },
    )

    assert [partition.instruments for partition in partitions] == [
        ("a-yes", "a-no"),
        ("b-yes", "b-no"),
        ("c-yes", "c-no"),
    ]
    owners = {
        instrument: partition.shard_id
        for partition in partitions
        for instrument in partition.instruments
    }
    assert owners["a-yes"] == owners["a-no"]
    assert owners["b-yes"] == owners["b-no"]
    assert owners["c-yes"] == owners["c-no"]


def test_connection_partitions_reject_oversized_affinity_group() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-main",
                subscribed_instruments=("a", "b", "c"),
            )
        ]
    )

    with pytest.raises(ValueError, match="affinity group exceeds"):
        build_connection_partitions(
            supervisor,
            venue="polymarket",
            instruments=("a", "b", "c"),
            max_instruments=2,
            affinity_key_by_instrument={
                "a": "market-a",
                "b": "market-a",
                "c": "market-a",
            },
        )


def test_connection_partitions_split_probe_scale_without_market_overlap() -> None:
    instruments = tuple(
        f"market-{market_index}-{outcome}"
        for market_index in range(500)
        for outcome in ("yes", "no")
    )
    affinity = {
        instrument: instrument.rsplit("-", 1)[0] for instrument in instruments
    }
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-main",
                subscribed_instruments=instruments,
            )
        ]
    )

    partitions = build_connection_partitions(
        supervisor,
        venue="polymarket",
        instruments=instruments,
        max_instruments=500,
        affinity_key_by_instrument=affinity,
    )

    assert [len(partition.instruments) for partition in partitions] == [500, 500]
    first_markets = {affinity[item] for item in partitions[0].instruments}
    second_markets = {affinity[item] for item in partitions[1].instruments}
    assert len(first_markets) == len(second_markets) == 250
    assert first_markets.isdisjoint(second_markets)


def test_subscription_plan_relation_index() -> None:
    assert subscription_plan_relation_ids_by_instrument(
        {
            "polymarket_assets": [
                {"asset_id": "a", "match_ids": ["r2", "r1", "r1"]},
                {"asset_id": "b", "match_ids": []},
            ]
        },
        venue="polymarket",
    ) == {"a": ("r1", "r2"), "b": ()}


def test_subscription_plan_affinity_index() -> None:
    assert subscription_plan_affinity_keys_by_instrument(
        {
            "polymarket_assets": [
                {"asset_id": "a-yes", "market_key": "market-a"},
                {"asset_id": "a-no", "market_key": "market-a"},
                {"asset_id": "b-yes", "condition_id": "condition-b"},
            ]
        },
        venue="polymarket",
    ) == {
        "a-yes": "market-a",
        "a-no": "market-a",
        "b-yes": "condition-b",
    }


def test_subscription_plan_affinity_index_rejects_conflicts() -> None:
    with pytest.raises(ValueError, match="multiple affinity keys"):
        subscription_plan_affinity_keys_by_instrument(
            {
                "polymarket_assets": [
                    {"asset_id": "a", "market_key": "market-a"},
                    {"asset_id": "a", "market_key": "market-b"},
                ]
            },
            venue="polymarket",
        )
