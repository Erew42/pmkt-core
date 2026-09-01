from pmkt.market_structure.discovery import discover_structures


def _family_from_artifact(artifact: dict) -> dict:
    families = artifact.get("families", [])
    assert families
    return families[0]


def test_time_slicing_distinguishes_deadline_and_point() -> None:
    markets = [
        {
            "market_id": "1",
            "question": "Strike Iran by Jan 31",
            "close_time": "2026-02-01",
            "token_ids": ["t1", "t2"],
        },
        {
            "market_id": "2",
            "question": "Strike Iran by Mar 31",
            "close_time": "2026-04-01",
            "token_ids": ["t3", "t4"],
        },
        {
            "market_id": "3",
            "question": "Strike Iran on Jan 31",
            "close_time": "2026-02-01",
            "token_ids": ["t5", "t6"],
        },
    ]

    artifact = discover_structures(markets)
    family = _family_from_artifact(artifact)
    time_slices = family.get("time_slices", [])
    kinds = {slice_item["time_kind"] for slice_item in time_slices}
    assert "deadline" in kinds
    assert "point" in kinds
    deadline_values = sorted(
        slice_item["time_value"]
        for slice_item in time_slices
        if slice_item["time_kind"] == "deadline"
    )
    assert deadline_values == ["2026-01-31", "2026-03-31"]


def test_ladder_detection_and_ordering() -> None:
    markets = [
        {"market_id": "1", "question": "Deportations <250k", "token_ids": ["t1", "t2"]},
        {"market_id": "2", "question": "Deportations 250k-500k", "token_ids": ["t3", "t4"]},
        {"market_id": "3", "question": "Deportations 500k-750k", "token_ids": ["t5", "t6"]},
        {"market_id": "4", "question": "Deportations >=750k", "token_ids": ["t7", "t8"]},
    ]

    artifact = discover_structures(markets)
    family = _family_from_artifact(artifact)
    time_slices = family.get("time_slices", [])
    assert time_slices
    structures = time_slices[0].get("structures", [])
    assert structures
    structure = structures[0]
    assert structure["structure_type"] in {"range_ladder", "threshold_ladder"}

    members = structure["members"]
    interval_kinds = [member["interval"]["kind"] for member in members if member.get("interval")]
    assert interval_kinds[0] in {"lt", "lte"}
    assert interval_kinds[-1] in {"gt", "gte"}
    range_lows = [
        member["interval"]["low"]
        for member in members
        if member.get("interval") and member["interval"]["kind"] == "range"
    ]
    assert range_lows == sorted(range_lows)


def test_ids_are_stable() -> None:
    markets = [
        {"market_id": "1", "question": "Will it rain by Jan 31", "close_time": "2026-02-01"},
        {"market_id": "2", "question": "Will it rain by Mar 31", "close_time": "2026-04-01"},
    ]

    first = discover_structures(markets)
    second = discover_structures(markets)
    family_first = _family_from_artifact(first)
    family_second = _family_from_artifact(second)
    assert family_first["family_id"] == family_second["family_id"]

    slices_first = [slice_item["slice_id"] for slice_item in family_first["time_slices"]]
    slices_second = [slice_item["slice_id"] for slice_item in family_second["time_slices"]]
    assert slices_first == slices_second


def test_canonicalization_groups_minor_variations() -> None:
    markets = [
        {"market_id": "1", "question": "Will it rain before Jan 31", "close_time": "2026-02-01"},
        {"market_id": "2", "question": "Will it rain by Jan 31", "close_time": "2026-02-01"},
    ]

    artifact = discover_structures(markets)
    assert len(artifact.get("families", [])) == 1


def test_overlap_warnings_only_when_overlap() -> None:
    singleton = [
        {"market_id": "1", "question": "Between 0 and 10", "token_ids": ["t1", "t2"]}
    ]
    artifact_single = discover_structures(singleton)
    structure_single = artifact_single["families"][0]["time_slices"][0]["structures"][0]
    assert "interval_overlap_detected" not in (structure_single.get("warnings") or [])

    overlapping = [
        {"market_id": "1", "question": "Between 0 and 10", "token_ids": ["t1", "t2"]},
        {"market_id": "2", "question": "Between 5 and 15", "token_ids": ["t3", "t4"]},
    ]
    artifact_overlap = discover_structures(overlapping)
    structure_overlap = artifact_overlap["families"][0]["time_slices"][0]["structures"][0]
    assert "interval_overlap_detected" in (structure_overlap.get("warnings") or [])


def test_interval_invariants_and_units() -> None:
    markets = [
        {"market_id": "1", "question": "Revenue at least $1.5m", "token_ids": ["t1", "t2"]},
        {"market_id": "2", "question": "Approval less than 60%", "token_ids": ["t3", "t4"]},
    ]

    artifact = discover_structures(markets)
    intervals = [
        member["interval"]
        for family in artifact["families"]
        for slice_item in family["time_slices"]
        for structure in slice_item["structures"]
        for member in structure["members"]
        if member.get("interval") is not None
    ]
    labels = [interval["label"] for interval in intervals]
    assert any("$" in label and "m" in label for label in labels)
    assert any("%" in label for label in labels)

    lt_interval = next(interval for interval in intervals if interval["kind"] == "lt")
    assert lt_interval["low"] is None
    assert lt_interval["low_inclusive"] is None
