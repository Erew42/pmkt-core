from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Optional

import typer

from pmkt.data.storage.parquet import read_parquet, write_parquet
from pmkt.market_structure.discovery import discover_structures, structures_to_group_records


def build_groups(
    out: Annotated[Path, typer.Option(help="Output parquet path.")],
    structures: Annotated[
        Optional[Path], typer.Option(help="Structures JSON path.")
    ] = None,
    markets: Annotated[
        Optional[Path], typer.Option(help="Markets parquet path (will run discovery).")
    ] = None,
    members_out: Annotated[
        Optional[Path], typer.Option(help="Optional group members parquet path.")
    ] = None,
    structures_out: Annotated[
        Optional[Path],
        typer.Option(help="Optional structures JSON path when using --markets."),
    ] = None,
    max_families: Annotated[
        Optional[int], typer.Option(help="Optional cap on number of families to process.")
    ] = None,
    max_members: Annotated[
        Optional[int], typer.Option(help="Optional cap on members per family/structure.")
    ] = None,
):
    """Build market groups parquet from structure JSON."""
    if not structures and not markets:
        print("Error: build-groups requires --structures or --markets", flush=True)
        raise typer.Exit(code=1)

    if structures:
        with structures.open("r", encoding="utf-8") as handle:
            artifact = json.load(handle)
    else:
        if markets is None:
            raise typer.BadParameter("--markets is required when --structures is not provided")
        markets_df = read_parquet(markets)
        artifact = discover_structures(
            markets_df,
            markets_path=str(markets),
            max_families=max_families,
            max_members=max_members,
        )
        if structures_out:
            structures_out.parent.mkdir(parents=True, exist_ok=True)
            with structures_out.open("w", encoding="utf-8") as handle:
                json.dump(
                    artifact,
                    handle,
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=True,
                    default=str,
                )
            print(f"Wrote market structures to {structures_out}")

    group_records, member_records = structures_to_group_records(artifact)

    import pandas as pd

    groups_df = pd.DataFrame(group_records)
    members_df = pd.DataFrame(member_records)

    groups_path = write_parquet(groups_df, out, overwrite=True)
    print(f"Wrote {len(groups_df)} groups to {groups_path}")

    final_members_out = members_out if members_out else out.with_name("market_group_members.parquet")
    members_path = write_parquet(members_df, final_members_out, overwrite=True)
    print(f"Wrote {len(members_df)} group members to {members_path}")

    _print_structure_summary(artifact)


def discover_structures_cmd(
    markets: Annotated[Path, typer.Option(help="Markets parquet path.")],
    out: Annotated[
        Path, typer.Option(help="Output JSON path.")
    ] = Path("data/silver/market_structures.v1.json"),
    features: Annotated[Optional[Path], typer.Option(help="Optional features parquet path.")] = None,
    max_families: Annotated[
        Optional[int], typer.Option(help="Optional cap on number of families to process.")
    ] = None,
    max_members: Annotated[
        Optional[int], typer.Option(help="Optional cap on members per family/structure.")
    ] = None,
):
    """Discover market structure JSON."""
    markets_df = read_parquet(markets)
    features_df = None
    if features:
        features_df = read_parquet(features)

    artifact = discover_structures(
        markets_df,
        features=features_df,
        markets_path=str(markets),
        features_path=str(features) if features else None,
        max_families=max_families,
        max_members=max_members,
    )

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        json.dump(
            artifact,
            handle,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            default=str,
        )

    print(f"Wrote market structures to {out}")
    _print_structure_summary(artifact, markets_df=markets_df)


def _print_structure_summary(artifact: dict, markets_df=None) -> None:
    families = artifact.get("families", [])
    slice_count = sum(len(family.get("time_slices", [])) for family in families)
    structures = [
        structure
        for family in families
        for slice_item in family.get("time_slices", [])
        for structure in slice_item.get("structures", [])
    ]
    counts: dict[str, int] = {}
    for structure in structures:
        key = str(structure.get("structure_type", "unknown"))
        counts[key] = counts.get(key, 0) + 1

    warning_counts: dict[str, int] = {}
    for structure in structures:
        for warning in structure.get("warnings", []) or []:
            warning_key = str(warning)
            warning_counts[warning_key] = warning_counts.get(warning_key, 0) + 1

    print(f"Families: {len(families)}")
    print(f"Time slices: {slice_count}")
    if counts:
        print("Structures by type:")
        for key in sorted(counts):
            print(f"  {key}: {counts[key]}")
    else:
        print("Structures by type: none")
    if warning_counts:
        print("Warnings by category:")
        for warning in sorted(warning_counts, key=lambda item: warning_counts[item], reverse=True):
            print(f"  {warning}: {warning_counts[warning]}")
    else:
        print("Warnings by category: none")

    if markets_df is not None and hasattr(markets_df, "columns"):
        if "token_ids" in markets_df.columns:
            total = len(markets_df)
            if total:
                token_counts = markets_df["token_ids"].apply(_has_tokens).sum()
                percent = token_counts / total * 100
                print(f"Token coverage: {percent:.1f}% ({token_counts}/{total})")
            else:
                print("Token coverage: n/a (0 markets)")
        else:
            print("Token coverage: n/a (token_ids missing)")


def _has_tokens(value) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple, set)):
        return len(value) > 0
    if isinstance(value, dict):
        return bool(value)
    if hasattr(value, "__len__") and not isinstance(value, (str, bytes)):
        try:
            return len(value) > 0
        except TypeError:
            return False
    return True
