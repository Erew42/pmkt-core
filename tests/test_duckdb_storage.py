import pandas as pd
import pytest

from pmkt.data.storage.duckdb import query_parquet, validate_view_name


def test_query_parquet_registers_view(tmp_path) -> None:
    path = tmp_path / "markets.parquet"
    pd.DataFrame(
        [
            {"token_id": "a", "mid": 0.4},
            {"token_id": "b", "mid": 0.6},
        ]
    ).to_parquet(path, index=False)

    result = query_parquet(
        "select count(*) as n, avg(mid) as avg_mid from markets",
        {"markets": path},
    )

    assert result.loc[0, "n"] == 2
    assert result.loc[0, "avg_mid"] == pytest.approx(0.5)


def test_query_parquet_supports_multiple_files(tmp_path) -> None:
    first = tmp_path / "part1.parquet"
    second = tmp_path / "part2.parquet"
    pd.DataFrame([{"token_id": "a", "mid": 0.4}]).to_parquet(first, index=False)
    pd.DataFrame([{"token_id": "b", "mid": 0.6}]).to_parquet(second, index=False)

    result = query_parquet(
        "select token_id from metrics order by token_id",
        {"metrics": [first, second]},
    )

    assert result["token_id"].tolist() == ["a", "b"]


def test_query_parquet_supports_glob_paths(tmp_path) -> None:
    first = tmp_path / "part1.parquet"
    second = tmp_path / "part2.parquet"
    pd.DataFrame([{"token_id": "a", "mid": 0.4}]).to_parquet(first, index=False)
    pd.DataFrame([{"token_id": "b", "mid": 0.6}]).to_parquet(second, index=False)

    result = query_parquet(
        "select token_id from metrics order by token_id",
        {"metrics": str(tmp_path / "*.parquet")},
    )

    assert result["token_id"].tolist() == ["a", "b"]


def test_query_parquet_rejects_missing_file(tmp_path) -> None:
    missing = tmp_path / "missing.parquet"

    with pytest.raises(FileNotFoundError, match="does not exist"):
        query_parquet("select * from metrics", {"metrics": missing})


def test_query_parquet_rejects_unmatched_glob(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="matched no files"):
        query_parquet("select * from metrics", {"metrics": str(tmp_path / "*.parquet")})


def test_query_parquet_requires_dataset() -> None:
    with pytest.raises(ValueError, match="At least one Parquet dataset"):
        query_parquet("select 1", {})


def test_validate_view_name_rejects_unsafe_names() -> None:
    with pytest.raises(ValueError):
        validate_view_name("bad-name")
