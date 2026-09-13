from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess
import sys
import textwrap


ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"

OPTIONAL_IMPORT_ROOTS = ("duckdb", "numpy", "pandas", "pyarrow", "websockets")
MINIMAL_IMPORT_MODULES = (
    "pmkt",
    "pmkt._http",
    "pmkt.config",
    "pmkt.data.canonical",
    "pmkt.data.prices",
    "pmkt.data.types",
    "pmkt.exchanges.kalshi.client",
    "pmkt.exchanges.polymarket.clob",
    "pmkt.exchanges.polymarket.gamma",
    "pmkt.exchanges.polymarket.subgraph",
    "pmkt.exchanges.read_auth",
    "pmkt.models",
    "pmkt.pagination",
    "pmkt.text.normalization",
    "pmkt.text.taxonomy",
    "pmkt.tokens",
)


def _array_values(section: str, key: str) -> list[str]:
    values: list[str] = []
    current_section = ""
    collecting = False
    for line in PYPROJECT.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            current_section = stripped.strip("[]")
            collecting = False
            continue
        if current_section != section:
            continue
        if not collecting and stripped == f"{key} = [":
            collecting = True
            continue
        if collecting:
            if stripped == "]":
                break
            match = re.match(r'"([^"]+)"', stripped.rstrip(","))
            if match:
                values.append(match.group(1))
    return values


def _requirement_names(requirements: list[str]) -> set[str]:
    return {re.split(r"[<>=~!;\[]", item, maxsplit=1)[0] for item in requirements}


def test_feature_extras_are_core_only() -> None:
    text = PYPROJECT.read_text(encoding="utf-8")
    extras = {
        match.group(1)
        for match in re.finditer(r"^([a-z][a-z0-9-]*) = \[$", text, re.MULTILINE)
    }

    assert {"data", "storage", "streaming", "test"} <= extras
    assert extras.isdisjoint(
        {"analysis", "calibration", "dashboard", "nlp", "review-codex-sdk", "trading"}
    )
    assert _requirement_names(_array_values("project.optional-dependencies", "data")) == {
        "duckdb",
        "pandas",
        "pyarrow",
    }
    assert _requirement_names(_array_values("project.optional-dependencies", "storage")) == {
        "duckdb",
        "pandas",
        "pyarrow",
    }
    assert _requirement_names(_array_values("project.optional-dependencies", "streaming")) == {
        "pandas",
        "pyarrow",
        "websockets",
    }


def test_base_dependencies_exclude_feature_and_private_packages() -> None:
    core = _requirement_names(_array_values("project", "dependencies"))

    assert core.isdisjoint(
        {
            "cryptography",
            "duckdb",
            "pandas",
            "pyarrow",
            "py-builder-relayer-client",
            "py-clob-client-v2",
            "websockets",
        }
    )


def test_minimal_core_imports_do_not_require_optional_dependencies() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    code = f"""
        import importlib
        import importlib.abc
        import sys

        blocked = {OPTIONAL_IMPORT_ROOTS!r}

        class Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.', 1)[0] in blocked:
                    raise ModuleNotFoundError(fullname, name=fullname)
                return None

        sys.meta_path.insert(0, Blocker())
        for module_name in {MINIMAL_IMPORT_MODULES!r}:
            importlib.import_module(module_name)
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stderr


def test_catalog_timestamp_parsing_does_not_require_optional_dependencies() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    code = f"""
        import importlib.abc
        import sys
        from datetime import datetime, timedelta, timezone

        blocked = {OPTIONAL_IMPORT_ROOTS!r}
        attempted = []

        class Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.', 1)[0] in blocked:
                    attempted.append(fullname)
                    raise ModuleNotFoundError(fullname, name=fullname)
                return None

        sys.meta_path.insert(0, Blocker())
        from pmkt.data.market_catalog.fs import parse_timestamp

        fractions = (
            ('1', 100000), ('12', 120000), ('123', 123000),
            ('1234', 123400), ('12345', 123450), ('123456', 123456),
            ('1234567', 123456), ('12345678', 123456),
            ('123456789', 123456), ('123456789012', 123456),
            ('999999999', 999999),
        )
        offsets = (
            ('Z', timedelta()), ('z', timedelta()),
            ('+02:00', timedelta(hours=2)), ('+0200', timedelta(hours=2)),
            ('+02', timedelta(hours=2)), ('-05:30', -timedelta(hours=5, minutes=30)),
        )
        for fraction, microseconds in fractions:
            for offset, delta in offsets:
                text = '2026-09-12T11:59:20.' + fraction + offset
                expected = datetime(
                    2026, 9, 12, 11, 59, 20, microseconds, tzinfo=timezone.utc
                ) - delta
                parsed = parse_timestamp(text)
                assert parsed == expected, (text, parsed, expected)
                assert type(parsed) is datetime

        assert parse_timestamp('2026-09-12') == datetime(2026, 9, 12, tzinfo=timezone.utc)
        assert parse_timestamp('2026-09-12 11:59:20') == datetime(
            2026, 9, 12, 11, 59, 20, tzinfo=timezone.utc
        )
        for text in ('2026-99-12T11:59:20.1234567Z', '2026-09-12T11:59:20.1234567badZ'):
            assert parse_timestamp(text) is None, text
        assert not attempted, attempted
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_base_console_help_does_not_require_optional_dependencies() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    code = f"""
        import importlib.abc
        import sys

        blocked = {OPTIONAL_IMPORT_ROOTS!r}

        class Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.', 1)[0] in blocked:
                    raise ModuleNotFoundError(fullname, name=fullname)
                return None

        sys.meta_path.insert(0, Blocker())
        from pmkt.cli.entrypoint import main
        main(['--help'])
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "Usage: pmkt" in result.stdout


def test_data_query_does_not_import_streaming_dependencies() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    code = """
        import importlib.abc
        import sys

        class Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.', 1)[0] == 'websockets':
                    raise ModuleNotFoundError(fullname, name=fullname)
                return None

        sys.meta_path.insert(0, Blocker())
        from pmkt.cli.entrypoint import main
        main(['query', 'SELECT 1 AS ok'])
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout
    assert "1" in result.stdout


def test_streaming_command_names_the_missing_extra_without_network() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    code = """
        import importlib.abc
        import sys

        class Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split('.', 1)[0] == 'websockets':
                    raise ModuleNotFoundError(fullname, name=fullname)
                return None

        sys.meta_path.insert(0, Blocker())
        from pmkt.cli.entrypoint import main
        main(['stream-books', '--token-id', 'offline-test'])
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 1
    assert "pmkt[streaming]" in result.stdout + result.stderr
