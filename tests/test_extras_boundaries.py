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
