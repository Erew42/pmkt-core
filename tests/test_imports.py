from __future__ import annotations

import ast
import importlib
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

PUBLIC_PACKAGES = {
    "cli",
    "data",
    "exchanges",
    "market_structure",
    "resolution",
    "streaming",
    "text",
}

REMOVED_PRIVATE_MODULES = (
    "pmkt.auth",
    "pmkt.cross_platform",
    "pmkt.execution",
    "pmkt.matching",
    "pmkt.opportunities",
    "pmkt.strategies",
    "pmkt.tracking",
    "pmkt.exchanges.kalshi.auth",
    "pmkt.exchanges.polymarket.sdk",
)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_top_level_pmkt_package_allowlist() -> None:
    package_root = SRC / "pmkt"
    found = {
        path.name
        for path in package_root.iterdir()
        if path.is_dir()
        and not path.name.startswith("__")
        and (path / "__init__.py").exists()
    }

    assert found == PUBLIC_PACKAGES


def test_core_data_layer_does_not_import_venue_clients() -> None:
    violations = [
        f"{path.relative_to(ROOT)} -> {module}"
        for path in (SRC / "pmkt" / "data").rglob("*.py")
        for module in _imports(path)
        if module == "pmkt.exchanges" or module.startswith("pmkt.exchanges.")
    ]

    assert violations == []


def test_public_modules_import() -> None:
    for module_name in (
        "pmkt",
        "pmkt.cli",
        "pmkt.config",
        "pmkt.data.canonical",
        "pmkt.data.features",
        "pmkt.data.market_data",
        "pmkt.data.storage.duckdb",
        "pmkt.exchanges.kalshi.client",
        "pmkt.exchanges.polymarket.clob",
        "pmkt.exchanges.polymarket.gamma",
        "pmkt.market_structure.discovery",
        "pmkt.resolution",
        "pmkt.streaming.collector",
        "pmkt.streaming.supervisor",
        "pmkt.text.normalization",
    ):
        importlib.import_module(module_name)


def test_removed_private_modules_are_not_importable() -> None:
    for module_name in REMOVED_PRIVATE_MODULES:
        assert importlib.util.find_spec(module_name) is None


def test_package_star_import_stays_lightweight() -> None:
    namespace: dict[str, object] = {}
    exec("from pmkt import *", namespace)

    assert sorted(name for name in namespace if name != "__builtins__") == ["__version__"]
