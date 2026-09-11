from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
INVENTORY_PATH = ROOT / "tests" / "fixtures" / "public_api_inventory.json"
TIERS = {"supported", "retained_native", "inherited"}


def _inventory() -> dict[str, Any]:
    return json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))


def _resolve(path: str) -> object:
    module_name, separator, name = path.rpartition(".")
    assert separator, f"inventory path must name an attribute: {path}"
    return getattr(importlib.import_module(module_name), name)


def test_facade_export_inventory_has_no_unclassified_drift() -> None:
    """Force facade changes through an explicit compatibility-tier decision."""

    inventory = _inventory()
    assert set(inventory["tier_meanings"]) == TIERS
    for module_name, contract in inventory["facades"].items():
        module = importlib.import_module(module_name)
        expected = contract["exports"]
        actual = list(module.__all__)

        assert sorted(actual) == expected, module_name
        assert len(actual) == len(set(actual)), module_name
        assert contract["default_tier"] in TIERS

        overrides = contract.get("tier_overrides", {})
        assert set(overrides) <= set(expected), module_name
        assert set(overrides.values()) <= TIERS, module_name

        # Resolve every export, including lazy facades. Being present here records
        # its tier; it does not promote inherited names into supported workflows.
        for name in expected:
            namespace: dict[str, object] = {}
            exec(f"from {module_name} import {name}", namespace)
            assert namespace[name] is not None


def test_direct_public_imports_resolve_and_have_explicit_tiers() -> None:
    inventory = _inventory()
    entries = inventory["direct_imports"]

    assert len({entry["path"] for entry in entries}) == len(entries)
    assert {entry["tier"] for entry in entries} <= TIERS
    for entry in entries:
        assert _resolve(entry["path"]) is not None


def test_package_root_remains_version_only() -> None:
    inventory = _inventory()

    assert inventory["facades"]["pmkt"]["exports"] == ["__version__"]
