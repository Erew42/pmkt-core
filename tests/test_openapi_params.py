import json
from pathlib import Path

from pmkt.exchanges.polymarket.clob import AsyncClobClient


ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "openapi" / "polymarket.min.json"
CONTRACT_PATH = ROOT / "openapi" / "param_contract.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def collect_params(spec: dict, path: str, method: str) -> dict[str, bool]:
    params = spec.get("paths", {}).get(path, {}).get(method, {}).get("parameters", [])
    if not isinstance(params, list):
        return {}
    collected: dict[str, bool] = {}
    for param in params:
        if not isinstance(param, dict):
            continue
        name = param.get("name")
        if not name:
            continue
        collected[name] = bool(param.get("required", False))
    return collected


def test_openapi_parameters_match_contract() -> None:
    spec = load_json(SPEC_PATH)
    contract = load_json(CONTRACT_PATH)

    for path, methods in contract.get("paths", {}).items():
        for method, details in methods.items():
            expected = details.get("params", {})
            actual = collect_params(spec, path, method)
            assert set(actual.keys()) == set(expected.keys())
            for name, expectations in expected.items():
                assert actual[name] == bool(expectations.get("required", False))


def test_public_clob_read_endpoints_are_in_openapi_contract() -> None:
    spec = load_json(SPEC_PATH)
    paths = spec.get("paths", {})

    for path in ("/book", "/price", "/midpoint", "/prices-history"):
        assert path in paths


def test_trading_methods_are_not_exposed_without_openapi_contract() -> None:
    assert not hasattr(AsyncClobClient, "post_order")
    assert not hasattr(AsyncClobClient, "cancel_order")
