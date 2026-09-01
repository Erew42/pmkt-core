from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pmkt._http import request_with_retry  # noqa: E402
from pmkt.tokens import extract_token_ids  # noqa: E402

DEFAULT_EXAMPLES_ROOT = ROOT / "generated" / "openapi" / "examples"
DEFAULT_MANIFEST_NAME = "manifest.json"

DEFAULT_GAMMA_BASE = "https://gamma-api.polymarket.com"
DEFAULT_CLOB_BASE = "https://clob.polymarket.com"
DEFAULT_TIMEOUT_S = 20.0
DEFAULT_MAX_RETRIES = 4
USER_AGENT = "pmkt-openapi-examples/0.1"


class UpdateError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh OpenAPI examples")
    parser.add_argument("--gamma-base-url", default=DEFAULT_GAMMA_BASE)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--token-id", default=None, help="Override token id for CLOB calls.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_EXAMPLES_ROOT),
        help="Directory to write example payloads.",
    )
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def resolve_output_dir(output_dir: str) -> Path:
    root = Path(output_dir)
    if not root.is_absolute():
        root = ROOT / root
    return root


def save_example(path: str, payload: Any, stamp: str, examples_root: Path) -> Path:
    name = path.lstrip("/").replace("/", "_") or "root"
    dest_dir = examples_root / name
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"{stamp}.json"
    dest_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return dest_path


def find_clob_token(
    gamma_client: httpx.Client,
    clob_client: httpx.Client,
    max_retries: int,
) -> str:
    response = request_with_retry(
        gamma_client,
        "GET",
        "/markets",
        params={"limit": 50, "offset": 0, "closed": "false"},
        max_retries=max_retries,
    )
    if response.status_code != 200:
        raise UpdateError(f"Gamma /markets returned {response.status_code}")
    markets = response.json()
    if not isinstance(markets, list):
        raise UpdateError("Gamma /markets response is not a list.")
    tokens = extract_token_ids(markets)
    if not tokens:
        raise UpdateError("No token_id values found in Gamma /markets response.")

    for token in tokens:
        r = request_with_retry(
            clob_client,
            "GET",
            "/book",
            params={"token_id": token},
            max_retries=max_retries,
        )
        if r.status_code == 200:
            return token
    raise UpdateError("No token_id with an orderbook found using closed=false markets.")


def fetch_json(
    client: httpx.Client,
    path: str,
    params: dict[str, Any] | None,
    max_retries: int,
) -> Any:
    response = request_with_retry(client, "GET", path, params=params, max_retries=max_retries)
    response.raise_for_status()
    try:
        return response.json()
    except ValueError as exc:
        raise UpdateError(f"{path} did not return JSON") from exc


def display_path(path: Path) -> str:
    try:
        rel_path = path.relative_to(ROOT)
    except ValueError:
        return path.as_posix()
    return rel_path.as_posix()


def write_manifest(manifest_path: Path, manifest: dict[str, Any]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")


def run() -> int:
    args = parse_args()
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    timeout = httpx.Timeout(args.timeout)
    stamp = utc_stamp()
    examples_root = resolve_output_dir(args.output_dir)
    examples_root.mkdir(parents=True, exist_ok=True)
    manifest_path = examples_root / DEFAULT_MANIFEST_NAME
    generated_at = utc_iso()

    with httpx.Client(
        base_url=args.gamma_base_url.rstrip("/"),
        timeout=timeout,
        headers=headers,
        follow_redirects=True,
    ) as gamma_client, httpx.Client(
        base_url=args.clob_base_url.rstrip("/"),
        timeout=timeout,
        headers=headers,
        follow_redirects=True,
    ) as clob_client:
        token_id = args.token_id
        if not token_id:
            token_id = find_clob_token(gamma_client, clob_client, args.max_retries)

        examples: dict[str, dict[str, Any]] = {}

        def record_example(path: str, payload: Any, params: dict[str, Any]) -> None:
            file_path = save_example(path, payload, stamp, examples_root)
            examples[path] = {
                "path": path,
                "method": "get",
                "params": params,
                "file": file_path,
                "bytes": file_path.stat().st_size,
            }

        markets_params = {"limit": 50, "offset": 0, "closed": "false"}
        markets = fetch_json(
            gamma_client,
            "/markets",
            params=markets_params,
            max_retries=args.max_retries,
        )
        record_example("/markets", markets, markets_params)

        events_params = {"limit": 50, "offset": 0, "closed": "false"}
        events = fetch_json(
            gamma_client,
            "/events",
            params=events_params,
            max_retries=args.max_retries,
        )
        record_example("/events", events, events_params)

        book_params = {"token_id": token_id}
        book = fetch_json(
            clob_client,
            "/book",
            params=book_params,
            max_retries=args.max_retries,
        )
        record_example("/book", book, book_params)

        price_params = {"token_id": token_id, "side": "BUY"}
        price = fetch_json(
            clob_client,
            "/price",
            params=price_params,
            max_retries=args.max_retries,
        )
        record_example("/price", price, price_params)

        midpoint_params = {"token_id": token_id}
        midpoint = fetch_json(
            clob_client,
            "/midpoint",
            params=midpoint_params,
            max_retries=args.max_retries,
        )
        record_example("/midpoint", midpoint, midpoint_params)

        prices_history_params = {"market": token_id, "interval": "1d"}
        prices_history = fetch_json(
            clob_client,
            "/prices-history",
            params=prices_history_params,
            max_retries=args.max_retries,
        )
        record_example(
            "/prices-history",
            prices_history,
            prices_history_params,
        )

    manifest_examples = []
    for path in sorted(examples):
        entry = examples[path]
        file_path = entry["file"]
        try:
            rel_file = file_path.relative_to(examples_root)
        except ValueError:
            rel_file = file_path
        manifest_examples.append(
            {
                "path": entry["path"],
                "method": entry["method"],
                "params": entry["params"],
                "file": rel_file.as_posix(),
                "bytes": entry["bytes"],
            }
        )
    manifest = {
        "schema_version": 1,
        "generated_at_utc": generated_at,
        "token_id": token_id,
        "gamma_base_url": args.gamma_base_url,
        "clob_base_url": args.clob_base_url,
        "examples": manifest_examples,
    }
    write_manifest(manifest_path, manifest)

    print(f"Saved examples for token_id={token_id}")
    for path in sorted(examples):
        rel = display_path(examples[path]["file"])
        print(f"- {path}: {rel}")
    print(f"Manifest: {display_path(manifest_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
