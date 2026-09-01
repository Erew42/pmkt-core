from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent / "src"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sync_upstream_docs import format_timestamp, utc_now  # noqa: E402
from pmkt._http import format_url, request_with_retry  # noqa: E402
from pmkt.tokens import extract_token_ids  # noqa: E402

DEFAULT_GAMMA_BASE = "https://gamma-api.polymarket.com"
DEFAULT_CLOB_BASE = "https://clob.polymarket.com"
DEFAULT_TIMEOUT_S = 20.0
DEFAULT_MAX_RETRIES = 4
DEFAULT_MAX_PAGES = 3
USER_AGENT = "pmkt-contract-check/0.1"

class CheckError(Exception):
    pass


@dataclass
class CheckResult:
    name: str
    url: str
    status_code: int | None
    ok: bool
    skipped: bool = False
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket API contract checks")
    parser.add_argument("--gamma-base-url", default=DEFAULT_GAMMA_BASE)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args()


def json_or_error(response: httpx.Response, name: str) -> Any:
    try:
        return response.json()
    except ValueError as exc:
        raise CheckError(f"{name}: response is not valid JSON") from exc


def summarize_body(response: httpx.Response, limit: int = 200) -> str:
    text = response.text.strip().replace("\n", " ")
    return text if len(text) <= limit else f"{text[:limit]}..."


def fetch_gamma_markets(
    client: httpx.Client,
    offset: int,
    max_retries: int,
) -> list[Any]:
    response = request_with_retry(
        client,
        "GET",
        "/markets",
        params={"limit": 50, "offset": offset},
        max_retries=max_retries,
    )
    if response.status_code != 200:
        raise CheckError(
            f"Gamma /markets returned {response.status_code}: {summarize_body(response)}"
        )
    markets = json_or_error(response, "gamma markets")
    if not isinstance(markets, list):
        raise CheckError("Gamma /markets response is not a list.")
    return markets


def select_token_with_orderbook(
    client: httpx.Client,
    tokens: list[str],
    max_retries: int,
) -> tuple[str | None, CheckResult | None]:
    for token in tokens:
        response = request_with_retry(
            client,
            "GET",
            "/book",
            params={"token_id": token},
            max_retries=max_retries,
        )
        if response.status_code == 404:
            continue
        if response.status_code != 200:
            error = summarize_body(response)
            return None, CheckResult(
                name="book",
                url=format_url(client.base_url, "/book"),
                status_code=response.status_code,
                ok=False,
                error=f"Unexpected status {response.status_code}: {error}",
            )
        try:
            data = json_or_error(response, "book")
        except CheckError as exc:
            return None, CheckResult(
                name="book",
                url=format_url(client.base_url, "/book"),
                status_code=response.status_code,
                ok=False,
                error=str(exc),
            )
        if not isinstance(data, dict):
            return None, CheckResult(
                name="book",
                url=format_url(client.base_url, "/book"),
                status_code=response.status_code,
                ok=False,
                error="Expected JSON object",
            )
        for key in ("bids", "asks"):
            if key not in data:
                return None, CheckResult(
                    name="book",
                    url=format_url(client.base_url, "/book"),
                    status_code=response.status_code,
                    ok=False,
                    error=f"Missing key: {key}",
                )
        return token, CheckResult(
            name="book",
            url=format_url(client.base_url, "/book"),
            status_code=response.status_code,
            ok=True,
        )
    return None, None


def run_check(
    client: httpx.Client,
    name: str,
    path: str,
    params: dict[str, Any],
    expected_type: type,
    required_keys: tuple[str, ...],
    max_retries: int,
    skip_on_status: set[int] | None = None,
) -> CheckResult:
    url = format_url(client.base_url, path)
    try:
        response = request_with_retry(
            client,
            "GET",
            path,
            params=params,
            max_retries=max_retries,
        )
    except httpx.RequestError as exc:
        return CheckResult(name=name, url=url, status_code=None, ok=False, error=str(exc))

    if skip_on_status and response.status_code in skip_on_status:
        return CheckResult(
            name=name,
            url=url,
            status_code=response.status_code,
            ok=True,
            skipped=True,
            error=summarize_body(response),
        )

    if response.status_code != 200:
        return CheckResult(
            name=name,
            url=url,
            status_code=response.status_code,
            ok=False,
            error=f"Unexpected status {response.status_code}: {summarize_body(response)}",
        )

    try:
        data = json_or_error(response, name)
    except CheckError as exc:
        return CheckResult(
            name=name,
            url=url,
            status_code=response.status_code,
            ok=False,
            error=str(exc),
        )
    if not isinstance(data, expected_type):
        return CheckResult(
            name=name,
            url=url,
            status_code=response.status_code,
            ok=False,
            error=f"Expected {expected_type.__name__}",
        )
    if isinstance(data, dict):
        for key in required_keys:
            if key not in data:
                return CheckResult(
                    name=name,
                    url=url,
                    status_code=response.status_code,
                    ok=False,
                    error=f"Missing key: {key}",
                )
    return CheckResult(name=name, url=url, status_code=response.status_code, ok=True)


def run() -> int:
    args = parse_args()
    timestamp = format_timestamp(utc_now())
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    timeout = httpx.Timeout(args.timeout)

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
        results: list[CheckResult] = []
        try:
            markets = fetch_gamma_markets(gamma_client, offset=0, max_retries=args.max_retries)
        except CheckError as exc:
            print(str(exc), file=sys.stderr)
            return 1

        tokens = extract_token_ids(markets)
        if not tokens:
            print(
                "No token_id values found in Gamma /markets response. Update the extractor.",
                file=sys.stderr,
            )
            return 1

        token_id, book_result = select_token_with_orderbook(
            clob_client,
            tokens,
            max_retries=args.max_retries,
        )

        pages_checked = 1
        while token_id is None and pages_checked < max(args.max_pages, 1):
            offset = pages_checked * 50
            try:
                markets = fetch_gamma_markets(
                    gamma_client,
                    offset=offset,
                    max_retries=args.max_retries,
                )
            except CheckError:
                break
            tokens = extract_token_ids(markets)
            if tokens:
                token_id, book_result = select_token_with_orderbook(
                    clob_client,
                    tokens,
                    max_retries=args.max_retries,
                )
            pages_checked += 1

        if token_id is None:
            if book_result:
                results.append(book_result)
                message = book_result.error or "CLOB /book failed."
            else:
                message = (
                    "No orderbook found for token_ids from Gamma /markets "
                    f"(checked {pages_checked} page(s))."
                )
            print(message, file=sys.stderr)
            return 1
        results.append(book_result)

        results.append(
            run_check(
                clob_client,
                name="price",
                path="/price",
                params={"token_id": token_id, "side": "BUY"},
                expected_type=dict,
                required_keys=("price",),
                max_retries=args.max_retries,
            )
        )
        results.append(
            run_check(
                clob_client,
                name="midpoint",
                path="/midpoint",
                params={"token_id": token_id},
                expected_type=dict,
                required_keys=("mid",),
                max_retries=args.max_retries,
            )
        )
        results.append(
            run_check(
                clob_client,
                name="prices-history",
                path="/prices-history",
                params={"market": token_id, "interval": "1d"},
                expected_type=dict,
                required_keys=("history",),
                max_retries=args.max_retries,
                skip_on_status={404},
            )
        )

    ok = all(result.ok for result in results)
    summary = {
        "timestamp_utc": timestamp,
        "token_id": token_id,
        "ok": ok,
        "checks": [
            {
                "name": result.name,
                "url": result.url,
                "status_code": result.status_code,
                "ok": result.ok,
                "skipped": result.skipped,
                "error": result.error,
            }
            for result in results
        ],
    }

    if args.json_output:
        print(json.dumps(summary, ensure_ascii=True))
    else:
        print(f"Contract check @ {timestamp}")
        print(f"token_id: {token_id}")
        for result in results:
            status = "ok" if result.ok else "fail"
            if result.skipped:
                status = "skipped"
            detail = f" ({result.error})" if result.error else ""
            print(f"- {result.name} {status} [{result.status_code}]{detail}")
        if ok:
            print("All checks passed.")
        else:
            print("One or more checks failed.", file=sys.stderr)

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(run())
