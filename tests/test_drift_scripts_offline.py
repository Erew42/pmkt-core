from __future__ import annotations

import json
from pathlib import Path
import sys

import httpx


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import contract_check  # noqa: E402
import sync_upstream_docs  # noqa: E402
import update_openapi_examples  # noqa: E402


def test_contract_check_selects_later_token_with_orderbook_offline() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/book"
        if request.url.params.get("token_id") == "token-without-book":
            return httpx.Response(404, json={"error": "not found"})
        return httpx.Response(200, json={"bids": [], "asks": []})

    with httpx.Client(
        base_url="https://clob.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        token, result = contract_check.select_token_with_orderbook(
            client,
            ["token-without-book", "token-with-book"],
            max_retries=0,
        )

    assert token == "token-with-book"
    assert result is not None
    assert result.ok


def test_update_openapi_examples_finds_later_clob_token_offline() -> None:
    def gamma_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(
            200,
            json=[
                {
                    "id": "market-1",
                    "clobTokenIds": ["token-without-book", "token-with-book"],
                }
            ],
        )

    def clob_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/book"
        if request.url.params.get("token_id") == "token-without-book":
            return httpx.Response(404, json={"error": "not found"})
        return httpx.Response(200, json={"bids": [], "asks": []})

    with httpx.Client(
        base_url="https://gamma.test",
        transport=httpx.MockTransport(gamma_handler),
    ) as gamma_client, httpx.Client(
        base_url="https://clob.test",
        transport=httpx.MockTransport(clob_handler),
    ) as clob_client:
        token = update_openapi_examples.find_clob_token(
            gamma_client,
            clob_client,
            max_retries=0,
        )

    assert token == "token-with-book"


def test_sync_upstream_docs_fetch_source_writes_snapshot_offline(tmp_path) -> None:
    html = """
    <html>
      <body>
        <h1>API docs</h1>
        <pre>{"status":"ok"}</pre>
      </body>
    </html>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://docs.test/reference"
        return httpx.Response(
            200,
            content=html.encode("utf-8"),
            headers={"etag": "v1", "last-modified": "Fri, 05 Jun 2026 12:00:00 GMT"},
        )

    snapshot_root = tmp_path / "snapshots"
    index_path = snapshot_root / "index.jsonl"
    source = sync_upstream_docs.Source(
        name="docs-test",
        url="https://docs.test/reference",
    )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        sync_upstream_docs.fetch_source(
            client,
            source,
            cached={},
            snapshot_root=snapshot_root,
            index_path=index_path,
        )

    latest_text = snapshot_root / "docs-test" / "latest.txt"
    index_entry = json.loads(index_path.read_text(encoding="utf-8").splitlines()[0])

    assert latest_text.exists()
    assert "API docs" in latest_text.read_text(encoding="utf-8")
    assert '{"status":"ok"}' in latest_text.read_text(encoding="utf-8")
    assert index_entry["name"] == "docs-test"
    assert index_entry["status_code"] == 200
    assert index_entry["sha256"]
