from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "docs" / "api" / "upstream_sources.json"
DEFAULT_SNAPSHOT_ROOT = ROOT / "generated" / "upstream_snapshots"

SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style|noscript)[^>]*>.*?</\\1>")
PRE_RE = re.compile(r"(?is)<pre[^>]*>(.*?)</pre>")
CODE_RE = re.compile(r"(?is)<code[^>]*>(.*?)</code>")
TAG_RE = re.compile(r"(?is)<[^>]+>")


@dataclass
class Source:
    name: str
    url: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_timestamp(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def format_filename_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d_%H%M%S")


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config: {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_last_metadata(index_path: Path) -> dict[str, dict[str, str]]:
    latest: dict[str, dict[str, str]] = {}
    if not index_path.exists():
        return latest
    with index_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = entry.get("name")
            if not name:
                continue
            latest[name] = {
                "etag": entry.get("etag") or "",
                "last_modified": entry.get("last_modified") or "",
            }
    return latest


def normalize_code_block(raw: str) -> str:
    text = html.unescape(raw)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]
    return "\n".join(lines).strip("\n")


def extract_text(html_text: str) -> str:
    if not html_text:
        return ""
    cleaned = SCRIPT_STYLE_RE.sub(" ", html_text)
    code_blocks: list[str] = []

    def capture(match: re.Match[str]) -> str:
        raw = match.group(1)
        raw = TAG_RE.sub("", raw)
        code_blocks.append(normalize_code_block(raw))
        return f" __CODE_BLOCK_{len(code_blocks) - 1}__ "

    cleaned = PRE_RE.sub(capture, cleaned)
    cleaned = CODE_RE.sub(capture, cleaned)
    text = TAG_RE.sub(" ", cleaned)
    text = html.unescape(text)
    text = re.sub(r"\\s+", " ", text).strip()
    for idx, code in enumerate(code_blocks):
        placeholder = f"__CODE_BLOCK_{idx}__"
        if code:
            text = text.replace(placeholder, f"\n{code}\n")
        else:
            text = text.replace(placeholder, "")
    text = re.sub(r"[ \\t]+\\n", "\n", text)
    text = re.sub(r"\\n[ \\t]+", "\n", text)
    text = re.sub(r"\\n{3,}", "\n\n", text)
    return text.strip()


def append_index_entry(entry: dict[str, Any], index_path: Path) -> None:
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with index_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=True) + "\n")


def build_sources(config: dict[str, Any]) -> list[Source]:
    sources = []
    for item in config.get("sources", []):
        name = item.get("name")
        url = item.get("url")
        if not name or not url:
            continue
        sources.append(Source(name=name, url=url))
    return sources


def fetch_source(
    client: httpx.Client,
    source: Source,
    cached: dict[str, str],
    snapshot_root: Path,
    index_path: Path,
) -> None:
    request_headers: dict[str, str] = {}
    if cached.get("etag"):
        request_headers["If-None-Match"] = cached["etag"]
    if cached.get("last_modified"):
        request_headers["If-Modified-Since"] = cached["last_modified"]
    timestamp = utc_now()
    timestamp_str = format_timestamp(timestamp)
    filename_stamp = format_filename_timestamp(timestamp)

    try:
        response = client.get(source.url, headers=request_headers)
    except httpx.RequestError as exc:
        print(f"ERROR {source.name}: {exc}", file=sys.stderr)
        append_index_entry(
            {
                "timestamp_utc": timestamp_str,
                "name": source.name,
                "url": source.url,
                "status_code": None,
                "sha256": None,
                "etag": cached.get("etag") or None,
                "last_modified": cached.get("last_modified") or None,
                "bytes": 0,
            },
            index_path,
        )
        return

    etag = response.headers.get("etag") or cached.get("etag") or None
    last_modified = response.headers.get("last-modified") or cached.get("last_modified") or None

    if response.status_code == 200:
        content = response.content
        sha256 = hashlib.sha256(content).hexdigest()
        bytes_len = len(content)
        dest_dir = snapshot_root / source.name
        dest_dir.mkdir(parents=True, exist_ok=True)
        html_path = dest_dir / f"{filename_stamp}.html"
        latest_html = dest_dir / "latest.html"
        html_path.write_bytes(content)
        latest_html.write_bytes(content)
        text = extract_text(response.text)
        txt_path = dest_dir / f"{filename_stamp}.txt"
        latest_txt = dest_dir / "latest.txt"
        txt_path.write_text(text, encoding="utf-8")
        latest_txt.write_text(text, encoding="utf-8")
        print(f"{source.name}: saved {html_path.name}")
    elif response.status_code == 304:
        sha256 = None
        bytes_len = 0
        print(f"{source.name}: not modified")
    else:
        sha256 = None
        bytes_len = len(response.content)
        print(f"{source.name}: status {response.status_code}")

    append_index_entry(
        {
            "timestamp_utc": timestamp_str,
            "name": source.name,
            "url": source.url,
            "status_code": response.status_code,
            "sha256": sha256,
            "etag": etag,
            "last_modified": last_modified,
            "bytes": bytes_len,
        },
        index_path,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync upstream docs snapshots")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_SNAPSHOT_ROOT),
        help="Directory to write upstream snapshots and index.",
    )
    return parser.parse_args()


def resolve_output_dir(output_dir: str) -> Path:
    root = Path(output_dir)
    if not root.is_absolute():
        root = ROOT / root
    return root


def main() -> int:
    args = parse_args()
    config = load_config()
    user_agent = config.get("user_agent") or "pmkt-doc-sync/0.1"
    timeout_s = config.get("timeout_s") or 20
    sources = build_sources(config)
    if not sources:
        print("No sources configured.", file=sys.stderr)
        return 1

    snapshot_root = resolve_output_dir(args.output_dir)
    snapshot_root.mkdir(parents=True, exist_ok=True)
    index_path = snapshot_root / "index.jsonl"
    last_metadata = load_last_metadata(index_path)
    headers = {"User-Agent": user_agent}
    with httpx.Client(
        headers=headers,
        timeout=timeout_s,
        follow_redirects=True,
    ) as client:
        for source in sources:
            cached = last_metadata.get(source.name, {})
            fetch_source(client, source, cached, snapshot_root, index_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
