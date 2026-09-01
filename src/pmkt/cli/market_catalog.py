"""Manual, public-read-only operation of the permanent market catalog."""

from __future__ import annotations

from typing import Annotated, Any, Literal, cast
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path

import typer

from pmkt._http import RequestPolicy
from pmkt.data.market_catalog import DiscoveryStream, MarketCatalogService
from pmkt.exchanges.kalshi.client import AsyncKalshiClient
from pmkt.exchanges.polymarket.gamma import GammaClient


markets_app = typer.Typer(
    help="Discover, refresh, inspect, and promote the non-trading market catalog."
)

# A complete census can run for tens of minutes. Preserve its in-memory pagination
# state across a temporary network or DNS outage instead of discarding every page
# after the default short request retry window.
MARKET_CATALOG_REQUEST_POLICY = RequestPolicy(
    max_retries=20,
    backoff_base_s=5.0,
    backoff_max_s=60.0,
    max_retry_after_s=300.0,
)


def _cutoff(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        explicit = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise typer.BadParameter("must be an ISO-8601 UTC timestamp") from exc
    if explicit.tzinfo is None or explicit.utcoffset() is None:
        raise typer.BadParameter("must include an explicit UTC offset")
    return explicit.astimezone(timezone.utc)


def _show(value: object) -> None:
    typer.echo(json.dumps(value, indent=2, sort_keys=True, default=str))


@markets_app.command("discover-new")
def discover_new_cmd(
    stream: Annotated[
        str | None,
        typer.Option(
            "--stream",
            help="polymarket, kalshi-conventional, or kalshi-mve.",
        ),
    ] = None,
    all_streams: Annotated[
        bool, typer.Option("--all", help="Run all three discovery streams in order.")
    ] = False,
    market_root: Annotated[
        Path, typer.Option(help="Catalog root containing current/history/releases.")
    ] = Path("data/markets"),
    bootstrap_cutoff: Annotated[
        str | None,
        typer.Option(
            help="Required ISO-8601 cutoff only when history bootstrap evidence is absent."
        ),
    ] = None,
    overlap_seconds: Annotated[
        int | None,
        typer.Option(help="Override the stream-specific safety overlap."),
    ] = None,
    max_pages: Annotated[
        int, typer.Option(help="Safety cap per cursor lane.")
    ] = 10_000,
    no_publish: Annotated[
        bool,
        typer.Option(
            "--no-publish",
            help="Validate public collection without publishing a release or pointer.",
        ),
    ] = False,
) -> None:
    """Discover markets created since each stream's overlap watermark."""
    valid = ("polymarket", "kalshi-conventional", "kalshi-mve")
    if all_streams == (stream is not None):
        raise typer.BadParameter("choose exactly one of --stream or --all")
    if max_pages < 1:
        raise typer.BadParameter("must be at least 1", param_hint="--max-pages")
    if overlap_seconds is not None and overlap_seconds < 0:
        raise typer.BadParameter("must be nonnegative", param_hint="--overlap-seconds")
    requested = valid if all_streams else (str(stream),)
    if any(item not in valid for item in requested):
        raise typer.BadParameter(
            "must be polymarket, kalshi-conventional, or kalshi-mve",
            param_hint="--stream",
        )
    service = MarketCatalogService(market_root)

    async def run() -> list[dict[str, object]]:
        outputs: list[dict[str, object]] = []
        for item in requested:
            stream_name = cast(DiscoveryStream, item)
            client_context = (
                GammaClient(request_policy=MARKET_CATALOG_REQUEST_POLICY)
                if stream_name == "polymarket"
                else AsyncKalshiClient(request_policy=MARKET_CATALOG_REQUEST_POLICY)
            )
            async with client_context as client:
                result = await service.discover(
                    stream_name,
                    bootstrap_cutoff=_cutoff(bootstrap_cutoff),
                    overlap_seconds=overlap_seconds,
                    max_pages=max_pages,
                    publish=not no_publish,
                    client=client,
                )
            outputs.append(result)
        return outputs

    _show(asyncio.run(run()))


@markets_app.command("status")
def status_cmd(
    market_root: Annotated[
        Path, typer.Option(help="Catalog root containing current/history/releases.")
    ] = Path("data/markets"),
    deep: Annotated[
        bool,
        typer.Option(
            "--deep",
            help="Also verify full SHA-256 content hashes for history artifacts.",
        ),
    ] = False,
) -> None:
    """Validate pointers and report catalog/cache freshness."""
    _show(MarketCatalogService(market_root).status(deep=deep))


@markets_app.command("refresh-current")
def refresh_current_cmd(
    scope: Annotated[
        str, typer.Option(help="standard, or all (including daily Kalshi MVE).")
    ] = "standard",
    market_root: Annotated[
        Path, typer.Option(help="Catalog root containing current/history/releases.")
    ] = Path("data/markets"),
    bootstrap_cutoff: Annotated[
        str | None,
        typer.Option(
            help=(
                "Fallback ISO-8601 UTC cutoff for the first --scope all refresh "
                "when neither current nor history evidence exists."
            )
        ),
    ] = None,
    max_pages: Annotated[
        int, typer.Option(help="Safety cap per cursor lane.")
    ] = 10_000,
) -> None:
    """Publish a complete current nonterminal market census."""
    if scope not in {"standard", "all"}:
        raise typer.BadParameter("must be standard or all", param_hint="--scope")
    if max_pages < 1:
        raise typer.BadParameter("must be at least 1", param_hint="--max-pages")

    async def run() -> dict[str, Any]:
        async with GammaClient(
            request_policy=MARKET_CATALOG_REQUEST_POLICY
        ) as polymarket, AsyncKalshiClient(
            request_policy=MARKET_CATALOG_REQUEST_POLICY
        ) as kalshi:
            return await MarketCatalogService(market_root).refresh_current(
                scope=cast(Literal["standard", "all"], scope),
                bootstrap_cutoff=_cutoff(bootstrap_cutoff),
                max_pages=max_pages,
                polymarket_client=polymarket,
                kalshi_client=kalshi,
            )

    result = asyncio.run(run())
    _show(result)


@markets_app.command("promote-history")
def promote_history_cmd(
    market_root: Annotated[
        Path, typer.Option(help="Catalog root containing current/history/releases.")
    ] = Path("data/markets"),
) -> None:
    """Hard-link the last history release and promote only new market keys."""
    _show(MarketCatalogService(market_root).promote_history())


@markets_app.command("compact-history")
def compact_history_cmd(
    market_root: Annotated[
        Path, typer.Option(help="Catalog root containing current/history/releases.")
    ] = Path("data/markets"),
    force: Annotated[
        bool, typer.Option(help="Run even when no compaction threshold is due.")
    ] = False,
) -> None:
    """Write a new latest-row base; never delete prior releases or missing markets."""

    async def run() -> dict[str, Any]:
        async with GammaClient(
            request_policy=MARKET_CATALOG_REQUEST_POLICY
        ) as polymarket, AsyncKalshiClient(
            request_policy=MARKET_CATALOG_REQUEST_POLICY
        ) as kalshi:
            return await MarketCatalogService(market_root).compact_history(
                force=force,
                polymarket_client=polymarket,
                kalshi_client=kalshi,
            )

    _show(asyncio.run(run()))


__all__ = ["markets_app"]
