from __future__ import annotations

import typer

from pmkt.cli.features import compute_features_cmd
from pmkt.cli.history import backfill_venue_history_cmd, record_topbooks_cmd
from pmkt.cli.ingest import ingest_kalshi_markets, ingest_markets, ingest_markets_keyset
from pmkt.cli.market_catalog import markets_app
from pmkt.cli.query import query_cmd
from pmkt.cli.reconstruction import reconstruct_book_tape_cmd
from pmkt.cli.recovery import recover_stream_run_cmd
from pmkt.cli.resolution import resolve_market_resolutions_cmd
from pmkt.cli.schema import dataset_app, schema_app
from pmkt.cli.streaming import (
    collect_books,
    collect_kalshi_books,
    stream_books,
    stream_kalshi_books,
)
from pmkt.cli.structures import build_groups, discover_structures_cmd


app = typer.Typer(help="Prediction-market data utilities.")
app.add_typer(schema_app, name="schema")
app.add_typer(dataset_app, name="dataset")
app.add_typer(markets_app, name="markets")

app.command("ingest-markets")(ingest_markets)
app.command("ingest-markets-keyset")(ingest_markets_keyset)
app.command("ingest-kalshi-markets")(ingest_kalshi_markets)
app.command("query")(query_cmd)
app.command("collect-books")(collect_books)
app.command("collect-kalshi-books")(collect_kalshi_books)
app.command("stream-books")(stream_books)
app.command("stream-kalshi-books")(stream_kalshi_books)
app.command("compute-features")(compute_features_cmd)
app.command("backfill-venue-history")(backfill_venue_history_cmd)
app.command("record-topbooks")(record_topbooks_cmd)
app.command("build-groups")(build_groups)
app.command("discover-structures")(discover_structures_cmd)
app.command("resolve-market-resolutions")(resolve_market_resolutions_cmd)
app.command("recover-stream-run")(recover_stream_run_cmd)
app.command("reconstruct-book-tape")(reconstruct_book_tape_cmd)


def main() -> None:
    app()
