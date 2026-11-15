"""Offline inspection of sealed raw WebSocket evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from pmkt.streaming.raw_reader import RawCaptureError, inspect_session


def inspect_raw_capture(
    session: Annotated[Path, typer.Argument(help="Raw capture session directory.")],
) -> None:
    """Verify sealed chunks and print counts, gaps and incomplete files as JSON.

    Exit 0: verified, no recorded gaps. Exit 1: verification failed or nothing
    to verify. Exit 2: verified, with missing chunks (including an archived
    prefix), incomplete files, or recorded sequence/disk-pause gaps.
    """
    try:
        report = inspect_session(session)
    except RawCaptureError as exc:
        typer.echo(f"FAILED: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if not report["sealed_chunk_count"]:
        raise typer.Exit(1)
    if (
        report["chunk_gaps"]
        or report["incomplete_files"]
        or report["local_event_counts"].get("seq_gap", 0)
        or report["local_event_counts"].get("disk_pause_gap", 0)
    ):
        raise typer.Exit(2)
