from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from pmkt.streaming.recovery import recover_stream_run


def recover_stream_run_cmd(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False, resolve_path=True)],
    finalize: Annotated[
        bool,
        typer.Option("--finalize", help="Finalize journaled groups into a crashed run manifest."),
    ] = False,
) -> None:
    """Inspect a crashed capture; report only unless --finalize is supplied."""
    report = recover_stream_run(run_dir, finalize=finalize)
    typer.echo(json.dumps(report.to_mapping(), indent=2, sort_keys=True))


__all__ = ["recover_stream_run_cmd"]
