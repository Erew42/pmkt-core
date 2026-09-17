from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from pmkt.streaming.legacy.recovery import recover_stream_run


def recover_stream_run_cmd(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False, resolve_path=True)],
    finalize: Annotated[
        bool,
        typer.Option("--finalize", help="Finalize journaled groups into a crashed run manifest."),
    ] = False,
) -> None:
    """Inspect a crashed capture; report only unless --finalize is supplied."""
    if (run_dir / "recording.sqlite").exists():
        from pmkt.streaming.recording_store import export_recording, inspect_recording
        result = export_recording(run_dir) if finalize else inspect_recording(run_dir)
        typer.echo(json.dumps(result, indent=2, sort_keys=True))
        return
    report = recover_stream_run(run_dir, finalize=finalize)
    typer.echo(json.dumps(report.to_mapping(), indent=2, sort_keys=True))


__all__ = ["recover_stream_run_cmd"]
