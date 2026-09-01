from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Optional

import pandas as pd
import typer

from pmkt.data.manifests import validate_run_manifest
from pmkt.data.registry import TableSpec, get_table_spec, infer_table_spec, list_table_specs
from pmkt.data.validation import infer_and_validate_frame, quality_flag_counts, validate_frame
from pmkt.streaming.capture_archive import archive_capture

schema_app = typer.Typer(help="Inspect canonical schema registry.")
dataset_app = typer.Typer(help="Validate and summarize local datasets.")


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(path, lines=suffix == ".jsonl")
    raise typer.BadParameter("dataset path must be .parquet, .csv, .json, or .jsonl")


def _spec_payload(spec: TableSpec) -> dict[str, object]:
    return {
        "name": spec.name,
        "version": spec.version,
        "primary_key": list(spec.primary_key),
        "description": spec.description,
        "fields": [
            {
                "name": field.name,
                "dtype": field.dtype,
                "nullable": field.nullable,
                "unit": field.unit,
                "allowed_values": list(field.allowed_values) if field.allowed_values else None,
                "description": field.description,
            }
            for field in spec.fields
        ],
    }


@schema_app.command("list")
def schema_list() -> None:
    """List registered canonical schemas."""
    for spec in list_table_specs():
        typer.echo(f"{spec.version}\t{spec.name}\t{len(spec.fields)} fields")


@schema_app.command("show")
def schema_show(
    schema: Annotated[str, typer.Argument(help="Schema name or version, e.g. topbook.v1.")],
) -> None:
    """Show one schema as JSON."""
    typer.echo(json.dumps(_spec_payload(get_table_spec(schema)), indent=2, sort_keys=True))


@dataset_app.command("validate")
def dataset_validate(
    path: Annotated[Path, typer.Argument(help="Dataset path to validate.")],
    schema: Annotated[
        Optional[str],
        typer.Option("--schema", "-s", help="Schema name/version. Inferred when omitted."),
    ] = None,
    strict: Annotated[
        bool,
        typer.Option("--strict/--allow-extra-columns", help="Fail validation on extra columns."),
    ] = False,
) -> None:
    """Validate a dataset against a registered schema."""
    df = _read_table(path)
    try:
        if schema:
            report = validate_frame(df, get_table_spec(schema), strict=strict)
        else:
            report = infer_and_validate_frame(df, strict=strict)
    except KeyError as exc:
        typer.echo(f"FAILED: {exc}")
        raise typer.Exit(code=1) from exc
    if report.ok:
        typer.echo(f"OK {report.schema}: {report.row_count} rows")
        return
    typer.echo(f"FAILED {report.schema}: {report.row_count} rows")
    for error in report.errors:
        typer.echo(f"- {error}")
    raise typer.Exit(code=1)


@dataset_app.command("validate-manifest")
def dataset_validate_manifest(
    path: Annotated[Path, typer.Argument(help="Run or collection manifest JSON path.")],
) -> None:
    """Validate manifest-referenced artifacts."""
    report = validate_run_manifest(path)
    if report.ok:
        typer.echo(f"OK manifest: {len(report.datasets)} datasets")
        for warning in report.all_warnings:
            typer.echo(f"Warning: {warning}")
        return
    typer.echo(f"FAILED manifest: {path}")
    for error in report.all_errors:
        typer.echo(f"- {error}")
    for warning in report.all_warnings:
        typer.echo(f"Warning: {warning}")
    raise typer.Exit(code=1)


@dataset_app.command("stats")
def dataset_stats(
    path: Annotated[Path, typer.Argument(help="Dataset path to summarize.")],
    schema: Annotated[
        Optional[str],
        typer.Option("--schema", "-s", help="Schema name/version. Inferred when omitted."),
    ] = None,
) -> None:
    """Print basic dataset stats and schema version coverage."""
    df = _read_table(path)
    spec = None
    try:
        if schema:
            spec = get_table_spec(schema)
        elif "schema_version" in df.columns and not df.empty:
            versions = sorted(set(df["schema_version"].dropna().astype(str)))
            if len(versions) == 1:
                spec = get_table_spec(versions[0])
        if spec is None:
            spec = infer_table_spec(df.columns)
    except KeyError:
        spec = None
    payload = {
        "path": str(path),
        "rows": len(df),
        "columns": list(df.columns),
        "schema": spec.version if spec else None,
        "schema_versions": (
            df["schema_version"].dropna().astype(str).value_counts().sort_index().to_dict()
            if "schema_version" in df.columns
            else {}
        ),
        "quality_flag_counts": quality_flag_counts(df),
    }
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


@dataset_app.command("archive-run")
def dataset_archive_run(
    manifest: Annotated[
        Path,
        typer.Argument(
            help=(
                "Finalized manifest.json or capture_connection_group.v1.json "
                "to archive."
            )
        ),
    ],
    out: Annotated[
        Optional[Path],
        typer.Option(
            "--out",
            help="Archive output path. Defaults to <run_dir>.pmkt.zip.",
        ),
    ] = None,
    delete_source: Annotated[
        bool,
        typer.Option(
            "--delete-source",
            help=(
                "Delete the unpacked run only after exact validation, archive "
                "verification, and sidecar publication. Restore is required for replay."
            ),
        ),
    ] = False,
    compress: Annotated[
        bool,
        typer.Option(
            "--compress",
            help=(
                "Use low-level DEFLATE. Usually slower with little benefit because "
                "Parquet payloads are already compressed."
            ),
        ),
    ] = False,
) -> None:
    """Collapse a finalized run or connection group into one verified archive."""

    try:
        result = archive_capture(
            manifest,
            archive_path=out,
            delete_source=delete_source,
            compress=compress,
        )
    except (FileExistsError, OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc), param_hint="manifest") from exc
    typer.echo(
        f"Archived {result.member_count} files to {result.archive_path} "
        f"({result.unpacked_bytes} unpacked bytes; "
        f"source_deleted={str(result.source_deleted).lower()})"
    )


__all__ = ["dataset_app", "schema_app"]
