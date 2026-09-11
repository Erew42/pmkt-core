"""Pinned, read-only access to immutable market-history catalogs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import json
import math
import os
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
import re
import time
from typing import Any, Literal, TYPE_CHECKING

from pmkt import __version__
from pmkt.data.market_catalog.reader import (
    CatalogOpenState,
    ValidationLevel,
    _is_network_path_text,
    _recorded_absolute,
    load_history_manifest,
    load_latest_history,
)
from pmkt.data.market_catalog.types import CatalogError, HISTORY_MANIFEST_SCHEMA
from pmkt.data.market_catalog.views import CATALOG_VIEW_CONTRACT_VERSION
from pmkt.errors import OptionalDependencyError, ResultLimitExceededError

if TYPE_CHECKING:
    import pyarrow as pa


CATALOG_REFERENCE_FORMAT_VERSION = "pmkt.catalog_reference.v1"

CatalogParameter = None | bool | int | float | str | bytes | date | datetime
ManifestIdentity = Literal[
    "history_pointer", "external_sha256", "internal_consistency", "saved_reference"
]


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(dict(value), indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode("utf-8")


def _bounded_arrow_batches(
    reader: Any, *, max_result_rows: int, max_result_bytes: int
) -> tuple[Any, list[Any]]:
    schema = reader.schema
    batches: list[Any] = []
    rows = 0
    result_bytes = 0
    for batch in reader:
        next_rows = rows + int(batch.num_rows)
        next_bytes = result_bytes + int(batch.nbytes)
        if next_rows > max_result_rows:
            batches.clear()
            raise ResultLimitExceededError(
                f"catalog query exceeded max_result_rows={max_result_rows}"
            )
        if next_bytes > max_result_bytes:
            batches.clear()
            raise ResultLimitExceededError(
                f"catalog query exceeded max_result_bytes={max_result_bytes}"
            )
        rows = next_rows
        result_bytes = next_bytes
        batches.append(batch)
    return schema, batches


def _table_from_batches(pa: Any, batches: list[Any], schema: Any) -> Any:
    return pa.Table.from_batches(batches, schema=schema)


@dataclass(frozen=True)
class CatalogReference:
    """Serializable identity of one manifest and its interpretation contract."""

    reference_format_version: str
    path_base: str
    manifest_locator: str
    manifest_sha256: str
    manifest_schema_version: str
    release_id: str
    dataset_family: str
    dataset_schema_versions: tuple[tuple[str, str], ...]
    view_contract_version: str
    identity_verification: ManifestIdentity

    def __post_init__(self) -> None:
        string_fields = {
            "reference_format_version": self.reference_format_version,
            "path_base": self.path_base,
            "manifest_locator": self.manifest_locator,
            "manifest_sha256": self.manifest_sha256,
            "manifest_schema_version": self.manifest_schema_version,
            "release_id": self.release_id,
            "dataset_family": self.dataset_family,
            "view_contract_version": self.view_contract_version,
            "identity_verification": self.identity_verification,
        }
        invalid_types = [
            name for name, value in string_fields.items() if not isinstance(value, str)
        ]
        if invalid_types:
            raise CatalogError(
                "catalog reference fields must be strings: "
                + ", ".join(invalid_types)
            )
        if self.reference_format_version != CATALOG_REFERENCE_FORMAT_VERSION:
            raise CatalogError(
                f"unsupported catalog reference version "
                f"{self.reference_format_version!r}"
            )
        if self.manifest_schema_version != HISTORY_MANIFEST_SCHEMA:
            raise CatalogError(
                f"unsupported catalog manifest schema "
                f"{self.manifest_schema_version!r}"
            )
        if self.view_contract_version != CATALOG_VIEW_CONTRACT_VERSION:
            raise CatalogError(
                f"unsupported catalog view contract {self.view_contract_version!r}"
            )
        if not (
            PureWindowsPath(self.path_base).is_absolute()
            or PurePosixPath(self.path_base).is_absolute()
        ):
            raise CatalogError("catalog reference path_base must be absolute")
        try:
            normalized_base = _recorded_absolute(self.path_base)
        except (TypeError, ValueError) as exc:
            raise CatalogError("catalog reference path_base is invalid") from exc
        if _is_network_path_text(self.path_base):
            raise CatalogError("catalog reference path_base must be a local filesystem path")
        object.__setattr__(self, "path_base", str(normalized_base))
        if not self.manifest_locator.strip():
            raise CatalogError("catalog reference has no manifest locator")
        if len(self.manifest_sha256) != 64 or any(
            character not in "0123456789abcdefABCDEF"
            for character in self.manifest_sha256
        ):
            raise CatalogError("catalog reference has an invalid manifest hash")
        if not self.release_id.strip():
            raise CatalogError("catalog reference has no release_id")
        if self.dataset_family != "market_history":
            raise CatalogError("catalog reference has unsupported dataset_family")
        if self.identity_verification not in {
            "history_pointer",
            "external_sha256",
            "internal_consistency",
            "saved_reference",
        }:
            raise CatalogError("catalog reference has invalid identity verification")
        if not isinstance(self.dataset_schema_versions, tuple) or any(
            not isinstance(item, tuple)
            or len(item) != 2
            or not all(isinstance(value, str) for value in item)
            for item in self.dataset_schema_versions
        ):
            raise CatalogError("catalog reference has invalid dataset schema versions")
        venues = [venue for venue, _schema in self.dataset_schema_versions]
        if venues != ["kalshi", "polymarket"]:
            raise CatalogError("catalog reference has invalid dataset schema versions")

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_family": self.dataset_family,
            "dataset_schema_versions": {
                venue: schema for venue, schema in self.dataset_schema_versions
            },
            "identity_verification": self.identity_verification,
            "manifest_locator": self.manifest_locator,
            "manifest_schema_version": self.manifest_schema_version,
            "manifest_sha256": self.manifest_sha256,
            "path_base": self.path_base,
            "reference_format_version": self.reference_format_version,
            "release_id": self.release_id,
            "view_contract_version": self.view_contract_version,
        }

    def write_json(self, path: str | Path, *, overwrite: bool = False) -> Path:
        """Write deterministic reference JSON with exclusive creation by default."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = _json_bytes(self.to_dict())
        if not overwrite:
            with target.open("xb") as handle:
                handle.write(payload)
            return target
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        try:
            temporary.write_bytes(payload)
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()
        return target

    @classmethod
    def read_json(cls, path: str | Path) -> CatalogReference:
        try:
            value = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CatalogError(f"catalog reference is not readable JSON: {path}") from exc
        if not isinstance(value, dict):
            raise CatalogError("catalog reference must be a JSON object")
        schemas = value.get("dataset_schema_versions")
        if not isinstance(schemas, dict):
            raise CatalogError("catalog reference has no dataset schema versions")
        try:
            return cls(
                reference_format_version=value["reference_format_version"],
                path_base=value["path_base"],
                manifest_locator=value["manifest_locator"],
                manifest_sha256=value["manifest_sha256"],
                manifest_schema_version=value["manifest_schema_version"],
                release_id=value["release_id"],
                dataset_family=value["dataset_family"],
                dataset_schema_versions=tuple(
                    (venue, schema)
                    for venue, schema in sorted(schemas.items())
                ),
                view_contract_version=value["view_contract_version"],
                identity_verification=value["identity_verification"],
            )
        except (KeyError, TypeError) as exc:
            if isinstance(exc, TypeError):
                raise CatalogError(
                    "catalog reference dataset schema versions are invalid"
                ) from exc
            raise CatalogError(
                f"catalog reference is missing {exc.args[0]}"
            ) from exc


@dataclass(frozen=True)
class CatalogValidationReport:
    validation_level: ValidationLevel
    identity_verification: ManifestIdentity
    performed_checks: tuple[str, ...]
    skipped_checks: tuple[str, ...]
    artifact_count: int
    parquet_file_count: int
    row_count: int
    elapsed_seconds: float
    validated_at_utc: datetime
    failures: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.failures


@dataclass(frozen=True)
class CatalogQueryResult:
    """A materialized bounded query result that outlives its snapshot."""

    _table: Any = field(repr=False)
    reference: CatalogReference
    validation_level: ValidationLevel
    sql: str
    parameters: tuple[CatalogParameter, ...]
    view_contract_version: str
    duckdb_version: str
    pyarrow_version: str
    package_version: str
    execution_seconds: float

    @property
    def row_count(self) -> int:
        return int(self._table.num_rows)

    @property
    def result_bytes(self) -> int:
        return int(self._table.nbytes)

    def to_arrow(self) -> pa.Table:
        return self._table

    def to_pandas(self) -> Any:
        return self._table.to_pandas()


class CatalogSnapshot:
    """One pinned catalog manifest with a private managed DuckDB connection."""

    def __init__(
        self,
        *,
        state: CatalogOpenState,
        reference: CatalogReference,
        validation: CatalogValidationReport,
    ) -> None:
        self._state = state
        self.reference = reference
        self.validation = validation
        self._connection: Any | None = None
        self._closed = False

    @staticmethod
    def _from_state(
        state: CatalogOpenState, *, recorded_path_base: str | Path | PurePath
    ) -> CatalogSnapshot:
        manifest = state.manifest
        schemas = tuple(
            sorted((artifact.venue, artifact.schema_version) for artifact in state.artifacts)
        )
        reference = CatalogReference(
            reference_format_version=CATALOG_REFERENCE_FORMAT_VERSION,
            path_base=str(_recorded_absolute(recorded_path_base)),
            manifest_locator=state.manifest_locator,
            manifest_sha256=state.manifest_sha256,
            manifest_schema_version=str(manifest["schema_version"]),
            release_id=str(manifest["release_id"]),
            dataset_family=str(manifest["dataset_family"]),
            dataset_schema_versions=schemas,
            view_contract_version=CATALOG_VIEW_CONTRACT_VERSION,
            identity_verification=state.identity_verification,  # type: ignore[arg-type]
        )
        report = CatalogValidationReport(
            validation_level=state.validation_level,
            identity_verification=state.identity_verification,  # type: ignore[arg-type]
            performed_checks=state.performed_checks,
            skipped_checks=state.skipped_checks,
            artifact_count=len(state.artifacts),
            parquet_file_count=state.parquet_file_count,
            row_count=state.row_count,
            elapsed_seconds=state.elapsed_seconds,
            validated_at_utc=datetime.now(timezone.utc),
        )
        return CatalogSnapshot(state=state, reference=reference, validation=report)

    @classmethod
    def open_latest_history(
        cls,
        market_root: str | Path,
        *,
        path_base: str | Path,
        validation: ValidationLevel = "full",
    ) -> CatalogSnapshot:
        state = load_latest_history(
            market_root, path_base=path_base, validation=validation
        )
        return cls._from_state(state, recorded_path_base=path_base)

    @classmethod
    def open_manifest(
        cls,
        manifest_path: str | Path,
        *,
        expected_sha256: str | None = None,
        path_base: str | Path,
        validation: ValidationLevel = "full",
    ) -> CatalogSnapshot:
        state = load_history_manifest(
            manifest_locator=str(manifest_path),
            expected_sha256=expected_sha256,
            recorded_path_base=path_base,
            validation=validation,
            identity_verification=(
                "external_sha256"
                if expected_sha256 is not None
                else "internal_consistency"
            ),
        )
        return cls._from_state(state, recorded_path_base=path_base)

    @classmethod
    def open(
        cls,
        reference: CatalogReference | str | Path,
        *,
        relocation: Mapping[str | Path, str | Path] | None = None,
        validation: ValidationLevel = "full",
    ) -> CatalogSnapshot:
        saved = (
            CatalogReference.read_json(reference)
            if isinstance(reference, (str, Path))
            else reference
        )
        if not isinstance(saved, CatalogReference):
            raise TypeError("reference must be CatalogReference or a JSON path")
        recorded_base = _recorded_absolute(saved.path_base)
        effective_base: str | Path = saved.path_base
        if relocation is not None:
            normalized = {
                _recorded_absolute(old): new
                for old, new in relocation.items()
            }
            if recorded_base not in normalized:
                raise CatalogError(
                    "relocation must map the reference's recorded path_base exactly"
                )
            effective_base = normalized[recorded_base]
        state = load_history_manifest(
            manifest_locator=saved.manifest_locator,
            expected_sha256=saved.manifest_sha256,
            recorded_path_base=recorded_base,
            effective_path_base=effective_base,
            validation=validation,
            identity_verification="saved_reference",
        )
        actual = cls._from_state(state, recorded_path_base=recorded_base)
        expected_identity = saved.to_dict()
        actual_identity = actual.reference.to_dict()
        actual_identity["identity_verification"] = saved.identity_verification
        if actual_identity != expected_identity:
            raise CatalogError("saved catalog reference disagrees with manifest identity")
        actual.reference = saved
        actual.validation = CatalogValidationReport(
            **{
                **actual.validation.__dict__,
                "identity_verification": "saved_reference",
            }
        )
        return actual

    def __enter__(self) -> CatalogSnapshot:
        if self._closed:
            raise RuntimeError("catalog snapshot is closed")
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self._closed = True

    @staticmethod
    def _runtime_versions() -> tuple[Any, Any]:
        try:
            import duckdb
        except ImportError as exc:
            raise OptionalDependencyError(
                "catalog queries require the 'data' extra: pip install 'pmkt[data]'"
            ) from exc
        try:
            import pyarrow as pa
        except ImportError as exc:
            raise OptionalDependencyError(
                "catalog queries require the 'data' extra: pip install 'pmkt[data]'"
            ) from exc
        match = re.match(r"^(\d+)\.(\d+)\.(\d+)", duckdb.__version__)
        if match is None:
            raise OptionalDependencyError(
                f"unsupported DuckDB version string {duckdb.__version__!r}; "
                "managed catalog queries require duckdb>=1.5.5"
            )
        duckdb_parts = tuple(int(part) for part in match.groups())
        if duckdb_parts < (1, 5, 5):
            raise OptionalDependencyError(
                "managed catalog queries require duckdb>=1.5.5"
            )
        arrow_major = int(pa.__version__.split(".", 1)[0])
        if arrow_major < 14:
            raise OptionalDependencyError(
                "managed catalog queries require pyarrow>=14"
            )
        return duckdb, pa

    def _ensure_connection(self) -> Any:
        if self._closed:
            raise RuntimeError("catalog snapshot is closed")
        if self._connection is not None:
            return self._connection
        duckdb, _pa = self._runtime_versions()
        from pmkt.data.market_catalog.fs import _quote_sql
        from pmkt.data.market_catalog.views import register_resolved_catalog_views

        connection = duckdb.connect(database=":memory:")
        paths = sorted(
            {
                path.resolve().as_posix()
                for artifact in self._state.artifacts
                for path in artifact.files
            }
        )
        literals = ", ".join(_quote_sql(path) for path in paths)
        try:
            # DuckDB freezes this permission once external access is disabled.
            connection.execute(f"SET allowed_paths = [{literals}]")
            connection.execute("SET autoinstall_known_extensions = false")
            connection.execute("SET autoload_known_extensions = false")
            connection.execute("SET python_enable_replacements = false")
            connection.execute("SET enable_external_access = false")
            register_resolved_catalog_views(
                connection,
                self._state.artifacts,
                view_contract_version=self.reference.view_contract_version,
            )
        except Exception:
            connection.close()
            raise
        self._connection = connection
        return connection

    @staticmethod
    def _normalize_parameters(
        parameters: Sequence[CatalogParameter],
    ) -> tuple[CatalogParameter, ...]:
        if isinstance(parameters, (str, bytes, bytearray)):
            raise TypeError("parameters must be a sequence of scalar values")
        normalized: list[CatalogParameter] = []
        for index, value in enumerate(parameters):
            if value is None or type(value) in {bool, int, str, bytes}:
                normalized.append(value)
            elif type(value) is float:
                if not math.isfinite(value):
                    raise ValueError(f"parameter {index} must be finite")
                normalized.append(value)
            elif isinstance(value, datetime):
                if value.tzinfo is None or value.utcoffset() is None:
                    raise ValueError(f"parameter {index} datetime must be timezone-aware")
                normalized.append(value.astimezone(timezone.utc))
            elif isinstance(value, date):
                normalized.append(value)
            else:
                raise TypeError(
                    f"parameter {index} has unsupported type {type(value).__name__}"
                )
        return tuple(normalized)

    @staticmethod
    def _result_limit(value: int, *, name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer")
        return value

    def query(
        self,
        sql: str,
        *,
        parameters: Sequence[CatalogParameter] = (),
        max_result_rows: int = 1_000_000,
        max_result_bytes: int = 268_435_456,
    ) -> CatalogQueryResult:
        if self._closed:
            raise RuntimeError("catalog snapshot is closed")
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        if not sql.strip():
            raise ValueError("sql must contain one SELECT statement")
        row_limit = self._result_limit(max_result_rows, name="max_result_rows")
        byte_limit = self._result_limit(max_result_bytes, name="max_result_bytes")
        bound_parameters = self._normalize_parameters(parameters)
        connection = self._ensure_connection()
        duckdb, pa = self._runtime_versions()
        statements = connection.extract_statements(sql)
        if (
            len(statements) != 1
            or statements[0].type != duckdb.StatementType.SELECT
        ):
            raise ValueError("catalog query must be exactly one parsed SELECT statement")

        started = time.perf_counter()
        reader: Any | None = None
        try:
            reader = connection.execute(sql, list(bound_parameters)).to_arrow_reader(
                batch_size=65_536
            )
            schema, batches = _bounded_arrow_batches(
                reader,
                max_result_rows=row_limit,
                max_result_bytes=byte_limit,
            )
        finally:
            if reader is not None:
                close = getattr(reader, "close", None)
                if close is not None:
                    close()
        table = _table_from_batches(pa, batches, schema)
        return CatalogQueryResult(
            _table=table,
            reference=self.reference,
            validation_level=self.validation.validation_level,
            sql=sql,
            parameters=bound_parameters,
            view_contract_version=self.reference.view_contract_version,
            duckdb_version=duckdb.__version__,
            pyarrow_version=pa.__version__,
            package_version=__version__,
            execution_seconds=time.perf_counter() - started,
        )


__all__ = [
    "CatalogQueryResult",
    "CatalogReference",
    "CatalogSnapshot",
    "CatalogValidationReport",
]
