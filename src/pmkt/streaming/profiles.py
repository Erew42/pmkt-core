from __future__ import annotations

import math
from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from pmkt.data.registry import (
    CAPTURE_INSTRUMENT_EVIDENCE_SCHEMA_VERSION,
    BOOK_TAPE_CONTROL_SCHEMA_VERSION,
    BOOK_TAPE_EVENT_SCHEMA_VERSION,
    BOOK_TAPE_LEVEL_SCHEMA_VERSION,
    DEPTH_SCHEMA_VERSION,
    FEED_HEALTH_SCHEMA_VERSION,
    STREAM_LIFECYCLE_SCHEMA_VERSION,
    TOPBOOK_SCHEMA_VERSION,
    TRADE_SCHEMA_VERSION,
    arrow_schema,
    get_table_spec,
)

from pmkt.streaming.collector import StreamDatasetSpec

TOPBOOK_CHANGE_TRIGGER_VERSION = "topbook-change.v1"
TOPBOOK_EXCLUDED_QUALITY_FLAGS = frozenset({"age_only", "heartbeat", "quote_age_only"})


class DatasetRole(str, Enum):
    PARSED_EVENT = "parsed_event"
    LEGACY_SNAPSHOT = "legacy_snapshot"
    LEGACY_LEVEL = "legacy_level"
    TOPBOOK_MAIN = "topbook_main"
    TOPBOOK_CHECKPOINT = "topbook_checkpoint"
    DEPTH_MAIN = "depth_main"
    TAPE_EVENT = "tape_event"
    TAPE_LEVEL = "tape_level"
    TAPE_CONTROL = "tape_control"
    TRADE = "trade"
    LIFECYCLE = "lifecycle"
    HEALTH = "health"
    INSTRUMENT_EVIDENCE = "instrument_evidence"
    RAW_JSONL = "raw_jsonl"


class ContractStatus(str, Enum):
    STABLE = "stable"
    EXPERIMENTAL = "experimental"
    INTERNAL = "internal"


class TopbookEmissionMode(str, Enum):
    DENSE = "dense"
    ON_CHANGE = "on_change"


class RawEventPolicy(str, Enum):
    PARSED_WITH_RAW = "parsed_with_raw"
    NONE = "none"


@dataclass(frozen=True)
class StorageProfileDefinition:
    name: str
    profile_version: str
    contract_status: ContractStatus
    mandatory_roles: frozenset[DatasetRole]
    optional_roles: frozenset[DatasetRole]
    default_enabled_roles: frozenset[DatasetRole]
    role_schema_versions: Mapping[DatasetRole, frozenset[str]]
    topbook_mode: TopbookEmissionMode
    committed_full_book_tape_required: bool
    topbook_checkpoint_interval_seconds: float
    book_checkpoint_interval_seconds: float | None
    feed_health_interval_seconds: float
    raw_event_policy: RawEventPolicy
    trade_required: bool = True
    lifecycle_required: bool = True
    change_trigger_version: str = TOPBOOK_CHANGE_TRIGGER_VERSION
    excluded_topbook_quality_flags: frozenset[str] = TOPBOOK_EXCLUDED_QUALITY_FLAGS
    tape_encoding_version: str = "book-tape.v1"
    health_fingerprint_version: str = "feed-health-fingerprint.v1"
    replay_evidence_version: str = "sparse-evidence.v1"

    def __post_init__(self) -> None:
        normalized = {
            DatasetRole(role): frozenset(str(version) for version in versions)
            for role, versions in self.role_schema_versions.items()
        }
        object.__setattr__(
            self,
            "role_schema_versions",
            MappingProxyType(normalized),
        )
        object.__setattr__(
            self,
            "excluded_topbook_quality_flags",
            frozenset(str(flag) for flag in self.excluded_topbook_quality_flags),
        )

    @property
    def known_roles(self) -> frozenset[DatasetRole]:
        return frozenset(self.role_schema_versions)


@dataclass(frozen=True)
class StorageProfileOverrides:
    keep_raw_jsonl: bool = False
    topbook_emission_per_event: bool = False
    emit_full_depth: bool = False
    emit_legacy_book_artifacts: bool = False

    def enabled_roles(self) -> frozenset[DatasetRole]:
        roles: set[DatasetRole] = set()
        if self.keep_raw_jsonl:
            roles.add(DatasetRole.RAW_JSONL)
        if self.emit_full_depth:
            roles.add(DatasetRole.DEPTH_MAIN)
        if self.emit_legacy_book_artifacts:
            roles.update({DatasetRole.LEGACY_SNAPSHOT, DatasetRole.LEGACY_LEVEL})
        return frozenset(roles)

    def to_mapping(self) -> dict[str, bool]:
        return {
            "keep_raw_jsonl": self.keep_raw_jsonl,
            "topbook_emission_per_event": self.topbook_emission_per_event,
            "emit_full_depth": self.emit_full_depth,
            "emit_legacy_book_artifacts": self.emit_legacy_book_artifacts,
        }


@dataclass(frozen=True)
class StorageProfileSelection:
    definition: StorageProfileDefinition
    overrides: StorageProfileOverrides
    enabled_roles: frozenset[DatasetRole]
    experimental_profile_acknowledged: bool = False

    @property
    def required_roles(self) -> frozenset[DatasetRole]:
        return self.definition.mandatory_roles

    @property
    def disabled_roles(self) -> frozenset[DatasetRole]:
        role_universe = frozenset(DatasetRole)
        if self.definition.profile_version == "1":
            role_universe -= frozenset({DatasetRole.INSTRUMENT_EVIDENCE})
        return role_universe - self.enabled_roles

    @property
    def effective_topbook_mode(self) -> TopbookEmissionMode:
        if self.overrides.topbook_emission_per_event:
            return TopbookEmissionMode.DENSE
        return self.definition.topbook_mode

    def to_manifest_mapping(self) -> dict[str, object]:
        definition = self.definition
        return {
            "name": definition.name,
            "profile_version": definition.profile_version,
            "contract_status": definition.contract_status.value,
            "experimental_profile_acknowledged": (
                self.experimental_profile_acknowledged
            ),
            "effective_overrides": self.overrides.to_mapping(),
            "required_roles": sorted(role.value for role in self.required_roles),
            "enabled_roles": sorted(role.value for role in self.enabled_roles),
            "disabled_roles": sorted(role.value for role in self.disabled_roles),
            "topbook_mode": self.effective_topbook_mode.value,
            "change_trigger_version": definition.change_trigger_version,
            "excluded_topbook_quality_flags": sorted(
                definition.excluded_topbook_quality_flags
            ),
            "tape_encoding_version": definition.tape_encoding_version,
            "health_fingerprint_version": definition.health_fingerprint_version,
            "replay_evidence_version": definition.replay_evidence_version,
            "topbook_checkpoint_interval_seconds": (
                definition.topbook_checkpoint_interval_seconds
            ),
            "book_checkpoint_interval_seconds": (
                definition.book_checkpoint_interval_seconds
            ),
            "feed_health_interval_seconds": definition.feed_health_interval_seconds,
        }


def build_storage_profile_manifest_mapping(
    profile: StorageProfileSelection | Mapping[str, Any],
    *,
    successfully_committed_roles: Iterable[str],
    terminal_completeness: str,
) -> dict[str, Any]:
    """Serialize one exact profile authority for clean and recovery manifests."""
    if isinstance(profile, StorageProfileSelection):
        payload = profile.to_manifest_mapping()
    elif isinstance(profile, Mapping):
        payload = dict(profile)
    else:
        raise TypeError("storage profile must be a selection or JSON object")

    enabled = _manifest_role_set(payload, "enabled_roles")
    required = _manifest_role_set(payload, "required_roles")
    committed = {str(role) for role in successfully_committed_roles}
    if any(not role.strip() for role in committed):
        raise ValueError("successfully committed roles must be non-empty strings")
    if not required <= enabled:
        raise ValueError("required roles must be a subset of enabled roles")
    if not committed <= enabled:
        raise ValueError("committed roles must be a subset of enabled roles")
    if terminal_completeness not in {"complete", "partial", "failed"}:
        raise ValueError("terminal completeness must be complete, partial, or failed")
    missing_required = required - committed
    if terminal_completeness == "complete" and missing_required:
        raise ValueError("complete profile is missing committed required roles")
    if terminal_completeness == "partial" and not committed:
        raise ValueError("partial profile requires at least one committed role")

    payload["successfully_committed_roles"] = sorted(committed)
    payload["terminal_completeness"] = terminal_completeness
    return payload


def _manifest_role_set(payload: Mapping[str, Any], key: str) -> set[str]:
    raw = payload.get(key)
    if not isinstance(raw, list) or not all(
        isinstance(role, str) and role.strip() for role in raw
    ):
        raise ValueError(f"storage profile {key} must be a list of role names")
    if len(raw) != len(set(raw)):
        raise ValueError(f"storage profile {key} contains duplicate roles")
    return set(raw)


_FULL_REQUIRED = frozenset(
    {
        DatasetRole.PARSED_EVENT,
        DatasetRole.LEGACY_SNAPSHOT,
        DatasetRole.LEGACY_LEVEL,
        DatasetRole.TOPBOOK_MAIN,
        DatasetRole.DEPTH_MAIN,
        DatasetRole.TAPE_EVENT,
        DatasetRole.TAPE_LEVEL,
        DatasetRole.TAPE_CONTROL,
        DatasetRole.TRADE,
        DatasetRole.LIFECYCLE,
        DatasetRole.HEALTH,
    }
)
_BOOK_TAPE_REQUIRED = frozenset(
    {
        DatasetRole.TAPE_EVENT,
        DatasetRole.TAPE_LEVEL,
        DatasetRole.TAPE_CONTROL,
        DatasetRole.TOPBOOK_MAIN,
        DatasetRole.TOPBOOK_CHECKPOINT,
        DatasetRole.TRADE,
        DatasetRole.LIFECYCLE,
        DatasetRole.HEALTH,
    }
)
_MM_COMPACT_REQUIRED = frozenset(
    {
        DatasetRole.TOPBOOK_MAIN,
        DatasetRole.TOPBOOK_CHECKPOINT,
        DatasetRole.TAPE_CONTROL,
        DatasetRole.TRADE,
        DatasetRole.LIFECYCLE,
        DatasetRole.HEALTH,
    }
)
_ADDITIVE_OPTIONAL = frozenset(
    {
        DatasetRole.RAW_JSONL,
        DatasetRole.DEPTH_MAIN,
        DatasetRole.LEGACY_SNAPSHOT,
        DatasetRole.LEGACY_LEVEL,
    }
)


_ROLE_SCHEMA_VERSIONS: Mapping[DatasetRole, frozenset[str]] = MappingProxyType(
    {
        DatasetRole.PARSED_EVENT: frozenset(
            {
                "legacy.polymarket.parsed_event.v1",
                "legacy.kalshi.parsed_event.v1",
            }
        ),
        DatasetRole.LEGACY_SNAPSHOT: frozenset(
            {
                "legacy.polymarket.snapshot.v1",
                "legacy.kalshi.snapshot.v1",
            }
        ),
        DatasetRole.LEGACY_LEVEL: frozenset(
            {
                "legacy.polymarket.level.v1",
                "legacy.kalshi.level.v1",
            }
        ),
        DatasetRole.TOPBOOK_MAIN: frozenset({TOPBOOK_SCHEMA_VERSION}),
        DatasetRole.TOPBOOK_CHECKPOINT: frozenset({TOPBOOK_SCHEMA_VERSION}),
        DatasetRole.DEPTH_MAIN: frozenset({DEPTH_SCHEMA_VERSION}),
        DatasetRole.TAPE_EVENT: frozenset({BOOK_TAPE_EVENT_SCHEMA_VERSION}),
        DatasetRole.TAPE_LEVEL: frozenset({BOOK_TAPE_LEVEL_SCHEMA_VERSION}),
        DatasetRole.TAPE_CONTROL: frozenset({BOOK_TAPE_CONTROL_SCHEMA_VERSION}),
        DatasetRole.TRADE: frozenset({TRADE_SCHEMA_VERSION}),
        DatasetRole.LIFECYCLE: frozenset({STREAM_LIFECYCLE_SCHEMA_VERSION}),
        DatasetRole.HEALTH: frozenset({FEED_HEALTH_SCHEMA_VERSION}),
        DatasetRole.INSTRUMENT_EVIDENCE: frozenset(
            {CAPTURE_INSTRUMENT_EVIDENCE_SCHEMA_VERSION}
        ),
        DatasetRole.RAW_JSONL: frozenset({"legacy.raw_jsonl.v1"}),
    }
)


def _schema_authority(
    roles: frozenset[DatasetRole],
) -> Mapping[DatasetRole, frozenset[str]]:
    return MappingProxyType({role: _ROLE_SCHEMA_VERSIONS[role] for role in roles})


_PROFILE_DEFINITION_VALUES = (
    StorageProfileDefinition(
        name="full",
        profile_version="1",
        contract_status=ContractStatus.STABLE,
        mandatory_roles=_FULL_REQUIRED,
        optional_roles=frozenset({DatasetRole.RAW_JSONL}),
        default_enabled_roles=_FULL_REQUIRED,
        role_schema_versions=_schema_authority(
            _FULL_REQUIRED | frozenset({DatasetRole.RAW_JSONL})
        ),
        topbook_mode=TopbookEmissionMode.DENSE,
        committed_full_book_tape_required=True,
        topbook_checkpoint_interval_seconds=300.0,
        book_checkpoint_interval_seconds=300.0,
        feed_health_interval_seconds=10.0,
        raw_event_policy=RawEventPolicy.PARSED_WITH_RAW,
    ),
    StorageProfileDefinition(
        name="book-tape",
        profile_version="1",
        contract_status=ContractStatus.EXPERIMENTAL,
        mandatory_roles=_BOOK_TAPE_REQUIRED,
        optional_roles=_ADDITIVE_OPTIONAL,
        default_enabled_roles=_BOOK_TAPE_REQUIRED,
        role_schema_versions=_schema_authority(
            _BOOK_TAPE_REQUIRED | _ADDITIVE_OPTIONAL
        ),
        topbook_mode=TopbookEmissionMode.ON_CHANGE,
        committed_full_book_tape_required=True,
        topbook_checkpoint_interval_seconds=300.0,
        book_checkpoint_interval_seconds=300.0,
        feed_health_interval_seconds=10.0,
        raw_event_policy=RawEventPolicy.NONE,
    ),
    StorageProfileDefinition(
        name="mm-compact",
        profile_version="1",
        contract_status=ContractStatus.EXPERIMENTAL,
        mandatory_roles=_MM_COMPACT_REQUIRED,
        optional_roles=_ADDITIVE_OPTIONAL,
        default_enabled_roles=_MM_COMPACT_REQUIRED,
        role_schema_versions=_schema_authority(
            _MM_COMPACT_REQUIRED | _ADDITIVE_OPTIONAL
        ),
        topbook_mode=TopbookEmissionMode.ON_CHANGE,
        committed_full_book_tape_required=False,
        topbook_checkpoint_interval_seconds=300.0,
        book_checkpoint_interval_seconds=None,
        feed_health_interval_seconds=10.0,
        raw_event_policy=RawEventPolicy.NONE,
    ),
    StorageProfileDefinition(
        name="full",
        profile_version="2",
        contract_status=ContractStatus.STABLE,
        mandatory_roles=_FULL_REQUIRED | frozenset({DatasetRole.INSTRUMENT_EVIDENCE}),
        optional_roles=frozenset({DatasetRole.RAW_JSONL}),
        default_enabled_roles=_FULL_REQUIRED
        | frozenset({DatasetRole.INSTRUMENT_EVIDENCE}),
        role_schema_versions=_schema_authority(
            _FULL_REQUIRED
            | frozenset({DatasetRole.INSTRUMENT_EVIDENCE, DatasetRole.RAW_JSONL})
        ),
        topbook_mode=TopbookEmissionMode.DENSE,
        committed_full_book_tape_required=True,
        topbook_checkpoint_interval_seconds=300.0,
        book_checkpoint_interval_seconds=300.0,
        feed_health_interval_seconds=10.0,
        raw_event_policy=RawEventPolicy.PARSED_WITH_RAW,
    ),
    StorageProfileDefinition(
        name="book-tape",
        profile_version="2",
        contract_status=ContractStatus.EXPERIMENTAL,
        mandatory_roles=_BOOK_TAPE_REQUIRED
        | frozenset({DatasetRole.INSTRUMENT_EVIDENCE}),
        optional_roles=_ADDITIVE_OPTIONAL,
        default_enabled_roles=_BOOK_TAPE_REQUIRED
        | frozenset({DatasetRole.INSTRUMENT_EVIDENCE}),
        role_schema_versions=_schema_authority(
            _BOOK_TAPE_REQUIRED
            | _ADDITIVE_OPTIONAL
            | frozenset({DatasetRole.INSTRUMENT_EVIDENCE})
        ),
        topbook_mode=TopbookEmissionMode.ON_CHANGE,
        committed_full_book_tape_required=True,
        topbook_checkpoint_interval_seconds=300.0,
        book_checkpoint_interval_seconds=300.0,
        feed_health_interval_seconds=10.0,
        raw_event_policy=RawEventPolicy.NONE,
    ),
    StorageProfileDefinition(
        name="mm-compact",
        profile_version="2",
        contract_status=ContractStatus.EXPERIMENTAL,
        mandatory_roles=_MM_COMPACT_REQUIRED
        | frozenset({DatasetRole.INSTRUMENT_EVIDENCE}),
        optional_roles=_ADDITIVE_OPTIONAL,
        default_enabled_roles=_MM_COMPACT_REQUIRED
        | frozenset({DatasetRole.INSTRUMENT_EVIDENCE}),
        role_schema_versions=_schema_authority(
            _MM_COMPACT_REQUIRED
            | _ADDITIVE_OPTIONAL
            | frozenset({DatasetRole.INSTRUMENT_EVIDENCE})
        ),
        topbook_mode=TopbookEmissionMode.ON_CHANGE,
        committed_full_book_tape_required=False,
        topbook_checkpoint_interval_seconds=300.0,
        book_checkpoint_interval_seconds=None,
        feed_health_interval_seconds=10.0,
        raw_event_policy=RawEventPolicy.NONE,
    ),
)

PROFILE_DEFINITIONS_BY_VERSION: Mapping[tuple[str, str], StorageProfileDefinition] = (
    MappingProxyType(
        {
            (definition.name, definition.profile_version): definition
            for definition in _PROFILE_DEFINITION_VALUES
        }
    )
)
_CURRENT_PROFILE_VERSIONS: Mapping[str, str] = MappingProxyType(
    {"full": "2", "book-tape": "2", "mm-compact": "2"}
)
PROFILE_DEFINITIONS: Mapping[str, StorageProfileDefinition] = MappingProxyType(
    {
        name: PROFILE_DEFINITIONS_BY_VERSION[(name, version)]
        for name, version in _CURRENT_PROFILE_VERSIONS.items()
    }
)


def validate_profile_definition(definition: StorageProfileDefinition) -> None:
    if not definition.name.strip():
        raise ValueError("storage profile name must not be empty")
    if not definition.profile_version.strip():
        raise ValueError("storage profile version must not be empty")
    if not definition.change_trigger_version.strip():
        raise ValueError("topbook change trigger version must not be empty")
    if not definition.excluded_topbook_quality_flags or any(
        not flag.strip() for flag in definition.excluded_topbook_quality_flags
    ):
        raise ValueError(
            "excluded topbook quality flags must contain non-empty strings"
        )
    if not definition.mandatory_roles <= definition.default_enabled_roles:
        raise ValueError("mandatory roles must be enabled by default")
    declared_roles = definition.mandatory_roles | definition.optional_roles
    if declared_roles != definition.known_roles:
        raise ValueError(
            "role schema authority must exactly cover mandatory and optional roles"
        )
    for role, versions in definition.role_schema_versions.items():
        if not versions or any(not version.strip() for version in versions):
            raise ValueError(
                f"role {role.value!r} must declare non-empty schema versions"
            )
    if not definition.default_enabled_roles <= definition.known_roles:
        raise ValueError("default enabled roles must be declared mandatory or optional")
    _validate_positive_interval(
        "topbook_checkpoint_interval_seconds",
        definition.topbook_checkpoint_interval_seconds,
    )
    _validate_positive_interval(
        "feed_health_interval_seconds", definition.feed_health_interval_seconds
    )
    if definition.book_checkpoint_interval_seconds is not None:
        _validate_positive_interval(
            "book_checkpoint_interval_seconds",
            definition.book_checkpoint_interval_seconds,
        )
    tape_roles = {DatasetRole.TAPE_EVENT, DatasetRole.TAPE_LEVEL}
    if (
        definition.committed_full_book_tape_required
        and not tape_roles <= definition.mandatory_roles
    ):
        raise ValueError("full-book tape profiles require tape event and level roles")
    if (
        DatasetRole.TAPE_EVENT in definition.mandatory_roles
        and DatasetRole.TAPE_LEVEL not in definition.mandatory_roles
    ):
        raise ValueError("tape event role requires the tape level role")
    if (
        definition.trade_required
        and DatasetRole.TRADE not in definition.mandatory_roles
    ):
        raise ValueError("trade-required profile is missing the trade role")
    if (
        definition.lifecycle_required
        and DatasetRole.LIFECYCLE not in definition.mandatory_roles
    ):
        raise ValueError("lifecycle-required profile is missing the lifecycle role")


def get_storage_profile_definition(
    name: str, profile_version: str
) -> StorageProfileDefinition:
    try:
        return PROFILE_DEFINITIONS_BY_VERSION[(name, profile_version)]
    except KeyError:
        known = ", ".join(
            f"{candidate}@{version}"
            for candidate, version in sorted(PROFILE_DEFINITIONS_BY_VERSION)
        )
        raise ValueError(
            f"unknown storage profile version {name!r}@{profile_version!r}; "
            f"expected one of: {known}"
        ) from None


def select_storage_profile(
    name: str,
    *,
    profile_version: str | None = None,
    overrides: StorageProfileOverrides | None = None,
    experimental_profile_acknowledged: bool = False,
    feed_health_interval_seconds: float | None = None,
    topbook_checkpoint_interval_seconds: float | None = None,
    book_checkpoint_interval_seconds: float | None = None,
) -> StorageProfileSelection:
    if profile_version is None:
        try:
            definition = PROFILE_DEFINITIONS[name]
        except KeyError:
            known = ", ".join(sorted(PROFILE_DEFINITIONS))
            raise ValueError(
                f"unknown storage profile {name!r}; expected one of: {known}"
            ) from None
    else:
        definition = get_storage_profile_definition(name, profile_version)
    definition = replace(
        definition,
        feed_health_interval_seconds=(
            definition.feed_health_interval_seconds
            if feed_health_interval_seconds is None
            else feed_health_interval_seconds
        ),
        topbook_checkpoint_interval_seconds=(
            definition.topbook_checkpoint_interval_seconds
            if topbook_checkpoint_interval_seconds is None
            else topbook_checkpoint_interval_seconds
        ),
        book_checkpoint_interval_seconds=(
            definition.book_checkpoint_interval_seconds
            if book_checkpoint_interval_seconds is None
            else book_checkpoint_interval_seconds
        ),
    )
    validate_profile_definition(definition)
    effective_overrides = overrides or StorageProfileOverrides()
    enabled_roles = (
        definition.default_enabled_roles | effective_overrides.enabled_roles()
    )
    undeclared = enabled_roles - definition.known_roles
    if undeclared:
        joined = ", ".join(sorted(role.value for role in undeclared))
        raise ValueError(f"profile {name!r} does not allow additive roles: {joined}")
    return StorageProfileSelection(
        definition=definition,
        overrides=effective_overrides,
        enabled_roles=enabled_roles,
        experimental_profile_acknowledged=bool(experimental_profile_acknowledged),
    )


def resolve_dataset_specs(
    selection: StorageProfileSelection,
    specs: Sequence[StreamDatasetSpec],
) -> tuple[StreamDatasetSpec, ...]:
    by_role: dict[DatasetRole, StreamDatasetSpec] = {}
    for spec in specs:
        if spec.role is None:
            continue
        try:
            role = DatasetRole(spec.role)
        except ValueError:
            raise ValueError(f"unknown StreamDatasetSpec role {spec.role!r}") from None
        if role not in selection.enabled_roles:
            continue
        if role in by_role:
            raise ValueError(f"duplicate StreamDatasetSpec role {role.value!r}")
        allowed_versions = selection.definition.role_schema_versions.get(role)
        if allowed_versions is None:
            raise ValueError(
                f"profile {selection.definition.name!r} does not declare role {role.value!r}"
            )
        if spec.schema_version not in allowed_versions:
            allowed = ", ".join(sorted(allowed_versions))
            raise ValueError(
                f"role {role.value!r} has schema version {spec.schema_version!r}; "
                f"expected one of: {allowed}"
            )
        assert spec.schema_version is not None
        try:
            expected_schema = arrow_schema(get_table_spec(spec.schema_version))
        except KeyError:
            pass
        else:
            if not spec.schema.equals(expected_schema, check_metadata=True):
                raise ValueError(
                    f"role {role.value!r} Arrow schema does not match {spec.schema_version}"
                )
        by_role[role] = spec
    parquet_roles = selection.enabled_roles - {DatasetRole.RAW_JSONL}
    missing = parquet_roles - set(by_role)
    if missing:
        joined = ", ".join(sorted(role.value for role in missing))
        raise ValueError(
            f"profile {selection.definition.name!r} has unresolved roles: {joined}"
        )
    return tuple(
        by_role[role] for role in sorted(parquet_roles, key=lambda item: item.value)
    )


def _validate_positive_interval(name: str, value: float) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and > 0")


for _definition in PROFILE_DEFINITIONS_BY_VERSION.values():
    validate_profile_definition(_definition)


__all__ = [
    "ContractStatus",
    "DatasetRole",
    "PROFILE_DEFINITIONS",
    "TOPBOOK_CHANGE_TRIGGER_VERSION",
    "TOPBOOK_EXCLUDED_QUALITY_FLAGS",
    "PROFILE_DEFINITIONS_BY_VERSION",
    "RawEventPolicy",
    "StorageProfileDefinition",
    "StorageProfileOverrides",
    "StorageProfileSelection",
    "TopbookEmissionMode",
    "build_storage_profile_manifest_mapping",
    "get_storage_profile_definition",
    "resolve_dataset_specs",
    "select_storage_profile",
    "validate_profile_definition",
]
