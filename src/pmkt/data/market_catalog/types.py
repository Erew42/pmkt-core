"""Catalog public types, schemas, and errors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal



DiscoveryStream = Literal["polymarket", "kalshi-conventional", "kalshi-mve"]


DISCOVERY_MANIFEST_SCHEMA = "pmkt.market_discovery_manifest.v1"


DISCOVERY_POINTER_SCHEMA = "pmkt.market_discovery_pointer.v1"


CURRENT_MANIFEST_SCHEMA = "pmkt.market_current_release.v1"


HISTORY_MANIFEST_SCHEMA = "pmkt.market_history_release.v1"


FAMILY_CLASSIFIER_VERSION = "operational_family.v1"


KALSHI_LEGACY_MVE_PREFIX = "KXMVE"


KALSHI_CATALOG_NATIVE_FAMILIES = (
    "kalshi_mve",
    "kalshi_conventional",
    "family_unknown",
)


class CatalogError(RuntimeError):
    """Raised when catalog completeness or lineage cannot be established."""


class FilterAgreementError(CatalogError):
    """Kalshi MVE filter partitions disagree for one fixed creation window."""

    def __init__(self, report: Mapping[str, Any]) -> None:
        self.report = dict(report)
        super().__init__(
            "Kalshi MVE filter agreement failed: "
            f"intersection={self.report['intersection'][:20]}, "
            f"missing={self.report['missing'][:20]}, "
            f"extra={self.report['extra'][:20]}"
        )


@dataclass(frozen=True)
class CollectionResult:
    rows: list[dict[str, Any]]
    high_watermark: datetime
    details: dict[str, Any]
