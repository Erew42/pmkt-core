"""Pure public records shared by read-only venue workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import math
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeVar

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


DataScope = Literal["production", "demo", "synthetic", "unknown"]
TransportOrigin = Literal["library_default", "caller_supplied"]
RequestOutcome = Literal[
    "success",
    "http_error",
    "transport_error",
    "timeout",
    "cancelled",
    "invalid_response",
    "error",
]
MappingStatus = Literal["mapped", "empty", "unknown", "inconsistent"]
DiscoveryStopReason = Literal[
    "result_limit", "page_limit", "source_exhausted", "empty_selection"
]
BookQuantityUnit = Literal["shares", "contracts"]
BookSideProvenance = Literal["direct", "complement_derived", "missing"]
IssueSeverity = Literal["info", "warning", "error"]
SourceCompleteness = Literal["confirmed", "unknown"]
PriceBasis = Literal["venue_defined"]
_MarketT = TypeVar("_MarketT")


def _require_identifier(value: object, name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


def _require_optional_identifier(value: object, name: str) -> None:
    if value is not None:
        _require_identifier(value, name)


@dataclass(frozen=True)
class PolymarketMarketRef:
    venue: ClassVar[str] = "polymarket"

    market_id: str
    condition_id: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        _require_identifier(self.market_id, "market_id")
        _require_optional_identifier(self.condition_id, "condition_id")

    def identity_key(self) -> tuple[str, ...]:
        return (self.venue, self.market_id)


@dataclass(frozen=True)
class PolymarketInstrumentRef:
    venue: ClassVar[str] = "polymarket"

    token_id: str
    market: PolymarketMarketRef | None = field(default=None, compare=False)
    outcome_index: int | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        _require_identifier(self.token_id, "token_id")
        if self.market is not None and not isinstance(self.market, PolymarketMarketRef):
            raise TypeError("market must be a PolymarketMarketRef")
        if self.outcome_index is not None:
            if isinstance(self.outcome_index, bool) or not isinstance(
                self.outcome_index, int
            ):
                raise TypeError("outcome_index must be an int")
            if self.outcome_index < 0:
                raise ValueError("outcome_index must be nonnegative")

    def identity_key(self) -> tuple[str, ...]:
        return (self.venue, self.token_id)


@dataclass(frozen=True)
class KalshiMarketRef:
    venue: ClassVar[str] = "kalshi"

    ticker: str
    series_ticker: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        _require_identifier(self.ticker, "ticker")
        _require_optional_identifier(self.series_ticker, "series_ticker")

    def identity_key(self) -> tuple[str, ...]:
        return (self.venue, self.ticker)


@dataclass(frozen=True)
class KalshiInstrumentRef:
    venue: ClassVar[str] = "kalshi"

    market: KalshiMarketRef
    side: Literal["yes", "no"]

    def __post_init__(self) -> None:
        if not isinstance(self.market, KalshiMarketRef):
            raise TypeError("market must be a KalshiMarketRef")
        if self.side not in ("yes", "no"):
            raise ValueError("side must be 'yes' or 'no'")

    def identity_key(self) -> tuple[str, ...]:
        return (self.venue, self.market.ticker, self.side)


MarketRef = PolymarketMarketRef | KalshiMarketRef
InstrumentRef = PolymarketInstrumentRef | KalshiInstrumentRef


@dataclass(frozen=True)
class PolymarketFilter:
    """Qualified filters for bounded Gamma keyset discovery."""

    condition_ids: tuple[str, ...] | None = None
    closed: bool | None = None
    tag_id: str | None = None
    related_tags: bool | None = None
    question_contains: str | None = None
    outcome_count: int | None = None
    has_instruments: bool | None = None

    def __post_init__(self) -> None:
        if self.condition_ids is not None:
            if not isinstance(self.condition_ids, tuple):
                raise TypeError("condition_ids must be a tuple or None")
            for condition_id in self.condition_ids:
                _require_identifier(condition_id, "condition_id")
        for name, value in (
            ("closed", self.closed),
            ("related_tags", self.related_tags),
            ("has_instruments", self.has_instruments),
        ):
            if value is not None and not isinstance(value, bool):
                raise TypeError(f"{name} must be a bool or None")
        if self.tag_id is not None:
            if not isinstance(self.tag_id, str):
                raise TypeError("tag_id must be a string or None")
            if not self.tag_id or not self.tag_id.isascii() or not self.tag_id.isdecimal():
                raise ValueError("tag_id must be a nonempty decimal string")
        if self.related_tags is not None and self.tag_id is None:
            raise ValueError("related_tags requires tag_id")
        if self.question_contains is not None:
            if not isinstance(self.question_contains, str):
                raise TypeError("question_contains must be a string or None")
            if not self.question_contains:
                raise ValueError("question_contains must not be empty")
        if self.outcome_count is not None:
            if isinstance(self.outcome_count, bool) or not isinstance(
                self.outcome_count, int
            ):
                raise TypeError("outcome_count must be an int or None")
            if self.outcome_count <= 0:
                raise ValueError("outcome_count must be positive")


KalshiQueryStatus = Literal["unopened", "open", "paused", "closed", "settled"]
KalshiMveFilter = Literal["only", "exclude"]


@dataclass(frozen=True)
class KalshiFilter:
    """Qualified filters for bounded Kalshi standard-market discovery."""

    tickers: tuple[str, ...] | None = None
    event_ticker: str | None = None
    series_ticker: str | None = None
    status: KalshiQueryStatus | None = None
    mve_filter: KalshiMveFilter | None = None
    question_contains: str | None = None
    has_instruments: bool | None = None

    def __post_init__(self) -> None:
        if self.tickers is not None:
            if not isinstance(self.tickers, tuple):
                raise TypeError("tickers must be a tuple or None")
            for ticker in self.tickers:
                _require_identifier(ticker, "ticker")
        _require_optional_identifier(self.event_ticker, "event_ticker")
        _require_optional_identifier(self.series_ticker, "series_ticker")
        if self.status is not None and self.status not in (
            "unopened",
            "open",
            "paused",
            "closed",
            "settled",
        ):
            raise ValueError("unsupported Kalshi status filter")
        if self.mve_filter is not None and self.mve_filter not in ("only", "exclude"):
            raise ValueError("mve_filter must be 'only', 'exclude', or None")
        if self.question_contains is not None:
            if not isinstance(self.question_contains, str):
                raise TypeError("question_contains must be a string or None")
            if not self.question_contains:
                raise ValueError("question_contains must not be empty")
        if self.has_instruments is not None and not isinstance(
            self.has_instruments, bool
        ):
            raise TypeError("has_instruments must be a bool or None")


@dataclass(frozen=True)
class DataIssue:
    """A bounded, count-preserving diagnostic for normalized public data."""

    code: str
    severity: IssueSeverity
    request_id: str | None = None
    row_locator: str | None = None
    field_locator: str | None = None
    occurrence_count: int = 1
    examples: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.code, "code")
        if self.severity not in ("info", "warning", "error"):
            raise ValueError("unsupported issue severity")
        _require_optional_identifier(self.request_id, "request_id")
        _require_optional_identifier(self.row_locator, "row_locator")
        _require_optional_identifier(self.field_locator, "field_locator")
        if isinstance(self.occurrence_count, bool) or not isinstance(
            self.occurrence_count, int
        ):
            raise TypeError("occurrence_count must be an int")
        if self.occurrence_count <= 0:
            raise ValueError("occurrence_count must be positive")
        if len(self.examples) > 20:
            raise ValueError("examples must contain at most 20 values")
        for example in self.examples:
            if not isinstance(example, str):
                raise TypeError("issue examples must be strings")
            if len(example) > 512:
                raise ValueError("issue examples must be at most 512 characters")


@dataclass(frozen=True)
class PolymarketMarket:
    """One normalized Gamma market observation."""

    ref: PolymarketMarketRef
    question: str | None
    observed_at_utc: datetime
    observation: RequestObservation
    interpretation_id: str
    package_version: str
    instruments: tuple[PolymarketInstrumentRef, ...]
    outcome_labels: tuple[str, ...]
    outcome_labels_valid: bool
    outcome_prices: tuple[float, ...] | None
    mapping_status: MappingStatus
    book_supported: bool
    closed: bool | None
    start_date: str | None
    end_date: str | None
    created_at: str | None
    updated_at: str | None
    issues: tuple[DataIssue, ...]
    native_payload: dict[str, object] = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.ref, PolymarketMarketRef):
            raise TypeError("ref must be a PolymarketMarketRef")
        if self.question is not None and not isinstance(self.question, str):
            raise TypeError("question must be a string or None")
        _require_utc(self.observed_at_utc, "observed_at_utc")
        _require_identifier(self.interpretation_id, "interpretation_id")
        _require_identifier(self.package_version, "package_version")
        if self.mapping_status not in (
            "mapped",
            "empty",
            "unknown",
            "inconsistent",
        ):
            raise ValueError("unsupported mapping_status")
        if not isinstance(self.book_supported, bool):
            raise TypeError("book_supported must be a bool")
        if not isinstance(self.outcome_labels_valid, bool):
            raise TypeError("outcome_labels_valid must be a bool")
        if self.closed is not None and not isinstance(self.closed, bool):
            raise TypeError("closed must be a bool or None")
        if self.mapping_status == "mapped":
            if not self.instruments or len(self.instruments) != len(self.outcome_labels):
                raise ValueError("mapped markets require one instrument per outcome label")
        elif self.instruments:
            raise ValueError("only mapped markets may contain instruments")
        if self.mapping_status == "empty" and self.outcome_labels:
            raise ValueError("empty mappings must have no outcome labels")
        if self.outcome_prices is not None and len(self.outcome_prices) != len(
            self.outcome_labels
        ):
            raise ValueError("outcome_prices must align with outcome_labels")
        if not isinstance(self.native_payload, dict):
            raise TypeError("native_payload must be a dict")

    def instrument_for_label(self, label: str) -> PolymarketInstrumentRef:
        """Return the uniquely mapped instrument for an exact outcome label."""

        if not isinstance(label, str):
            raise TypeError("label must be a string")
        if self.mapping_status != "mapped":
            raise ValueError("outcome mapping is not validated")
        indexes = tuple(
            index for index, candidate in enumerate(self.outcome_labels) if candidate == label
        )
        if not indexes:
            raise KeyError(f"outcome label {label!r} is not present")
        if len(indexes) != 1:
            raise ValueError(f"outcome label {label!r} is ambiguous")
        return self.instruments[indexes[0]]


@dataclass(frozen=True)
class KalshiMarket:
    """One normalized Kalshi market metadata observation."""

    ref: KalshiMarketRef
    title: str | None
    question: str | None
    observed_at_utc: datetime
    observation: RequestObservation
    interpretation_id: str
    package_version: str
    instruments: tuple[KalshiInstrumentRef, ...]
    mapping_status: MappingStatus
    book_supported: bool
    market_type: str | None
    status: str | None
    event_ticker: str | None
    open_time: str | None
    close_time: str | None
    expected_expiration_time: str | None
    expiration_time: str | None
    created_time: str | None
    updated_time: str | None
    settlement_ts: str | None
    issues: tuple[DataIssue, ...]
    native_payload: dict[str, object] = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.ref, KalshiMarketRef):
            raise TypeError("ref must be a KalshiMarketRef")
        for name in (
            "title",
            "question",
            "market_type",
            "status",
            "event_ticker",
            "open_time",
            "close_time",
            "expected_expiration_time",
            "expiration_time",
            "created_time",
            "updated_time",
            "settlement_ts",
        ):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{name} must be a string or None")
        _require_utc(self.observed_at_utc, "observed_at_utc")
        _require_identifier(self.interpretation_id, "interpretation_id")
        _require_identifier(self.package_version, "package_version")
        if self.mapping_status not in (
            "mapped",
            "empty",
            "unknown",
            "inconsistent",
        ):
            raise ValueError("unsupported mapping_status")
        if not isinstance(self.book_supported, bool):
            raise TypeError("book_supported must be a bool")
        if self.mapping_status == "mapped":
            if tuple(instrument.side for instrument in self.instruments) != (
                "yes",
                "no",
            ):
                raise ValueError("mapped Kalshi markets require YES then NO instruments")
        elif self.instruments:
            raise ValueError("only mapped markets may contain instruments")
        if self.book_supported is not (self.mapping_status == "mapped"):
            raise ValueError("book_supported must agree with a mapped binary market")
        if not isinstance(self.native_payload, dict):
            raise TypeError("native_payload must be a dict")


@dataclass(frozen=True)
class DiscoveryReport:
    """Bounded traversal metadata for one discovery operation."""

    started_at_utc: datetime
    finished_at_utc: datetime
    stop_reason: DiscoveryStopReason
    pages_fetched: int
    rows_scanned: int
    unique_markets_seen: int
    duplicates_seen: int
    requested_filters: tuple[tuple[str, object], ...]
    applied_server_filters: tuple[str, ...]
    applied_local_filters: tuple[str, ...]
    unknown_filter_counts: tuple[tuple[str, int], ...]
    source_scope: str
    data_scope: DataScope
    interpretation_id: str
    package_version: str
    observations: tuple[RequestObservation, ...]
    issues: tuple[DataIssue, ...]
    endpoints: tuple[str, ...]
    pagination_strategy: str
    ordering: str
    selection_strategy: str
    requested_selector_count: int
    queried_chunks: int
    traversal_complete: bool

    def __post_init__(self) -> None:
        _require_utc(self.started_at_utc, "started_at_utc")
        _require_utc(self.finished_at_utc, "finished_at_utc")
        if self.finished_at_utc < self.started_at_utc:
            raise ValueError("finished_at_utc must not precede started_at_utc")
        if self.stop_reason not in (
            "result_limit",
            "page_limit",
            "source_exhausted",
            "empty_selection",
        ):
            raise ValueError("unsupported discovery stop_reason")
        for name in (
            "pages_fetched",
            "rows_scanned",
            "unique_markets_seen",
            "duplicates_seen",
            "requested_selector_count",
            "queried_chunks",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an int")
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
        _require_identifier(self.source_scope, "source_scope")
        _require_identifier(self.interpretation_id, "interpretation_id")
        _require_identifier(self.package_version, "package_version")
        _require_identifier(self.pagination_strategy, "pagination_strategy")
        _require_identifier(self.ordering, "ordering")
        _require_identifier(self.selection_strategy, "selection_strategy")
        if not isinstance(self.traversal_complete, bool):
            raise TypeError("traversal_complete must be a bool")


@dataclass(frozen=True)
class DiscoveryResult(Generic[_MarketT]):
    items: tuple[_MarketT, ...]
    report: DiscoveryReport


@dataclass(frozen=True)
class BookLevel:
    price: float
    quantity: float

    def __post_init__(self) -> None:
        for name in ("price", "quantity"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{name} must be a number")
            if not math.isfinite(float(value)):
                raise ValueError(f"{name} must be finite")
        if not 0 <= self.price <= 1:
            raise ValueError("price must be between 0 and 1")
        if self.quantity < 0:
            raise ValueError("quantity must be nonnegative")


@dataclass(frozen=True)
class BookSnapshot:
    """One normalized REST book for one outcome instrument."""

    instrument: InstrumentRef
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    quantity_unit: BookQuantityUnit
    exchange_timestamp_utc: datetime | None
    endpoint: str
    source_scope: str
    data_scope: DataScope
    observation: RequestObservation
    observations: tuple[RequestObservation, ...]
    interpretation_id: str
    package_version: str
    quote_normalization_policy: str | None
    valid_state: bool
    quality_flags: tuple[str, ...]
    bid_provenance: BookSideProvenance
    ask_provenance: BookSideProvenance
    native_bid_count: int
    native_ask_count: int
    pre_trim_bid_count: int
    pre_trim_ask_count: int
    returned_bid_count: int
    returned_ask_count: int
    native_payload: dict[str, object] = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(
            self.instrument, (PolymarketInstrumentRef, KalshiInstrumentRef)
        ):
            raise TypeError("instrument must be an InstrumentRef")
        if self.quantity_unit not in ("shares", "contracts"):
            raise ValueError("unsupported quantity_unit")
        if self.exchange_timestamp_utc is not None:
            _require_utc(self.exchange_timestamp_utc, "exchange_timestamp_utc")
        _require_identifier(self.endpoint, "endpoint")
        _require_identifier(self.source_scope, "source_scope")
        _require_identifier(self.interpretation_id, "interpretation_id")
        _require_identifier(self.package_version, "package_version")
        _require_optional_identifier(
            self.quote_normalization_policy, "quote_normalization_policy"
        )
        if not self.observations:
            raise ValueError("observations must contain at least one request observation")
        if self.observations[-1] != self.observation:
            raise ValueError("observation must be the final request observation")
        if not isinstance(self.valid_state, bool):
            raise TypeError("valid_state must be a bool")
        for name in (
            "native_bid_count",
            "native_ask_count",
            "pre_trim_bid_count",
            "pre_trim_ask_count",
            "returned_bid_count",
            "returned_ask_count",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an int")
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
        if not isinstance(self.native_payload, dict):
            raise TypeError("native_payload must be a dict")


@dataclass(frozen=True)
class SampledPricePoint:
    """One sourced CLOB sample, with no implied trade or quote meaning."""

    timestamp_utc: datetime
    price: float

    def __post_init__(self) -> None:
        _require_utc(self.timestamp_utc, "timestamp_utc")
        if isinstance(self.price, bool) or not isinstance(self.price, (int, float)):
            raise TypeError("price must be a number")
        if not math.isfinite(float(self.price)):
            raise ValueError("price must be finite")
        if not 0 <= self.price <= 1:
            raise ValueError("price must be between 0 and 1")


@dataclass(frozen=True)
class HistoryQueryWindow:
    """The UTC boundary markers and dataset used by one history request."""

    start_utc: datetime
    end_utc: datetime
    dataset: str
    endpoint: str

    def __post_init__(self) -> None:
        _require_utc(self.start_utc, "start_utc")
        _require_utc(self.end_utc, "end_utc")
        if self.end_utc <= self.start_utc:
            raise ValueError("history query end must follow its start")
        _require_identifier(self.dataset, "dataset")
        if not isinstance(self.endpoint, str) or not self.endpoint.startswith("/"):
            raise ValueError("endpoint must be an absolute path template")


@dataclass(frozen=True)
class HistoryCoverage:
    """Auditable traversal, extent, and row reconciliation for history."""

    requested_start_utc: datetime
    requested_end_utc: datetime
    queried_windows: tuple[HistoryQueryWindow, ...]
    datasets: tuple[str, ...]
    observed_start_utc: datetime | None
    observed_end_utc: datetime | None
    requests_complete: bool
    source_completeness: SourceCompleteness
    raw_rows: int
    accepted_rows: int
    rejected_rows: int
    duplicate_rows: int
    conflicting_rows: int
    outside_window_rows: int

    def __post_init__(self) -> None:
        _require_utc(self.requested_start_utc, "requested_start_utc")
        _require_utc(self.requested_end_utc, "requested_end_utc")
        if self.requested_end_utc <= self.requested_start_utc:
            raise ValueError("requested history end must follow its start")
        if not self.queried_windows:
            raise ValueError("queried_windows must contain at least one request")
        if not self.datasets:
            raise ValueError("datasets must contain at least one dataset")
        for dataset in self.datasets:
            _require_identifier(dataset, "dataset")
        if (self.observed_start_utc is None) != (self.observed_end_utc is None):
            raise ValueError("observed history bounds must both be present or absent")
        if self.observed_start_utc is not None:
            _require_utc(self.observed_start_utc, "observed_start_utc")
            assert self.observed_end_utc is not None
            _require_utc(self.observed_end_utc, "observed_end_utc")
            if self.observed_end_utc < self.observed_start_utc:
                raise ValueError("observed history end must not precede its start")
        if not isinstance(self.requests_complete, bool):
            raise TypeError("requests_complete must be a bool")
        if self.source_completeness not in ("confirmed", "unknown"):
            raise ValueError("unsupported source_completeness")
        for name in (
            "raw_rows",
            "accepted_rows",
            "rejected_rows",
            "duplicate_rows",
            "conflicting_rows",
            "outside_window_rows",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an int")
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
        if self.conflicting_rows > self.rejected_rows:
            raise ValueError("conflicting_rows must be a subset of rejected_rows")
        partitioned = (
            self.accepted_rows
            + self.rejected_rows
            + self.duplicate_rows
            + self.outside_window_rows
        )
        if self.raw_rows != partitioned:
            raise ValueError("history row counters do not partition raw_rows")


@dataclass(frozen=True)
class PriceHistoryResult:
    """Normalized sampled CLOB history with retained provenance and coverage."""

    instrument: PolymarketInstrumentRef
    points: tuple[SampledPricePoint, ...]
    requested_start_utc: datetime
    requested_end_utc: datetime
    queried_start_utc: datetime
    queried_end_utc: datetime
    sampling_minutes: int
    observed_start_utc: datetime | None
    observed_end_utc: datetime | None
    source: str
    dataset: str
    price_basis: PriceBasis
    observation: RequestObservation
    observations: tuple[RequestObservation, ...]
    issues: tuple[DataIssue, ...]
    interpretation_id: str
    package_version: str
    coverage: HistoryCoverage
    native_payloads: tuple[dict[str, object], ...] = field(
        repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, PolymarketInstrumentRef):
            raise TypeError("instrument must be a PolymarketInstrumentRef")
        _require_utc(self.requested_start_utc, "requested_start_utc")
        _require_utc(self.requested_end_utc, "requested_end_utc")
        _require_utc(self.queried_start_utc, "queried_start_utc")
        _require_utc(self.queried_end_utc, "queried_end_utc")
        if self.requested_end_utc <= self.requested_start_utc:
            raise ValueError("requested history end must follow its start")
        if self.queried_end_utc <= self.queried_start_utc:
            raise ValueError("queried history end must follow its start")
        if isinstance(self.sampling_minutes, bool) or not isinstance(
            self.sampling_minutes, int
        ):
            raise TypeError("sampling_minutes must be an int")
        if self.sampling_minutes <= 0:
            raise ValueError("sampling_minutes must be positive")
        if (self.observed_start_utc is None) != (self.observed_end_utc is None):
            raise ValueError("observed history bounds must both be present or absent")
        if self.observed_start_utc is not None:
            _require_utc(self.observed_start_utc, "observed_start_utc")
            assert self.observed_end_utc is not None
            _require_utc(self.observed_end_utc, "observed_end_utc")
        _require_identifier(self.source, "source")
        _require_identifier(self.dataset, "dataset")
        if self.price_basis != "venue_defined":
            raise ValueError("unsupported price_basis")
        if not self.observations or self.observations[-1] != self.observation:
            raise ValueError("observation must be the final request observation")
        _require_identifier(self.interpretation_id, "interpretation_id")
        _require_identifier(self.package_version, "package_version")
        if self.coverage.accepted_rows != len(self.points):
            raise ValueError("coverage accepted_rows must equal returned point count")
        if any(not isinstance(payload, dict) for payload in self.native_payloads):
            raise TypeError("native_payloads must contain dictionaries")

    def to_arrow(self) -> pa.Table:
        """Materialize points as an Arrow table, loading the data extra lazily."""

        return _price_history_to_arrow(self)

    def to_pandas(self) -> pd.DataFrame:
        """Materialize points as a pandas frame, loading the data extra lazily."""

        return _price_history_to_pandas(self)


def _price_history_metadata(result: PriceHistoryResult) -> dict[str, str]:
    return {
        "instrument_token_id": result.instrument.token_id,
        "source": result.source,
        "dataset": result.dataset,
        "price_basis": result.price_basis,
        "sampling_minutes": str(result.sampling_minutes),
        "interpretation_id": result.interpretation_id,
        "package_version": result.package_version,
    }


def _price_history_to_arrow(result: PriceHistoryResult) -> Any:
    try:
        import pyarrow as pa
    except ImportError as exc:
        from pmkt.errors import OptionalDependencyError

        raise OptionalDependencyError(
            "PriceHistoryResult.to_arrow requires pyarrow; install pmkt[data]"
        ) from exc
    schema = pa.schema(
        [
            pa.field("token_id", pa.string(), nullable=False),
            pa.field("timestamp_utc", pa.timestamp("s", tz="UTC"), nullable=False),
            pa.field("price", pa.float64(), nullable=False),
            pa.field("price_basis", pa.string(), nullable=False),
        ],
        metadata={
            key.encode("utf-8"): value.encode("utf-8")
            for key, value in _price_history_metadata(result).items()
        },
    )
    return pa.Table.from_arrays(
        [
            pa.array(
                [result.instrument.token_id] * len(result.points), type=pa.string()
            ),
            pa.array(
                [point.timestamp_utc for point in result.points],
                type=pa.timestamp("s", tz="UTC"),
            ),
            pa.array([point.price for point in result.points], type=pa.float64()),
            pa.array([result.price_basis] * len(result.points), type=pa.string()),
        ],
        schema=schema,
    )


def _price_history_to_pandas(result: PriceHistoryResult) -> Any:
    try:
        import pandas as pd
    except ImportError as exc:
        from pmkt.errors import OptionalDependencyError

        raise OptionalDependencyError(
            "PriceHistoryResult.to_pandas requires pandas; install pmkt[data]"
        ) from exc
    frame = pd.DataFrame(
        {
            "token_id": pd.Series(
                [result.instrument.token_id] * len(result.points), dtype="string"
            ),
            "timestamp_utc": pd.Series(
                [point.timestamp_utc for point in result.points],
                dtype="datetime64[ns, UTC]",
            ),
            "price": pd.Series(
                [point.price for point in result.points], dtype="float64"
            ),
            "price_basis": pd.Series(
                [result.price_basis] * len(result.points), dtype="string"
            ),
        }
    )
    frame.attrs.update(_price_history_metadata(result))
    return frame


@dataclass(frozen=True)
class RequestObservation:
    """Sanitized provenance for one HTTP request within a workflow operation."""

    request_id: str
    venue: str
    data_scope: DataScope
    transport_origin: TransportOrigin
    origin: str
    endpoint_template: str
    effective_parameters: tuple[tuple[str, str], ...]
    started_at_utc: datetime
    received_at_utc: datetime | None
    attempt_count: int
    outcome: RequestOutcome
    status_code: int | None = None
    response_identities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.request_id, "request_id")
        _require_identifier(self.venue, "venue")
        if self.data_scope not in ("production", "demo", "synthetic", "unknown"):
            raise ValueError("unsupported data_scope")
        if self.transport_origin not in ("library_default", "caller_supplied"):
            raise ValueError("unsupported transport_origin")
        if self.outcome not in (
            "success",
            "http_error",
            "transport_error",
            "timeout",
            "cancelled",
            "invalid_response",
            "error",
        ):
            raise ValueError("unsupported request outcome")
        _require_identifier(self.origin, "origin")
        if not self.endpoint_template.startswith("/"):
            raise ValueError("endpoint_template must be an absolute path template")
        _require_utc(self.started_at_utc, "started_at_utc")
        if self.received_at_utc is not None:
            _require_utc(self.received_at_utc, "received_at_utc")
        if isinstance(self.attempt_count, bool) or not isinstance(self.attempt_count, int):
            raise TypeError("attempt_count must be an int")
        if self.attempt_count < 0:
            raise ValueError("attempt_count must be nonnegative")
        if self.status_code is not None and (
            isinstance(self.status_code, bool) or not isinstance(self.status_code, int)
        ):
            raise TypeError("status_code must be an int")
        for name, value in self.effective_parameters:
            _require_identifier(name, "effective parameter name")
            if not isinstance(value, str):
                raise TypeError("effective parameter values must be strings")
        for identity in self.response_identities:
            _require_identifier(identity, "response identity")


def _require_utc(value: datetime, name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{name} must be timezone-aware UTC")


__all__ = [
    "BookLevel",
    "BookQuantityUnit",
    "BookSideProvenance",
    "BookSnapshot",
    "DataScope",
    "DataIssue",
    "DiscoveryReport",
    "DiscoveryResult",
    "DiscoveryStopReason",
    "HistoryCoverage",
    "HistoryQueryWindow",
    "InstrumentRef",
    "KalshiFilter",
    "KalshiInstrumentRef",
    "KalshiMarket",
    "KalshiMarketRef",
    "KalshiMveFilter",
    "KalshiQueryStatus",
    "MarketRef",
    "MappingStatus",
    "PolymarketFilter",
    "PolymarketInstrumentRef",
    "PolymarketMarket",
    "PolymarketMarketRef",
    "PriceBasis",
    "PriceHistoryResult",
    "RequestObservation",
    "RequestOutcome",
    "SampledPricePoint",
    "SourceCompleteness",
    "TransportOrigin",
]
