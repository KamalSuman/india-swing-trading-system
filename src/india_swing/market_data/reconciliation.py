from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.historical_prices.models import (
    NseEodSessionArtifact,
    RawNseEodBar,
)
from india_swing.identity import content_id

from .models import (
    HistoricalDailyCandle,
    HistoricalDailyCandleBatch,
    SHA256_IDENTIFIER,
)


HISTORICAL_RECONCILIATION_SCHEMA_VERSION = (
    "provider-nse-eod-reconciliation/v1"
)
HISTORICAL_RECONCILIATION_POLICY_VERSION = (
    "provider-nse-exact-raw-ohlcv/v1"
)
HISTORICAL_RECONCILIATION_DATASET = "historical-candle-reconciliation"
HISTORICAL_RECONCILIATION_PROVIDER = "NSE_RECONCILIATION"

_FIELD_NAME = re.compile(r"(?:open|high|low|close|volume)\Z")


class HistoricalReconciliationError(ValueError):
    pass


class HistoricalReconciliationIntegrityError(HistoricalReconciliationError):
    pass


class HistoricalReconciliationStatus(str, Enum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
    MISSING_NSE_BAR = "MISSING_NSE_BAR"


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha(value: str, field_name: str) -> None:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class HistoricalCandleDifference:
    field_name: str
    provider_value: str
    nse_value: str

    def __post_init__(self) -> None:
        if (
            type(self.field_name) is not str
            or _FIELD_NAME.fullmatch(self.field_name) is None
        ):
            raise ValueError("difference field_name is unsupported")
        for value, name in (
            (self.provider_value, "provider_value"),
            (self.nse_value, "nse_value"),
        ):
            if (
                type(value) is not str
                or not value
                or value != value.strip()
                or len(value) > 128
            ):
                raise ValueError(f"difference {name} must be bounded canonical text")


@dataclass(frozen=True, slots=True)
class HistoricalCandleReconciliationRow:
    session: date
    nse_artifact_id: str
    nse_bar_id: str | None
    status: HistoricalReconciliationStatus
    differences: tuple[HistoricalCandleDifference, ...]
    row_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.session) is not date:
            raise TypeError("reconciliation session must be an exact date")
        _sha(self.nse_artifact_id, "nse_artifact_id")
        if self.nse_bar_id is not None:
            _sha(self.nse_bar_id, "nse_bar_id")
        if type(self.status) is not HistoricalReconciliationStatus:
            raise TypeError("reconciliation status must be exact")
        if type(self.differences) is not tuple or any(
            type(value) is not HistoricalCandleDifference
            for value in self.differences
        ):
            raise TypeError("differences must be an exact immutable tuple")
        if tuple(value.field_name for value in self.differences) != tuple(
            sorted({value.field_name for value in self.differences})
        ):
            raise ValueError("differences must be field-sorted and unique")
        if self.status is HistoricalReconciliationStatus.MATCH:
            if self.nse_bar_id is None or self.differences:
                raise ValueError("matching reconciliation row shape is invalid")
        elif self.status is HistoricalReconciliationStatus.MISMATCH:
            if self.nse_bar_id is None or not self.differences:
                raise ValueError("mismatching reconciliation row shape is invalid")
        elif self.nse_bar_id is not None or self.differences:
            raise ValueError("missing-bar reconciliation row shape is invalid")
        object.__setattr__(self, "row_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": HISTORICAL_RECONCILIATION_SCHEMA_VERSION,
                "session": self.session,
                "nse_artifact_id": self.nse_artifact_id,
                "nse_bar_id": self.nse_bar_id,
                "status": self.status,
                "differences": self.differences,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.row_id != self._calculated_id():
            raise HistoricalReconciliationIntegrityError(
                "historical reconciliation row identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalCandleReconciliationReport:
    historical_batch_id: str
    historical_request_id: str
    provider: str
    provider_version: str
    listing_key: str
    security_series: str
    isin: str
    reconciled_at: datetime
    rows: tuple[HistoricalCandleReconciliationRow, ...]
    passed: bool
    actionable: bool = False
    schema_version: str = HISTORICAL_RECONCILIATION_SCHEMA_VERSION
    policy_version: str = HISTORICAL_RECONCILIATION_POLICY_VERSION
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.historical_batch_id, "historical_batch_id")
        _sha(self.historical_request_id, "historical_request_id")
        for value, name in (
            (self.provider, "provider"),
            (self.provider_version, "provider_version"),
            (self.listing_key, "listing_key"),
            (self.security_series, "security_series"),
            (self.isin, "isin"),
        ):
            if type(value) is not str or not value or value != value.strip():
                raise ValueError(f"{name} must be canonical text")
        object.__setattr__(
            self,
            "reconciled_at",
            _aware_utc(self.reconciled_at, "reconciled_at"),
        )
        if type(self.rows) is not tuple or not self.rows or any(
            type(value) is not HistoricalCandleReconciliationRow
            for value in self.rows
        ):
            raise TypeError("reconciliation rows must be a non-empty exact tuple")
        if tuple(value.session for value in self.rows) != tuple(
            sorted({value.session for value in self.rows})
        ):
            raise ValueError("reconciliation rows must be session-sorted and unique")
        for row in self.rows:
            row.verify_content_identity()
        expected_passed = all(
            value.status is HistoricalReconciliationStatus.MATCH
            for value in self.rows
        )
        if type(self.passed) is not bool or self.passed != expected_passed:
            raise ValueError("reconciliation passed flag disagrees with rows")
        if self.actionable is not False:
            raise ValueError("reconciliation reports cannot authorize trading")
        if (
            self.schema_version != HISTORICAL_RECONCILIATION_SCHEMA_VERSION
            or self.policy_version != HISTORICAL_RECONCILIATION_POLICY_VERSION
        ):
            raise ValueError("unsupported historical reconciliation contract")
        object.__setattr__(self, "report_id", self._calculated_id())

    @property
    def record_count(self) -> int:
        return len(self.rows)

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "report_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for row in self.rows:
            row.verify_content_identity()
        if self.report_id != self._calculated_id():
            raise HistoricalReconciliationIntegrityError(
                "historical reconciliation report identity failed"
            )


def _differences(
    candle: HistoricalDailyCandle,
    bar: RawNseEodBar,
) -> tuple[HistoricalCandleDifference, ...]:
    values = (
        ("open", candle.open, bar.open),
        ("high", candle.high, bar.high),
        ("low", candle.low, bar.low),
        ("close", candle.close, bar.close),
        ("volume", candle.volume, bar.volume),
    )
    return tuple(
        HistoricalCandleDifference(
            field_name=name,
            provider_value=str(provider_value),
            nse_value=str(nse_value),
        )
        for name, provider_value, nse_value in values
        if provider_value != nse_value
    )


def reconcile_historical_batch(
    batch: HistoricalDailyCandleBatch,
    nse_artifacts: tuple[NseEodSessionArtifact, ...],
    *,
    reconciled_at: datetime,
) -> HistoricalCandleReconciliationReport:
    if type(batch) is not HistoricalDailyCandleBatch:
        raise TypeError("batch must be an exact HistoricalDailyCandleBatch")
    batch.verify_content_identity()
    if type(nse_artifacts) is not tuple or any(
        type(value) is not NseEodSessionArtifact for value in nse_artifacts
    ):
        raise TypeError("nse_artifacts must be an exact immutable tuple")
    if not nse_artifacts:
        raise HistoricalReconciliationError(
            "exact NSE artifacts are required for every provider session"
        )
    for artifact in nse_artifacts:
        artifact.verify_content_identity()
    by_session = {value.market_session: value for value in nse_artifacts}
    if (
        len(by_session) != len(nse_artifacts)
        or tuple(sorted(by_session)) != batch.request.sessions
    ):
        raise HistoricalReconciliationError(
            "NSE artifact sessions must exactly equal provider sessions"
        )
    reconciled_at = _aware_utc(reconciled_at, "reconciled_at")
    if reconciled_at < batch.observed_at or any(
        reconciled_at < value.knowledge_time for value in nse_artifacts
    ):
        raise HistoricalReconciliationError(
            "reconciliation time predates required evidence"
        )

    binding = batch.request.binding
    symbol = binding.listing_key.removeprefix("NSE:")
    rows: list[HistoricalCandleReconciliationRow] = []
    for candle in batch.candles:
        artifact = by_session[candle.session]
        matches = tuple(
            value
            for value in artifact.bars
            if (
                value.validated_isin == binding.isin
                and value.symbol == symbol
                and value.series == binding.security_series
            )
        )
        if not matches:
            rows.append(
                HistoricalCandleReconciliationRow(
                    session=candle.session,
                    nse_artifact_id=artifact.artifact_id,
                    nse_bar_id=None,
                    status=HistoricalReconciliationStatus.MISSING_NSE_BAR,
                    differences=(),
                )
            )
            continue
        if len(matches) != 1:
            raise HistoricalReconciliationIntegrityError(
                "NSE artifact contains ambiguous reconciliation bars"
            )
        bar = matches[0]
        differences = _differences(candle, bar)
        rows.append(
            HistoricalCandleReconciliationRow(
                session=candle.session,
                nse_artifact_id=artifact.artifact_id,
                nse_bar_id=bar.bar_id,
                status=(
                    HistoricalReconciliationStatus.MISMATCH
                    if differences
                    else HistoricalReconciliationStatus.MATCH
                ),
                differences=tuple(
                    sorted(differences, key=lambda value: value.field_name)
                ),
            )
        )
    rows_tuple = tuple(rows)
    return HistoricalCandleReconciliationReport(
        historical_batch_id=batch.batch_id,
        historical_request_id=batch.request.request_id,
        provider=batch.provider,
        provider_version=batch.provider_version,
        listing_key=binding.listing_key,
        security_series=binding.security_series,
        isin=binding.isin,
        reconciled_at=reconciled_at,
        rows=rows_tuple,
        passed=all(
            value.status is HistoricalReconciliationStatus.MATCH
            for value in rows_tuple
        ),
    )

