from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal

from india_swing.execution.simulator import SimulationBar
from india_swing.historical_prices.models import NseEodSessionArtifact
from india_swing.identity import content_id
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference.universe import UniverseSnapshot

from .baselines import PointInTimeInstrument
from .engine import EvaluationDataReadiness, EvaluationDataset


EVALUATION_DATASET_ASSEMBLY_SCHEMA_VERSION = "evaluation-dataset-assembly/v1"
POINT_IN_TIME_PRICE_SESSION_SCHEMA_VERSION = "point-in-time-price-session/v1"
EFFECTIVE_TICK_SIZE_SCHEMA_VERSION = "effective-tick-size/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"[A-Z0-9]{12}\Z")
ZERO = Decimal("0")


class EvaluationDatasetAssemblyError(ValueError):
    pass


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise EvaluationDatasetAssemblyError(
            f"{name} must be a full lowercase SHA-256"
        )


def _text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise EvaluationDatasetAssemblyError(f"{name} is required")


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise EvaluationDatasetAssemblyError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _positive_decimal(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= ZERO:
        raise EvaluationDatasetAssemblyError(f"{name} must be a positive Decimal")


def _sorted_sha_tuple(values: tuple[str, ...], name: str) -> None:
    if (
        type(values) is not tuple
        or not values
        or values != tuple(sorted(set(values)))
    ):
        raise EvaluationDatasetAssemblyError(
            f"{name} must be a non-empty sorted unique tuple"
        )
    for value in values:
        _sha(value, name)


def _evaluation_readiness(value: ReferenceReadiness) -> EvaluationDataReadiness:
    if value is ReferenceReadiness.SYNTHETIC_TEST:
        return EvaluationDataReadiness.SYNTHETIC
    if value is ReferenceReadiness.POINT_IN_TIME_VERIFIED:
        return EvaluationDataReadiness.POINT_IN_TIME_VERIFIED
    return EvaluationDataReadiness.COLLECTION_ONLY


@dataclass(frozen=True, slots=True)
class PointInTimePriceBar:
    session: date
    symbol: str
    series: str
    isin: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    raw_bar_id: str
    tradable: bool = True
    lower_circuit_sell_locked: bool = False
    bar_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.session) is not date:
            raise TypeError("price bar session must be a date")
        for value, name in ((self.symbol, "symbol"), (self.series, "series")):
            _text(value, f"price_bar.{name}")
            if value != value.strip().upper():
                raise EvaluationDatasetAssemblyError(
                    f"price_bar.{name} must be normalized uppercase text"
                )
        if not isinstance(self.isin, str) or _ISIN.fullmatch(self.isin) is None:
            raise EvaluationDatasetAssemblyError(
                "price_bar.isin must be normalized 12-character text"
            )
        for value, name in (
            (self.open, "open"),
            (self.high, "high"),
            (self.low, "low"),
            (self.close, "close"),
        ):
            _positive_decimal(value, f"price_bar.{name}")
        if self.high < max(self.open, self.low, self.close):
            raise EvaluationDatasetAssemblyError("price bar high is inconsistent")
        if self.low > min(self.open, self.high, self.close):
            raise EvaluationDatasetAssemblyError("price bar low is inconsistent")
        if type(self.volume) is not int or self.volume < 0:
            raise EvaluationDatasetAssemblyError(
                "price_bar.volume must be a non-negative integer"
            )
        if type(self.tradable) is not bool or type(
            self.lower_circuit_sell_locked
        ) is not bool:
            raise TypeError("price-bar execution flags must be bool")
        if self.tradable and self.volume == 0:
            raise EvaluationDatasetAssemblyError(
                "tradable price bars require positive volume"
            )
        if self.lower_circuit_sell_locked and not self.tradable:
            raise EvaluationDatasetAssemblyError(
                "circuit-locked price bars must remain exchange-tradable"
            )
        _sha(self.raw_bar_id, "price_bar.raw_bar_id")
        object.__setattr__(self, "bar_id", self._calculated_id())

    @property
    def listing_key(self) -> tuple[str, str]:
        return (self.symbol, self.series)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "point-in-time-price-bar/v1",
                "session": self.session,
                "symbol": self.symbol,
                "series": self.series,
                "isin": self.isin,
                "open": self.open,
                "high": self.high,
                "low": self.low,
                "close": self.close,
                "volume": self.volume,
                "raw_bar_id": self.raw_bar_id,
                "tradable": self.tradable,
                "lower_circuit_sell_locked": self.lower_circuit_sell_locked,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.bar_id != self._calculated_id():
            raise EvaluationDatasetAssemblyError(
                "point-in-time price-bar content identity failed"
            )

    def to_simulation_bar(self) -> SimulationBar:
        self.verify_content_identity()
        return SimulationBar(
            session=self.session,
            symbol=self.symbol,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            tradable=self.tradable,
            lower_circuit_sell_locked=self.lower_circuit_sell_locked,
        )


@dataclass(frozen=True, slots=True)
class PointInTimePriceSession:
    market_session: date
    cutoff: datetime
    knowledge_time: datetime
    source_artifact_id: str
    source_snapshot_ids: tuple[str, ...]
    bars: tuple[PointInTimePriceBar, ...]
    explicit_nontrading_listing_ids: tuple[str, ...]
    readiness: EvaluationDataReadiness
    actionable: bool
    schema_version: str = POINT_IN_TIME_PRICE_SESSION_SCHEMA_VERSION
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise TypeError("price session must be a date")
        object.__setattr__(self, "cutoff", _aware_utc(self.cutoff, "price_session.cutoff"))
        object.__setattr__(
            self,
            "knowledge_time",
            _aware_utc(self.knowledge_time, "price_session.knowledge_time"),
        )
        if self.knowledge_time > self.cutoff:
            raise EvaluationDatasetAssemblyError(
                "price session was not known by its cutoff"
            )
        _sha(self.source_artifact_id, "price_session.source_artifact_id")
        _sorted_sha_tuple(
            self.source_snapshot_ids,
            "price_session.source_snapshot_ids",
        )
        if type(self.bars) is not tuple or not self.bars or any(
            type(value) is not PointInTimePriceBar for value in self.bars
        ):
            raise TypeError("price session bars must be a non-empty exact tuple")
        if self.bars != tuple(
            sorted(self.bars, key=lambda value: (value.symbol, value.series))
        ):
            raise EvaluationDatasetAssemblyError(
                "price session bars must be listing-key ordered"
            )
        if len({value.listing_key for value in self.bars}) != len(self.bars):
            raise EvaluationDatasetAssemblyError(
                "price session contains duplicate listing keys"
            )
        for value in self.bars:
            value.verify_content_identity()
            if value.session != self.market_session:
                raise EvaluationDatasetAssemblyError(
                    "price bar belongs to another market session"
                )
        if (
            type(self.explicit_nontrading_listing_ids) is not tuple
            or self.explicit_nontrading_listing_ids
            != tuple(sorted(set(self.explicit_nontrading_listing_ids)))
        ):
            raise EvaluationDatasetAssemblyError(
                "explicit nontrading listing IDs must be sorted and unique"
            )
        for value in self.explicit_nontrading_listing_ids:
            _text(value, "explicit nontrading listing ID")
        if type(self.readiness) is not EvaluationDataReadiness:
            raise TypeError("price session readiness must be exact")
        if type(self.actionable) is not bool:
            raise TypeError("price session actionable must be bool")
        if (self.readiness is EvaluationDataReadiness.COLLECTION_ONLY) != (
            not self.actionable
        ):
            raise EvaluationDatasetAssemblyError(
                "only collection-only price sessions may be non-actionable"
            )
        if self.schema_version != POINT_IN_TIME_PRICE_SESSION_SCHEMA_VERSION:
            raise EvaluationDatasetAssemblyError(
                "unsupported point-in-time price-session schema"
            )
        object.__setattr__(self, "snapshot_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "market_session": self.market_session,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
                "source_artifact_id": self.source_artifact_id,
                "source_snapshot_ids": self.source_snapshot_ids,
                "bars": self.bars,
                "explicit_nontrading_listing_ids": self.explicit_nontrading_listing_ids,
                "readiness": self.readiness,
                "actionable": self.actionable,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.bars:
            if type(value) is not PointInTimePriceBar:
                raise EvaluationDatasetAssemblyError(
                    "price-session graph contains an invalid bar type"
                )
            value.verify_content_identity()
        if self.snapshot_id != self._calculated_id():
            raise EvaluationDatasetAssemblyError(
                "point-in-time price-session content identity failed"
            )


@dataclass(frozen=True, slots=True)
class EffectiveTickSize:
    instrument_id: str
    listing_id: str
    effective_from_session: date
    effective_to_exclusive: date | None
    tick_size: Decimal
    knowledge_time: datetime
    source_snapshot_id: str
    readiness: ReferenceReadiness
    schema_version: str = EFFECTIVE_TICK_SIZE_SCHEMA_VERSION
    specification_id: str = field(init=False)

    def __post_init__(self) -> None:
        _text(self.instrument_id, "tick_size.instrument_id")
        _text(self.listing_id, "tick_size.listing_id")
        if type(self.effective_from_session) is not date:
            raise TypeError("tick-size effective_from_session must be a date")
        if self.effective_to_exclusive is not None:
            if type(self.effective_to_exclusive) is not date:
                raise TypeError("tick-size effective_to_exclusive must be a date")
            if self.effective_to_exclusive <= self.effective_from_session:
                raise EvaluationDatasetAssemblyError(
                    "tick-size effective interval must be positive"
                )
        _positive_decimal(self.tick_size, "tick_size.tick_size")
        object.__setattr__(
            self,
            "knowledge_time",
            _aware_utc(self.knowledge_time, "tick_size.knowledge_time"),
        )
        _sha(self.source_snapshot_id, "tick_size.source_snapshot_id")
        if type(self.readiness) is not ReferenceReadiness:
            raise TypeError("tick-size readiness must be exact")
        if self.schema_version != EFFECTIVE_TICK_SIZE_SCHEMA_VERSION:
            raise EvaluationDatasetAssemblyError("unsupported tick-size schema")
        object.__setattr__(self, "specification_id", self._calculated_id())

    def is_effective_on(self, session: date) -> bool:
        if type(session) is not date:
            raise TypeError("tick-size lookup session must be a date")
        return self.effective_from_session <= session and (
            self.effective_to_exclusive is None
            or session < self.effective_to_exclusive
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "instrument_id": self.instrument_id,
                "listing_id": self.listing_id,
                "effective_from_session": self.effective_from_session,
                "effective_to_exclusive": self.effective_to_exclusive,
                "tick_size": self.tick_size,
                "knowledge_time": self.knowledge_time,
                "source_snapshot_id": self.source_snapshot_id,
                "readiness": self.readiness,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.specification_id != self._calculated_id():
            raise EvaluationDatasetAssemblyError(
                "tick-size specification content identity failed"
            )


@dataclass(frozen=True, slots=True)
class EvaluationSessionEvidence:
    market_session: date
    calendar_snapshot_id: str
    universe_snapshot_id: str
    price_snapshot_id: str
    price_source_artifact_id: str
    price_source_snapshot_ids: tuple[str, ...]
    cutoff: datetime
    actionable_listing_ids: tuple[str, ...]
    explicit_nontrading_listing_ids: tuple[str, ...]
    tick_size_specification_ids: tuple[str, ...]
    evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise TypeError("session evidence market_session must be a date")
        for value, name in (
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
            (self.universe_snapshot_id, "universe_snapshot_id"),
            (self.price_snapshot_id, "price_snapshot_id"),
            (self.price_source_artifact_id, "price_source_artifact_id"),
        ):
            _sha(value, f"session_evidence.{name}")
        _sorted_sha_tuple(
            self.price_source_snapshot_ids,
            "session_evidence.price_source_snapshot_ids",
        )
        object.__setattr__(self, "cutoff", _aware_utc(self.cutoff, "session_evidence.cutoff"))
        for values, name in (
            (self.actionable_listing_ids, "actionable_listing_ids"),
            (
                self.explicit_nontrading_listing_ids,
                "explicit_nontrading_listing_ids",
            ),
        ):
            if type(values) is not tuple or values != tuple(sorted(set(values))):
                raise EvaluationDatasetAssemblyError(
                    f"session evidence {name} must be sorted and unique"
                )
            for value in values:
                _text(value, f"session evidence {name}")
        if not set(self.explicit_nontrading_listing_ids).issubset(
            self.actionable_listing_ids
        ):
            raise EvaluationDatasetAssemblyError(
                "explicit nontrading listings must be actionable universe members"
            )
        _sorted_sha_tuple(
            self.tick_size_specification_ids,
            "session_evidence.tick_size_specification_ids",
        )
        object.__setattr__(self, "evidence_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "evaluation-session-evidence/v1",
                "market_session": self.market_session,
                "calendar_snapshot_id": self.calendar_snapshot_id,
                "universe_snapshot_id": self.universe_snapshot_id,
                "price_snapshot_id": self.price_snapshot_id,
                "price_source_artifact_id": self.price_source_artifact_id,
                "price_source_snapshot_ids": self.price_source_snapshot_ids,
                "cutoff": self.cutoff,
                "actionable_listing_ids": self.actionable_listing_ids,
                "explicit_nontrading_listing_ids": self.explicit_nontrading_listing_ids,
                "tick_size_specification_ids": self.tick_size_specification_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.evidence_id != self._calculated_id():
            raise EvaluationDatasetAssemblyError(
                "evaluation-session evidence content identity failed"
            )


@dataclass(frozen=True, slots=True)
class AssembledEvaluationDataset:
    dataset: EvaluationDataset
    instruments: tuple[PointInTimeInstrument, ...]
    session_evidence: tuple[EvaluationSessionEvidence, ...]
    tick_sizes: tuple[EffectiveTickSize, ...]
    schema_version: str = EVALUATION_DATASET_ASSEMBLY_SCHEMA_VERSION
    assembly_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.dataset) is not EvaluationDataset:
            raise TypeError("assembled dataset must contain an exact EvaluationDataset")
        self.dataset.verify_content_identity()
        if (
            type(self.instruments) is not tuple
            or not self.instruments
            or any(type(value) is not PointInTimeInstrument for value in self.instruments)
            or self.instruments
            != tuple(sorted(self.instruments, key=lambda value: value.symbol))
        ):
            raise EvaluationDatasetAssemblyError(
                "assembled instruments must be a non-empty symbol-ordered exact tuple"
            )
        for value in self.instruments:
            value.verify_content_identity()
        if len({value.symbol for value in self.instruments}) != len(self.instruments):
            raise EvaluationDatasetAssemblyError(
                "assembled instruments contain duplicate symbols"
            )
        if len({value.stable_instrument_id for value in self.instruments}) != len(
            self.instruments
        ) or any(value.stable_instrument_id is None for value in self.instruments):
            raise EvaluationDatasetAssemblyError(
                "assembled instruments require unique adjudicated stable identities"
            )
        if (
            type(self.session_evidence) is not tuple
            or any(
                type(value) is not EvaluationSessionEvidence
                for value in self.session_evidence
            )
            or tuple(value.market_session for value in self.session_evidence)
            != self.dataset.sessions
        ):
            raise EvaluationDatasetAssemblyError(
                "session evidence must cover the dataset calendar exactly"
            )
        for value in self.session_evidence:
            value.verify_content_identity()
        if tuple(
            sorted(value.universe_snapshot_id for value in self.session_evidence)
        ) != self.dataset.universe_snapshot_ids:
            raise EvaluationDatasetAssemblyError(
                "session evidence differs from dataset universe lineage"
            )
        if (
            type(self.tick_sizes) is not tuple
            or not self.tick_sizes
            or any(type(value) is not EffectiveTickSize for value in self.tick_sizes)
        ):
            raise EvaluationDatasetAssemblyError(
                "assembled tick sizes must be a non-empty exact tuple"
            )
        for value in self.tick_sizes:
            value.verify_content_identity()
        if {value.specification_id for value in self.tick_sizes} != {
            specification_id
            for item in self.session_evidence
            for specification_id in item.tick_size_specification_ids
        }:
            raise EvaluationDatasetAssemblyError(
                "assembled tick-size specifications differ from session evidence"
            )
        if self.schema_version != EVALUATION_DATASET_ASSEMBLY_SCHEMA_VERSION:
            raise EvaluationDatasetAssemblyError(
                "unsupported evaluation-dataset assembly schema"
            )
        object.__setattr__(self, "assembly_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "dataset": self.dataset,
                "instruments": self.instruments,
                "session_evidence": self.session_evidence,
                "tick_sizes": self.tick_sizes,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.dataset.verify_content_identity()
        for value in self.instruments:
            if type(value) is not PointInTimeInstrument:
                raise EvaluationDatasetAssemblyError(
                    "assembled instrument graph contains an invalid type"
                )
            value.verify_content_identity()
        for value in self.session_evidence:
            if type(value) is not EvaluationSessionEvidence:
                raise EvaluationDatasetAssemblyError(
                    "assembled evidence graph contains an invalid type"
                )
            value.verify_content_identity()
        for value in self.tick_sizes:
            if type(value) is not EffectiveTickSize:
                raise EvaluationDatasetAssemblyError(
                    "assembled tick-size graph contains an invalid type"
                )
            value.verify_content_identity()
        if self.assembly_id != self._calculated_id():
            raise EvaluationDatasetAssemblyError(
                "assembled evaluation-dataset content identity failed"
            )


def point_in_time_price_session_from_nse(
    artifact: NseEodSessionArtifact,
) -> PointInTimePriceSession:
    """Normalize one replay-verified NSE artifact without upgrading readiness."""

    if type(artifact) is not NseEodSessionArtifact:
        raise TypeError("historical price artifact must be exact")
    artifact.verify_content_identity()
    bars = tuple(
        sorted(
            (
                PointInTimePriceBar(
                    session=value.market_session,
                    symbol=value.symbol,
                    series=value.series,
                    isin=value.validated_isin,
                    open=value.open,
                    high=value.high,
                    low=value.low,
                    close=value.close,
                    volume=value.volume,
                    raw_bar_id=value.bar_id,
                )
                for value in artifact.bars
            ),
            key=lambda value: (value.symbol, value.series),
        )
    )
    source_snapshot_ids = tuple(
        sorted(
            {
                artifact.artifact_id,
                artifact.source_bundle_manifest.artifact_id,
                artifact.source_bundle_manifest.manifest_id,
                *(value.report_ref_id for value in artifact.report_refs),
            }
        )
    )
    return PointInTimePriceSession(
        market_session=artifact.market_session,
        cutoff=artifact.cutoff,
        knowledge_time=artifact.knowledge_time,
        source_artifact_id=artifact.artifact_id,
        source_snapshot_ids=source_snapshot_ids,
        bars=bars,
        explicit_nontrading_listing_ids=(),
        readiness=_evaluation_readiness(artifact.readiness),
        actionable=artifact.actionable,
    )


def _tick_size_for(
    *,
    entry: object,
    session: date,
    cutoff: datetime,
    expected_readiness: ReferenceReadiness,
    tick_sizes: tuple[EffectiveTickSize, ...],
) -> EffectiveTickSize:
    from india_swing.reference.universe import UniverseEntry

    if type(entry) is not UniverseEntry:
        raise TypeError("tick-size lookup requires an exact UniverseEntry")
    matches = tuple(
        value
        for value in tick_sizes
        if value.instrument_id == entry.listing.instrument_id
        and value.listing_id == entry.listing.listing_id
        and value.is_effective_on(session)
        and value.knowledge_time <= cutoff
        and value.readiness is expected_readiness
    )
    if len(matches) != 1:
        raise EvaluationDatasetAssemblyError(
            "every actionable listing/session requires exactly one timely tick size"
        )
    return matches[0]


def assemble_evaluation_dataset(
    *,
    calendars: tuple[CalendarSnapshot, ...],
    universes: tuple[UniverseSnapshot, ...],
    price_sessions: tuple[PointInTimePriceSession, ...],
    tick_sizes: tuple[EffectiveTickSize, ...],
) -> AssembledEvaluationDataset:
    """Create an evaluation dataset only from complete, mutually bound evidence."""

    if type(calendars) is not tuple or not calendars or any(
        type(value) is not CalendarSnapshot for value in calendars
    ):
        raise TypeError("calendars must be a non-empty exact tuple")
    for value in calendars:
        value.verify_content_identity()
    first_calendar = calendars[0]
    if first_calendar.readiness is ReferenceReadiness.COLLECTION_ONLY:
        raise EvaluationDatasetAssemblyError(
            "collection-only calendars cannot enter an evaluation dataset"
        )
    if type(universes) is not tuple or not universes or any(
        type(value) is not UniverseSnapshot for value in universes
    ):
        raise TypeError("universes must be a non-empty exact tuple")
    if universes != tuple(sorted(universes, key=lambda value: value.market_session)):
        raise EvaluationDatasetAssemblyError("universes must be session ordered")
    if len({value.market_session for value in universes}) != len(universes):
        raise EvaluationDatasetAssemblyError("universes contain duplicate sessions")
    if type(price_sessions) is not tuple or not price_sessions or any(
        type(value) is not PointInTimePriceSession for value in price_sessions
    ):
        raise TypeError("price_sessions must be a non-empty exact tuple")
    if price_sessions != tuple(
        sorted(price_sessions, key=lambda value: value.market_session)
    ):
        raise EvaluationDatasetAssemblyError("price sessions must be session ordered")
    if len({value.market_session for value in price_sessions}) != len(price_sessions):
        raise EvaluationDatasetAssemblyError("price sessions contain duplicate sessions")
    if type(tick_sizes) is not tuple or not tick_sizes or any(
        type(value) is not EffectiveTickSize for value in tick_sizes
    ):
        raise TypeError("tick_sizes must be a non-empty exact tuple")
    if tick_sizes != tuple(
        sorted(
            tick_sizes,
            key=lambda value: (
                value.instrument_id,
                value.listing_id,
                value.effective_from_session,
                value.specification_id,
            ),
        )
    ):
        raise EvaluationDatasetAssemblyError("tick sizes must be deterministically ordered")
    for value in tick_sizes:
        value.verify_content_identity()

    expected_evaluation_readiness = _evaluation_readiness(first_calendar.readiness)
    if expected_evaluation_readiness is EvaluationDataReadiness.COLLECTION_ONLY:
        raise EvaluationDatasetAssemblyError("evaluation evidence is not verified")
    supplied_price_sessions = tuple(value.market_session for value in price_sessions)
    supplied_universe_sessions = tuple(value.market_session for value in universes)
    expected_sessions = supplied_price_sessions
    if supplied_universe_sessions != expected_sessions:
        raise EvaluationDatasetAssemblyError(
            "universes and price sessions must cover the same ordered sessions"
        )
    if len(calendars) != len(expected_sessions):
        raise EvaluationDatasetAssemblyError(
            "one calendar vintage is required for every evaluation session"
        )
    for index, (session, calendar) in enumerate(zip(expected_sessions, calendars)):
        if (
            calendar.exchange != first_calendar.exchange
            or calendar.segment != first_calendar.segment
            or calendar.readiness is not first_calendar.readiness
        ):
            raise EvaluationDatasetAssemblyError(
                "calendar vintages must share exchange, segment, and readiness"
            )
        calendar.require_session(session)
        if index < len(expected_sessions) - 1 and (
            calendar.next_session(session).day != expected_sessions[index + 1]
        ):
            raise EvaluationDatasetAssemblyError(
                "price sessions must follow each as-of calendar without gaps"
            )

    universe_by_session = {value.market_session: value for value in universes}
    price_by_session = {value.market_session: value for value in price_sessions}
    simulation_bars: list[SimulationBar] = []
    evidence: list[EvaluationSessionEvidence] = []
    accumulated: dict[
        str, dict[str, object]
    ] = {}

    for index, session in enumerate(expected_sessions):
        calendar = calendars[index]
        calendar_day = calendar.require_session(session)
        universe = universe_by_session[session]
        prices = price_by_session[session]
        universe.verify_content_identity()
        prices.verify_content_identity()
        if (
            universe.exchange != calendar.exchange
            or universe.segment != calendar.segment
            or universe.calendar_snapshot_id != calendar.snapshot_id
        ):
            raise EvaluationDatasetAssemblyError(
                "universe is not bound to the supplied calendar"
            )
        if universe.readiness is not calendar.readiness:
            raise EvaluationDatasetAssemblyError(
                "universe and calendar readiness differ"
            )
        if calendar.cutoff > prices.cutoff:
            raise EvaluationDatasetAssemblyError(
                "calendar vintage was not sealed by the price-session cutoff"
            )
        if (
            prices.readiness is not expected_evaluation_readiness
            or not prices.actionable
        ):
            raise EvaluationDatasetAssemblyError(
                "price sessions must be actionable at the dataset readiness"
            )
        if universe.cutoff > prices.cutoff:
            raise EvaluationDatasetAssemblyError(
                "universe was not available by the price-session cutoff"
            )
        if calendar_day.reference.knowledge_time > prices.cutoff:
            raise EvaluationDatasetAssemblyError(
                "calendar session was not known by the price-session cutoff"
            )
        if index < len(expected_sessions) - 1 and (
            calendar.next_session(session).reference.knowledge_time > prices.cutoff
        ):
            raise EvaluationDatasetAssemblyError(
                "next calendar session was not known by the decision cutoff"
            )

        entries_by_key = {
            (value.listing.tradingsymbol, value.listing.series): value
            for value in universe.entries
        }
        if len(entries_by_key) != len(universe.entries):
            raise EvaluationDatasetAssemblyError(
                "universe contains duplicate listing keys"
            )
        for bar in prices.bars:
            if bar.listing_key not in entries_by_key:
                raise EvaluationDatasetAssemblyError(
                    "price session contains a listing absent from its universe snapshot"
                )
        actionable = universe.actionable_entries
        if not actionable:
            raise EvaluationDatasetAssemblyError(
                "every evaluation session requires an actionable universe"
            )
        if len({value.listing.tradingsymbol for value in actionable}) != len(actionable):
            raise EvaluationDatasetAssemblyError(
                "actionable universe symbols must be unique across series"
            )
        price_by_key = {value.listing_key: value for value in prices.bars}
        missing_listing_ids = tuple(
            sorted(
                value.listing.listing_id
                for value in actionable
                if (value.listing.tradingsymbol, value.listing.series)
                not in price_by_key
            )
        )
        if missing_listing_ids != prices.explicit_nontrading_listing_ids:
            raise EvaluationDatasetAssemblyError(
                "missing actionable bars require exact explicit nontrading evidence"
            )

        session_tick_ids: list[str] = []
        for entry in actionable:
            listing = entry.listing
            if listing.isin is None:
                raise EvaluationDatasetAssemblyError(
                    "actionable listings require adjudicated ISIN identity"
                )
            tick = _tick_size_for(
                entry=entry,
                session=session,
                cutoff=prices.cutoff,
                expected_readiness=calendar.readiness,
                tick_sizes=tick_sizes,
            )
            session_tick_ids.append(tick.specification_id)
            key = (listing.tradingsymbol, listing.series)
            bar = price_by_key.get(key)
            if bar is not None:
                if bar.isin != listing.isin:
                    raise EvaluationDatasetAssemblyError(
                        "price bar ISIN differs from its adjudicated listing identity"
                    )
                simulation_bars.append(bar.to_simulation_bar())
            record = accumulated.setdefault(
                listing.instrument_id,
                {
                    "listing_id": listing.listing_id,
                    "symbol": listing.tradingsymbol,
                    "isin": listing.isin,
                    "tick_size": tick.tick_size,
                    "sessions": [],
                    "bindings": [],
                },
            )
            if (
                record["listing_id"] != listing.listing_id
                or record["symbol"] != listing.tradingsymbol
                or record["isin"] != listing.isin
                or record["tick_size"] != tick.tick_size
            ):
                raise EvaluationDatasetAssemblyError(
                    "baseline evaluation cannot cross listing, symbol, ISIN, or tick-size transitions"
                )
            sessions = record["sessions"]
            bindings = record["bindings"]
            assert isinstance(sessions, list) and isinstance(bindings, list)
            sessions.append(session)
            bindings.append((session, universe.snapshot_id))

        evidence.append(
            EvaluationSessionEvidence(
                market_session=session,
                calendar_snapshot_id=calendar.snapshot_id,
                universe_snapshot_id=universe.snapshot_id,
                price_snapshot_id=prices.snapshot_id,
                price_source_artifact_id=prices.source_artifact_id,
                price_source_snapshot_ids=prices.source_snapshot_ids,
                cutoff=prices.cutoff,
                actionable_listing_ids=tuple(
                    sorted(value.listing.listing_id for value in actionable)
                ),
                explicit_nontrading_listing_ids=missing_listing_ids,
                tick_size_specification_ids=tuple(sorted(set(session_tick_ids))),
            )
        )

    instruments = tuple(
        sorted(
            (
                PointInTimeInstrument(
                    symbol=str(value["symbol"]),
                    isin=str(value["isin"]),
                    universe_snapshot_id=value["bindings"][0][1],
                    eligible_sessions=tuple(value["sessions"]),
                    tick_size=value["tick_size"],
                    stable_instrument_id=stable_id,
                    eligibility_bindings=tuple(value["bindings"]),
                )
                for stable_id, value in accumulated.items()
            ),
            key=lambda value: value.symbol,
        )
    )
    referenced_tick_size_ids = {
        specification_id
        for item in evidence
        for specification_id in item.tick_size_specification_ids
    }
    if referenced_tick_size_ids != {
        value.specification_id for value in tick_sizes
    }:
        raise EvaluationDatasetAssemblyError(
            "tick-size input must exactly equal the evidence used by the dataset"
        )
    dataset = EvaluationDataset(
        sessions=expected_sessions,
        bars=tuple(
            sorted(simulation_bars, key=lambda value: (value.session, value.symbol))
        ),
        source_snapshot_ids=tuple(
            sorted(
                {
                    *(value.snapshot_id for value in calendars),
                    *(value.snapshot_id for value in price_sessions),
                    *(
                        snapshot_id
                        for value in price_sessions
                        for snapshot_id in value.source_snapshot_ids
                    ),
                    *(value.specification_id for value in tick_sizes),
                }
            )
        ),
        universe_snapshot_ids=tuple(
            sorted(value.snapshot_id for value in universes)
        ),
        readiness=expected_evaluation_readiness,
    )
    return AssembledEvaluationDataset(
        dataset=dataset,
        instruments=instruments,
        session_evidence=tuple(evidence),
        tick_sizes=tick_sizes,
    )
