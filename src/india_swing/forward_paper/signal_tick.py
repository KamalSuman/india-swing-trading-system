"""Exact signal-session tick evidence derived from one NSE security master."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Protocol, Union

from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.identity import content_id
from india_swing.identity_decisions import (
    stable_instrument_id_for_isin,
    stable_listing_id_for_series,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import (
    ParsedNseCmSecurityMaster,
    SourceRowDisposition,
)
from india_swing.tick_sizes.effective_session import (
    VerifiedPromotedEffectiveSessionTickPanel,
)


FORWARD_PAPER_SIGNAL_TICK_ENTRY_SCHEMA_VERSION = "forward-paper-signal-tick-entry/v1"
FORWARD_PAPER_SIGNAL_TICK_PANEL_SCHEMA_VERSION = "forward-paper-signal-tick-panel/v1"
FORWARD_PAPER_SIGNAL_TICK_POLICY_VERSION = (
    "exact-current-master-normal-market-eq-sm-signal-session-only-v1"
)
FORWARD_PAPER_SIGNAL_TICK_CODEC_VERSION = "forward-paper-signal-tick-json/v1"
MAXIMUM_FORWARD_PAPER_SIGNAL_TICK_BYTES = 32 * 1024 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9&-]{0,31}\Z")
_SERIES = frozenset({"EQ", "SM"})
_DIRECTORY = "forward-paper-signal-ticks"
_ONE_DAY = timedelta(days=1)


class ForwardPaperSignalTickError(ValueError):
    """Signal tick evidence is malformed, inconsistent, or unavailable."""


class ForwardPaperSignalTickNotFound(ForwardPaperSignalTickError):
    pass


class ForwardPaperSignalTickConflict(ForwardPaperSignalTickError):
    pass


def _fail(message: str) -> None:
    raise ForwardPaperSignalTickError(message)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _utc(value: object) -> datetime:
    if type(value) is not datetime:
        _fail("forward paper signal tick timestamp is invalid")
    try:
        offset = value.utcoffset()
    except Exception:
        offset = None
    if value.tzinfo is None or offset is None:
        _fail("forward paper signal tick timestamp is invalid")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class ForwardPaperSignalTickEntry:
    symbol: str
    series: str
    validated_isin: str
    stable_instrument_id: str
    stable_listing_id: str
    source_record_id: str
    tick_specification: EffectiveTickSize
    entry_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.symbol) is not str
            or _SYMBOL.fullmatch(self.symbol) is None
            or self.series not in _SERIES
            or not _sha(self.source_record_id)
            or not _sha(self.stable_instrument_id)
            or not _sha(self.stable_listing_id)
            or type(self.tick_specification) is not EffectiveTickSize
        ):
            _fail("forward paper signal tick entry is invalid")
        failed = False
        try:
            expected_instrument = stable_instrument_id_for_isin(self.validated_isin)
            expected_listing = stable_listing_id_for_series(
                expected_instrument, self.series
            )
            self.tick_specification.verify_content_identity()
        except Exception:
            failed = True
        if failed:
            _fail("forward paper signal tick entry failed verification")
        specification = self.tick_specification
        if (
            self.stable_instrument_id != expected_instrument
            or self.stable_listing_id != expected_listing
            or specification.instrument_id != self.stable_instrument_id
            or specification.listing_id != self.stable_listing_id
            or specification.readiness is not ReferenceReadiness.POINT_IN_TIME_VERIFIED
            or specification.effective_to_exclusive
            != specification.effective_from_session + _ONE_DAY
        ):
            _fail("forward paper signal tick entry lineage is invalid")
        object.__setattr__(self, "entry_id", self._calculated_id())

    @property
    def market_session(self) -> date:
        return self.tick_specification.effective_from_session

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_SIGNAL_TICK_ENTRY_SCHEMA_VERSION,
                "policy": FORWARD_PAPER_SIGNAL_TICK_POLICY_VERSION,
                "symbol": self.symbol,
                "series": self.series,
                "validated_isin": self.validated_isin,
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "source_record_id": self.source_record_id,
                "tick_specification_id": self.tick_specification.specification_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.tick_specification.verify_content_identity()
        if self.entry_id != self._calculated_id():
            _fail("forward paper signal tick entry identity failed")


@dataclass(frozen=True, slots=True)
class ForwardPaperSignalTickPanel:
    signal_session: date
    cutoff: datetime
    knowledge_time: datetime
    source_security_master_id: str
    source_schema_version: str
    entries: tuple[ForwardPaperSignalTickEntry, ...]
    excluded_record_count: int
    schema_version: str = FORWARD_PAPER_SIGNAL_TICK_PANEL_SCHEMA_VERSION
    policy_version: str = FORWARD_PAPER_SIGNAL_TICK_POLICY_VERSION
    panel_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "cutoff", _utc(self.cutoff))
        object.__setattr__(self, "knowledge_time", _utc(self.knowledge_time))
        self._validate()
        object.__setattr__(self, "panel_id", self._calculated_id())

    def _validate(self) -> None:
        if (
            type(self.signal_session) is not date
            or self.knowledge_time > self.cutoff
            or not _sha(self.source_security_master_id)
            or type(self.source_schema_version) is not str
            or not self.source_schema_version
            or type(self.entries) is not tuple
            or not self.entries
            or any(type(value) is not ForwardPaperSignalTickEntry for value in self.entries)
            or self.entries
            != tuple(
                sorted(
                    self.entries,
                    key=lambda value: (
                        value.stable_instrument_id,
                        value.stable_listing_id,
                        value.entry_id,
                    ),
                )
            )
            or type(self.excluded_record_count) is not int
            or self.excluded_record_count < 0
            or self.schema_version != FORWARD_PAPER_SIGNAL_TICK_PANEL_SCHEMA_VERSION
            or self.policy_version != FORWARD_PAPER_SIGNAL_TICK_POLICY_VERSION
        ):
            _fail("forward paper signal tick panel is invalid")
        listing_ids: set[str] = set()
        listing_keys: set[tuple[str, str]] = set()
        specification_ids: set[str] = set()
        for entry in self.entries:
            entry.verify_content_identity()
            specification = entry.tick_specification
            if (
                entry.market_session != self.signal_session
                or specification.knowledge_time != self.knowledge_time
                or specification.source_snapshot_id != self.source_security_master_id
                or entry.stable_listing_id in listing_ids
                or (entry.symbol, entry.series) in listing_keys
                or specification.specification_id in specification_ids
            ):
                _fail("forward paper signal tick panel lineage is invalid")
            listing_ids.add(entry.stable_listing_id)
            listing_keys.add((entry.symbol, entry.series))
            specification_ids.add(specification.specification_id)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": self.schema_version,
                "policy": self.policy_version,
                "signal_session": self.signal_session,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
                "source_security_master_id": self.source_security_master_id,
                "source_schema_version": self.source_schema_version,
                "entry_ids": tuple(value.entry_id for value in self.entries),
                "excluded_record_count": self.excluded_record_count,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.panel_id != self._calculated_id():
            _fail("forward paper signal tick panel identity failed")

    @property
    def collection_only(self) -> bool:
        return True

    @property
    def actionable(self) -> bool:
        return False

    @property
    def feature_eligible(self) -> bool:
        return False

    @property
    def alert_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False


def materialize_forward_paper_signal_tick_panel(
    source: ParsedNseCmSecurityMaster,
    *,
    knowledge_time: datetime,
    cutoff: datetime,
) -> ForwardPaperSignalTickPanel:
    if type(source) is not ParsedNseCmSecurityMaster:
        _fail("forward paper signal tick source is invalid")
    known = _utc(knowledge_time)
    decision_cutoff = _utc(cutoff)
    if known > decision_cutoff:
        _fail("forward paper signal tick source is future-known")
    entries: list[ForwardPaperSignalTickEntry] = []
    excluded = 0
    for record in source.records:
        normal_market = record.market_eligibility[0]
        retained = (
            record.security_series in _SERIES
            and record.validated_isin is not None
            and record.disposition is SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
            and record.delete_flag == "N"
            and normal_market.status == 6
            and normal_market.eligible
        )
        if not retained:
            excluded += 1
            continue
        stable_instrument_id = stable_instrument_id_for_isin(record.validated_isin)
        stable_listing_id = stable_listing_id_for_series(
            stable_instrument_id, record.security_series
        )
        specification = EffectiveTickSize(
            instrument_id=stable_instrument_id,
            listing_id=stable_listing_id,
            effective_from_session=source.claimed_report_date,
            effective_to_exclusive=source.claimed_report_date + _ONE_DAY,
            tick_size=Decimal(record.bid_interval_paise) / Decimal(100),
            knowledge_time=known,
            source_snapshot_id=source.raw_sha256,
            readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
        )
        entries.append(
            ForwardPaperSignalTickEntry(
                symbol=record.ticker_symbol,
                series=record.security_series,
                validated_isin=record.validated_isin,
                stable_instrument_id=stable_instrument_id,
                stable_listing_id=stable_listing_id,
                source_record_id=record.source_record_id,
                tick_specification=specification,
            )
        )
    return ForwardPaperSignalTickPanel(
        signal_session=source.claimed_report_date,
        cutoff=decision_cutoff,
        knowledge_time=known,
        source_security_master_id=source.raw_sha256,
        source_schema_version=source.source_schema_version,
        entries=tuple(
            sorted(
                entries,
                key=lambda value: (
                    value.stable_instrument_id,
                    value.stable_listing_id,
                    value.entry_id,
                ),
            )
        ),
        excluded_record_count=excluded,
    )


def _entry_value(value: ForwardPaperSignalTickEntry) -> dict[str, object]:
    specification = value.tick_specification
    return {
        "entry_id": value.entry_id,
        "series": value.series,
        "source_record_id": value.source_record_id,
        "stable_instrument_id": value.stable_instrument_id,
        "stable_listing_id": value.stable_listing_id,
        "symbol": value.symbol,
        "tick_specification": {
            "effective_from_session": specification.effective_from_session.isoformat(),
            "effective_to_exclusive": specification.effective_to_exclusive.isoformat(),
            "instrument_id": specification.instrument_id,
            "knowledge_time": specification.knowledge_time.isoformat(),
            "listing_id": specification.listing_id,
            "readiness": specification.readiness.value,
            "source_snapshot_id": specification.source_snapshot_id,
            "specification_id": specification.specification_id,
            "tick_size": str(specification.tick_size),
        },
        "validated_isin": value.validated_isin,
    }


def encode_forward_paper_signal_tick_panel(
    value: ForwardPaperSignalTickPanel,
) -> bytes:
    if type(value) is not ForwardPaperSignalTickPanel:
        _fail("forward paper signal tick panel is invalid")
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_version": FORWARD_PAPER_SIGNAL_TICK_CODEC_VERSION,
                "panel": {
                    "cutoff": value.cutoff.isoformat(),
                    "entries": [_entry_value(item) for item in value.entries],
                    "excluded_record_count": value.excluded_record_count,
                    "knowledge_time": value.knowledge_time.isoformat(),
                    "panel_id": value.panel_id,
                    "policy_version": value.policy_version,
                    "schema_version": value.schema_version,
                    "signal_session": value.signal_session.isoformat(),
                    "source_schema_version": value.source_schema_version,
                    "source_security_master_id": value.source_security_master_id,
                },
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_FORWARD_PAPER_SIGNAL_TICK_BYTES:
        _fail("forward paper signal tick payload exceeds its size limit")
    return payload


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _fail("forward paper signal tick payload contains a duplicate key")
        result[key] = value
    return result


def decode_forward_paper_signal_tick_panel(payload: bytes) -> ForwardPaperSignalTickPanel:
    if type(payload) is not bytes or not (
        0 < len(payload) <= MAXIMUM_FORWARD_PAPER_SIGNAL_TICK_BYTES
    ):
        _fail("forward paper signal tick payload is invalid")
    failed = False
    panel = None
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique,
            parse_float=lambda _value: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if type(raw) is not dict or set(raw) != {"codec_version", "panel"}:
            raise ValueError
        if raw["codec_version"] != FORWARD_PAPER_SIGNAL_TICK_CODEC_VERSION:
            raise ValueError
        body = raw["panel"]
        if type(body) is not dict or set(body) != {
            "cutoff",
            "entries",
            "excluded_record_count",
            "knowledge_time",
            "panel_id",
            "policy_version",
            "schema_version",
            "signal_session",
            "source_schema_version",
            "source_security_master_id",
        }:
            raise ValueError
        entries = []
        for raw_entry in body["entries"]:
            if type(raw_entry) is not dict or set(raw_entry) != {
                "entry_id",
                "series",
                "source_record_id",
                "stable_instrument_id",
                "stable_listing_id",
                "symbol",
                "tick_specification",
                "validated_isin",
            }:
                raise ValueError
            spec = raw_entry["tick_specification"]
            if type(spec) is not dict or set(spec) != {
                "effective_from_session",
                "effective_to_exclusive",
                "instrument_id",
                "knowledge_time",
                "listing_id",
                "readiness",
                "source_snapshot_id",
                "specification_id",
                "tick_size",
            }:
                raise ValueError
            tick = EffectiveTickSize(
                instrument_id=spec["instrument_id"],
                listing_id=spec["listing_id"],
                effective_from_session=date.fromisoformat(spec["effective_from_session"]),
                effective_to_exclusive=date.fromisoformat(spec["effective_to_exclusive"]),
                tick_size=Decimal(spec["tick_size"]),
                knowledge_time=datetime.fromisoformat(spec["knowledge_time"]),
                source_snapshot_id=spec["source_snapshot_id"],
                readiness=ReferenceReadiness(spec["readiness"]),
            )
            if tick.specification_id != spec["specification_id"]:
                raise ValueError
            entry = ForwardPaperSignalTickEntry(
                symbol=raw_entry["symbol"],
                series=raw_entry["series"],
                validated_isin=raw_entry["validated_isin"],
                stable_instrument_id=raw_entry["stable_instrument_id"],
                stable_listing_id=raw_entry["stable_listing_id"],
                source_record_id=raw_entry["source_record_id"],
                tick_specification=tick,
            )
            if entry.entry_id != raw_entry["entry_id"]:
                raise ValueError
            entries.append(entry)
        panel = ForwardPaperSignalTickPanel(
            signal_session=date.fromisoformat(body["signal_session"]),
            cutoff=datetime.fromisoformat(body["cutoff"]),
            knowledge_time=datetime.fromisoformat(body["knowledge_time"]),
            source_security_master_id=body["source_security_master_id"],
            source_schema_version=body["source_schema_version"],
            entries=tuple(entries),
            excluded_record_count=body["excluded_record_count"],
            schema_version=body["schema_version"],
            policy_version=body["policy_version"],
        )
        if panel.panel_id != body["panel_id"]:
            raise ValueError
        if encode_forward_paper_signal_tick_panel(panel) != payload:
            raise ValueError
    except Exception:
        failed = True
    if failed or panel is None:
        _fail("forward paper signal tick payload failed verification")
    return panel


class LocalForwardPaperSignalTickPanelStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root) / _DIRECTORY

    def path_for(self, panel_id: str) -> Path:
        if not _sha(panel_id):
            _fail("forward paper signal tick panel identity is invalid")
        return self.root / f"{panel_id}.json"

    def put(self, value: ForwardPaperSignalTickPanel) -> ForwardPaperSignalTickPanel:
        if type(value) is not ForwardPaperSignalTickPanel:
            _fail("forward paper signal tick panel is invalid")
        payload = encode_forward_paper_signal_tick_panel(value)
        path = self.path_for(value.panel_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            try:
                existing = path.read_bytes()
            except OSError:
                raise ForwardPaperSignalTickConflict(
                    "forward paper signal tick artifact conflicts"
                ) from None
            if existing != payload:
                raise ForwardPaperSignalTickConflict(
                    "forward paper signal tick artifact conflicts"
                )
        return self.get(value.panel_id)

    def get(self, panel_id: str) -> ForwardPaperSignalTickPanel:
        path = self.path_for(panel_id)
        try:
            payload = path.read_bytes()
        except FileNotFoundError:
            raise ForwardPaperSignalTickNotFound(
                "forward paper signal tick artifact was not found"
            ) from None
        except OSError:
            _fail("forward paper signal tick artifact could not be read")
        panel = decode_forward_paper_signal_tick_panel(payload)
        if panel.panel_id != panel_id:
            _fail("forward paper signal tick artifact identity is invalid")
        return panel


ForwardPaperTickPanel = Union[
    ForwardPaperSignalTickPanel,
    VerifiedPromotedEffectiveSessionTickPanel,
]


def is_forward_paper_tick_panel(value: object) -> bool:
    return type(value) in (
        ForwardPaperSignalTickPanel,
        VerifiedPromotedEffectiveSessionTickPanel,
    )


class _PromotedTickResolver(Protocol):
    def get(self, panel_id: str) -> VerifiedPromotedEffectiveSessionTickPanel: ...


class ExactForwardPaperTickPanelResolver:
    """Resolve an exact ID from the direct store, then the legacy store."""

    def __init__(
        self,
        signal_ticks: LocalForwardPaperSignalTickPanelStore,
        promoted_ticks: _PromotedTickResolver,
    ) -> None:
        self.signal_ticks = signal_ticks
        self.promoted_ticks = promoted_ticks

    def get(self, panel_id: str) -> ForwardPaperTickPanel:
        try:
            return self.signal_ticks.get(panel_id)
        except ForwardPaperSignalTickNotFound:
            pass
        value = self.promoted_ticks.get(panel_id)
        if type(value) is not VerifiedPromotedEffectiveSessionTickPanel:
            _fail("forward paper tick resolver returned an invalid panel")
        value.verify_content_identity()
        if value.panel_id != panel_id:
            _fail("forward paper tick resolver returned another panel")
        return value
