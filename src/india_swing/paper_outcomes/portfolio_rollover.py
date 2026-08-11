from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import re

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.market_data.models import require_canonical_listing_keys
from india_swing.operations.portfolio_store import (
    SwingPortfolioEvidenceBinding,
    SwingPortfolioEvidenceKind,
    SwingPortfolioSnapshotArtifact,
    SwingPortfolioVerificationStatus,
    decode_swing_portfolio_artifact,
    encode_swing_portfolio_artifact,
)
from india_swing.risk.swing_portfolio import SwingPortfolioSnapshot

from .models import PaperOutcomeObservation, PaperOutcomeStatus
from .portfolio import PaperPortfolioError, PaperPortfolioPosition, PaperPortfolioState


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"IN[A-Z0-9]{9}[0-9]\Z")
_MARK_SCHEMA = "paper-portfolio-mark/v1"
_ROLLOVER_SCHEMA = "paper-portfolio-rollover/v1"
_ROLLOVER_CODEC = "paper-portfolio-rollover-json/v1"
_SOURCE_VERSION = "paper-portfolio-rollover/v1"
_MAXIMUM_ROLLOVER_BYTES = 512 * 1024
_UNRESOLVED = frozenset({PaperOutcomeStatus.WAITING, PaperOutcomeStatus.BLOCKED})


class PaperPortfolioRolloverError(PaperPortfolioError):
    pass


class PaperPortfolioRolloverNotFound(PaperPortfolioRolloverError):
    pass


class PaperPortfolioRolloverConflict(PaperPortfolioRolloverError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperPortfolioRolloverError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise PaperPortfolioRolloverError(f"{name} must be timezone-aware")
    try:
        if value.utcoffset() is None:
            raise ValueError
        return value.astimezone(timezone.utc)
    except Exception:
        raise PaperPortfolioRolloverError(f"{name} has invalid timezone behavior") from None


def _finite(value: object, name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise PaperPortfolioRolloverError(f"{name} must be a finite Decimal")
    return value


def _identity(value: object, omitted: set[str]) -> str:
    return content_id(
        {
            item.name: getattr(value, item.name)
            for item in fields(value)
            if item.name not in omitted
        },
        length=64,
    )


def _canonical_keys(value: tuple[str, ...]) -> None:
    if type(value) is not tuple:
        raise PaperPortfolioRolloverError("open listing keys must be an exact tuple")
    if not value:
        return
    try:
        require_canonical_listing_keys(value)
    except Exception:
        raise PaperPortfolioRolloverError("open listing keys are invalid") from None


@dataclass(frozen=True, slots=True)
class PaperPortfolioMark:
    registration_id: str
    position_id: str
    symbol: str
    listing_key: str
    series: str
    validated_isin: str
    artifact_id: str
    calendar_snapshot_id: str
    bar_id: str
    observation_id: str
    market_session: date
    session_close_at: datetime
    knowledge_time: datetime
    close: Decimal
    price_basis: str
    schema_version: str = _MARK_SCHEMA
    mark_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.registration_id, "registration_id"),
            (self.position_id, "position_id"),
            (self.artifact_id, "artifact_id"),
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
            (self.bar_id, "bar_id"),
            (self.observation_id, "observation_id"),
        ):
            _sha(value, name)
        if (
            type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.strip().upper()
        ):
            raise PaperPortfolioRolloverError("paper portfolio mark symbol is invalid")
        _canonical_keys((self.listing_key,))
        if self.listing_key != f"NSE:{self.symbol}":
            raise PaperPortfolioRolloverError("paper portfolio mark listing differs")
        if (
            type(self.series) is not str
            or not self.series
            or self.series != self.series.strip().upper()
        ):
            raise PaperPortfolioRolloverError("paper portfolio mark series is invalid")
        if type(self.validated_isin) is not str or _ISIN.fullmatch(self.validated_isin) is None:
            raise PaperPortfolioRolloverError("paper portfolio mark ISIN is invalid")
        if type(self.market_session) is not date:
            raise PaperPortfolioRolloverError("paper portfolio mark session is invalid")
        object.__setattr__(
            self,
            "session_close_at",
            _utc(self.session_close_at, "session_close_at"),
        )
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "knowledge_time"),
        )
        if self.knowledge_time <= self.session_close_at:
            raise PaperPortfolioRolloverError("paper portfolio mark knowledge time is invalid")
        if _finite(self.close, "close") <= 0:
            raise PaperPortfolioRolloverError("paper portfolio mark close must be positive")
        if self.price_basis != "RAW_UNADJUSTED":
            raise PaperPortfolioRolloverError("paper portfolio mark price basis is unsupported")
        if self.schema_version != _MARK_SCHEMA:
            raise PaperPortfolioRolloverError("unsupported paper portfolio mark schema")
        object.__setattr__(self, "mark_id", _identity(self, {"mark_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperPortfolioMark(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "mark_id"
                }
            )
        except Exception:
            raise PaperPortfolioRolloverError("paper portfolio mark identity failed") from None
        if fresh.mark_id != self.mark_id:
            raise PaperPortfolioRolloverError("paper portfolio mark identity failed")


def build_paper_portfolio_mark(
    *,
    position: PaperPortfolioPosition,
    listing_key: str,
    observation: PaperOutcomeObservation,
) -> PaperPortfolioMark:
    if type(position) is not PaperPortfolioPosition:
        raise PaperPortfolioRolloverError("paper portfolio position must be exact")
    if type(observation) is not PaperOutcomeObservation:
        raise PaperPortfolioRolloverError("paper portfolio observation must be exact")
    try:
        position.verify_content_identity()
        observation.verify_content_identity()
    except Exception:
        raise PaperPortfolioRolloverError("paper portfolio mark input identity failed") from None
    if position.outcome_status is not PaperOutcomeStatus.OPEN:
        raise PaperPortfolioRolloverError("only open positions may be marked")
    if (
        not observation.traded
        or observation.close is None
        or observation.bar_id is None
        or observation.symbol != position.symbol
    ):
        raise PaperPortfolioRolloverError("paper portfolio mark observation differs")
    return PaperPortfolioMark(
        registration_id=position.registration_id,
        position_id=position.position_id,
        symbol=position.symbol,
        listing_key=listing_key,
        series=observation.series,
        validated_isin=observation.validated_isin,
        artifact_id=observation.artifact_id,
        calendar_snapshot_id=observation.calendar_snapshot_id,
        bar_id=observation.bar_id,
        observation_id=observation.observation_id,
        market_session=observation.market_session,
        session_close_at=observation.session_close_at,
        knowledge_time=observation.knowledge_time,
        close=observation.close,
        price_basis=observation.price_basis,
    )


@dataclass(frozen=True, slots=True)
class PaperPortfolioRollover:
    genesis_artifact_id: str
    previous_rollover_id: str | None
    previous_portfolio_artifact_id: str
    paper_portfolio_state_id: str
    paper_portfolio_batch_id: str
    as_of: datetime
    marks: tuple[PaperPortfolioMark, ...]
    open_listing_keys: tuple[str, ...]
    starting_capital: Decimal
    realized_equity: Decimal
    peak_realized_pnl: Decimal
    open_entry_notional: Decimal
    reserved_open_costs: Decimal
    cash_available: Decimal
    gross_exposure: Decimal
    unrealized_gross_pnl: Decimal
    unrealized_net_pnl: Decimal
    nav: Decimal
    prior_peak_nav: Decimal
    peak_nav: Decimal
    nav_drawdown: Decimal
    portfolio_artifact: SwingPortfolioSnapshotArtifact
    schema_version: str = _ROLLOVER_SCHEMA
    rollover_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.genesis_artifact_id, "genesis_artifact_id"),
            (self.previous_portfolio_artifact_id, "previous_portfolio_artifact_id"),
            (self.paper_portfolio_state_id, "paper_portfolio_state_id"),
            (self.paper_portfolio_batch_id, "paper_portfolio_batch_id"),
        ):
            _sha(value, name)
        if self.previous_rollover_id is not None:
            _sha(self.previous_rollover_id, "previous_rollover_id")
        object.__setattr__(self, "as_of", _utc(self.as_of, "as_of"))
        if (
            type(self.marks) is not tuple
            or any(type(value) is not PaperPortfolioMark for value in self.marks)
        ):
            raise PaperPortfolioRolloverError("paper portfolio marks must be an exact tuple")
        for value in self.marks:
            value.verify_content_identity()
        registration_ids = tuple(value.registration_id for value in self.marks)
        if registration_ids != tuple(sorted(set(registration_ids))):
            raise PaperPortfolioRolloverError("paper portfolio marks are not canonical")
        _canonical_keys(self.open_listing_keys)
        if self.open_listing_keys != tuple(sorted(value.listing_key for value in self.marks)):
            raise PaperPortfolioRolloverError("paper portfolio open listings differ")
        for value, name in (
            (self.starting_capital, "starting_capital"),
            (self.realized_equity, "realized_equity"),
            (self.peak_realized_pnl, "peak_realized_pnl"),
            (self.open_entry_notional, "open_entry_notional"),
            (self.reserved_open_costs, "reserved_open_costs"),
            (self.cash_available, "cash_available"),
            (self.gross_exposure, "gross_exposure"),
            (self.unrealized_gross_pnl, "unrealized_gross_pnl"),
            (self.unrealized_net_pnl, "unrealized_net_pnl"),
            (self.nav, "nav"),
            (self.prior_peak_nav, "prior_peak_nav"),
            (self.peak_nav, "peak_nav"),
            (self.nav_drawdown, "nav_drawdown"),
        ):
            _finite(value, name)
        if (
            self.starting_capital <= 0
            or self.realized_equity <= 0
            or self.peak_realized_pnl < 0
            or self.cash_available < 0
            or self.gross_exposure < 0
            or self.open_entry_notional < 0
            or self.reserved_open_costs < 0
            or self.nav <= 0
            or self.prior_peak_nav <= 0
            or self.peak_nav <= 0
            or self.nav_drawdown < 0
        ):
            raise PaperPortfolioRolloverError("paper portfolio rollover amounts are invalid")
        if self.nav != self.cash_available + self.gross_exposure:
            raise PaperPortfolioRolloverError("paper portfolio NAV does not reconcile")
        if self.unrealized_gross_pnl != self.gross_exposure - self.open_entry_notional:
            raise PaperPortfolioRolloverError("paper portfolio gross mark P&L differs")
        if (
            self.unrealized_net_pnl
            != self.unrealized_gross_pnl - self.reserved_open_costs
            or self.nav != self.realized_equity + self.unrealized_net_pnl
        ):
            raise PaperPortfolioRolloverError("paper portfolio net mark P&L differs")
        if self.peak_nav != max(self.prior_peak_nav, self.nav):
            raise PaperPortfolioRolloverError("paper portfolio peak NAV differs")
        if self.nav_drawdown != self.peak_nav - self.nav:
            raise PaperPortfolioRolloverError("paper portfolio NAV drawdown differs")
        if type(self.portfolio_artifact) is not SwingPortfolioSnapshotArtifact:
            raise PaperPortfolioRolloverError("rollover portfolio artifact must be exact")
        try:
            self.portfolio_artifact.verify_content_identity()
        except Exception:
            raise PaperPortfolioRolloverError("rollover portfolio artifact identity failed") from None
        portfolio = self.portfolio_artifact.portfolio
        if (
            self.portfolio_artifact.verification_status
            is not SwingPortfolioVerificationStatus.DERIVED_RECONCILED_PAPER_ONLY
            or self.portfolio_artifact.reconciled_at != self.as_of
            or portfolio.as_of != self.as_of
            or portfolio.capital != self.nav
            or portfolio.cash_available != self.cash_available
            or portfolio.gross_exposure != self.gross_exposure
            or portfolio.open_positions != len(self.marks)
        ):
            raise PaperPortfolioRolloverError("rollover portfolio snapshot differs")
        if self.schema_version != _ROLLOVER_SCHEMA:
            raise PaperPortfolioRolloverError("unsupported paper portfolio rollover schema")
        object.__setattr__(self, "rollover_id", _identity(self, {"rollover_id"}))

    def verify_content_identity(self) -> None:
        try:
            self.portfolio_artifact.verify_content_identity()
            for value in self.marks:
                value.verify_content_identity()
            fresh = PaperPortfolioRollover(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "rollover_id"
                }
            )
        except Exception:
            raise PaperPortfolioRolloverError("paper portfolio rollover identity failed") from None
        if fresh.rollover_id != self.rollover_id:
            raise PaperPortfolioRolloverError("paper portfolio rollover identity failed")


def _mark_body(value: PaperPortfolioMark) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "artifact_id": value.artifact_id,
        "bar_id": value.bar_id,
        "calendar_snapshot_id": value.calendar_snapshot_id,
        "close": str(value.close),
        "knowledge_time": value.knowledge_time.isoformat(),
        "listing_key": value.listing_key,
        "mark_id": value.mark_id,
        "market_session": value.market_session.isoformat(),
        "observation_id": value.observation_id,
        "position_id": value.position_id,
        "price_basis": value.price_basis,
        "registration_id": value.registration_id,
        "schema_version": value.schema_version,
        "series": value.series,
        "session_close_at": value.session_close_at.isoformat(),
        "symbol": value.symbol,
        "validated_isin": value.validated_isin,
    }


def encode_paper_portfolio_rollover(value: PaperPortfolioRollover) -> bytes:
    if type(value) is not PaperPortfolioRollover:
        raise PaperPortfolioRolloverError("paper portfolio rollover must be exact")
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": _ROLLOVER_CODEC,
                "rollover": {
                    "as_of": value.as_of.isoformat(),
                    "cash_available": str(value.cash_available),
                    "genesis_artifact_id": value.genesis_artifact_id,
                    "gross_exposure": str(value.gross_exposure),
                    "marks": [_mark_body(item) for item in value.marks],
                    "nav": str(value.nav),
                    "nav_drawdown": str(value.nav_drawdown),
                    "open_entry_notional": str(value.open_entry_notional),
                    "open_listing_keys": list(value.open_listing_keys),
                    "paper_portfolio_batch_id": value.paper_portfolio_batch_id,
                    "paper_portfolio_state_id": value.paper_portfolio_state_id,
                    "peak_realized_pnl": str(value.peak_realized_pnl),
                    "peak_nav": str(value.peak_nav),
                    "portfolio_artifact_json": encode_swing_portfolio_artifact(
                        value.portfolio_artifact
                    ).decode("utf-8"),
                    "previous_portfolio_artifact_id": value.previous_portfolio_artifact_id,
                    "previous_rollover_id": value.previous_rollover_id,
                    "prior_peak_nav": str(value.prior_peak_nav),
                    "realized_equity": str(value.realized_equity),
                    "reserved_open_costs": str(value.reserved_open_costs),
                    "rollover_id": value.rollover_id,
                    "schema_version": value.schema_version,
                    "starting_capital": str(value.starting_capital),
                    "unrealized_gross_pnl": str(value.unrealized_gross_pnl),
                    "unrealized_net_pnl": str(value.unrealized_net_pnl),
                },
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_ROLLOVER_BYTES:
        raise PaperPortfolioRolloverError("paper portfolio rollover exceeds its size limit")
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _strict_object(value: object, expected: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise ValueError
    return value


def _strict_datetime(value: object) -> datetime:
    if type(value) is not str:
        raise ValueError
    result = datetime.fromisoformat(value)
    if (
        result.tzinfo is None
        or result.astimezone(timezone.utc).isoformat() != value
    ):
        raise ValueError
    return result


def _strict_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise ValueError
    result = Decimal(value)
    if not result.is_finite() or str(result) != value:
        raise ValueError
    return result


_MARK_FIELDS = {
    "artifact_id",
    "bar_id",
    "calendar_snapshot_id",
    "close",
    "knowledge_time",
    "listing_key",
    "mark_id",
    "market_session",
    "observation_id",
    "position_id",
    "price_basis",
    "registration_id",
    "schema_version",
    "series",
    "session_close_at",
    "symbol",
    "validated_isin",
}
_ROLLOVER_FIELDS = {
    "as_of",
    "cash_available",
    "genesis_artifact_id",
    "gross_exposure",
    "marks",
    "nav",
    "nav_drawdown",
    "open_entry_notional",
    "open_listing_keys",
    "paper_portfolio_batch_id",
    "paper_portfolio_state_id",
    "peak_realized_pnl",
    "peak_nav",
    "portfolio_artifact_json",
    "previous_portfolio_artifact_id",
    "previous_rollover_id",
    "prior_peak_nav",
    "realized_equity",
    "reserved_open_costs",
    "rollover_id",
    "schema_version",
    "starting_capital",
    "unrealized_gross_pnl",
    "unrealized_net_pnl",
}


def decode_paper_portfolio_rollover(payload: bytes) -> PaperPortfolioRollover:
    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_ROLLOVER_BYTES:
        raise PaperPortfolioRolloverError("stored paper portfolio rollover is invalid")
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        envelope = _strict_object(root, {"codec_schema_version", "rollover"})
        if envelope["codec_schema_version"] != _ROLLOVER_CODEC:
            raise ValueError
        raw = _strict_object(envelope["rollover"], _ROLLOVER_FIELDS)
        raw_marks = raw["marks"]
        if type(raw_marks) is not list:
            raise ValueError
        marks = []
        for item in raw_marks:
            item = _strict_object(item, _MARK_FIELDS)
            mark_id = _sha(item["mark_id"], "mark_id")
            mark = PaperPortfolioMark(
                registration_id=item["registration_id"],
                position_id=item["position_id"],
                symbol=item["symbol"],
                listing_key=item["listing_key"],
                series=item["series"],
                validated_isin=item["validated_isin"],
                artifact_id=item["artifact_id"],
                calendar_snapshot_id=item["calendar_snapshot_id"],
                bar_id=item["bar_id"],
                observation_id=item["observation_id"],
                market_session=date.fromisoformat(item["market_session"]),
                session_close_at=_strict_datetime(item["session_close_at"]),
                knowledge_time=_strict_datetime(item["knowledge_time"]),
                close=_strict_decimal(item["close"]),
                price_basis=item["price_basis"],
                schema_version=item["schema_version"],
            )
            if mark.mark_id != mark_id:
                raise ValueError
            marks.append(mark)
        if type(raw["open_listing_keys"]) is not list:
            raise ValueError
        artifact_json = raw["portfolio_artifact_json"]
        if type(artifact_json) is not str:
            raise ValueError
        stored_id = _sha(raw["rollover_id"], "rollover_id")
        value = PaperPortfolioRollover(
            genesis_artifact_id=raw["genesis_artifact_id"],
            previous_rollover_id=raw["previous_rollover_id"],
            previous_portfolio_artifact_id=raw["previous_portfolio_artifact_id"],
            paper_portfolio_state_id=raw["paper_portfolio_state_id"],
            paper_portfolio_batch_id=raw["paper_portfolio_batch_id"],
            as_of=_strict_datetime(raw["as_of"]),
            marks=tuple(marks),
            open_listing_keys=tuple(raw["open_listing_keys"]),
            starting_capital=_strict_decimal(raw["starting_capital"]),
            realized_equity=_strict_decimal(raw["realized_equity"]),
            peak_realized_pnl=_strict_decimal(raw["peak_realized_pnl"]),
            open_entry_notional=_strict_decimal(raw["open_entry_notional"]),
            reserved_open_costs=_strict_decimal(raw["reserved_open_costs"]),
            cash_available=_strict_decimal(raw["cash_available"]),
            gross_exposure=_strict_decimal(raw["gross_exposure"]),
            unrealized_gross_pnl=_strict_decimal(raw["unrealized_gross_pnl"]),
            unrealized_net_pnl=_strict_decimal(raw["unrealized_net_pnl"]),
            nav=_strict_decimal(raw["nav"]),
            prior_peak_nav=_strict_decimal(raw["prior_peak_nav"]),
            peak_nav=_strict_decimal(raw["peak_nav"]),
            nav_drawdown=_strict_decimal(raw["nav_drawdown"]),
            portfolio_artifact=decode_swing_portfolio_artifact(
                artifact_json.encode("utf-8")
            ),
            schema_version=raw["schema_version"],
        )
        if value.rollover_id != stored_id or encode_paper_portfolio_rollover(value) != payload:
            raise ValueError
        return value
    except PaperPortfolioRolloverError:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise PaperPortfolioRolloverError(
            "stored paper portfolio rollover is invalid"
        ) from None


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalPaperPortfolioRolloverStore:
    """Create-once exact-ID store; it deliberately exposes no list/latest API."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def rollovers_root(self) -> Path:
        return self.root / "rollovers"

    def path_for(self, rollover_id: str) -> Path:
        return self.rollovers_root / f"{_sha(rollover_id, 'rollover_id')}.json"

    def get(self, rollover_id: str) -> PaperPortfolioRollover:
        path = self.path_for(rollover_id)
        if not path.exists() or not path.is_file() or _is_link_like(path):
            raise PaperPortfolioRolloverNotFound(
                "paper portfolio rollover was not found"
            )
        try:
            value = decode_paper_portfolio_rollover(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_ROLLOVER_BYTES)
            )
        except PaperPortfolioRolloverError:
            raise
        except Exception:
            raise PaperPortfolioRolloverError(
                "paper portfolio rollover could not be read"
            ) from None
        if value.rollover_id != rollover_id:
            raise PaperPortfolioRolloverError(
                "paper portfolio rollover differs from its path"
            )
        return value

    def put(self, value: PaperPortfolioRollover) -> PaperPortfolioRollover:
        if type(value) is not PaperPortfolioRollover:
            raise PaperPortfolioRolloverError("paper portfolio rollover must be exact")
        payload = encode_paper_portfolio_rollover(value)
        try:
            self.rollovers_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.root) or _is_link_like(self.rollovers_root):
                raise PaperPortfolioRolloverConflict(
                    "paper portfolio rollover store is unsafe"
                )
            target = self.path_for(value.rollover_id)
            with advisory_file_lock(self.rollovers_root / ".rollovers.lock"):
                if target.exists():
                    stored = self.get(value.rollover_id)
                    if stored != value:
                        raise PaperPortfolioRolloverConflict(
                            "rollover ID already stores different content"
                        )
                    return stored
                descriptor, name = tempfile.mkstemp(
                    prefix=".paper-rollover-",
                    suffix=".tmp",
                    dir=self.rollovers_root,
                )
                temporary = Path(name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except PaperPortfolioRolloverError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PaperPortfolioRolloverConflict(
                "paper portfolio rollover store is unavailable"
            ) from None
        return self.get(value.rollover_id)


def _evidence_id(kind: str, body: object) -> str:
    return content_id({"kind": kind, "body": body}, length=64)


def roll_paper_portfolio(
    *,
    state: PaperPortfolioState,
    genesis_artifact: SwingPortfolioSnapshotArtifact,
    marks: tuple[PaperPortfolioMark, ...],
    as_of: datetime,
    previous: PaperPortfolioRollover | None = None,
) -> PaperPortfolioRollover:
    """Close one paper-accounting day into a portfolio snapshot for the next run."""

    if type(state) is not PaperPortfolioState:
        raise PaperPortfolioRolloverError("paper portfolio state must be exact")
    if type(genesis_artifact) is not SwingPortfolioSnapshotArtifact:
        raise PaperPortfolioRolloverError("paper portfolio genesis must be exact")
    if type(marks) is not tuple or any(type(value) is not PaperPortfolioMark for value in marks):
        raise PaperPortfolioRolloverError("paper portfolio marks must be an exact tuple")
    try:
        state.verify_content_identity()
        genesis_artifact.verify_content_identity()
        for value in marks:
            value.verify_content_identity()
    except Exception:
        raise PaperPortfolioRolloverError("paper portfolio rollover input identity failed") from None
    as_of = _utc(as_of, "as_of")
    genesis = genesis_artifact.portfolio
    if (
        genesis_artifact.verification_status
        is not SwingPortfolioVerificationStatus.MANUAL_RECONCILED_PAPER_ONLY
        or genesis.cash_available != genesis.capital
        or genesis.gross_exposure != 0
        or genesis.open_risk != 0
        or genesis.open_positions != 0
        or genesis.daily_realized_pnl != 0
        or genesis.pilot_realized_pnl != 0
        or genesis.as_of >= state.as_of
    ):
        raise PaperPortfolioRolloverError("paper portfolio genesis is invalid")
    if state.as_of > as_of:
        raise PaperPortfolioRolloverError("paper portfolio rollover is future-known")

    if previous is None:
        if (
            state.previous_state_id is not None
            or state.previous_batch_id is not None
            or state.prior_cumulative_realized_pnl != 0
            or state.prior_peak_realized_pnl != 0
        ):
            raise PaperPortfolioRolloverError("paper portfolio predecessor is missing")
        previous_rollover_id = None
        previous_artifact_id = genesis_artifact.artifact_id
        prior_peak_nav = genesis.capital
    else:
        if type(previous) is not PaperPortfolioRollover:
            raise PaperPortfolioRolloverError("paper portfolio predecessor must be exact")
        try:
            previous.verify_content_identity()
        except Exception:
            raise PaperPortfolioRolloverError("paper portfolio predecessor identity failed") from None
        if (
            previous.genesis_artifact_id != genesis_artifact.artifact_id
            or state.previous_state_id != previous.paper_portfolio_state_id
            or state.previous_batch_id != previous.paper_portfolio_batch_id
            or state.prior_cumulative_realized_pnl
            != previous.portfolio_artifact.portfolio.pilot_realized_pnl
            or state.prior_peak_realized_pnl != previous.peak_realized_pnl
            or previous.as_of >= state.as_of
            or previous.as_of >= as_of
        ):
            raise PaperPortfolioRolloverError("paper portfolio predecessor differs")
        previous_rollover_id = previous.rollover_id
        previous_artifact_id = previous.portfolio_artifact.artifact_id
        prior_peak_nav = previous.peak_nav

    if any(value.outcome_status in _UNRESOLVED for value in state.positions):
        raise PaperPortfolioRolloverError("paper portfolio has unresolved positions")
    open_positions = tuple(
        value for value in state.positions if value.outcome_status is PaperOutcomeStatus.OPEN
    )
    ordered_positions = tuple(sorted(open_positions, key=lambda value: value.registration_id))
    ordered_marks = tuple(sorted(marks, key=lambda value: value.registration_id))
    if marks != ordered_marks or len(marks) != len(ordered_positions):
        raise PaperPortfolioRolloverError("paper portfolio mark coverage differs")
    for position, mark in zip(ordered_positions, ordered_marks, strict=True):
        if (
            mark.registration_id != position.registration_id
            or mark.position_id != position.position_id
            or mark.symbol != position.symbol
            or mark.knowledge_time > state.as_of
            or mark.knowledge_time > as_of
        ):
            raise PaperPortfolioRolloverError("paper portfolio mark lineage differs")
    if marks and (
        len({value.market_session for value in marks}) != 1
        or len({value.calendar_snapshot_id for value in marks}) != 1
    ):
        raise PaperPortfolioRolloverError("paper portfolio marks span different sessions")

    with localcontext() as context:
        context.prec = 50
        starting_capital = +genesis.capital
        realized_equity = +(starting_capital + state.cumulative_realized_pnl)
        open_entry_notional = +sum(
            (value.entry_notional for value in ordered_positions), Decimal("0")
        )
        reserved_open_costs = +sum(
            (value.estimated_cost for value in ordered_positions), Decimal("0")
        )
        cash_available = +(
            realized_equity - open_entry_notional - reserved_open_costs
        )
        gross_exposure = +sum(
            (mark.close * position.quantity for position, mark in zip(ordered_positions, marks, strict=True)),
            Decimal("0"),
        )
        unrealized_gross_pnl = +(gross_exposure - open_entry_notional)
        unrealized_net_pnl = +(unrealized_gross_pnl - reserved_open_costs)
        nav = +(cash_available + gross_exposure)
        peak_nav = +max(prior_peak_nav, nav)
        nav_drawdown = +(peak_nav - nav)
    if realized_equity <= 0 or cash_available < 0 or nav <= 0 or state.open_risk > nav:
        raise PaperPortfolioRolloverError("paper portfolio has exhausted safe capital")

    open_listing_keys = tuple(sorted(value.listing_key for value in marks))
    _canonical_keys(open_listing_keys)
    cash_evidence_id = _evidence_id(
        "PAPER_CASH_LEDGER",
        {
            "state_id": state.state_id,
            "starting_capital": starting_capital,
            "cumulative_realized_pnl": state.cumulative_realized_pnl,
            "peak_realized_pnl": state.peak_realized_pnl,
            "open_entry_notional": open_entry_notional,
            "reserved_open_costs": reserved_open_costs,
            "cash_available": cash_available,
        },
    )
    marks_evidence_id = _evidence_id(
        "PAPER_POSITION_MARKS",
        {
            "state_id": state.state_id,
            "mark_ids": tuple(value.mark_id for value in marks),
            "open_listing_keys": open_listing_keys,
        },
    )
    risk_evidence_id = _evidence_id(
        "ENGINE_RISK_LEDGER",
        {
            "state_id": state.state_id,
            "open_risk": state.open_risk,
            "risk_halt_reasons": state.risk_halt_reasons,
        },
    )
    pnl_evidence_id = _evidence_id(
        "ENGINE_PNL_LEDGER",
        {
            "state_id": state.state_id,
            "daily_realized_pnl": state.daily_realized_pnl,
            "cumulative_realized_pnl": state.cumulative_realized_pnl,
            "nav": nav,
            "peak_nav": peak_nav,
            "nav_drawdown": nav_drawdown,
        },
    )
    mark_observed_at = max((value.knowledge_time for value in marks), default=state.as_of)
    evidence = (
        SwingPortfolioEvidenceBinding(
            kind=SwingPortfolioEvidenceKind.BROKER_FUNDS,
            evidence_id=cash_evidence_id,
            observed_at=state.as_of,
            source_version=_SOURCE_VERSION,
        ),
        SwingPortfolioEvidenceBinding(
            kind=SwingPortfolioEvidenceKind.BROKER_POSITIONS,
            evidence_id=marks_evidence_id,
            observed_at=mark_observed_at,
            source_version=_SOURCE_VERSION,
        ),
        SwingPortfolioEvidenceBinding(
            kind=SwingPortfolioEvidenceKind.ENGINE_RISK_LEDGER,
            evidence_id=risk_evidence_id,
            observed_at=state.as_of,
            source_version=_SOURCE_VERSION,
        ),
        SwingPortfolioEvidenceBinding(
            kind=SwingPortfolioEvidenceKind.ENGINE_PNL_LEDGER,
            evidence_id=pnl_evidence_id,
            observed_at=state.as_of,
            source_version=_SOURCE_VERSION,
        ),
    )
    snapshot = SwingPortfolioSnapshot(
        capital=nav,
        cash_available=cash_available,
        gross_exposure=gross_exposure,
        open_risk=state.open_risk,
        open_positions=len(marks),
        daily_realized_pnl=state.daily_realized_pnl,
        pilot_realized_pnl=state.cumulative_realized_pnl,
        as_of=as_of,
    )
    artifact = SwingPortfolioSnapshotArtifact(
        portfolio=snapshot,
        portfolio_snapshot_id=snapshot.portfolio_snapshot_id,
        evidence=evidence,
        reconciled_at=as_of,
        verification_status=SwingPortfolioVerificationStatus.DERIVED_RECONCILED_PAPER_ONLY,
    )
    result = PaperPortfolioRollover(
        genesis_artifact_id=genesis_artifact.artifact_id,
        previous_rollover_id=previous_rollover_id,
        previous_portfolio_artifact_id=previous_artifact_id,
        paper_portfolio_state_id=state.state_id,
        paper_portfolio_batch_id=state.batch_id,
        as_of=as_of,
        marks=marks,
        open_listing_keys=open_listing_keys,
        starting_capital=starting_capital,
        realized_equity=realized_equity,
        peak_realized_pnl=state.peak_realized_pnl,
        open_entry_notional=open_entry_notional,
        reserved_open_costs=reserved_open_costs,
        cash_available=cash_available,
        gross_exposure=gross_exposure,
        unrealized_gross_pnl=unrealized_gross_pnl,
        unrealized_net_pnl=unrealized_net_pnl,
        nav=nav,
        prior_peak_nav=prior_peak_nav,
        peak_nav=peak_nav,
        nav_drawdown=nav_drawdown,
        portfolio_artifact=artifact,
    )
    result.verify_content_identity()
    return result
