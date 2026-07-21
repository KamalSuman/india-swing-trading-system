from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.paper_trades import LocalPaperTradeLedger

from .models import PaperOutcomeStatus
from .operational import (
    LocalPaperOutcomeRunStore,
    PaperOutcomeEvidenceSource,
    PaperOutcomeJobSpec,
    PaperOutcomeOperationalError,
    PaperOutcomeRunRecord,
    decode_paper_outcome_job_spec,
    encode_paper_outcome_job_spec,
    run_paper_outcome_job,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_STATE_FILE = re.compile(r"([0-9a-f]{64})\.json\Z")
_BATCH_SCHEMA = "paper-portfolio-batch-spec/v1"
_POSITION_SCHEMA = "paper-portfolio-position/v1"
_STATE_SCHEMA = "paper-portfolio-state/v1"
_STATE_CODEC = "paper-portfolio-state-json/v1"
_BATCH_CODEC = "paper-portfolio-batch-spec-json/v1"
_MAXIMUM_STATE_BYTES = 16 * 1024 * 1024
_ACTIVE = frozenset(
    {PaperOutcomeStatus.WAITING, PaperOutcomeStatus.OPEN, PaperOutcomeStatus.BLOCKED}
)


class PaperPortfolioError(PaperOutcomeOperationalError):
    pass


class PaperPortfolioNotFound(PaperPortfolioError):
    pass


class PaperPortfolioConflict(PaperPortfolioError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperPortfolioError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise PaperPortfolioError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _finite(value: object, name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise PaperPortfolioError(f"{name} must be a finite Decimal")
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


@dataclass(frozen=True, slots=True)
class PaperPortfolioBatchSpec:
    as_of: datetime
    outcome_jobs: tuple[PaperOutcomeJobSpec, ...]
    previous_batch_id: str | None = None
    expected_previous_state_id: str | None = None
    daily_loss_limit: Decimal = Decimal("1000")
    cumulative_loss_limit: Decimal = Decimal("2000")
    schema_version: str = _BATCH_SCHEMA
    batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _utc(self.as_of, "as_of"))
        if type(self.outcome_jobs) is not tuple or not self.outcome_jobs:
            raise PaperPortfolioError("paper portfolio batch requires outcome jobs")
        if any(type(value) is not PaperOutcomeJobSpec for value in self.outcome_jobs):
            raise PaperPortfolioError("paper portfolio outcome jobs must be exact")
        for value in self.outcome_jobs:
            value.verify_content_identity()
            if value.as_of != self.as_of:
                raise PaperPortfolioError("paper portfolio job cutoff differs from its batch")
        registration_ids = tuple(value.registration_id for value in self.outcome_jobs)
        if (
            len(set(registration_ids)) != len(registration_ids)
            or registration_ids != tuple(sorted(registration_ids))
        ):
            raise PaperPortfolioError(
                "paper portfolio jobs must be unique and registration ordered"
            )
        if (self.previous_batch_id is None) != (self.expected_previous_state_id is None):
            raise PaperPortfolioError("previous paper portfolio binding is incomplete")
        if self.previous_batch_id is not None:
            _sha(self.previous_batch_id, "previous_batch_id")
            _sha(self.expected_previous_state_id, "expected_previous_state_id")
        for value, name in (
            (self.daily_loss_limit, "daily_loss_limit"),
            (self.cumulative_loss_limit, "cumulative_loss_limit"),
        ):
            if _finite(value, name) <= 0:
                raise PaperPortfolioError(f"{name} must be positive")
        if self.schema_version != _BATCH_SCHEMA:
            raise PaperPortfolioError("unsupported paper portfolio batch spec")
        object.__setattr__(self, "batch_id", _identity(self, {"batch_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperPortfolioBatchSpec(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "batch_id"
                }
            )
        except Exception:
            raise PaperPortfolioError("paper portfolio batch identity failed") from None
        if fresh.batch_id != self.batch_id:
            raise PaperPortfolioError("paper portfolio batch identity failed")


def encode_paper_portfolio_batch_spec(value: PaperPortfolioBatchSpec) -> bytes:
    if type(value) is not PaperPortfolioBatchSpec:
        raise PaperPortfolioError("paper portfolio batch spec must be exact")
    value.verify_content_identity()
    jobs = [json.loads(encode_paper_outcome_job_spec(item)) for item in value.outcome_jobs]
    payload = (
        json.dumps(
            {
                "codec_schema_version": _BATCH_CODEC,
                "spec": {
                    "as_of": value.as_of.isoformat(),
                    "batch_id": value.batch_id,
                    "cumulative_loss_limit": str(value.cumulative_loss_limit),
                    "daily_loss_limit": str(value.daily_loss_limit),
                    "expected_previous_state_id": value.expected_previous_state_id,
                    "outcome_jobs": jobs,
                    "previous_batch_id": value.previous_batch_id,
                    "schema_version": value.schema_version,
                },
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_STATE_BYTES:
        raise PaperPortfolioError("paper portfolio batch spec is too large")
    return payload


def decode_paper_portfolio_batch_spec(payload: bytes) -> PaperPortfolioBatchSpec:
    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_STATE_BYTES:
        raise PaperPortfolioError("paper portfolio batch bytes are invalid")
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != {"codec_schema_version", "spec"}:
            raise ValueError
        if root["codec_schema_version"] != _BATCH_CODEC:
            raise ValueError
        raw = root["spec"]
        expected = {
            "as_of", "batch_id", "cumulative_loss_limit", "daily_loss_limit",
            "expected_previous_state_id", "outcome_jobs", "previous_batch_id",
            "schema_version",
        }
        if type(raw) is not dict or set(raw) != expected or type(raw["outcome_jobs"]) is not list:
            raise ValueError
        jobs = tuple(
            decode_paper_outcome_job_spec(
                (json.dumps(item, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
            )
            for item in raw["outcome_jobs"]
        )
        stored_id = raw["batch_id"]
        value = PaperPortfolioBatchSpec(
            as_of=datetime.fromisoformat(raw["as_of"]),
            outcome_jobs=jobs,
            previous_batch_id=raw["previous_batch_id"],
            expected_previous_state_id=raw["expected_previous_state_id"],
            daily_loss_limit=Decimal(raw["daily_loss_limit"]),
            cumulative_loss_limit=Decimal(raw["cumulative_loss_limit"]),
            schema_version=raw["schema_version"],
        )
        if value.batch_id != stored_id or encode_paper_portfolio_batch_spec(value) != payload:
            raise ValueError
        return value
    except Exception:
        raise PaperPortfolioError("paper portfolio batch spec is invalid") from None


def load_paper_portfolio_batch_spec_file(path: Path) -> PaperPortfolioBatchSpec:
    try:
        payload = read_stable_regular_file(path, maximum_bytes=_MAXIMUM_STATE_BYTES)
    except Exception:
        raise PaperPortfolioError("paper portfolio batch file is unavailable") from None
    return decode_paper_portfolio_batch_spec(payload)


@dataclass(frozen=True, slots=True)
class PaperPortfolioPosition:
    registration_id: str
    symbol: str
    outcome_status: PaperOutcomeStatus
    job_spec_id: str
    record_id: str
    replay_id: str
    event_ids: tuple[str, ...]
    quantity: int
    planned_risk: Decimal
    entry_notional: Decimal
    estimated_cost: Decimal
    gross_pnl: Decimal | None
    estimated_net_pnl: Decimal | None
    realized_r: Decimal | None
    schema_version: str = _POSITION_SCHEMA
    position_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.registration_id, "registration_id"),
            (self.job_spec_id, "job_spec_id"),
            (self.record_id, "record_id"),
            (self.replay_id, "replay_id"),
        ):
            _sha(value, name)
        if type(self.symbol) is not str or not self.symbol or self.symbol != self.symbol.upper():
            raise PaperPortfolioError("paper position symbol is invalid")
        if type(self.outcome_status) is not PaperOutcomeStatus:
            raise PaperPortfolioError("paper position status must be exact")
        if type(self.event_ids) is not tuple or len(set(self.event_ids)) != len(self.event_ids):
            raise PaperPortfolioError("paper position event IDs must be unique")
        for value in self.event_ids:
            _sha(value, "event_id")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise PaperPortfolioError("paper position quantity must be positive")
        for value, name in (
            (self.planned_risk, "planned_risk"),
            (self.entry_notional, "entry_notional"),
            (self.estimated_cost, "estimated_cost"),
        ):
            if _finite(value, name) < 0:
                raise PaperPortfolioError(f"{name} cannot be negative")
        if self.planned_risk <= 0 or self.estimated_cost <= 0:
            raise PaperPortfolioError("paper position risk and costs must be positive")
        pnl_values = (self.gross_pnl, self.estimated_net_pnl, self.realized_r)
        if self.outcome_status is PaperOutcomeStatus.CLOSED:
            if any(value is None for value in pnl_values):
                raise PaperPortfolioError("closed paper position requires P&L")
            for value in pnl_values:
                _finite(value, "paper position P&L")
            if self.realized_r != self.estimated_net_pnl / self.planned_risk:
                raise PaperPortfolioError("paper position realized R differs")
        elif any(value is not None for value in pnl_values):
            raise PaperPortfolioError("non-closed paper position cannot carry P&L")
        if self.outcome_status is PaperOutcomeStatus.OPEN and self.entry_notional <= 0:
            raise PaperPortfolioError("open paper position requires entry notional")
        if self.schema_version != _POSITION_SCHEMA:
            raise PaperPortfolioError("unsupported paper portfolio position")
        object.__setattr__(self, "position_id", _identity(self, {"position_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperPortfolioPosition(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "position_id"
                }
            )
        except Exception:
            raise PaperPortfolioError("paper position identity failed") from None
        if fresh.position_id != self.position_id:
            raise PaperPortfolioError("paper position identity failed")


@dataclass(frozen=True, slots=True)
class PaperPortfolioState:
    batch_id: str
    outcome_job_spec_ids: tuple[str, ...]
    previous_batch_id: str | None
    previous_state_id: str | None
    as_of: datetime
    positions: tuple[PaperPortfolioPosition, ...]
    newly_closed_registration_ids: tuple[str, ...]
    daily_realized_pnl: Decimal
    prior_cumulative_realized_pnl: Decimal
    cumulative_realized_pnl: Decimal
    prior_peak_realized_pnl: Decimal
    peak_realized_pnl: Decimal
    drawdown: Decimal
    total_estimated_costs: Decimal
    open_risk: Decimal
    open_notional: Decimal
    closed_count: int
    winning_count: int
    losing_count: int
    win_rate: Decimal
    expectancy_pnl: Decimal
    expectancy_r: Decimal
    daily_loss_limit: Decimal
    cumulative_loss_limit: Decimal
    risk_halt_reasons: tuple[str, ...]
    report_message: str
    schema_version: str = _STATE_SCHEMA
    state_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.batch_id, "batch_id")
        if (
            type(self.outcome_job_spec_ids) is not tuple
            or not self.outcome_job_spec_ids
            or self.outcome_job_spec_ids
            != tuple(sorted(set(self.outcome_job_spec_ids)))
        ):
            raise PaperPortfolioError("paper portfolio job spec IDs are invalid")
        for value in self.outcome_job_spec_ids:
            _sha(value, "outcome_job_spec_id")
        if (self.previous_batch_id is None) != (self.previous_state_id is None):
            raise PaperPortfolioError("paper portfolio predecessor binding is incomplete")
        if self.previous_batch_id is not None:
            _sha(self.previous_batch_id, "previous_batch_id")
            _sha(self.previous_state_id, "previous_state_id")
        object.__setattr__(self, "as_of", _utc(self.as_of, "as_of"))
        if type(self.positions) is not tuple or not self.positions:
            raise PaperPortfolioError("paper portfolio state requires positions")
        if any(type(value) is not PaperPortfolioPosition for value in self.positions):
            raise PaperPortfolioError("paper portfolio positions must be exact")
        for value in self.positions:
            value.verify_content_identity()
        ids = tuple(value.registration_id for value in self.positions)
        if ids != tuple(sorted(set(ids))):
            raise PaperPortfolioError("paper portfolio positions must be unique and ordered")
        position_job_ids = {value.job_spec_id for value in self.positions}
        if not set(self.outcome_job_spec_ids).issubset(position_job_ids):
            raise PaperPortfolioError("paper portfolio job lineage is incomplete")
        if (
            type(self.newly_closed_registration_ids) is not tuple
            or self.newly_closed_registration_ids
            != tuple(sorted(set(self.newly_closed_registration_ids)))
            or not set(self.newly_closed_registration_ids).issubset(ids)
        ):
            raise PaperPortfolioError("newly closed position IDs are invalid")
        decimals = (
            self.daily_realized_pnl,
            self.prior_cumulative_realized_pnl,
            self.cumulative_realized_pnl,
            self.prior_peak_realized_pnl,
            self.peak_realized_pnl,
            self.drawdown,
            self.total_estimated_costs,
            self.open_risk,
            self.open_notional,
            self.win_rate,
            self.expectancy_pnl,
            self.expectancy_r,
            self.daily_loss_limit,
            self.cumulative_loss_limit,
        )
        for value in decimals:
            _finite(value, "paper portfolio aggregate")
        for value in (self.closed_count, self.winning_count, self.losing_count):
            if type(value) is not int or value < 0:
                raise PaperPortfolioError("paper portfolio counts are invalid")
        if self.daily_loss_limit <= 0 or self.cumulative_loss_limit <= 0:
            raise PaperPortfolioError("paper portfolio loss limits must be positive")
        if self.cumulative_realized_pnl != self.prior_cumulative_realized_pnl + self.daily_realized_pnl:
            raise PaperPortfolioError("cumulative paper P&L chain differs")
        if self.peak_realized_pnl != max(
            self.prior_peak_realized_pnl, self.cumulative_realized_pnl
        ):
            raise PaperPortfolioError("paper portfolio peak P&L differs")
        if self.drawdown != self.peak_realized_pnl - self.cumulative_realized_pnl or self.drawdown < 0:
            raise PaperPortfolioError("paper portfolio drawdown differs")
        closed = tuple(value for value in self.positions if value.outcome_status is PaperOutcomeStatus.CLOSED)
        newly_closed = tuple(
            value
            for value in self.positions
            if value.registration_id in self.newly_closed_registration_ids
        )
        if (
            any(value.outcome_status is not PaperOutcomeStatus.CLOSED for value in newly_closed)
            or self.daily_realized_pnl
            != sum((value.estimated_net_pnl for value in newly_closed), Decimal("0"))
        ):
            raise PaperPortfolioError("daily paper P&L attribution differs")
        wins = sum(1 for value in closed if value.estimated_net_pnl >= 0)
        losses = len(closed) - wins
        expected_rate = Decimal(wins) / Decimal(len(closed)) if closed else Decimal("0")
        expected_pnl = (
            sum((value.estimated_net_pnl for value in closed), Decimal("0"))
            / Decimal(len(closed))
            if closed else Decimal("0")
        )
        expected_r = (
            sum((value.realized_r for value in closed), Decimal("0"))
            / Decimal(len(closed))
            if closed else Decimal("0")
        )
        open_positions = tuple(
            value for value in self.positions if value.outcome_status is PaperOutcomeStatus.OPEN
        )
        if (
            self.closed_count != len(closed)
            or self.winning_count != wins
            or self.losing_count != losses
            or self.win_rate != expected_rate
            or self.expectancy_pnl != expected_pnl
            or self.expectancy_r != expected_r
            or self.total_estimated_costs
            != sum((value.estimated_cost for value in closed), Decimal("0"))
            or self.open_risk
            != sum((value.planned_risk for value in open_positions), Decimal("0"))
            or self.open_notional
            != sum((value.entry_notional for value in open_positions), Decimal("0"))
        ):
            raise PaperPortfolioError("paper portfolio aggregates do not replay")
        expected_halts = []
        if self.daily_realized_pnl <= -self.daily_loss_limit:
            expected_halts.append("DAILY_REALIZED_LOSS_HALT")
        if self.cumulative_realized_pnl <= -self.cumulative_loss_limit:
            expected_halts.append("CUMULATIVE_REALIZED_LOSS_HALT")
        if self.risk_halt_reasons != tuple(sorted(expected_halts)):
            raise PaperPortfolioError("paper portfolio halt reasons are invalid")
        expected_report = _report(
            as_of=self.as_of,
            positions=self.positions,
            daily_pnl=self.daily_realized_pnl,
            cumulative_pnl=self.cumulative_realized_pnl,
            drawdown=self.drawdown,
            halt_reasons=self.risk_halt_reasons,
        )
        if (
            type(self.report_message) is not str
            or self.report_message != expected_report
            or len(self.report_message) > 4096
        ):
            raise PaperPortfolioError("paper portfolio report message is invalid")
        if self.schema_version != _STATE_SCHEMA:
            raise PaperPortfolioError("unsupported paper portfolio state")
        object.__setattr__(self, "state_id", _identity(self, {"state_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperPortfolioState(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "state_id"
                }
            )
        except Exception:
            raise PaperPortfolioError("paper portfolio state identity failed") from None
        if fresh.state_id != self.state_id:
            raise PaperPortfolioError("paper portfolio state identity failed")


def _position_from_record(
    record: PaperOutcomeRunRecord,
    ledger: LocalPaperTradeLedger,
) -> PaperPortfolioPosition:
    registration = ledger.get_registration(record.registration_id)
    summary = ledger.summary(record.registration_id)
    planned_risk = (registration.entry_high - registration.stop) * registration.quantity
    entry_notional = (
        Decimal("0")
        if summary.entry_price is None
        else summary.entry_price * registration.quantity
    )
    realized_r = (
        None
        if record.estimated_net_pnl is None
        else record.estimated_net_pnl / planned_risk
    )
    return PaperPortfolioPosition(
        registration_id=record.registration_id,
        symbol=registration.symbol,
        outcome_status=record.outcome_status,
        job_spec_id=record.job_spec_id,
        record_id=record.record_id,
        replay_id=record.replay_id,
        event_ids=record.event_ids,
        quantity=registration.quantity,
        planned_risk=planned_risk,
        entry_notional=entry_notional,
        estimated_cost=registration.estimated_round_trip_cost,
        gross_pnl=record.gross_pnl,
        estimated_net_pnl=record.estimated_net_pnl,
        realized_r=realized_r,
    )


def _report(
    *,
    as_of: datetime,
    positions: tuple[PaperPortfolioPosition, ...],
    daily_pnl: Decimal,
    cumulative_pnl: Decimal,
    drawdown: Decimal,
    halt_reasons: tuple[str, ...],
) -> str:
    closed = tuple(value for value in positions if value.outcome_status is PaperOutcomeStatus.CLOSED)
    open_count = sum(1 for value in positions if value.outcome_status is PaperOutcomeStatus.OPEN)
    wins = sum(1 for value in closed if value.estimated_net_pnl >= 0)
    lines = (
        "PAPER-ONLY DAILY PORTFOLIO — NOT BROKER PERFORMANCE",
        f"As of: {as_of.isoformat()}",
        f"Tracked positions: {len(positions)}",
        f"Open positions: {open_count}",
        f"Closed positions: {len(closed)}",
        f"Wins / losses: {wins} / {len(closed) - wins}",
        f"Daily estimated realized P&L: Rs {daily_pnl}",
        f"Cumulative estimated realized P&L: Rs {cumulative_pnl}",
        f"Peak-to-current drawdown: Rs {drawdown}",
        f"Risk halt: {', '.join(halt_reasons) if halt_reasons else 'NONE'}",
        "Paper fills and costs are estimates; reconcile against broker evidence before reuse.",
    )
    return "\n".join(lines)


class LocalPaperPortfolioStateStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def states_root(self) -> Path:
        return self.root / "states"

    def path_for(self, batch_id: str) -> Path:
        _sha(batch_id, "batch_id")
        return self.states_root / f"{batch_id}.json"

    def get(self, batch_id: str) -> PaperPortfolioState:
        path = self.path_for(batch_id)
        if not path.exists():
            raise PaperPortfolioNotFound("paper portfolio state was not found")
        if not path.is_file() or _is_link_like(path):
            raise PaperPortfolioError("paper portfolio state path is unsafe")
        try:
            value = decode_paper_portfolio_state(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_STATE_BYTES)
            )
        except PaperPortfolioError:
            raise
        except Exception:
            raise PaperPortfolioError("paper portfolio state could not be read") from None
        if value.batch_id != batch_id:
            raise PaperPortfolioError("paper portfolio state differs from its path")
        return value

    def list_states(self) -> tuple[PaperPortfolioState, ...]:
        if not self.states_root.exists():
            return ()
        if not self.states_root.is_dir() or _is_link_like(self.states_root):
            raise PaperPortfolioError("paper portfolio state set is unsafe")
        values: list[PaperPortfolioState] = []
        for path in sorted(self.states_root.iterdir(), key=lambda item: item.name):
            if path.name == ".paper-portfolio.lock":
                if not path.is_file() or _is_link_like(path):
                    raise PaperPortfolioError("paper portfolio state set is unsafe")
                continue
            match = _STATE_FILE.fullmatch(path.name)
            if match is None or not path.is_file() or _is_link_like(path):
                raise PaperPortfolioError("paper portfolio state file set is invalid")
            values.append(self.get(match.group(1)))
        return tuple(values)

    def put(self, value: PaperPortfolioState) -> PaperPortfolioState:
        return self._put(value, allow_unbound_first=False)

    def put_restored(self, value: PaperPortfolioState) -> PaperPortfolioState:
        """Install one externally pinned state into an empty restore store."""

        return self._put(value, allow_unbound_first=True)

    def _put(
        self,
        value: PaperPortfolioState,
        *,
        allow_unbound_first: bool,
    ) -> PaperPortfolioState:
        if type(value) is not PaperPortfolioState:
            raise PaperPortfolioError("paper portfolio state must be exact")
        payload = encode_paper_portfolio_state(value)
        try:
            self.states_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.states_root):
                raise PaperPortfolioError("paper portfolio store root is unsafe")
            target = self.path_for(value.batch_id)
            with advisory_file_lock(self.states_root / ".paper-portfolio.lock"):
                if target.exists():
                    stored = self.get(value.batch_id)
                    if stored != value:
                        raise PaperPortfolioConflict(
                            "paper portfolio batch already has another state"
                        )
                    return stored
                stored_states = self.list_states()
                if not stored_states:
                    if value.previous_state_id is not None and not allow_unbound_first:
                        raise PaperPortfolioConflict(
                            "paper portfolio predecessor state is absent"
                        )
                elif value.previous_state_id is None:
                    raise PaperPortfolioConflict(
                        "paper portfolio cannot create another genesis"
                    )
                else:
                    predecessor_ids = {
                        item.previous_state_id
                        for item in stored_states
                        if item.previous_state_id is not None
                    }
                    leaves = tuple(
                        item
                        for item in stored_states
                        if item.state_id not in predecessor_ids
                    )
                    if (
                        len(leaves) != 1
                        or leaves[0].state_id != value.previous_state_id
                        or leaves[0].batch_id != value.previous_batch_id
                    ):
                        raise PaperPortfolioConflict(
                            "paper portfolio append is not on the unique leaf"
                        )
                descriptor, name = tempfile.mkstemp(
                    prefix=".paper-portfolio-", suffix=".tmp", dir=self.states_root
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
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PaperPortfolioConflict("paper portfolio store is unavailable") from None
        return self.get(value.batch_id)


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _position_body(value: PaperPortfolioPosition) -> dict[str, object]:
    return {
        "entry_notional": str(value.entry_notional),
        "estimated_cost": str(value.estimated_cost),
        "estimated_net_pnl": None if value.estimated_net_pnl is None else str(value.estimated_net_pnl),
        "event_ids": list(value.event_ids),
        "gross_pnl": None if value.gross_pnl is None else str(value.gross_pnl),
        "job_spec_id": value.job_spec_id,
        "outcome_status": value.outcome_status.value,
        "planned_risk": str(value.planned_risk),
        "position_id": value.position_id,
        "quantity": value.quantity,
        "realized_r": None if value.realized_r is None else str(value.realized_r),
        "record_id": value.record_id,
        "registration_id": value.registration_id,
        "replay_id": value.replay_id,
        "schema_version": value.schema_version,
        "symbol": value.symbol,
    }


def _state_body(value: PaperPortfolioState) -> dict[str, object]:
    return {
        "as_of": value.as_of.isoformat(),
        "batch_id": value.batch_id,
        "closed_count": value.closed_count,
        "cumulative_loss_limit": str(value.cumulative_loss_limit),
        "cumulative_realized_pnl": str(value.cumulative_realized_pnl),
        "daily_loss_limit": str(value.daily_loss_limit),
        "daily_realized_pnl": str(value.daily_realized_pnl),
        "drawdown": str(value.drawdown),
        "expectancy_pnl": str(value.expectancy_pnl),
        "expectancy_r": str(value.expectancy_r),
        "losing_count": value.losing_count,
        "newly_closed_registration_ids": list(value.newly_closed_registration_ids),
        "open_notional": str(value.open_notional),
        "open_risk": str(value.open_risk),
        "outcome_job_spec_ids": list(value.outcome_job_spec_ids),
        "peak_realized_pnl": str(value.peak_realized_pnl),
        "positions": [_position_body(item) for item in value.positions],
        "previous_batch_id": value.previous_batch_id,
        "previous_state_id": value.previous_state_id,
        "prior_cumulative_realized_pnl": str(value.prior_cumulative_realized_pnl),
        "prior_peak_realized_pnl": str(value.prior_peak_realized_pnl),
        "report_message": value.report_message,
        "risk_halt_reasons": list(value.risk_halt_reasons),
        "schema_version": value.schema_version,
        "state_id": value.state_id,
        "total_estimated_costs": str(value.total_estimated_costs),
        "win_rate": str(value.win_rate),
        "winning_count": value.winning_count,
    }


def encode_paper_portfolio_state(value: PaperPortfolioState) -> bytes:
    if type(value) is not PaperPortfolioState:
        raise PaperPortfolioError("paper portfolio state must be exact")
    value.verify_content_identity()
    payload = (
        json.dumps(
            {"codec_schema_version": _STATE_CODEC, "state": _state_body(value)},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_STATE_BYTES:
        raise PaperPortfolioError("paper portfolio state is too large")
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def decode_paper_portfolio_state(payload: bytes) -> PaperPortfolioState:
    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_STATE_BYTES:
        raise PaperPortfolioError("paper portfolio state bytes are invalid")
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != {"codec_schema_version", "state"}:
            raise ValueError
        if root["codec_schema_version"] != _STATE_CODEC:
            raise ValueError
        raw = root["state"]
        if type(raw) is not dict or set(raw) != set(_state_body_placeholder()):
            raise ValueError
        positions = tuple(_position_from_raw(item) for item in raw["positions"])
        stored_id = raw["state_id"]
        value = PaperPortfolioState(
            batch_id=raw["batch_id"],
            outcome_job_spec_ids=tuple(raw["outcome_job_spec_ids"]),
            previous_batch_id=raw["previous_batch_id"],
            previous_state_id=raw["previous_state_id"],
            as_of=datetime.fromisoformat(raw["as_of"]),
            positions=positions,
            newly_closed_registration_ids=tuple(raw["newly_closed_registration_ids"]),
            daily_realized_pnl=Decimal(raw["daily_realized_pnl"]),
            prior_cumulative_realized_pnl=Decimal(raw["prior_cumulative_realized_pnl"]),
            cumulative_realized_pnl=Decimal(raw["cumulative_realized_pnl"]),
            prior_peak_realized_pnl=Decimal(raw["prior_peak_realized_pnl"]),
            peak_realized_pnl=Decimal(raw["peak_realized_pnl"]),
            drawdown=Decimal(raw["drawdown"]),
            total_estimated_costs=Decimal(raw["total_estimated_costs"]),
            open_risk=Decimal(raw["open_risk"]),
            open_notional=Decimal(raw["open_notional"]),
            closed_count=raw["closed_count"],
            winning_count=raw["winning_count"],
            losing_count=raw["losing_count"],
            win_rate=Decimal(raw["win_rate"]),
            expectancy_pnl=Decimal(raw["expectancy_pnl"]),
            expectancy_r=Decimal(raw["expectancy_r"]),
            daily_loss_limit=Decimal(raw["daily_loss_limit"]),
            cumulative_loss_limit=Decimal(raw["cumulative_loss_limit"]),
            risk_halt_reasons=tuple(raw["risk_halt_reasons"]),
            report_message=raw["report_message"],
            schema_version=raw["schema_version"],
        )
        if value.state_id != stored_id or encode_paper_portfolio_state(value) != payload:
            raise ValueError
        return value
    except Exception:
        raise PaperPortfolioError("stored paper portfolio state is invalid") from None


def _position_from_raw(raw: object) -> PaperPortfolioPosition:
    expected = {
        "entry_notional", "estimated_cost", "estimated_net_pnl", "event_ids",
        "gross_pnl", "job_spec_id", "outcome_status", "planned_risk", "position_id",
        "quantity", "realized_r", "record_id", "registration_id", "replay_id",
        "schema_version", "symbol",
    }
    if type(raw) is not dict or set(raw) != expected:
        raise ValueError
    stored_id = raw["position_id"]
    value = PaperPortfolioPosition(
        registration_id=raw["registration_id"],
        symbol=raw["symbol"],
        outcome_status=PaperOutcomeStatus(raw["outcome_status"]),
        job_spec_id=raw["job_spec_id"],
        record_id=raw["record_id"],
        replay_id=raw["replay_id"],
        event_ids=tuple(raw["event_ids"]),
        quantity=raw["quantity"],
        planned_risk=Decimal(raw["planned_risk"]),
        entry_notional=Decimal(raw["entry_notional"]),
        estimated_cost=Decimal(raw["estimated_cost"]),
        gross_pnl=None if raw["gross_pnl"] is None else Decimal(raw["gross_pnl"]),
        estimated_net_pnl=(
            None if raw["estimated_net_pnl"] is None else Decimal(raw["estimated_net_pnl"])
        ),
        realized_r=None if raw["realized_r"] is None else Decimal(raw["realized_r"]),
        schema_version=raw["schema_version"],
    )
    if value.position_id != stored_id:
        raise ValueError
    return value


def _state_body_placeholder() -> dict[str, object]:
    keys = (
        "as_of", "batch_id", "closed_count", "cumulative_loss_limit",
        "cumulative_realized_pnl", "daily_loss_limit", "daily_realized_pnl",
        "drawdown", "expectancy_pnl", "expectancy_r",
        "losing_count", "newly_closed_registration_ids", "open_notional", "open_risk",
        "outcome_job_spec_ids", "peak_realized_pnl", "positions", "previous_batch_id", "previous_state_id",
        "prior_cumulative_realized_pnl", "prior_peak_realized_pnl", "report_message",
        "risk_halt_reasons", "schema_version", "state_id", "total_estimated_costs",
        "win_rate", "winning_count",
    )
    return {key: None for key in keys}


def run_paper_portfolio_batch(
    *,
    spec: PaperPortfolioBatchSpec,
    evidence_source: PaperOutcomeEvidenceSource,
    ledger: LocalPaperTradeLedger,
    outcome_store: LocalPaperOutcomeRunStore,
    portfolio_store: LocalPaperPortfolioStateStore,
) -> PaperPortfolioState:
    if type(spec) is not PaperPortfolioBatchSpec:
        raise PaperPortfolioError("paper portfolio batch spec must be exact")
    if type(ledger) is not LocalPaperTradeLedger:
        raise PaperPortfolioError("paper ledger must be exact")
    if type(outcome_store) is not LocalPaperOutcomeRunStore:
        raise PaperPortfolioError("paper outcome store must be exact")
    if type(portfolio_store) is not LocalPaperPortfolioStateStore:
        raise PaperPortfolioError("paper portfolio store must be exact")
    spec.verify_content_identity()
    try:
        existing = portfolio_store.get(spec.batch_id)
    except PaperPortfolioNotFound:
        existing = None
    if existing is not None:
        return existing

    stored_states = portfolio_store.list_states()
    previous: PaperPortfolioState | None = None
    if spec.previous_batch_id is None:
        if stored_states:
            raise PaperPortfolioError("paper portfolio predecessor is required")
    else:
        previous = portfolio_store.get(spec.previous_batch_id)
        if previous.state_id != spec.expected_previous_state_id or previous.as_of >= spec.as_of:
            raise PaperPortfolioError("previous paper portfolio state differs")
        predecessor_ids = {
            value.previous_state_id
            for value in stored_states
            if value.previous_state_id is not None
        }
        leaves = tuple(
            value for value in stored_states if value.state_id not in predecessor_ids
        )
        if len(leaves) != 1 or leaves[0].state_id != previous.state_id:
            raise PaperPortfolioError("paper portfolio predecessor is not the unique leaf")
    prior_positions = {} if previous is None else {
        value.registration_id: value for value in previous.positions
    }
    job_ids = {value.registration_id for value in spec.outcome_jobs}
    missing_active = {
        value.registration_id
        for value in prior_positions.values()
        if value.outcome_status in _ACTIVE and value.registration_id not in job_ids
    }
    if missing_active:
        raise PaperPortfolioError("paper portfolio batch omits an active position")

    updated = dict(prior_positions)
    try:
        for job in spec.outcome_jobs:
            record = run_paper_outcome_job(
                spec=job,
                evidence_source=evidence_source,
                ledger=ledger,
                record_store=outcome_store,
            )
            updated[job.registration_id] = _position_from_record(record, ledger)
    except PaperPortfolioError:
        raise
    except Exception:
        raise PaperPortfolioError("paper portfolio outcome execution failed safely") from None

    positions = tuple(sorted(updated.values(), key=lambda value: value.registration_id))
    newly_closed = tuple(
        value.registration_id
        for value in positions
        if value.outcome_status is PaperOutcomeStatus.CLOSED
        and (
            value.registration_id not in prior_positions
            or prior_positions[value.registration_id].outcome_status
            is not PaperOutcomeStatus.CLOSED
        )
    )
    daily_pnl = sum(
        (
            value.estimated_net_pnl
            for value in positions
            if value.registration_id in newly_closed
        ),
        Decimal("0"),
    )
    prior_cumulative = Decimal("0") if previous is None else previous.cumulative_realized_pnl
    cumulative = prior_cumulative + daily_pnl
    prior_peak = Decimal("0") if previous is None else previous.peak_realized_pnl
    peak = max(prior_peak, cumulative)
    drawdown = peak - cumulative
    halt_reasons = []
    if daily_pnl <= -spec.daily_loss_limit:
        halt_reasons.append("DAILY_REALIZED_LOSS_HALT")
    if cumulative <= -spec.cumulative_loss_limit:
        halt_reasons.append("CUMULATIVE_REALIZED_LOSS_HALT")
    closed = tuple(value for value in positions if value.outcome_status is PaperOutcomeStatus.CLOSED)
    wins = sum(1 for value in closed if value.estimated_net_pnl >= 0)
    losses = len(closed) - wins
    win_rate = Decimal(wins) / Decimal(len(closed)) if closed else Decimal("0")
    expectancy_pnl = (
        sum((value.estimated_net_pnl for value in closed), Decimal("0"))
        / Decimal(len(closed))
        if closed else Decimal("0")
    )
    expectancy_r = (
        sum((value.realized_r for value in closed), Decimal("0"))
        / Decimal(len(closed))
        if closed else Decimal("0")
    )
    open_positions = tuple(
        value for value in positions if value.outcome_status is PaperOutcomeStatus.OPEN
    )
    reasons = tuple(sorted(halt_reasons))
    state = PaperPortfolioState(
        batch_id=spec.batch_id,
        outcome_job_spec_ids=tuple(sorted(value.job_spec_id for value in spec.outcome_jobs)),
        previous_batch_id=None if previous is None else previous.batch_id,
        previous_state_id=None if previous is None else previous.state_id,
        as_of=spec.as_of,
        positions=positions,
        newly_closed_registration_ids=tuple(sorted(newly_closed)),
        daily_realized_pnl=daily_pnl,
        prior_cumulative_realized_pnl=prior_cumulative,
        cumulative_realized_pnl=cumulative,
        prior_peak_realized_pnl=prior_peak,
        peak_realized_pnl=peak,
        drawdown=drawdown,
        total_estimated_costs=sum(
            (value.estimated_cost for value in closed), Decimal("0")
        ),
        open_risk=sum((value.planned_risk for value in open_positions), Decimal("0")),
        open_notional=sum(
            (value.entry_notional for value in open_positions), Decimal("0")
        ),
        closed_count=len(closed),
        winning_count=wins,
        losing_count=losses,
        win_rate=win_rate,
        expectancy_pnl=expectancy_pnl,
        expectancy_r=expectancy_r,
        daily_loss_limit=spec.daily_loss_limit,
        cumulative_loss_limit=spec.cumulative_loss_limit,
        risk_halt_reasons=reasons,
        report_message=_report(
            as_of=spec.as_of,
            positions=positions,
            daily_pnl=daily_pnl,
            cumulative_pnl=cumulative,
            drawdown=drawdown,
            halt_reasons=reasons,
        ),
    )
    return portfolio_store.put(state)
