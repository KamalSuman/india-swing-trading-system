from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.shadow_alerts import ShadowAlert

from .models import (
    PaperTradeConflict,
    PaperTradeError,
    PaperTradeEvent,
    PaperTradeEventType,
    PaperTradeIntegrityError,
    PaperTradeRegistration,
    PaperTradeStatus,
    PaperTradeSummary,
    registration_from_shadow_alert,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_EVENT_FILE = re.compile(r"([0-9]{20})-([0-9a-f]{64})\.json\Z")
_MAX_BYTES = 1024 * 1024
_REGISTRATION_CODEC = "paper-trade-registration-json/v1"
_EVENT_CODEC = "paper-trade-event-json/v2"
IST = timezone(timedelta(hours=5, minutes=30))


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _json_value(value: object) -> object:
    if isinstance(value, PaperTradeEventType):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if value is None or type(value) in {str, int, bool}:
        return value
    raise TypeError("unsupported paper trade value")


def _encode(value: object, codec: str) -> bytes:
    if type(value) not in {PaperTradeRegistration, PaperTradeEvent}:
        raise TypeError("paper trade artifact must be exact")
    value.verify_content_identity()
    name = "registration" if type(value) is PaperTradeRegistration else "event"
    payload = {
        "codec_schema_version": codec,
        name: {item.name: _json_value(getattr(value, item.name)) for item in fields(value)},
    }
    return (
        json.dumps(payload, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PaperTradeIntegrityError("paper trade JSON contains duplicate keys")
        result[key] = value
    return result


def _load(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
    except PaperTradeIntegrityError:
        raise
    except Exception:
        raise PaperTradeIntegrityError("stored paper trade JSON is invalid") from None
    if type(value) is not dict:
        raise PaperTradeIntegrityError("stored paper trade envelope is invalid")
    return value


def _decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise PaperTradeIntegrityError("stored paper trade decimal is invalid")
    try:
        return Decimal(value)
    except Exception:
        raise PaperTradeIntegrityError("stored paper trade decimal is invalid") from None


def _date(value: object) -> date:
    if type(value) is not str:
        raise PaperTradeIntegrityError("stored paper trade date is invalid")
    try:
        return date.fromisoformat(value)
    except Exception:
        raise PaperTradeIntegrityError("stored paper trade date is invalid") from None


def _decode_registration(payload: bytes) -> PaperTradeRegistration:
    try:
        root = _load(payload)
        if set(root) != {"codec_schema_version", "registration"} or root["codec_schema_version"] != _REGISTRATION_CODEC:
            raise PaperTradeIntegrityError("stored paper registration envelope is invalid")
        raw = root["registration"]
        if type(raw) is not dict or set(raw) != {item.name for item in fields(PaperTradeRegistration)}:
            raise PaperTradeIntegrityError("stored paper registration fields are invalid")
        stored_id = raw["registration_id"]
        value = PaperTradeRegistration(
            alert_id=raw["alert_id"],
            source_run_id=raw["source_run_id"],
            source_pipeline_integrity_hash=raw["source_pipeline_integrity_hash"],
            source_decision_integrity_hash=raw["source_decision_integrity_hash"],
            signal_id=raw["signal_id"],
            symbol=raw["symbol"],
            quantity=raw["quantity"],
            decision_time=datetime.fromisoformat(raw["decision_time"]),
            earliest_entry_at=datetime.fromisoformat(raw["earliest_entry_at"]),
            entry_expires_at=datetime.fromisoformat(raw["entry_expires_at"]),
            entry_low=_decimal(raw["entry_low"]),
            entry_high=_decimal(raw["entry_high"]),
            stop=_decimal(raw["stop"]),
            target=_decimal(raw["target"]),
            max_holding_sessions=raw["max_holding_sessions"],
            estimated_round_trip_cost=_decimal(raw["estimated_round_trip_cost"]),
            mode=raw["mode"],
            actionable=raw["actionable"],
            schema_version=raw["schema_version"],
        )
        if value.registration_id != stored_id:
            raise PaperTradeIntegrityError("stored paper registration identity differs")
        return value
    except PaperTradeIntegrityError:
        raise
    except Exception:
        raise PaperTradeIntegrityError("stored paper registration is invalid") from None


def encode_paper_trade_registration(value: PaperTradeRegistration) -> bytes:
    return _encode(value, _REGISTRATION_CODEC)


def decode_paper_trade_registration(payload: bytes) -> PaperTradeRegistration:
    return _decode_registration(payload)


def _decode_event(payload: bytes) -> PaperTradeEvent:
    try:
        root = _load(payload)
        if set(root) != {"codec_schema_version", "event"} or root["codec_schema_version"] != _EVENT_CODEC:
            raise PaperTradeIntegrityError("stored paper event envelope is invalid")
        raw = root["event"]
        if type(raw) is not dict or set(raw) != {item.name for item in fields(PaperTradeEvent)}:
            raise PaperTradeIntegrityError("stored paper event fields are invalid")
        stored_id = raw["event_id"]
        value = PaperTradeEvent(
            registration_id=raw["registration_id"],
            alert_id=raw["alert_id"],
            sequence=raw["sequence"],
            previous_event_id=raw["previous_event_id"],
            event_type=PaperTradeEventType(raw["event_type"]),
            occurred_at=datetime.fromisoformat(raw["occurred_at"]),
            observed_price=None if raw["observed_price"] is None else _decimal(raw["observed_price"]),
            evidence_id=raw["evidence_id"],
            reason_code=raw["reason_code"],
            market_session=None if raw["market_session"] is None else _date(raw["market_session"]),
            replay_id=raw["replay_id"],
            outcome_policy_id=raw["outcome_policy_id"],
            instrument_binding_id=raw["instrument_binding_id"],
            calendar_snapshot_id=raw["calendar_snapshot_id"],
            schema_version=raw["schema_version"],
        )
        if value.event_id != stored_id:
            raise PaperTradeIntegrityError("stored paper event identity differs")
        return value
    except PaperTradeIntegrityError:
        raise
    except Exception:
        raise PaperTradeIntegrityError("stored paper event is invalid") from None


def validate_paper_trade_history(
    registration: PaperTradeRegistration,
    events: tuple[PaperTradeEvent, ...],
) -> None:
    prior: PaperTradeEvent | None = None
    status = PaperTradeStatus.ALERTED
    first_session = registration.earliest_entry_at.astimezone(IST).date()
    expiry_session = registration.entry_expires_at.astimezone(IST).date()
    for sequence, event in enumerate(events, start=1):
        event.verify_content_identity()
        if event.registration_id != registration.registration_id or event.alert_id != registration.alert_id:
            raise PaperTradeIntegrityError("paper event belongs to another registration")
        if event.sequence != sequence or event.previous_event_id != (None if prior is None else prior.event_id):
            raise PaperTradeIntegrityError("paper event predecessor chain is broken")
        if prior is not None and event.occurred_at < prior.occurred_at:
            raise PaperTradeIntegrityError("paper event time moved backwards")
        if event.occurred_at < registration.decision_time:
            raise PaperTradeConflict("paper event predates its alert")
        if status in {PaperTradeStatus.CLOSED, PaperTradeStatus.EXPIRED, PaperTradeStatus.INVALIDATED}:
            raise PaperTradeConflict("no event may follow a terminal paper outcome")
        if event.event_type is PaperTradeEventType.ENTRY_RECORDED:
            if status is not PaperTradeStatus.ALERTED:
                raise PaperTradeConflict("paper entry can be recorded only once")
            if not first_session <= event.market_session <= expiry_session:
                raise PaperTradeConflict("paper entry lies outside its validity window")
            if not registration.entry_low <= event.observed_price <= registration.entry_high:
                raise PaperTradeConflict("paper entry lies outside its planned range")
            status = PaperTradeStatus.OPEN
        elif event.event_type is PaperTradeEventType.EXIT_RECORDED:
            if status is not PaperTradeStatus.OPEN:
                raise PaperTradeConflict("paper exit requires a recorded entry")
            assert prior is not None
            if event.market_session < prior.market_session:
                raise PaperTradeConflict("paper exit session precedes its entry session")
            same_session = event.market_session == prior.market_session
            same_session_stop = (
                same_session
                and event.reason_code == "STOP_EXIT"
                and event.replay_id is not None
                and event.replay_id == prior.replay_id
                and event.observed_price <= registration.stop
            )
            if same_session:
                if not same_session_stop:
                    raise PaperTradeConflict(
                        "same-session paper exit requires a matching automated stop"
                    )
            elif event.evidence_id == prior.evidence_id:
                raise PaperTradeConflict(
                    "paper exit requires later, independently identified evidence"
                )
            status = PaperTradeStatus.CLOSED
        elif event.event_type is PaperTradeEventType.EXPIRED:
            if status is not PaperTradeStatus.ALERTED or event.occurred_at < registration.entry_expires_at:
                raise PaperTradeConflict("paper alert cannot expire at this point")
            status = PaperTradeStatus.EXPIRED
        elif event.event_type is PaperTradeEventType.INVALIDATED:
            status = PaperTradeStatus.INVALIDATED
        prior = event


class LocalPaperTradeLedger:
    """Create-once paper registrations and append-only simulated outcome chains."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def registrations_root(self) -> Path:
        return self.root / "registrations"

    @property
    def events_root(self) -> Path:
        return self.root / "events"

    def registration_path(self, registration_id: str) -> Path:
        if type(registration_id) is not str or _SHA256.fullmatch(registration_id) is None:
            raise PaperTradeError("registration_id must be a lowercase SHA-256")
        return self.registrations_root / f"{registration_id}.json"

    def register(self, alert: ShadowAlert) -> PaperTradeRegistration:
        try:
            value = registration_from_shadow_alert(alert)
        except Exception:
            raise PaperTradeError("paper registration input is invalid") from None
        return self.register_value(value)

    def register_value(
        self,
        value: PaperTradeRegistration,
    ) -> PaperTradeRegistration:
        """Register an already-verified paper-only decision from any trusted adapter."""

        if type(value) is not PaperTradeRegistration:
            raise PaperTradeError("paper registration must be exact")
        try:
            value.verify_content_identity()
            payload = encode_paper_trade_registration(value)
        except Exception:
            raise PaperTradeError("paper registration input is invalid") from None
        target = self.registration_path(value.registration_id)
        try:
            self.registrations_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.registrations_root):
                raise PaperTradeIntegrityError("paper registration root cannot be a link")
            with advisory_file_lock(self.registrations_root / ".paper-registration.lock"):
                if target.exists():
                    stored = self.get_registration(value.registration_id)
                    if stored != value:
                        raise PaperTradeConflict("alert already has another paper registration")
                    return stored
                self._create_once(target, payload, self.registrations_root)
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PaperTradeConflict("paper registration store is unavailable") from None
        return self.get_registration(value.registration_id)

    @staticmethod
    def _create_once(target: Path, payload: bytes, directory: Path) -> None:
        descriptor, name = tempfile.mkstemp(prefix=".paper-trade-", suffix=".tmp", dir=directory)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def get_registration(self, registration_id: str) -> PaperTradeRegistration:
        path = self.registration_path(registration_id)
        if not path.exists() or not path.is_file() or _is_link_like(path):
            raise PaperTradeIntegrityError("paper registration was not found safely")
        try:
            value = decode_paper_trade_registration(
                read_stable_regular_file(path, maximum_bytes=_MAX_BYTES)
            )
        except Exception:
            raise PaperTradeIntegrityError("paper registration could not be read") from None
        if value.registration_id != registration_id:
            raise PaperTradeIntegrityError("paper registration differs from its path")
        return value

    def list_events(self, registration_id: str) -> tuple[PaperTradeEvent, ...]:
        registration = self.get_registration(registration_id)
        directory = self.events_root / registration.registration_id
        if not directory.exists():
            return ()
        if not directory.is_dir() or _is_link_like(directory):
            raise PaperTradeIntegrityError("paper event path is invalid")
        events: list[PaperTradeEvent] = []
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            match = _EVENT_FILE.fullmatch(path.name)
            if match is None or not path.is_file() or _is_link_like(path):
                raise PaperTradeIntegrityError("paper event file set is invalid")
            try:
                event = _decode_event(read_stable_regular_file(path, maximum_bytes=_MAX_BYTES))
            except Exception:
                raise PaperTradeIntegrityError("paper event could not be read") from None
            if event.sequence != int(match.group(1)) or event.event_id != match.group(2):
                raise PaperTradeIntegrityError("paper event filename identity differs")
            events.append(event)
        result = tuple(events)
        validate_paper_trade_history(registration, result)
        return result

    def append(
        self,
        *,
        registration_id: str,
        event_type: PaperTradeEventType,
        occurred_at: datetime,
        observed_price: Decimal | None = None,
        evidence_id: str | None = None,
        reason_code: str | None = None,
        market_session: date | None = None,
        replay_id: str | None = None,
        outcome_policy_id: str | None = None,
        instrument_binding_id: str | None = None,
        calendar_snapshot_id: str | None = None,
    ) -> PaperTradeEvent:
        registration = self.get_registration(registration_id)
        if type(event_type) is not PaperTradeEventType:
            raise PaperTradeError("paper event type must be exact")
        self.events_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.events_root):
            raise PaperTradeIntegrityError("paper events root cannot be a link")
        directory = self.events_root / registration.registration_id
        directory.mkdir(exist_ok=True)
        if _is_link_like(directory):
            raise PaperTradeIntegrityError("paper event path cannot be a link")
        try:
            with advisory_file_lock(self.events_root / ".paper-events.lock"):
                existing = self.list_events(registration_id)
                event = PaperTradeEvent(
                    registration_id=registration.registration_id,
                    alert_id=registration.alert_id,
                    sequence=len(existing) + 1,
                    previous_event_id=existing[-1].event_id if existing else None,
                    event_type=event_type,
                    occurred_at=occurred_at,
                    observed_price=observed_price,
                    evidence_id=evidence_id,
                    reason_code=reason_code,
                    market_session=market_session,
                    replay_id=replay_id,
                    outcome_policy_id=outcome_policy_id,
                    instrument_binding_id=instrument_binding_id,
                    calendar_snapshot_id=calendar_snapshot_id,
                )
                validate_paper_trade_history(registration, existing + (event,))
                target = directory / f"{event.sequence:020d}-{event.event_id}.json"
                self._create_once(target, _encode(event, _EVENT_CODEC), directory)
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PaperTradeConflict("paper event store is unavailable") from None
        return event

    def summary(self, registration_id: str) -> PaperTradeSummary:
        registration = self.get_registration(registration_id)
        events = self.list_events(registration_id)
        entry = next((item for item in events if item.event_type is PaperTradeEventType.ENTRY_RECORDED), None)
        exit_event = next((item for item in events if item.event_type is PaperTradeEventType.EXIT_RECORDED), None)
        if any(item.event_type is PaperTradeEventType.INVALIDATED for item in events):
            status = PaperTradeStatus.INVALIDATED
        elif any(item.event_type is PaperTradeEventType.EXPIRED for item in events):
            status = PaperTradeStatus.EXPIRED
        elif exit_event is not None:
            status = PaperTradeStatus.CLOSED
        elif entry is not None:
            status = PaperTradeStatus.OPEN
        else:
            status = PaperTradeStatus.ALERTED
        gross = None
        net = None
        if entry is not None and exit_event is not None:
            gross = (exit_event.observed_price - entry.observed_price) * registration.quantity
            net = gross - registration.estimated_round_trip_cost
        return PaperTradeSummary(
            registration_id=registration.registration_id,
            alert_id=registration.alert_id,
            status=status,
            entry_price=None if entry is None else entry.observed_price,
            exit_price=None if exit_event is None else exit_event.observed_price,
            gross_pnl=gross,
            estimated_net_pnl=net,
            event_ids=tuple(item.event_id for item in events),
        )
