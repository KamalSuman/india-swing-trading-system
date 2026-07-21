from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.recommendations.store import LocalSwingDecisionOutbox

from .models import (
    SwingOperationalError,
    SwingOperationalFailureCode,
    SwingOperationalRunRecord,
    SwingOperationalRunResult,
    SwingOperationalRunSpec,
    SwingOperationalStatus,
    operational_record_from_result,
)
from .runner import (
    SwingPortfolioSource,
    SwingQuoteSource,
    execute_swing_operational_run,
)
from india_swing.recommendations.models import SwingDecisionAction


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CODEC_SCHEMA_VERSION = "swing-operational-run-record-json/v1"
_MAXIMUM_RECORD_BYTES = 512 * 1024


class SwingOperationalStoreError(SwingOperationalError):
    pass


class SwingOperationalRunNotFound(SwingOperationalStoreError):
    pass


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def encode_operational_run_record(record: SwingOperationalRunRecord) -> bytes:
    if type(record) is not SwingOperationalRunRecord:
        raise SwingOperationalStoreError("record must be exact")
    record.verify_content_identity()
    data = {
        "action": record.action.value,
        "completed_at": record.completed_at.isoformat(),
        "decision_id": record.decision_id,
        "evaluated_at": (
            None if record.evaluated_at is None else record.evaluated_at.isoformat()
        ),
        "failure_codes": [value.value for value in record.failure_codes],
        "message": record.message,
        "message_sha256": record.message_sha256,
        "mode": record.mode,
        "notification_id": record.notification_id,
        "package_id": record.package_id,
        "paper_registration_id": record.paper_registration_id,
        "portfolio_snapshot_id": record.portfolio_snapshot_id,
        "portfolio_source_id": record.portfolio_source_id,
        "proposal_batch_id": record.proposal_batch_id,
        "quote_batch_id": record.quote_batch_id,
        "quote_source_id": record.quote_source_id,
        "record_id": record.record_id,
        "run_id": record.run_id,
        "schema_version": record.schema_version,
        "spec_id": record.spec_id,
        "started_at": record.started_at.isoformat(),
        "status": record.status.value,
        "target_session": record.target_session.isoformat(),
    }
    return (
        json.dumps(
            {"codec_schema_version": _CODEC_SCHEMA_VERSION, "record": data},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


_RECORD_FIELDS = {
    "action",
    "completed_at",
    "decision_id",
    "evaluated_at",
    "failure_codes",
    "message",
    "message_sha256",
    "mode",
    "notification_id",
    "package_id",
    "paper_registration_id",
    "portfolio_snapshot_id",
    "portfolio_source_id",
    "proposal_batch_id",
    "quote_batch_id",
    "quote_source_id",
    "record_id",
    "run_id",
    "schema_version",
    "spec_id",
    "started_at",
    "status",
    "target_session",
}


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SwingOperationalStoreError("operational JSON has duplicate keys")
        result[key] = value
    return result


def decode_operational_run_record(payload: bytes) -> SwingOperationalRunRecord:
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if (
            type(root) is not dict
            or set(root) != {"codec_schema_version", "record"}
            or root["codec_schema_version"] != _CODEC_SCHEMA_VERSION
        ):
            raise SwingOperationalStoreError("operational envelope is invalid")
        value = root["record"]
        if type(value) is not dict or set(value) != _RECORD_FIELDS:
            raise SwingOperationalStoreError("operational record fields are invalid")
        evaluated_at = value["evaluated_at"]
        record = SwingOperationalRunRecord(
            spec_id=value["spec_id"],
            run_id=value["run_id"],
            target_session=date.fromisoformat(value["target_session"]),
            status=SwingOperationalStatus(value["status"]),
            action=SwingDecisionAction(value["action"]),
            started_at=datetime.fromisoformat(value["started_at"]),
            completed_at=datetime.fromisoformat(value["completed_at"]),
            evaluated_at=(
                None if evaluated_at is None else datetime.fromisoformat(evaluated_at)
            ),
            quote_source_id=value["quote_source_id"],
            portfolio_source_id=value["portfolio_source_id"],
            proposal_batch_id=value["proposal_batch_id"],
            quote_batch_id=value["quote_batch_id"],
            portfolio_snapshot_id=value["portfolio_snapshot_id"],
            decision_id=value["decision_id"],
            package_id=value["package_id"],
            notification_id=value["notification_id"],
            paper_registration_id=value["paper_registration_id"],
            failure_codes=tuple(
                SwingOperationalFailureCode(item) for item in value["failure_codes"]
            ),
            message=value["message"],
            message_sha256=value["message_sha256"],
            mode=value["mode"],
            schema_version=value["schema_version"],
        )
        if record.record_id != value["record_id"]:
            raise SwingOperationalStoreError("stored operational identity differs")
        return record
    except SwingOperationalStoreError:
        raise
    except Exception:
        raise SwingOperationalStoreError("stored operational record is invalid") from None


class LocalSwingOperationalRunStore:
    """One terminal, append-only operational result for one immutable run spec."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def runs_root(self) -> Path:
        return self.root / "runs"

    def path_for(self, spec_id: str) -> Path:
        if type(spec_id) is not str or _SHA256.fullmatch(spec_id) is None:
            raise SwingOperationalStoreError("spec_id must be a lowercase SHA-256")
        return self.runs_root / f"{spec_id}.json"

    def publish(self, result: SwingOperationalRunResult) -> SwingOperationalRunRecord:
        if type(result) is not SwingOperationalRunResult:
            raise SwingOperationalStoreError("operational result must be exact")
        try:
            result.verify_content_identity()
            record = operational_record_from_result(result)
        except SwingOperationalError:
            raise SwingOperationalStoreError("operational result is invalid") from None
        return self.put_record(record)

    def put_record(self, record: SwingOperationalRunRecord) -> SwingOperationalRunRecord:
        """Create or verify one already-decoded immutable terminal record."""

        if type(record) is not SwingOperationalRunRecord:
            raise SwingOperationalStoreError("record must be exact")
        try:
            record.verify_content_identity()
            payload = encode_operational_run_record(record)
        except Exception:
            raise SwingOperationalStoreError("operational record is invalid") from None
        target = self.path_for(record.spec_id)
        try:
            self.runs_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.runs_root):
                raise SwingOperationalStoreError("operational run root cannot be a link")
            with advisory_file_lock(self.runs_root / ".operational-runs.lock"):
                if target.exists():
                    stored = self.get(record.spec_id)
                    if stored != record:
                        raise SwingOperationalStoreError(
                            "run spec already stores a different terminal result"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".operational-run-",
                    suffix=".tmp",
                    dir=self.runs_root,
                )
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except SwingOperationalStoreError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise SwingOperationalStoreError("operational run store is unavailable") from None
        return self.get(record.spec_id)

    def get(self, spec_id: str) -> SwingOperationalRunRecord:
        path = self.path_for(spec_id)
        if not path.exists():
            raise SwingOperationalRunNotFound("operational run was not found")
        if not path.is_file() or _is_link_like(path):
            raise SwingOperationalStoreError("operational run must be a regular file")
        try:
            record = decode_operational_run_record(
                read_stable_regular_file(
                    path,
                    maximum_bytes=_MAXIMUM_RECORD_BYTES,
                )
            )
        except SwingOperationalStoreError:
            raise
        except FileSafetyError:
            raise SwingOperationalStoreError("operational run could not be read safely") from None
        if record.spec_id != spec_id:
            raise SwingOperationalStoreError("operational run differs from its path")
        return record

    def list_records(self) -> tuple[SwingOperationalRunRecord, ...]:
        if not self.runs_root.exists():
            return ()
        if not self.runs_root.is_dir() or _is_link_like(self.runs_root):
            raise SwingOperationalStoreError("operational run root is invalid")
        records: list[SwingOperationalRunRecord] = []
        for path in sorted(self.runs_root.iterdir(), key=lambda value: value.name):
            if path.name == ".operational-runs.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise SwingOperationalStoreError("operational run file set is invalid")
            records.append(self.get(path.stem))
        return tuple(
            sorted(
                records,
                key=lambda value: (value.target_session, value.started_at, value.spec_id),
            )
        )


def publish_swing_operational_run(
    *,
    result: SwingOperationalRunResult,
    run_store: LocalSwingOperationalRunStore,
    decision_outbox: LocalSwingDecisionOutbox | None = None,
    paper_ledger: LocalPaperTradeLedger | None = None,
) -> SwingOperationalRunRecord:
    """Publish idempotent side effects first and the terminal manifest last."""

    if type(result) is not SwingOperationalRunResult:
        raise SwingOperationalStoreError("operational result must be exact")
    if type(run_store) is not LocalSwingOperationalRunStore:
        raise SwingOperationalStoreError("run_store must be exact")
    result.verify_content_identity()
    if result.status is SwingOperationalStatus.COMPLETE:
        if type(decision_outbox) is not LocalSwingDecisionOutbox:
            raise SwingOperationalStoreError("complete run requires a decision outbox")
        stored_notification = decision_outbox.put(result.decision_package)
        if stored_notification.notification_id != result.decision_package.notification.notification_id:
            raise SwingOperationalStoreError("published notification identity differs")
        if result.paper_registration is not None:
            if type(paper_ledger) is not LocalPaperTradeLedger:
                raise SwingOperationalStoreError("BUY run requires a paper ledger")
            stored_registration = paper_ledger.register_value(result.paper_registration)
            if stored_registration.registration_id != result.paper_registration.registration_id:
                raise SwingOperationalStoreError("paper registration identity differs")
    return run_store.publish(result)


def run_and_publish_swing_operation(
    *,
    spec: SwingOperationalRunSpec,
    quote_source: SwingQuoteSource,
    portfolio_source: SwingPortfolioSource,
    clock: Callable[[], datetime],
    run_store: LocalSwingOperationalRunStore,
    decision_outbox: LocalSwingDecisionOutbox,
    paper_ledger: LocalPaperTradeLedger | None = None,
) -> tuple[SwingOperationalRunResult, SwingOperationalRunRecord]:
    """Single schedulable service call: acquire, decide, hand off, then seal."""

    result = execute_swing_operational_run(
        spec=spec,
        quote_source=quote_source,
        portfolio_source=portfolio_source,
        clock=clock,
    )
    record = publish_swing_operational_run(
        result=result,
        run_store=run_store,
        decision_outbox=decision_outbox,
        paper_ledger=paper_ledger,
    )
    return result, record
