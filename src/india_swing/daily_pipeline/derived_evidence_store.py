from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from datetime import date, datetime
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.reference.models import ReferenceReadiness

from .derived_evidence import (
    DAILY_DERIVED_EVIDENCE_CODEC_VERSION,
    DailyDerivedEvidence,
    DailyDerivedEvidenceConflict,
    DailyDerivedEvidenceIntegrityError,
    DailyDerivedEvidenceNotFound,
)


DAILY_DERIVED_EVIDENCE_STORE_SCHEMA_VERSION = "local-daily-derived-evidence/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_EVIDENCE_BYTES = 2 * 1024 * 1024


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DailyDerivedEvidenceIntegrityError(
                "derived evidence contains a duplicate JSON key"
            )
        result[key] = value
    return result


def _data(value: DailyDerivedEvidence) -> dict[str, object]:
    return {
        "actionable": value.actionable,
        "calendar_snapshot_id": value.calendar_snapshot_id,
        "current_security_master_artifact_id": (
            value.current_security_master_artifact_id
        ),
        "cutoff": value.cutoff.isoformat(),
        "evidence_id": value.evidence_id,
        "historical_price_artifact_ids": list(value.historical_price_artifact_ids),
        "liquidity_snapshot_id": value.liquidity_snapshot_id,
        "market_session": value.market_session.isoformat(),
        "minimum_history_sessions": value.minimum_history_sessions,
        "policy_version": value.policy_version,
        "readiness": value.readiness.value,
        "reason_codes": list(value.reason_codes),
        "run_id": value.run_id,
        "schema_version": value.schema_version,
        "tick_size_snapshot_id": value.tick_size_snapshot_id,
        "universe_snapshot_id": value.universe_snapshot_id,
    }


_FIELDS = {
    "actionable",
    "calendar_snapshot_id",
    "current_security_master_artifact_id",
    "cutoff",
    "evidence_id",
    "historical_price_artifact_ids",
    "liquidity_snapshot_id",
    "market_session",
    "minimum_history_sessions",
    "policy_version",
    "readiness",
    "reason_codes",
    "run_id",
    "schema_version",
    "tick_size_snapshot_id",
    "universe_snapshot_id",
}


def _decode(value: object) -> DailyDerivedEvidence:
    if type(value) is not dict or set(value) != _FIELDS:
        raise DailyDerivedEvidenceIntegrityError(
            "stored derived evidence has an invalid shape"
        )
    try:
        result = DailyDerivedEvidence(
            run_id=value["run_id"],
            market_session=date.fromisoformat(value["market_session"]),
            cutoff=datetime.fromisoformat(value["cutoff"]),
            calendar_snapshot_id=value["calendar_snapshot_id"],
            current_security_master_artifact_id=value[
                "current_security_master_artifact_id"
            ],
            historical_price_artifact_ids=tuple(value["historical_price_artifact_ids"]),
            tick_size_snapshot_id=value["tick_size_snapshot_id"],
            liquidity_snapshot_id=value["liquidity_snapshot_id"],
            universe_snapshot_id=value["universe_snapshot_id"],
            minimum_history_sessions=value["minimum_history_sessions"],
            reason_codes=tuple(value["reason_codes"]),
            readiness=ReferenceReadiness(value["readiness"]),
            actionable=value["actionable"],
            policy_version=value["policy_version"],
            schema_version=value["schema_version"],
        )
        if result.evidence_id != value["evidence_id"]:
            raise DailyDerivedEvidenceIntegrityError(
                "stored derived evidence ID differs from content"
            )
        return result
    except DailyDerivedEvidenceIntegrityError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise DailyDerivedEvidenceIntegrityError(
            "stored derived evidence is invalid"
        ) from exc


class LocalDailyDerivedEvidenceStore:
    """Create-once storage for IDs of replay-verified daily derived snapshots."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def evidence_root(self) -> Path:
        return self.root / "derived_evidence"

    def path_for(self, evidence_id: str) -> Path:
        if not isinstance(evidence_id, str) or _SHA256.fullmatch(evidence_id) is None:
            raise DailyDerivedEvidenceIntegrityError(
                "derived evidence ID must be a full lowercase SHA-256"
            )
        return self.evidence_root / f"{evidence_id}.json"

    def publish(self, value: DailyDerivedEvidence) -> DailyDerivedEvidence:
        if type(value) is not DailyDerivedEvidence:
            raise TypeError("daily derived evidence must be exact")
        value.verify_content_identity()
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.evidence_root):
            raise DailyDerivedEvidenceIntegrityError(
                "derived evidence root cannot be a link"
            )
        target = self.path_for(value.evidence_id)
        payload = self._payload(value)
        try:
            with advisory_file_lock(self.evidence_root / ".derived-evidence.lock"):
                if target.exists():
                    stored = self.get(value.evidence_id)
                    if stored != value:
                        raise DailyDerivedEvidenceConflict(
                            "derived evidence ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".derived-evidence-",
                    suffix=".tmp",
                    dir=self.evidence_root,
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
        except (FileLockUnavailable, FileSafetyError) as exc:
            raise DailyDerivedEvidenceConflict(
                "derived evidence store unavailable"
            ) from exc
        return self.get(value.evidence_id)

    def get(self, evidence_id: str) -> DailyDerivedEvidence:
        path = self.path_for(evidence_id)
        if not path.exists():
            raise DailyDerivedEvidenceNotFound(evidence_id)
        if not path.is_file() or _is_link_like(path):
            raise DailyDerivedEvidenceIntegrityError(
                "derived evidence must be a regular file"
            )
        try:
            raw = json.loads(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_EVIDENCE_BYTES).decode(
                    "utf-8"
                ),
                object_pairs_hook=_unique_object,
                parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
            )
            if (
                type(raw) is not dict
                or set(raw) != {"codec_version", "evidence", "store_schema_version"}
                or raw["codec_version"] != DAILY_DERIVED_EVIDENCE_CODEC_VERSION
                or raw["store_schema_version"]
                != DAILY_DERIVED_EVIDENCE_STORE_SCHEMA_VERSION
            ):
                raise DailyDerivedEvidenceIntegrityError(
                    "stored derived evidence envelope is invalid"
                )
            value = _decode(raw["evidence"])
            if value.evidence_id != evidence_id:
                raise DailyDerivedEvidenceIntegrityError(
                    "stored derived evidence differs from its path"
                )
            return value
        except DailyDerivedEvidenceIntegrityError:
            raise
        except (
            FileSafetyError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise DailyDerivedEvidenceIntegrityError(
                "stored derived evidence is invalid"
            ) from exc

    def list_evidence(self) -> tuple[DailyDerivedEvidence, ...]:
        if not self.evidence_root.exists():
            return ()
        if not self.evidence_root.is_dir() or _is_link_like(self.evidence_root):
            raise DailyDerivedEvidenceIntegrityError(
                "derived evidence root is unsafe"
            )
        values = []
        for path in sorted(self.evidence_root.iterdir(), key=lambda item: item.name):
            if path.name == ".derived-evidence.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise DailyDerivedEvidenceIntegrityError(
                    "derived evidence file set is invalid"
                )
            values.append(self.get(path.stem))
        return tuple(
            sorted(values, key=lambda value: (value.market_session, value.evidence_id))
        )

    @staticmethod
    def _payload(value: DailyDerivedEvidence) -> bytes:
        return (
            json.dumps(
                {
                    "codec_version": DAILY_DERIVED_EVIDENCE_CODEC_VERSION,
                    "evidence": _data(value),
                    "store_schema_version": DAILY_DERIVED_EVIDENCE_STORE_SCHEMA_VERSION,
                },
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
