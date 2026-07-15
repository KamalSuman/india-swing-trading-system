from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import tempfile
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any


AUDIT_SCHEMA_VERSION = "audit-v1"
_RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class AuditExistsError(FileExistsError):
    """Raised when an immutable audit record already exists."""


class InvalidAuditRunId(ValueError):
    """Raised when a run ID cannot safely be used as an audit filename."""


class AuditIntegrityError(ValueError):
    """Raised when an audit record is malformed or fails hash verification."""


def json_value(value: Any) -> Any:
    if is_dataclass(value):
        return json_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_value(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value


def validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise InvalidAuditRunId(
            "run_id must be 1-128 ASCII characters, begin with an alphanumeric "
            "character, and contain only alphanumerics, underscores, or hyphens"
        )
    return run_id


def _canonical_payload(payload: Any) -> str:
    return json.dumps(
        json_value(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _audit_path(output_dir: Path, run_id: str) -> Path:
    return Path(output_dir) / f"{validate_run_id(run_id)}.json"


def _fsync_directory(directory: Path) -> None:
    """Best-effort directory sync after publishing a completed local record."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        # Some platforms and filesystems do not support syncing directories.
        pass
    finally:
        os.close(descriptor)


def verify_audit_envelope(envelope: Any) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        raise AuditIntegrityError("audit record must be a JSON object")
    if envelope.get("schema_version") != AUDIT_SCHEMA_VERSION:
        raise AuditIntegrityError("audit record has an unsupported schema version")
    if "payload" not in envelope:
        raise AuditIntegrityError("audit record is missing its payload")

    audit_hash = envelope.get("audit_hash")
    if not isinstance(audit_hash, str) or _SHA256_PATTERN.fullmatch(audit_hash) is None:
        raise AuditIntegrityError("audit record has an invalid SHA-256 hash")
    expected_hash = hashlib.sha256(
        _canonical_payload(envelope["payload"]).encode("utf-8")
    ).hexdigest()
    if not hmac.compare_digest(audit_hash, expected_hash):
        raise AuditIntegrityError("audit record hash verification failed")
    return envelope


class AuditWriter:
    schema_version = AUDIT_SCHEMA_VERSION

    def write(self, output_dir: Path, run_id: str, payload: Any) -> Path:
        path = _audit_path(output_dir, run_id)
        output_dir = path.parent
        output_dir.mkdir(parents=True, exist_ok=True)
        canonical_payload = _canonical_payload(payload)
        audit_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
        envelope = {
            "schema_version": self.schema_version,
            "audit_hash": audit_hash,
            "payload": json.loads(canonical_payload),
        }
        serialized_envelope = json.dumps(
            envelope,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        ) + "\n"

        descriptor, temporary_name = tempfile.mkstemp(
            dir=output_dir,
            prefix=f".{run_id}.",
            suffix=".tmp",
            text=True,
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(serialized_envelope)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                # A same-directory hard link publishes the fully flushed file
                # atomically and fails if the immutable destination already exists.
                os.link(temporary_path, path)
            except FileExistsError as exc:
                raise AuditExistsError(f"audit record already exists: {path}") from exc
            _fsync_directory(output_dir)
        finally:
            temporary_path.unlink(missing_ok=True)
        return path


class AuditReader:
    schema_version = AUDIT_SCHEMA_VERSION

    def read(self, output_dir: Path, run_id: str) -> dict[str, Any]:
        path = _audit_path(output_dir, run_id)
        try:
            with path.open("r", encoding="utf-8") as handle:
                envelope = json.load(handle)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuditIntegrityError("audit record is not valid UTF-8 JSON") from exc
        return verify_audit_envelope(envelope)
