from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any


AUDIT_SCHEMA_VERSION = "audit-v1"
_RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "access_token",
    "refresh_token",
    "request_token",
    "session_token",
    "auth_token",
    "cookie",
    "set_cookie",
)
_SENSITIVE_VALUE_MARKERS = re.compile(
    r"(?:authorization\s*:|set-cookie\s*:|cookie\s*:|"
    r"(?:api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"client[_-]?secret|password|private[_-]?key)\s*[=:])",
    re.IGNORECASE,
)
_PIPELINE_RESULT_KEYS = frozenset(
    {
        "run_id",
        "pipeline_version",
        "snapshot_id",
        "decision",
        "status",
        "integrity_hash",
    }
)


class AuditExistsError(FileExistsError):
    """Raised when an immutable audit record already exists."""


class InvalidAuditRunId(ValueError):
    """Raised when a run ID cannot safely be used as an audit filename."""


class AuditIntegrityError(ValueError):
    """Raised when an audit record is malformed or fails hash verification."""


def _is_sensitive_key(name: str) -> bool:
    normalized = name.casefold().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _validate_payload_before_write(value: Any, seen: set[int] | None = None) -> None:
    """Reject secrets and invoke content-integrity hooks before serialization."""

    if seen is None:
        seen = set()
    if value is None or isinstance(value, (bool, int, Decimal, date, datetime, Enum)):
        return
    if isinstance(value, str):
        if _SENSITIVE_VALUE_MARKERS.search(value):
            raise AuditIntegrityError("audit payload contains a secret-bearing marker")
        return

    marker = id(value)
    if marker in seen:
        return
    seen.add(marker)
    try:
        verifier = getattr(value, "verify_integrity", None)
        if verifier is None:
            verifier = getattr(value, "verify_content_identity", None)
        if verifier is not None:
            try:
                verifier()
            except Exception as exc:
                raise AuditIntegrityError(
                    "audit payload failed embedded integrity verification"
                ) from exc

        if is_dataclass(value):
            for item in fields(value):
                if _is_sensitive_key(item.name):
                    raise AuditIntegrityError(
                        "audit payload contains a sensitive dataclass field"
                    )
                _validate_payload_before_write(getattr(value, item.name), seen)
        elif isinstance(value, Mapping):
            if _PIPELINE_RESULT_KEYS.issubset(
                {key for key in value if isinstance(key, str)}
            ):
                raise AuditIntegrityError(
                    "untyped pipeline-result mappings cannot be audited"
                )
            for key, item in value.items():
                if isinstance(key, str) and _is_sensitive_key(key):
                    raise AuditIntegrityError(
                        "audit payload contains a sensitive mapping key"
                    )
                _validate_payload_before_write(key, seen)
                _validate_payload_before_write(item, seen)
        elif isinstance(value, (tuple, list, set, frozenset)):
            for item in value:
                _validate_payload_before_write(item, seen)
    finally:
        seen.remove(marker)


def _pipeline_result_run_ids(value: Any, seen: set[int] | None = None) -> set[str]:
    """Find embedded typed pipeline results without accepting look-alike mappings."""

    if seen is None:
        seen = set()
    if value is None or isinstance(
        value,
        (bool, int, str, Decimal, date, datetime, Enum),
    ):
        return set()
    marker = id(value)
    if marker in seen:
        return set()
    seen.add(marker)
    try:
        from india_swing.pipeline import PipelineResult

        if type(value) is PipelineResult:
            return {value.run_id}
        found: set[str] = set()
        if is_dataclass(value):
            for item in fields(value):
                found.update(_pipeline_result_run_ids(getattr(value, item.name), seen))
        elif isinstance(value, Mapping):
            for key, item in value.items():
                found.update(_pipeline_result_run_ids(key, seen))
                found.update(_pipeline_result_run_ids(item, seen))
        elif isinstance(value, (tuple, list, set, frozenset)):
            for item in value:
                found.update(_pipeline_result_run_ids(item, seen))
        return found
    finally:
        seen.remove(marker)


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

    def write_pipeline_result(
        self,
        output_dir: Path,
        result: Any,
        payload: Mapping[str, Any] | None = None,
    ) -> Path:
        from india_swing.pipeline import PipelineResult

        if type(result) is not PipelineResult:
            raise TypeError("result must be an exact PipelineResult")
        manifest = dict(payload or {})
        existing = manifest.get("result")
        if existing is not None and existing is not result:
            raise AuditIntegrityError(
                "pipeline audit payload contains a different result object"
            )
        manifest["result"] = result
        return self.write(output_dir, result.run_id, manifest)

    def write(self, output_dir: Path, run_id: str, payload: Any) -> Path:
        _validate_payload_before_write(payload)
        embedded_run_ids = _pipeline_result_run_ids(payload)
        if embedded_run_ids and embedded_run_ids != {run_id}:
            raise AuditIntegrityError(
                "audit filename run_id does not match its embedded pipeline result"
            )
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
