from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .models import (
    ShadowAlert,
    ShadowAlertError,
    ShadowAlertKind,
    ShadowNotification,
    notification_from_alert,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_NOTIFICATION_BYTES = 1024 * 1024
_CODEC_SCHEMA_VERSION = "shadow-notification-json/v1"


class ShadowNotificationStoreError(ShadowAlertError):
    pass


class ShadowNotificationNotFound(ShadowNotificationStoreError):
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


def _encode(value: ShadowNotification) -> bytes:
    if type(value) is not ShadowNotification:
        raise ShadowNotificationStoreError("notification must be exact")
    payload = {
        "codec_schema_version": _CODEC_SCHEMA_VERSION,
        "notification": {
            "alert_id": value.alert_id,
            "kind": value.kind.value,
            "message": value.message,
            "message_sha256": value.message_sha256,
            "mode": value.mode,
            "reference_readiness": value.reference_readiness,
            "schema_version": value.schema_version,
            "signal_id": value.signal_id,
            "source_decision_integrity_hash": value.source_decision_integrity_hash,
            "source_pipeline_integrity_hash": value.source_pipeline_integrity_hash,
            "source_run_id": value.source_run_id,
        },
    }
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ShadowNotificationStoreError("notification JSON has duplicate keys")
        value[key] = item
    return value


def _exact_object(value: object, keys: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise ShadowNotificationStoreError(f"stored {name} fields are invalid")
    return value


def _decode(payload: bytes) -> ShadowNotification:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root = _exact_object(
            raw,
            {"codec_schema_version", "notification"},
            "notification envelope",
        )
        if root["codec_schema_version"] != _CODEC_SCHEMA_VERSION:
            raise ShadowNotificationStoreError("notification codec is unsupported")
        value = _exact_object(
            root["notification"],
            {
                "alert_id",
                "kind",
                "message",
                "message_sha256",
                "mode",
                "reference_readiness",
                "schema_version",
                "signal_id",
                "source_decision_integrity_hash",
                "source_pipeline_integrity_hash",
                "source_run_id",
            },
            "notification",
        )
        return ShadowNotification(
            alert_id=value["alert_id"],
            source_run_id=value["source_run_id"],
            source_pipeline_integrity_hash=value[
                "source_pipeline_integrity_hash"
            ],
            source_decision_integrity_hash=value[
                "source_decision_integrity_hash"
            ],
            signal_id=value["signal_id"],
            kind=ShadowAlertKind(value["kind"]),
            reference_readiness=value["reference_readiness"],
            message=value["message"],
            message_sha256=value["message_sha256"],
            mode=value["mode"],
            schema_version=value["schema_version"],
        )
    except ShadowNotificationStoreError:
        raise
    except Exception:
        raise ShadowNotificationStoreError("stored notification is invalid") from None


class LocalShadowNotificationOutbox:
    """Create-once local handoff for a later external notification adapter."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def notifications_root(self) -> Path:
        return self.root / "notifications"

    def path_for(self, alert_id: str) -> Path:
        if type(alert_id) is not str or _SHA256.fullmatch(alert_id) is None:
            raise ShadowNotificationStoreError(
                "alert_id must be a full lowercase SHA-256"
            )
        return self.notifications_root / f"{alert_id}.json"

    def put(self, alert: ShadowAlert) -> ShadowNotification:
        if type(alert) is not ShadowAlert:
            raise ShadowNotificationStoreError("shadow alert must be exact")
        try:
            alert.verify_integrity()
            value = notification_from_alert(alert)
            payload = _encode(value)
        except ShadowAlertError:
            raise ShadowNotificationStoreError("shadow alert is invalid") from None
        target = self.path_for(value.alert_id)
        try:
            self.notifications_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.notifications_root):
                raise ShadowNotificationStoreError(
                    "notification root cannot be a link"
                )
            with advisory_file_lock(self.notifications_root / ".shadow-alert.lock"):
                if target.exists():
                    stored_bytes = read_stable_regular_file(
                        target,
                        maximum_bytes=_MAXIMUM_NOTIFICATION_BYTES,
                    )
                    if stored_bytes != payload:
                        raise ShadowNotificationStoreError(
                            "alert ID already stores different notification content"
                        )
                    return _decode(stored_bytes)
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".shadow-alert-",
                    suffix=".tmp",
                    dir=self.notifications_root,
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
        except ShadowNotificationStoreError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise ShadowNotificationStoreError(
                "notification outbox is unavailable"
            ) from None
        return self.get(value.alert_id)

    def get(self, alert_id: str) -> ShadowNotification:
        path = self.path_for(alert_id)
        if not path.exists():
            raise ShadowNotificationNotFound("notification was not found")
        if not path.is_file() or _is_link_like(path):
            raise ShadowNotificationStoreError(
                "notification must be a regular file"
            )
        try:
            value = _decode(
                read_stable_regular_file(
                    path,
                    maximum_bytes=_MAXIMUM_NOTIFICATION_BYTES,
                )
            )
        except ShadowNotificationStoreError:
            raise
        except FileSafetyError:
            raise ShadowNotificationStoreError(
                "notification could not be read safely"
            ) from None
        if value.alert_id != alert_id:
            raise ShadowNotificationStoreError(
                "stored notification differs from its path"
            )
        return value
