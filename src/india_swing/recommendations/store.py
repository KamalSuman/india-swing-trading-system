from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from datetime import datetime
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .models import (
    SwingDecisionAction,
    SwingDecisionError,
    SwingDecisionNotification,
    SwingDecisionPackage,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CODEC_SCHEMA_VERSION = "swing-decision-notification-json/v1"
_MAXIMUM_NOTIFICATION_BYTES = 256 * 1024


class SwingDecisionOutboxError(SwingDecisionError):
    pass


class SwingDecisionNotificationNotFound(SwingDecisionOutboxError):
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


def encode_swing_decision_notification(value: SwingDecisionNotification) -> bytes:
    if type(value) is not SwingDecisionNotification:
        raise SwingDecisionOutboxError("notification must be exact")
    value.verify_content_identity()
    payload = {
        "codec_schema_version": _CODEC_SCHEMA_VERSION,
        "notification": {
            "action": value.action.value,
            "decision_id": value.decision_id,
            "evaluated_at": value.evaluated_at.isoformat(),
            "message": value.message,
            "message_sha256": value.message_sha256,
            "mode": value.mode,
            "notification_id": value.notification_id,
            "schema_version": value.schema_version,
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
            raise SwingDecisionOutboxError("notification JSON has duplicate keys")
        value[key] = item
    return value


def _exact_object(value: object, keys: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise SwingDecisionOutboxError(f"stored {name} fields are invalid")
    return value


def decode_swing_decision_notification(payload: bytes) -> SwingDecisionNotification:
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
            raise SwingDecisionOutboxError("notification codec is unsupported")
        value = _exact_object(
            root["notification"],
            {
                "action",
                "decision_id",
                "evaluated_at",
                "message",
                "message_sha256",
                "mode",
                "notification_id",
                "schema_version",
            },
            "notification",
        )
        notification = SwingDecisionNotification(
            decision_id=value["decision_id"],
            action=SwingDecisionAction(value["action"]),
            evaluated_at=datetime.fromisoformat(value["evaluated_at"]),
            message=value["message"],
            message_sha256=value["message_sha256"],
            mode=value["mode"],
            schema_version=value["schema_version"],
        )
        if notification.notification_id != value["notification_id"]:
            raise SwingDecisionOutboxError("stored notification identity differs")
        return notification
    except SwingDecisionOutboxError:
        raise
    except Exception:
        raise SwingDecisionOutboxError("stored notification is invalid") from None


class LocalSwingDecisionOutbox:
    """Create-once local handoff keyed by the immutable daily decision ID."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def notifications_root(self) -> Path:
        return self.root / "notifications"

    def path_for(self, decision_id: str) -> Path:
        if type(decision_id) is not str or _SHA256.fullmatch(decision_id) is None:
            raise SwingDecisionOutboxError(
                "decision_id must be a full lowercase SHA-256"
            )
        return self.notifications_root / f"{decision_id}.json"

    def put(self, package: SwingDecisionPackage) -> SwingDecisionNotification:
        if type(package) is not SwingDecisionPackage:
            raise SwingDecisionOutboxError("decision package must be exact")
        try:
            package.verify_content_identity()
            value = package.notification
        except SwingDecisionError:
            raise SwingDecisionOutboxError("decision package is invalid") from None
        return self.put_notification(value)

    def put_notification(
        self,
        value: SwingDecisionNotification,
    ) -> SwingDecisionNotification:
        """Create or verify an already-decoded immutable notification."""

        if type(value) is not SwingDecisionNotification:
            raise SwingDecisionOutboxError("notification must be exact")
        try:
            value.verify_content_identity()
            payload = encode_swing_decision_notification(value)
        except Exception:
            raise SwingDecisionOutboxError("notification is invalid") from None
        target = self.path_for(value.decision_id)
        try:
            self.notifications_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.notifications_root):
                raise SwingDecisionOutboxError(
                    "notification root cannot be a link"
                )
            with advisory_file_lock(self.notifications_root / ".swing-decision.lock"):
                if target.exists():
                    stored_bytes = read_stable_regular_file(
                        target,
                        maximum_bytes=_MAXIMUM_NOTIFICATION_BYTES,
                    )
                    if stored_bytes != payload:
                        raise SwingDecisionOutboxError(
                            "decision ID already stores different notification content"
                        )
                    return decode_swing_decision_notification(stored_bytes)
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".swing-decision-",
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
        except SwingDecisionOutboxError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise SwingDecisionOutboxError(
                "decision notification outbox is unavailable"
            ) from None
        return self.get(value.decision_id)

    def get(self, decision_id: str) -> SwingDecisionNotification:
        path = self.path_for(decision_id)
        if not path.exists():
            raise SwingDecisionNotificationNotFound(
                "decision notification was not found"
            )
        if not path.is_file() or _is_link_like(path):
            raise SwingDecisionOutboxError(
                "decision notification must be a regular file"
            )
        try:
            value = decode_swing_decision_notification(
                read_stable_regular_file(
                    path,
                    maximum_bytes=_MAXIMUM_NOTIFICATION_BYTES,
                )
            )
        except SwingDecisionOutboxError:
            raise
        except FileSafetyError:
            raise SwingDecisionOutboxError(
                "decision notification could not be read safely"
            ) from None
        if value.decision_id != decision_id:
            raise SwingDecisionOutboxError(
                "stored notification differs from its path"
            )
        return value
