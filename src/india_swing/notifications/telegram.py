from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CHAT_ID = re.compile(r"-?[1-9][0-9]{0,19}\Z")
_TOKEN = re.compile(r"[0-9]{5,20}:[A-Za-z0-9_-]{20,120}\Z")
_CODEC = "telegram-delivery-receipt-json/v1"
_RECEIPT_SCHEMA = "telegram-delivery-receipt/v1"
_REQUEST_SCHEMA = "telegram-delivery-request/v1"
_MAXIMUM_MESSAGE_CHARACTERS = 4096
_MAXIMUM_RESPONSE_BYTES = 1024 * 1024
_MAXIMUM_RECEIPT_BYTES = 256 * 1024
_TIMEOUT_SECONDS = 15


class TelegramDeliveryError(RuntimeError):
    pass


class TelegramDeliveryReceiptNotFound(TelegramDeliveryError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise TelegramDeliveryError(f"{name} must be a lowercase SHA-256")
    return value


def _aware_utc(value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise TelegramDeliveryError("delivery time must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise TelegramDeliveryError("delivery time is invalid") from None
    if offset is None:
        raise TelegramDeliveryError("delivery time must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class TelegramBotConfig:
    bot_token: str = field(repr=False)
    chat_id: str

    def __post_init__(self) -> None:
        if type(self.bot_token) is not str or _TOKEN.fullmatch(self.bot_token) is None:
            raise TelegramDeliveryError("Telegram bot token is invalid")
        if type(self.chat_id) is not str or _CHAT_ID.fullmatch(self.chat_id) is None:
            raise TelegramDeliveryError("Telegram chat ID is invalid")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "TelegramBotConfig":
        source = os.environ if environ is None else environ
        try:
            return cls(
                bot_token=source["INDIA_SWING_TELEGRAM_BOT_TOKEN"],
                chat_id=source["INDIA_SWING_TELEGRAM_CHAT_ID"],
            )
        except TelegramDeliveryError:
            raise
        except Exception:
            raise TelegramDeliveryError("Telegram configuration is missing") from None

    @property
    def chat_binding_id(self) -> str:
        return content_id(
            {"channel": "TELEGRAM", "chat_id": self.chat_id},
            length=64,
        )


@dataclass(frozen=True, slots=True)
class TelegramDeliveryRequest:
    delivery_key: str
    text: str
    message_sha256: str
    category: str = "SWING_OPERATIONAL_RESULT"
    schema_version: str = _REQUEST_SCHEMA
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.delivery_key, "delivery_key")
        if (
            type(self.text) is not str
            or not self.text
            or len(self.text) > _MAXIMUM_MESSAGE_CHARACTERS
        ):
            raise TelegramDeliveryError("Telegram message length is invalid")
        _sha(self.message_sha256, "message_sha256")
        if hashlib.sha256(self.text.encode("utf-8")).hexdigest() != self.message_sha256:
            raise TelegramDeliveryError("Telegram message hash differs")
        if self.category != "SWING_OPERATIONAL_RESULT":
            raise TelegramDeliveryError("Telegram delivery category is invalid")
        if self.schema_version != _REQUEST_SCHEMA:
            raise TelegramDeliveryError("Telegram request schema is unsupported")
        object.__setattr__(self, "request_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "request_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        fresh = TelegramDeliveryRequest(
            delivery_key=self.delivery_key,
            text=self.text,
            message_sha256=self.message_sha256,
            category=self.category,
            schema_version=self.schema_version,
        )
        if self.request_id != fresh.request_id:
            raise TelegramDeliveryError("Telegram request identity failed")


@dataclass(frozen=True, slots=True)
class TelegramDeliveryReceipt:
    request_id: str
    delivery_key: str
    message_sha256: str
    chat_binding_id: str
    telegram_message_id: int
    delivered_at: datetime
    schema_version: str = _RECEIPT_SCHEMA
    receipt_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.request_id, "request_id"),
            (self.delivery_key, "delivery_key"),
            (self.message_sha256, "message_sha256"),
            (self.chat_binding_id, "chat_binding_id"),
        ):
            _sha(value, name)
        if type(self.telegram_message_id) is not int or self.telegram_message_id <= 0:
            raise TelegramDeliveryError("Telegram message ID is invalid")
        object.__setattr__(self, "delivered_at", _aware_utc(self.delivered_at))
        if self.schema_version != _RECEIPT_SCHEMA:
            raise TelegramDeliveryError("Telegram receipt schema is unsupported")
        object.__setattr__(self, "receipt_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "receipt_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        fresh = TelegramDeliveryReceipt(
            request_id=self.request_id,
            delivery_key=self.delivery_key,
            message_sha256=self.message_sha256,
            chat_binding_id=self.chat_binding_id,
            telegram_message_id=self.telegram_message_id,
            delivered_at=self.delivered_at,
            schema_version=self.schema_version,
        )
        if self.receipt_id != fresh.receipt_id:
            raise TelegramDeliveryError("Telegram receipt identity failed")


class TelegramHTTPTransport(Protocol):
    def post_json(
        self,
        *,
        url: str,
        payload: bytes,
        timeout_seconds: int,
        maximum_response_bytes: int,
    ) -> bytes: ...


class UrllibTelegramHTTPTransport:
    def post_json(
        self,
        *,
        url: str,
        payload: bytes,
        timeout_seconds: int,
        maximum_response_bytes: int,
    ) -> bytes:
        request = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                if response.getcode() != 200:
                    raise TelegramDeliveryError("Telegram HTTP status is invalid")
                body = response.read(maximum_response_bytes + 1)
        except TelegramDeliveryError:
            raise
        except Exception:
            raise TelegramDeliveryError("Telegram HTTPS request failed") from None
        if type(body) is not bytes or not (0 < len(body) <= maximum_response_bytes):
            raise TelegramDeliveryError("Telegram response size is invalid")
        return body


def _telegram_message_id(payload: bytes) -> int:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(value) is not dict or value.get("ok") is not True:
            raise TelegramDeliveryError("Telegram rejected the message")
        result = value.get("result")
        if type(result) is not dict:
            raise TelegramDeliveryError("Telegram response result is invalid")
        message_id = result.get("message_id")
        if type(message_id) is not int or message_id <= 0:
            raise TelegramDeliveryError("Telegram response message ID is invalid")
        return message_id
    except TelegramDeliveryError:
        raise
    except Exception:
        raise TelegramDeliveryError("Telegram response is invalid") from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TelegramDeliveryError("Telegram JSON has duplicate keys")
        result[key] = value
    return result


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _encode_receipt(value: TelegramDeliveryReceipt) -> bytes:
    if type(value) is not TelegramDeliveryReceipt:
        raise TelegramDeliveryError("Telegram receipt must be exact")
    value.verify_content_identity()
    return (
        json.dumps(
            {
                "codec_schema_version": _CODEC,
                "receipt": {
                    "chat_binding_id": value.chat_binding_id,
                    "delivered_at": value.delivered_at.isoformat(),
                    "delivery_key": value.delivery_key,
                    "message_sha256": value.message_sha256,
                    "receipt_id": value.receipt_id,
                    "request_id": value.request_id,
                    "schema_version": value.schema_version,
                    "telegram_message_id": value.telegram_message_id,
                },
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _decode_receipt(payload: bytes) -> TelegramDeliveryReceipt:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if (
            type(raw) is not dict
            or set(raw) != {"codec_schema_version", "receipt"}
            or raw["codec_schema_version"] != _CODEC
        ):
            raise TelegramDeliveryError("stored Telegram envelope is invalid")
        value = raw["receipt"]
        expected = {
            "chat_binding_id",
            "delivered_at",
            "delivery_key",
            "message_sha256",
            "receipt_id",
            "request_id",
            "schema_version",
            "telegram_message_id",
        }
        if type(value) is not dict or set(value) != expected:
            raise TelegramDeliveryError("stored Telegram receipt fields are invalid")
        receipt = TelegramDeliveryReceipt(
            request_id=value["request_id"],
            delivery_key=value["delivery_key"],
            message_sha256=value["message_sha256"],
            chat_binding_id=value["chat_binding_id"],
            telegram_message_id=value["telegram_message_id"],
            delivered_at=datetime.fromisoformat(value["delivered_at"]),
            schema_version=value["schema_version"],
        )
        if receipt.receipt_id != value["receipt_id"] or _encode_receipt(receipt) != payload:
            raise TelegramDeliveryError("stored Telegram receipt identity differs")
        return receipt
    except TelegramDeliveryError:
        raise
    except Exception:
        raise TelegramDeliveryError("stored Telegram receipt is invalid") from None


class LocalTelegramDeliveryReceiptStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def path_for(self, delivery_key: str) -> Path:
        _sha(delivery_key, "delivery_key")
        return self.root / f"{delivery_key}.json"

    def get(self, delivery_key: str) -> TelegramDeliveryReceipt:
        path = self.path_for(delivery_key)
        if not path.exists():
            raise TelegramDeliveryReceiptNotFound("Telegram receipt was not found")
        if not path.is_file() or _is_link_like(path):
            raise TelegramDeliveryError("Telegram receipt path is invalid")
        try:
            receipt = _decode_receipt(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_RECEIPT_BYTES)
            )
        except TelegramDeliveryError:
            raise
        except FileSafetyError:
            raise TelegramDeliveryError("Telegram receipt could not be read safely") from None
        if receipt.delivery_key != delivery_key:
            raise TelegramDeliveryError("Telegram receipt differs from its path")
        return receipt

    def put(self, value: TelegramDeliveryReceipt) -> TelegramDeliveryReceipt:
        if type(value) is not TelegramDeliveryReceipt:
            raise TelegramDeliveryError("Telegram receipt must be exact")
        payload = _encode_receipt(value)
        target = self.path_for(value.delivery_key)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.root):
                raise TelegramDeliveryError("Telegram receipt root cannot be a link")
            with advisory_file_lock(self.root / ".telegram-delivery.lock"):
                if target.exists():
                    stored = self.get(value.delivery_key)
                    if stored != value:
                        raise TelegramDeliveryError(
                            "Telegram delivery already has a different receipt"
                        )
                    return stored
                descriptor, name = tempfile.mkstemp(
                    prefix=".telegram-delivery-",
                    suffix=".tmp",
                    dir=self.root,
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
        except TelegramDeliveryError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise TelegramDeliveryError("Telegram receipt store is unavailable") from None
        return self.get(value.delivery_key)


def deliver_telegram_notification(
    *,
    request: TelegramDeliveryRequest,
    config: TelegramBotConfig,
    transport: TelegramHTTPTransport,
    receipt_store: LocalTelegramDeliveryReceiptStore,
    clock: Callable[[], datetime],
) -> TelegramDeliveryReceipt:
    """Deliver at least once; a durable local receipt suppresses ordinary retries."""

    if type(request) is not TelegramDeliveryRequest:
        raise TelegramDeliveryError("Telegram request must be exact")
    request.verify_content_identity()
    if type(config) is not TelegramBotConfig:
        raise TelegramDeliveryError("Telegram config must be exact")
    if type(receipt_store) is not LocalTelegramDeliveryReceiptStore:
        raise TelegramDeliveryError("Telegram receipt store must be exact")
    if not callable(clock):
        raise TelegramDeliveryError("Telegram delivery clock is required")
    try:
        stored = receipt_store.get(request.delivery_key)
    except TelegramDeliveryReceiptNotFound:
        stored = None
    except Exception:
        raise TelegramDeliveryError("Telegram receipt verification failed") from None
    if stored is not None:
        if (
            stored.request_id != request.request_id
            or stored.message_sha256 != request.message_sha256
            or stored.chat_binding_id != config.chat_binding_id
        ):
            raise TelegramDeliveryError("Telegram receipt differs from the request")
        return stored

    payload = (
        json.dumps(
            {
                "chat_id": config.chat_id,
                "disable_web_page_preview": True,
                "protect_content": True,
                "text": request.text,
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    ).encode("utf-8")
    try:
        delivered_at = _aware_utc(clock())
        response = transport.post_json(
            url=f"https://api.telegram.org/bot{config.bot_token}/sendMessage",
            payload=payload,
            timeout_seconds=_TIMEOUT_SECONDS,
            maximum_response_bytes=_MAXIMUM_RESPONSE_BYTES,
        )
        message_id = _telegram_message_id(response)
        receipt = TelegramDeliveryReceipt(
            request_id=request.request_id,
            delivery_key=request.delivery_key,
            message_sha256=request.message_sha256,
            chat_binding_id=config.chat_binding_id,
            telegram_message_id=message_id,
            delivered_at=delivered_at,
        )
        return receipt_store.put(receipt)
    except Exception:
        raise TelegramDeliveryError("Telegram delivery failed") from None
