"""Durable, paper-only Telegram delivery for promoted operational results.

The module deliberately separates trading authority from delivery authority.
Promoted operational records remain ``notification_eligible=False`` and
``execution_eligible=False``; an operator-configured paper-pilot process may
deliver their already-sealed advisory text only after the corresponding state
manifest has been durably published.

Exactly-once delivery cannot be proven across an external HTTP call.  This
boundary therefore chooses the conservative alternative: create a durable GCS
claim before sending, publish a durable receipt afterwards, and never retry an
orphaned claim automatically.  An orphaned claim means delivery is uncertain
and requires operator review; it is never converted into a duplicate send.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol

try:
    from google.api_core.exceptions import NotFound, PreconditionFailed
except Exception:  # pragma: no cover - exercised only without the optional SDK
    NotFound = None  # type: ignore[assignment]
    PreconditionFailed = None  # type: ignore[assignment]

from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.identity import content_id
from india_swing.notifications import (
    LocalTelegramDeliveryReceiptStore,
    TelegramBotConfig,
    TelegramDeliveryReceipt,
    TelegramDeliveryRequest,
    TelegramHTTPTransport,
    deliver_telegram_notification,
)
from india_swing.promoted_operational_persistence import (
    PromotedOperationalAdvisoryRecord,
    PromotedOperationalTerminalRecord,
)


class PromotedPaperPilotNotificationError(ValueError):
    pass


class PromotedPaperPilotNotificationClaimExists(
    PromotedPaperPilotNotificationError
):
    pass


_ERR = "promoted paper-pilot notification is invalid"
_ERR_UNCERTAIN = "promoted paper-pilot notification delivery is uncertain"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_STATE_MANIFEST_PATH = re.compile(
    r"promoted-operational-state/v1/(\d{4}-\d{2}-\d{2})/([0-9a-f]{64})/"
    r"manifests/([0-9a-f]{64})\.json\Z"
)
_CLAIM_SCHEMA = "promoted-paper-pilot-notification-claim/v1"
_RECEIPT_SCHEMA = "promoted-paper-pilot-notification-receipt/v1"
_CLAIM_CODEC = "promoted-paper-pilot-notification-claim-json/v1"
_RECEIPT_CODEC = "promoted-paper-pilot-notification-receipt-json/v1"
MAXIMUM_PROMOTED_PAPER_PILOT_NOTIFICATION_BYTES = 64 * 1024
_CONTENT_TYPE = "application/json"


def _sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedPaperPilotNotificationError(_ERR)
    return value


def _positive_integer(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise PromotedPaperPilotNotificationError(_ERR)
    return value


def _bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        raise PromotedPaperPilotNotificationError(_ERR)
    return value


def _canonical_json(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    except Exception:
        raise PromotedPaperPilotNotificationError(_ERR) from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedPaperPilotNotificationError(_ERR)
        result[key] = value
    return result


def _decode_json(payload: bytes) -> object:
    if type(payload) is not bytes or not (
        0 < len(payload) <= MAXIMUM_PROMOTED_PAPER_PILOT_NOTIFICATION_BYTES
    ):
        raise PromotedPaperPilotNotificationError(_ERR)
    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _value: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except PromotedPaperPilotNotificationError:
        raise
    except Exception:
        raise PromotedPaperPilotNotificationError(_ERR) from None


@dataclass(frozen=True, slots=True)
class PromotedPaperPilotNotificationClaim:
    target_session: date
    operational_run_spec_id: str
    terminal_id: str
    advisory_id: str
    state_publication_id: str
    state_manifest_object_name: str
    state_manifest_generation: int
    state_manifest_sha256: str
    request_id: str
    message_sha256: str
    chat_binding_id: str
    schema_version: str = _CLAIM_SCHEMA
    claim_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.target_session) is not date:
            raise PromotedPaperPilotNotificationError(_ERR)
        for value in (
            self.operational_run_spec_id,
            self.terminal_id,
            self.advisory_id,
            self.state_publication_id,
            self.state_manifest_sha256,
            self.request_id,
            self.message_sha256,
            self.chat_binding_id,
        ):
            _sha(value)
        if (
            type(self.state_manifest_object_name) is not str
            or not self.state_manifest_object_name
            or len(self.state_manifest_object_name.encode("utf-8")) > 1024
        ):
            raise PromotedPaperPilotNotificationError(_ERR)
        manifest_match = _STATE_MANIFEST_PATH.fullmatch(
            self.state_manifest_object_name
        )
        if (
            manifest_match is None
            or manifest_match.group(1) != self.target_session.isoformat()
            or manifest_match.group(2) != self.operational_run_spec_id
            or manifest_match.group(3) != self.state_publication_id
        ):
            raise PromotedPaperPilotNotificationError(_ERR)
        _positive_integer(self.state_manifest_generation)
        if self.schema_version != _CLAIM_SCHEMA:
            raise PromotedPaperPilotNotificationError(_ERR)
        object.__setattr__(self, "claim_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "advisory_id": self.advisory_id,
                "chat_binding_id": self.chat_binding_id,
                "message_sha256": self.message_sha256,
                "operational_run_spec_id": self.operational_run_spec_id,
                "request_id": self.request_id,
                "schema_version": self.schema_version,
                "state_manifest_generation": self.state_manifest_generation,
                "state_manifest_object_name": self.state_manifest_object_name,
                "state_manifest_sha256": self.state_manifest_sha256,
                "state_publication_id": self.state_publication_id,
                "target_session": self.target_session.isoformat(),
                "terminal_id": self.terminal_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.claim_id != self._calculated_id():
            raise PromotedPaperPilotNotificationError(_ERR)


@dataclass(frozen=True, slots=True)
class PromotedPaperPilotNotificationReceipt:
    claim_id: str
    terminal_id: str
    state_publication_id: str
    telegram_receipt: TelegramDeliveryReceipt
    schema_version: str = _RECEIPT_SCHEMA
    notification_receipt_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.claim_id)
        _sha(self.terminal_id)
        _sha(self.state_publication_id)
        if type(self.telegram_receipt) is not TelegramDeliveryReceipt:
            raise PromotedPaperPilotNotificationError(_ERR)
        self.telegram_receipt.verify_content_identity()
        if self.schema_version != _RECEIPT_SCHEMA:
            raise PromotedPaperPilotNotificationError(_ERR)
        object.__setattr__(
            self, "notification_receipt_id", self._calculated_id()
        )

    def _calculated_id(self) -> str:
        receipt = self.telegram_receipt
        return content_id(
            {
                "claim_id": self.claim_id,
                "schema_version": self.schema_version,
                "state_publication_id": self.state_publication_id,
                "telegram_receipt": {
                    "chat_binding_id": receipt.chat_binding_id,
                    "delivered_at": receipt.delivered_at.isoformat(),
                    "delivery_key": receipt.delivery_key,
                    "message_sha256": receipt.message_sha256,
                    "receipt_id": receipt.receipt_id,
                    "request_id": receipt.request_id,
                    "schema_version": receipt.schema_version,
                    "telegram_message_id": receipt.telegram_message_id,
                },
                "terminal_id": self.terminal_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.telegram_receipt.verify_content_identity()
        if self.notification_receipt_id != self._calculated_id():
            raise PromotedPaperPilotNotificationError(_ERR)


def _claim_body(value: PromotedPaperPilotNotificationClaim) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "advisory_id": value.advisory_id,
        "chat_binding_id": value.chat_binding_id,
        "claim_id": value.claim_id,
        "message_sha256": value.message_sha256,
        "operational_run_spec_id": value.operational_run_spec_id,
        "request_id": value.request_id,
        "schema_version": value.schema_version,
        "state_manifest_generation": value.state_manifest_generation,
        "state_manifest_object_name": value.state_manifest_object_name,
        "state_manifest_sha256": value.state_manifest_sha256,
        "state_publication_id": value.state_publication_id,
        "target_session": value.target_session.isoformat(),
        "terminal_id": value.terminal_id,
    }


def encode_promoted_paper_pilot_notification_claim(
    value: PromotedPaperPilotNotificationClaim,
) -> bytes:
    if type(value) is not PromotedPaperPilotNotificationClaim:
        raise PromotedPaperPilotNotificationError(_ERR)
    return _canonical_json(
        {"codec_schema_version": _CLAIM_CODEC, "claim": _claim_body(value)}
    )


def decode_promoted_paper_pilot_notification_claim(
    payload: bytes,
) -> PromotedPaperPilotNotificationClaim:
    raw = _decode_json(payload)
    if (
        type(raw) is not dict
        or set(raw) != {"codec_schema_version", "claim"}
        or raw["codec_schema_version"] != _CLAIM_CODEC
        or type(raw["claim"]) is not dict
    ):
        raise PromotedPaperPilotNotificationError(_ERR)
    value = raw["claim"]
    expected = {
        "advisory_id",
        "chat_binding_id",
        "claim_id",
        "message_sha256",
        "operational_run_spec_id",
        "request_id",
        "schema_version",
        "state_manifest_generation",
        "state_manifest_object_name",
        "state_manifest_sha256",
        "state_publication_id",
        "target_session",
        "terminal_id",
    }
    if set(value) != expected:
        raise PromotedPaperPilotNotificationError(_ERR)
    try:
        claim = PromotedPaperPilotNotificationClaim(
            target_session=date.fromisoformat(value["target_session"]),
            operational_run_spec_id=value["operational_run_spec_id"],
            terminal_id=value["terminal_id"],
            advisory_id=value["advisory_id"],
            state_publication_id=value["state_publication_id"],
            state_manifest_object_name=value["state_manifest_object_name"],
            state_manifest_generation=value["state_manifest_generation"],
            state_manifest_sha256=value["state_manifest_sha256"],
            request_id=value["request_id"],
            message_sha256=value["message_sha256"],
            chat_binding_id=value["chat_binding_id"],
            schema_version=value["schema_version"],
        )
    except Exception:
        raise PromotedPaperPilotNotificationError(_ERR) from None
    if claim.claim_id != value["claim_id"] or _claim_body(claim) != value:
        raise PromotedPaperPilotNotificationError(_ERR)
    if encode_promoted_paper_pilot_notification_claim(claim) != payload:
        raise PromotedPaperPilotNotificationError(_ERR)
    return claim


def _receipt_body(
    value: PromotedPaperPilotNotificationReceipt,
) -> dict[str, object]:
    value.verify_content_identity()
    receipt = value.telegram_receipt
    return {
        "claim_id": value.claim_id,
        "notification_receipt_id": value.notification_receipt_id,
        "schema_version": value.schema_version,
        "state_publication_id": value.state_publication_id,
        "telegram_receipt": {
            "chat_binding_id": receipt.chat_binding_id,
            "delivered_at": receipt.delivered_at.isoformat(),
            "delivery_key": receipt.delivery_key,
            "message_sha256": receipt.message_sha256,
            "receipt_id": receipt.receipt_id,
            "request_id": receipt.request_id,
            "schema_version": receipt.schema_version,
            "telegram_message_id": receipt.telegram_message_id,
        },
        "terminal_id": value.terminal_id,
    }


def encode_promoted_paper_pilot_notification_receipt(
    value: PromotedPaperPilotNotificationReceipt,
) -> bytes:
    if type(value) is not PromotedPaperPilotNotificationReceipt:
        raise PromotedPaperPilotNotificationError(_ERR)
    return _canonical_json(
        {"codec_schema_version": _RECEIPT_CODEC, "receipt": _receipt_body(value)}
    )


def decode_promoted_paper_pilot_notification_receipt(
    payload: bytes,
) -> PromotedPaperPilotNotificationReceipt:
    raw = _decode_json(payload)
    if (
        type(raw) is not dict
        or set(raw) != {"codec_schema_version", "receipt"}
        or raw["codec_schema_version"] != _RECEIPT_CODEC
        or type(raw["receipt"]) is not dict
    ):
        raise PromotedPaperPilotNotificationError(_ERR)
    value = raw["receipt"]
    if set(value) != {
        "claim_id",
        "notification_receipt_id",
        "schema_version",
        "state_publication_id",
        "telegram_receipt",
        "terminal_id",
    } or type(value["telegram_receipt"]) is not dict:
        raise PromotedPaperPilotNotificationError(_ERR)
    nested = value["telegram_receipt"]
    if set(nested) != {
        "chat_binding_id",
        "delivered_at",
        "delivery_key",
        "message_sha256",
        "receipt_id",
        "request_id",
        "schema_version",
        "telegram_message_id",
    }:
        raise PromotedPaperPilotNotificationError(_ERR)
    try:
        telegram_receipt = TelegramDeliveryReceipt(
            request_id=nested["request_id"],
            delivery_key=nested["delivery_key"],
            message_sha256=nested["message_sha256"],
            chat_binding_id=nested["chat_binding_id"],
            telegram_message_id=nested["telegram_message_id"],
            delivered_at=datetime.fromisoformat(nested["delivered_at"]),
            schema_version=nested["schema_version"],
        )
        receipt = PromotedPaperPilotNotificationReceipt(
            claim_id=value["claim_id"],
            terminal_id=value["terminal_id"],
            state_publication_id=value["state_publication_id"],
            telegram_receipt=telegram_receipt,
            schema_version=value["schema_version"],
        )
    except Exception:
        raise PromotedPaperPilotNotificationError(_ERR) from None
    if (
        telegram_receipt.receipt_id != nested["receipt_id"]
        or receipt.notification_receipt_id != value["notification_receipt_id"]
        or _receipt_body(receipt) != value
        or encode_promoted_paper_pilot_notification_receipt(receipt) != payload
    ):
        raise PromotedPaperPilotNotificationError(_ERR)
    return receipt


def promoted_paper_pilot_notification_claim_object_name(
    claim: PromotedPaperPilotNotificationClaim,
) -> str:
    if type(claim) is not PromotedPaperPilotNotificationClaim:
        raise PromotedPaperPilotNotificationError(_ERR)
    claim.verify_content_identity()
    return (
        "promoted-paper-pilot-notifications/v1/"
        f"{claim.target_session.isoformat()}/{claim.terminal_id}/"
        f"{claim.chat_binding_id}/claim.json"
    )


def promoted_paper_pilot_notification_receipt_object_name(
    claim: PromotedPaperPilotNotificationClaim,
) -> str:
    return promoted_paper_pilot_notification_claim_object_name(claim).replace(
        "/claim.json", "/receipt.json"
    )


class PromotedPaperPilotNotificationStore(Protocol):
    def get_receipt_optional(
        self, *, bucket: str, claim: PromotedPaperPilotNotificationClaim
    ) -> PromotedPaperPilotNotificationReceipt | None: ...

    def create_claim(
        self, *, bucket: str, claim: PromotedPaperPilotNotificationClaim
    ) -> PublishedStateObject: ...

    def put_receipt(
        self,
        *,
        bucket: str,
        claim: PromotedPaperPilotNotificationClaim,
        receipt: PromotedPaperPilotNotificationReceipt,
    ) -> PublishedStateObject: ...


class GoogleCloudStoragePromotedPaperPilotNotificationStore:
    """Exact-object GCS notification store; never lists or selects latest."""

    def __init__(self, client: object) -> None:
        if client is None:
            raise PromotedPaperPilotNotificationError(_ERR)
        self._client = client

    def _read_optional(
        self, *, bucket: str, object_name: str
    ) -> tuple[bytes, int] | None:
        bucket = _bucket(bucket)
        try:
            blob = self._client.bucket(bucket).blob(object_name)
            blob.reload(retry=None)
            generation = blob.generation
        except Exception as error:
            if NotFound is not None and isinstance(error, NotFound):
                return None
            raise PromotedPaperPilotNotificationError(_ERR) from None
        if type(generation) is not int or generation <= 0:
            raise PromotedPaperPilotNotificationError(_ERR)
        try:
            pinned = self._client.bucket(bucket).blob(
                object_name, generation=generation
            )
            payload = pinned.download_as_bytes(
                end=MAXIMUM_PROMOTED_PAPER_PILOT_NOTIFICATION_BYTES,
                raw_download=True,
                if_generation_match=generation,
                retry=None,
            )
            pinned_generation = pinned.generation
        except Exception:
            raise PromotedPaperPilotNotificationError(_ERR) from None
        if (
            type(pinned_generation) is not int
            or pinned_generation != generation
            or type(payload) is not bytes
            or not (
                0
                < len(payload)
                <= MAXIMUM_PROMOTED_PAPER_PILOT_NOTIFICATION_BYTES
            )
        ):
            raise PromotedPaperPilotNotificationError(_ERR)
        return payload, generation

    def _upload_create_only(
        self, *, bucket: str, object_name: str, payload: bytes
    ) -> PublishedStateObject:
        bucket = _bucket(bucket)
        try:
            blob = self._client.bucket(bucket).blob(object_name)
            blob.upload_from_string(
                payload,
                content_type=_CONTENT_TYPE,
                if_generation_match=0,
                checksum="auto",
                retry=None,
            )
            generation = blob.generation
        except Exception as error:
            if PreconditionFailed is not None and isinstance(
                error, PreconditionFailed
            ):
                raise PromotedPaperPilotNotificationClaimExists(_ERR) from None
            raise PromotedPaperPilotNotificationError(_ERR) from None
        if type(generation) is not int or generation <= 0:
            raise PromotedPaperPilotNotificationError(_ERR)
        observed = self._read_optional(bucket=bucket, object_name=object_name)
        if observed is None or observed[0] != payload or observed[1] != generation:
            raise PromotedPaperPilotNotificationError(_ERR)
        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )

    def get_receipt_optional(
        self, *, bucket: str, claim: PromotedPaperPilotNotificationClaim
    ) -> PromotedPaperPilotNotificationReceipt | None:
        object_name = promoted_paper_pilot_notification_receipt_object_name(
            claim
        )
        observed = self._read_optional(bucket=bucket, object_name=object_name)
        if observed is None:
            return None
        receipt = decode_promoted_paper_pilot_notification_receipt(observed[0])
        if (
            receipt.claim_id != claim.claim_id
            or receipt.terminal_id != claim.terminal_id
            or receipt.state_publication_id != claim.state_publication_id
            or receipt.telegram_receipt.request_id != claim.request_id
            or receipt.telegram_receipt.message_sha256 != claim.message_sha256
            or receipt.telegram_receipt.chat_binding_id != claim.chat_binding_id
        ):
            raise PromotedPaperPilotNotificationError(_ERR)
        return receipt

    def create_claim(
        self, *, bucket: str, claim: PromotedPaperPilotNotificationClaim
    ) -> PublishedStateObject:
        payload = encode_promoted_paper_pilot_notification_claim(claim)
        return self._upload_create_only(
            bucket=bucket,
            object_name=promoted_paper_pilot_notification_claim_object_name(
                claim
            ),
            payload=payload,
        )

    def put_receipt(
        self,
        *,
        bucket: str,
        claim: PromotedPaperPilotNotificationClaim,
        receipt: PromotedPaperPilotNotificationReceipt,
    ) -> PublishedStateObject:
        if (
            receipt.claim_id != claim.claim_id
            or receipt.terminal_id != claim.terminal_id
            or receipt.state_publication_id != claim.state_publication_id
        ):
            raise PromotedPaperPilotNotificationError(_ERR)
        payload = encode_promoted_paper_pilot_notification_receipt(receipt)
        object_name = promoted_paper_pilot_notification_receipt_object_name(
            claim
        )
        try:
            return self._upload_create_only(
                bucket=bucket, object_name=object_name, payload=payload
            )
        except PromotedPaperPilotNotificationClaimExists:
            stored = self.get_receipt_optional(bucket=bucket, claim=claim)
            if stored != receipt:
                raise PromotedPaperPilotNotificationError(_ERR) from None
            observed = self._read_optional(bucket=bucket, object_name=object_name)
            if observed is None or observed[0] != payload:
                raise PromotedPaperPilotNotificationError(_ERR)
            return PublishedStateObject(
                object_name=object_name,
                generation=observed[1],
                byte_count=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
            )


@dataclass(frozen=True, slots=True)
class CompletedPromotedPaperPilotNotification:
    claim: PromotedPaperPilotNotificationClaim
    receipt: PromotedPaperPilotNotificationReceipt
    replayed: bool

    def __post_init__(self) -> None:
        if type(self.claim) is not PromotedPaperPilotNotificationClaim:
            raise PromotedPaperPilotNotificationError(_ERR)
        if type(self.receipt) is not PromotedPaperPilotNotificationReceipt:
            raise PromotedPaperPilotNotificationError(_ERR)
        if type(self.replayed) is not bool:
            raise PromotedPaperPilotNotificationError(_ERR)
        self.claim.verify_content_identity()
        self.receipt.verify_content_identity()
        if (
            self.receipt.claim_id != self.claim.claim_id
            or self.receipt.terminal_id != self.claim.terminal_id
            or self.receipt.state_publication_id
            != self.claim.state_publication_id
        ):
            raise PromotedPaperPilotNotificationError(_ERR)


def build_promoted_paper_pilot_message(
    *,
    advisory: PromotedOperationalAdvisoryRecord,
    terminal: PromotedOperationalTerminalRecord,
    state_publication_id: str,
) -> str:
    if (
        type(advisory) is not PromotedOperationalAdvisoryRecord
        or type(terminal) is not PromotedOperationalTerminalRecord
    ):
        raise PromotedPaperPilotNotificationError(_ERR)
    advisory.verify_content_identity()
    terminal.verify_content_identity()
    _sha(state_publication_id)
    if (
        terminal.advisory_id != advisory.advisory_id
        or terminal.spec_id != advisory.spec_id
        or terminal.target_session != advisory.target_session
        or terminal.action is not advisory.action
        or terminal.status is not advisory.status
        or terminal.paper_only is not True
        or terminal.notification_eligible is not False
        or terminal.execution_eligible is not False
    ):
        raise PromotedPaperPilotNotificationError(_ERR)
    return (
        advisory.advisory_text.rstrip("\n")
        + "\n\nPaper-pilot audit:\n"
        + f"- Terminal ID: {terminal.terminal_id}\n"
        + f"- State publication ID: {state_publication_id}\n"
    )


def _deliver_promoted_paper_pilot_notification(
    *,
    bucket: str,
    terminal: PromotedOperationalTerminalRecord,
    advisory: PromotedOperationalAdvisoryRecord,
    state_publication_id: str,
    state_manifest_object_name: str,
    state_manifest_generation: int,
    state_manifest_sha256: str,
    config: TelegramBotConfig,
    transport: TelegramHTTPTransport,
    receipt_store: LocalTelegramDeliveryReceiptStore,
    durable_store: PromotedPaperPilotNotificationStore,
    clock: Callable[[], datetime],
) -> CompletedPromotedPaperPilotNotification:
    """Deliver once after durable state publication, or fail uncertain.

    A pre-existing durable receipt replays without network access.  A
    pre-existing claim without a receipt is never retried automatically.
    """

    try:
        bucket = _bucket(bucket)
        if type(config) is not TelegramBotConfig:
            raise PromotedPaperPilotNotificationError(_ERR)
        if type(receipt_store) is not LocalTelegramDeliveryReceiptStore:
            raise PromotedPaperPilotNotificationError(_ERR)
        if not callable(clock):
            raise PromotedPaperPilotNotificationError(_ERR)
        text = build_promoted_paper_pilot_message(
            advisory=advisory,
            terminal=terminal,
            state_publication_id=state_publication_id,
        )
        request = TelegramDeliveryRequest(
            delivery_key=terminal.terminal_id,
            text=text,
            message_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
            category="SWING_OPERATIONAL_RESULT",
        )
        claim = PromotedPaperPilotNotificationClaim(
            target_session=terminal.target_session,
            operational_run_spec_id=terminal.spec_id,
            terminal_id=terminal.terminal_id,
            advisory_id=advisory.advisory_id,
            state_publication_id=_sha(state_publication_id),
            state_manifest_object_name=state_manifest_object_name,
            state_manifest_generation=_positive_integer(
                state_manifest_generation
            ),
            state_manifest_sha256=_sha(state_manifest_sha256),
            request_id=request.request_id,
            message_sha256=request.message_sha256,
            chat_binding_id=config.chat_binding_id,
        )

        existing = durable_store.get_receipt_optional(
            bucket=bucket, claim=claim
        )
        if existing is not None:
            return CompletedPromotedPaperPilotNotification(
                claim=claim, receipt=existing, replayed=True
            )

        try:
            durable_store.create_claim(bucket=bucket, claim=claim)
        except PromotedPaperPilotNotificationClaimExists:
            existing = durable_store.get_receipt_optional(
                bucket=bucket, claim=claim
            )
            if existing is not None:
                return CompletedPromotedPaperPilotNotification(
                    claim=claim, receipt=existing, replayed=True
                )
            raise PromotedPaperPilotNotificationError(_ERR_UNCERTAIN) from None

        telegram_receipt = deliver_telegram_notification(
            request=request,
            config=config,
            transport=transport,
            receipt_store=receipt_store,
            clock=clock,
        )
        receipt = PromotedPaperPilotNotificationReceipt(
            claim_id=claim.claim_id,
            terminal_id=terminal.terminal_id,
            state_publication_id=state_publication_id,
            telegram_receipt=telegram_receipt,
        )
        durable_store.put_receipt(
            bucket=bucket, claim=claim, receipt=receipt
        )
        confirmed = durable_store.get_receipt_optional(
            bucket=bucket, claim=claim
        )
        if confirmed != receipt:
            raise PromotedPaperPilotNotificationError(_ERR_UNCERTAIN)
        return CompletedPromotedPaperPilotNotification(
            claim=claim, receipt=confirmed, replayed=False
        )
    except PromotedPaperPilotNotificationError:
        raise
    except Exception:
        raise PromotedPaperPilotNotificationError(_ERR_UNCERTAIN) from None


def deliver_promoted_paper_pilot_notification(
    *,
    bucket: str,
    terminal: PromotedOperationalTerminalRecord,
    advisory: PromotedOperationalAdvisoryRecord,
    state_publication_id: str,
    state_manifest_object_name: str,
    state_manifest_generation: int,
    state_manifest_sha256: str,
    config: TelegramBotConfig,
    transport: TelegramHTTPTransport,
    receipt_store: LocalTelegramDeliveryReceiptStore,
    durable_store: PromotedPaperPilotNotificationStore,
    clock: Callable[[], datetime],
) -> CompletedPromotedPaperPilotNotification:
    """Sanitized public boundary with no retained exception context."""

    failed = False
    failure_message = _ERR_UNCERTAIN
    result: CompletedPromotedPaperPilotNotification | None = None
    try:
        result = _deliver_promoted_paper_pilot_notification(
            bucket=bucket,
            terminal=terminal,
            advisory=advisory,
            state_publication_id=state_publication_id,
            state_manifest_object_name=state_manifest_object_name,
            state_manifest_generation=state_manifest_generation,
            state_manifest_sha256=state_manifest_sha256,
            config=config,
            transport=transport,
            receipt_store=receipt_store,
            durable_store=durable_store,
            clock=clock,
        )
    except PromotedPaperPilotNotificationError as error:
        failed = True
        if str(error) == _ERR:
            failure_message = _ERR
    except Exception:
        failed = True
    if failed or result is None:
        raise PromotedPaperPilotNotificationError(failure_message)
    return result
