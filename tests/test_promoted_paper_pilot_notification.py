from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import india_swing.promoted_paper_pilot_notification as notification_module

from india_swing.notifications import (
    LocalTelegramDeliveryReceiptStore,
    TelegramBotConfig,
)
from india_swing.promoted_operational_persistence import (
    build_promoted_operational_advisory,
    build_promoted_operational_terminal_record,
)
from india_swing.promoted_paper_pilot_notification import (
    CompletedPromotedPaperPilotNotification,
    GoogleCloudStoragePromotedPaperPilotNotificationStore,
    PromotedPaperPilotNotificationClaimExists,
    PromotedPaperPilotNotificationError,
    decode_promoted_paper_pilot_notification_claim,
    decode_promoted_paper_pilot_notification_receipt,
    deliver_promoted_paper_pilot_notification,
    encode_promoted_paper_pilot_notification_claim,
    encode_promoted_paper_pilot_notification_receipt,
)


class _NotFound(Exception):
    pass


class _PreconditionFailed(Exception):
    pass

from tests import test_promoted_operational_persistence as _persistence_tests


_NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
_BUCKET = "paper-pilot-state-123"


def _artifacts():
    result = _persistence_tests._complete_no_trade_result()
    advisory = build_promoted_operational_advisory(result)
    terminal = build_promoted_operational_terminal_record(
        result, advisory, None
    )
    return terminal, advisory


class _Transport:
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        self.events = events
        self.fail = fail
        self.calls = 0

    def post_json(self, **_kwargs):
        self.events.append("telegram")
        self.calls += 1
        if self.fail:
            raise RuntimeError("secret provider failure")
        return b'{"ok":true,"result":{"message_id":77}}'


class _Store:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.claim = None
        self.receipt = None

    def get_receipt_optional(self, *, bucket, claim):
        self.events.append("get-receipt")
        return self.receipt

    def create_claim(self, *, bucket, claim):
        self.events.append("claim")
        if self.claim is not None:
            raise PromotedPaperPilotNotificationClaimExists("already")
        self.claim = claim
        return object()

    def put_receipt(self, *, bucket, claim, receipt):
        self.events.append("receipt")
        self.receipt = receipt
        return object()


class _GCSBlob:
    def __init__(self, client, bucket, name, requested_generation=None):
        self.client = client
        self.key = (bucket, name)
        self.requested_generation = requested_generation
        self.generation = requested_generation

    def upload_from_string(
        self,
        payload,
        *,
        content_type,
        if_generation_match,
        checksum,
        retry,
    ):
        self.client.events.append(("upload", self.key[1]))
        if self.key in self.client.objects:
            raise _PreconditionFailed("existing object")
        generation = self.client.next_generation
        self.client.next_generation += 1
        self.client.objects[self.key] = (payload, generation)
        self.generation = generation

    def reload(self, *, retry=None):
        self.client.events.append(("reload", self.key[1]))
        if self.key not in self.client.objects:
            raise _NotFound("absent object")
        self.generation = self.client.objects[self.key][1]

    def download_as_bytes(
        self,
        *,
        end,
        raw_download,
        if_generation_match,
        retry,
    ):
        self.client.events.append(("download", self.key[1]))
        payload, generation = self.client.objects[self.key]
        if (
            self.requested_generation != generation
            or if_generation_match != generation
        ):
            raise RuntimeError("wrong generation")
        self.generation = generation
        return payload[: end + 1]


class _GCSBucket:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def blob(self, name, generation=None):
        self.client.events.append(("blob", name))
        return _GCSBlob(self.client, self.name, name, generation)


class _GCSClient:
    def __init__(self):
        self.objects = {}
        self.next_generation = 1
        self.events = []

    def bucket(self, name):
        return _GCSBucket(self, name)


def _deliver(
    root: Path,
    *,
    store: _Store,
    transport: _Transport,
) -> CompletedPromotedPaperPilotNotification:
    terminal, advisory = _artifacts()
    return deliver_promoted_paper_pilot_notification(
        bucket=_BUCKET,
        terminal=terminal,
        advisory=advisory,
        state_publication_id="1" * 64,
        state_manifest_object_name=(
            "promoted-operational-state/v1/"
            + terminal.target_session.isoformat()
            + "/"
            + terminal.spec_id
            + "/manifests/"
            + "1" * 64
            + ".json"
        ),
        state_manifest_generation=4,
        state_manifest_sha256="2" * 64,
        config=TelegramBotConfig(
            bot_token="12345:" + "a" * 24, chat_id="123456"
        ),
        transport=transport,
        receipt_store=LocalTelegramDeliveryReceiptStore(root),
        durable_store=store,
        clock=lambda: _NOW,
    )


class PromotedPaperPilotNotificationTests(unittest.TestCase):
    def test_claim_precedes_telegram_and_receipt_is_durable_and_canonical(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events: list[str] = []
            store = _Store(events)
            result = _deliver(
                Path(directory),
                store=store,
                transport=_Transport(events),
            )
        self.assertFalse(result.replayed)
        self.assertEqual(
            events,
            ["get-receipt", "claim", "telegram", "receipt", "get-receipt"],
        )
        self.assertEqual(
            decode_promoted_paper_pilot_notification_claim(
                encode_promoted_paper_pilot_notification_claim(result.claim)
            ),
            result.claim,
        )
        self.assertEqual(
            decode_promoted_paper_pilot_notification_receipt(
                encode_promoted_paper_pilot_notification_receipt(result.receipt)
            ),
            result.receipt,
        )

    def test_durable_receipt_replays_without_telegram(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events: list[str] = []
            store = _Store(events)
            first_transport = _Transport(events)
            first = _deliver(
                Path(directory), store=store, transport=first_transport
            )
            events.clear()
            second_transport = _Transport(events, fail=True)
            second = _deliver(
                Path(directory), store=store, transport=second_transport
            )
        self.assertTrue(second.replayed)
        self.assertEqual(second.receipt, first.receipt)
        self.assertEqual(events, ["get-receipt"])
        self.assertEqual(second_transport.calls, 0)

    def test_orphaned_claim_fails_uncertain_without_duplicate_send(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events: list[str] = []
            store = _Store(events)
            transport = _Transport(events, fail=True)
            with self.assertRaises(PromotedPaperPilotNotificationError):
                _deliver(Path(directory), store=store, transport=transport)
            self.assertIsNotNone(store.claim)
            self.assertIsNone(store.receipt)
            events.clear()
            second_transport = _Transport(events)
            with self.assertRaises(PromotedPaperPilotNotificationError):
                _deliver(
                    Path(directory),
                    store=store,
                    transport=second_transport,
                )
        self.assertEqual(second_transport.calls, 0)
        self.assertNotIn("telegram", events)

    def test_codecs_reject_noncanonical_or_tampered_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events: list[str] = []
            result = _deliver(
                Path(directory),
                store=_Store(events),
                transport=_Transport(events),
            )
        payload = encode_promoted_paper_pilot_notification_claim(result.claim)
        raw = json.loads(payload)
        raw["claim"]["message_sha256"] = "9" * 64
        tampered = (
            json.dumps(raw, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode()
        with self.assertRaises(PromotedPaperPilotNotificationError):
            decode_promoted_paper_pilot_notification_claim(tampered)

        receipt_payload = encode_promoted_paper_pilot_notification_receipt(
            result.receipt
        )
        self.assertEqual(
            hashlib.sha256(receipt_payload).hexdigest(),
            hashlib.sha256(
                encode_promoted_paper_pilot_notification_receipt(result.receipt)
            ).hexdigest(),
        )

    def test_production_gcs_store_pins_generations_and_replays_without_listing(
        self,
    ) -> None:
        client = _GCSClient()
        durable = GoogleCloudStoragePromotedPaperPilotNotificationStore(client)
        patches = (
            mock.patch.object(notification_module, "NotFound", _NotFound),
            mock.patch.object(
                notification_module, "PreconditionFailed", _PreconditionFailed
            ),
        )
        with patches[0], patches[1], tempfile.TemporaryDirectory() as first_directory:
            terminal, advisory = _artifacts()
            common = dict(
                bucket=_BUCKET,
                terminal=terminal,
                advisory=advisory,
                state_publication_id="1" * 64,
                state_manifest_object_name=(
                    "promoted-operational-state/v1/"
                    + terminal.target_session.isoformat()
                    + "/"
                    + terminal.spec_id
                    + "/manifests/"
                    + "1" * 64
                    + ".json"
                ),
                state_manifest_generation=4,
                state_manifest_sha256="2" * 64,
                config=TelegramBotConfig(
                    bot_token="12345:" + "a" * 24, chat_id="123456"
                ),
                durable_store=durable,
                clock=lambda: _NOW,
            )
            events: list[str] = []
            first_transport = _Transport(events)
            first = deliver_promoted_paper_pilot_notification(
                **common,
                transport=first_transport,
                receipt_store=LocalTelegramDeliveryReceiptStore(
                    Path(first_directory)
                ),
            )
        with tempfile.TemporaryDirectory() as second_directory:
            second_transport = _Transport([], fail=True)
            second = deliver_promoted_paper_pilot_notification(
                **common,
                transport=second_transport,
                receipt_store=LocalTelegramDeliveryReceiptStore(
                    Path(second_directory)
                ),
            )
        self.assertFalse(first.replayed)
        self.assertTrue(second.replayed)
        self.assertEqual(first.receipt, second.receipt)
        self.assertEqual(first_transport.calls, 1)
        self.assertEqual(second_transport.calls, 0)
        self.assertTrue(any(event[0] == "download" for event in client.events))
        self.assertFalse(any("list" in event[0] for event in client.events))

    def test_every_error_is_static_and_sanitized(self) -> None:
        secret = "SUPER_SECRET_CHAT_MARKER"
        with tempfile.TemporaryDirectory() as directory:
            events: list[str] = []
            store = _Store(events)
            store.claim = object()
            try:
                _deliver(
                    Path(directory),
                    store=store,
                    transport=_Transport(events),
                )
            except PromotedPaperPilotNotificationError as error:
                self.assertNotIn(secret, str(error))
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
            else:
                self.fail("expected fail-closed uncertain delivery")


if __name__ == "__main__":
    unittest.main()
