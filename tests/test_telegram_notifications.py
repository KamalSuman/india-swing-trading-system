from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from india_swing.notifications import (
    LocalTelegramDeliveryReceiptStore,
    TelegramBotConfig,
    TelegramDeliveryError,
    TelegramDeliveryRequest,
    deliver_telegram_notification,
)


_REPO_ROOT = Path(__file__).resolve().parent.parent
_TOKEN = "12345:abcdefghijklmnopqrstuvwxyz_123456"
_NOW = datetime(2026, 7, 22, 4, 0, tzinfo=timezone.utc)


class FakeTelegramTransport:
    def __init__(self, *, response: bytes | None = None, fail: bool = False) -> None:
        self.response = response or b'{"ok":true,"result":{"message_id":321}}'
        self.fail = fail
        self.calls: list[dict[str, object]] = []

    def post_json(self, **values) -> bytes:
        self.calls.append(values)
        if self.fail:
            raise RuntimeError("secret transport failure " + _TOKEN)
        return self.response


def _request(text: str = "Paper-only test alert") -> TelegramDeliveryRequest:
    return TelegramDeliveryRequest(
        delivery_key="1" * 64,
        text=text,
        message_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )


class TelegramNotificationTests(unittest.TestCase):
    def test_send_uses_protected_https_payload_and_receipt_suppresses_retry(self) -> None:
        config = TelegramBotConfig(bot_token=_TOKEN, chat_id="123456789")
        transport = FakeTelegramTransport()
        request = _request()
        with tempfile.TemporaryDirectory() as directory:
            store = LocalTelegramDeliveryReceiptStore(Path(directory))
            first = deliver_telegram_notification(
                request=request,
                config=config,
                transport=transport,
                receipt_store=store,
                clock=lambda: _NOW,
            )
            second = deliver_telegram_notification(
                request=request,
                config=config,
                transport=transport,
                receipt_store=store,
                clock=lambda: (_ for _ in ()).throw(RuntimeError()),
            )

            self.assertEqual(first, second)
            self.assertEqual(len(transport.calls), 1)
            call = transport.calls[0]
            self.assertEqual(
                call["url"],
                f"https://api.telegram.org/bot{_TOKEN}/sendMessage",
            )
            self.assertEqual(call["timeout_seconds"], 15)
            self.assertEqual(call["maximum_response_bytes"], 1024 * 1024)
            payload = json.loads(call["payload"])
            self.assertEqual(payload["chat_id"], config.chat_id)
            self.assertEqual(payload["text"], request.text)
            self.assertIs(payload["protect_content"], True)
            self.assertIs(payload["disable_web_page_preview"], True)
            self.assertEqual(first.telegram_message_id, 321)
            self.assertEqual(first.chat_binding_id, config.chat_binding_id)
            self.assertNotIn(_TOKEN, repr(config))

    def test_transport_and_response_failures_are_sanitized_and_not_receipted(self) -> None:
        config = TelegramBotConfig(bot_token=_TOKEN, chat_id="123456789")
        for transport in (
            FakeTelegramTransport(fail=True),
            FakeTelegramTransport(response=b'{"ok":false,"description":"secret"}'),
            FakeTelegramTransport(response=b'{"ok":true,"result":{"message_id":true}}'),
            FakeTelegramTransport(response=b'{"ok":true,"ok":true,"result":{}}'),
        ):
            with self.subTest(response=transport.response):
                with tempfile.TemporaryDirectory() as directory:
                    store = LocalTelegramDeliveryReceiptStore(Path(directory))
                    with self.assertRaises(TelegramDeliveryError) as raised:
                        deliver_telegram_notification(
                            request=_request(),
                            config=config,
                            transport=transport,
                            receipt_store=store,
                            clock=lambda: _NOW,
                        )
                    self.assertNotIn(_TOKEN, str(raised.exception))
                    self.assertEqual(list(Path(directory).glob("*.json")), [])

    def test_store_detects_tamper_and_conflicting_chat_binding(self) -> None:
        config = TelegramBotConfig(bot_token=_TOKEN, chat_id="123456789")
        with tempfile.TemporaryDirectory() as directory:
            store = LocalTelegramDeliveryReceiptStore(Path(directory))
            receipt = deliver_telegram_notification(
                request=_request(),
                config=config,
                transport=FakeTelegramTransport(),
                receipt_store=store,
                clock=lambda: _NOW,
            )
            path = store.path_for(receipt.delivery_key)
            original = path.read_bytes()
            path.write_bytes(original.replace(b'"telegram_message_id":321', b'"telegram_message_id":322'))
            with self.assertRaises(TelegramDeliveryError):
                store.get(receipt.delivery_key)
            path.write_bytes(original)
            with self.assertRaises(TelegramDeliveryError):
                deliver_telegram_notification(
                    request=_request(),
                    config=TelegramBotConfig(bot_token=_TOKEN, chat_id="987654321"),
                    transport=FakeTelegramTransport(),
                    receipt_store=store,
                    clock=lambda: _NOW,
                )

    def test_config_request_and_clock_reject_unsafe_values_before_network(self) -> None:
        for token, chat_id in (
            ("secret", "123"),
            (_TOKEN, "@public-channel"),
            (_TOKEN, "0"),
        ):
            with self.subTest(token=token, chat_id=chat_id):
                with self.assertRaises(TelegramDeliveryError):
                    TelegramBotConfig(bot_token=token, chat_id=chat_id)
        with self.assertRaises(TelegramDeliveryError):
            _request("x" * 4097)
        transport = FakeTelegramTransport()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(TelegramDeliveryError):
                deliver_telegram_notification(
                    request=_request(),
                    config=TelegramBotConfig(bot_token=_TOKEN, chat_id="123"),
                    transport=transport,
                    receipt_store=LocalTelegramDeliveryReceiptStore(Path(directory)),
                    clock=lambda: datetime(2026, 7, 22),
                )
        self.assertEqual(len(transport.calls), 0)

    def test_module_has_no_broker_or_dynamic_execution_capability(self) -> None:
        source = (
            _REPO_ROOT / "src/india_swing/notifications/telegram.py"
        ).read_text(encoding="utf-8")
        lowered = source.casefold()
        for forbidden in (
            "place_order",
            "modify_order",
            "cancel_order",
            "subprocess",
            "pickle",
            "eval(",
            "exec(",
        ):
            self.assertNotIn(forbidden, lowered)
        ast.parse(source)


if __name__ == "__main__":
    unittest.main()
