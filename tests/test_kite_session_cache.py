from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.market_data.config import KiteCredentials
from india_swing.market_data.kite_session_cache import (
    KITE_SESSION_CACHE_SCHEMA_VERSION,
    KiteDailySessionCache,
    KiteSessionCacheError,
    WindowsDPAPISecretProtector,
    _canonical_json_bytes,
    _entropy_for,
    _session_expiry,
    default_kite_session_cache_path,
)


API_KEY = "distinct-app-key"
ACCESS_TOKEN = "distinct-access-token"
AUTHENTICATED_AT = datetime(2026, 7, 26, 14, 0, tzinfo=timezone.utc)


class RecordingProtector:
    def __init__(self) -> None:
        self.protect_calls: list[tuple[bytes, bytes]] = []
        self.unprotect_calls: list[tuple[bytes, bytes]] = []
        self._values: dict[bytes, bytes] = {}

    def protect(self, plaintext: bytes, *, entropy: bytes) -> bytes:
        self.protect_calls.append((plaintext, entropy))
        ciphertext = f"ciphertext-{len(self.protect_calls)}".encode("ascii")
        self._values[ciphertext] = plaintext
        return ciphertext

    def unprotect(self, ciphertext: bytes, *, entropy: bytes) -> bytes:
        self.unprotect_calls.append((ciphertext, entropy))
        try:
            return self._values[ciphertext]
        except KeyError:
            raise KiteSessionCacheError(
                "Kite session cache encryption operation failed"
            ) from None

    def install(self, ciphertext: bytes, plaintext: bytes) -> None:
        self._values[ciphertext] = plaintext


class KiteDailySessionCacheTests(unittest.TestCase):
    def test_default_path_is_outside_the_repository_under_local_app_data(
        self,
    ) -> None:
        result = default_kite_session_cache_path(
            {"LOCALAPPDATA": r"C:\Users\kamal\AppData\Local"}
        )

        self.assertEqual(
            result,
            Path(
                r"C:\Users\kamal\AppData\Local"
                r"\IndiaSwingTradingSystem\credentials\kite-session-v1.json"
            ),
        )

    def test_default_path_requires_local_app_data(self) -> None:
        with self.assertRaises(KiteSessionCacheError):
            default_kite_session_cache_path({})

    def test_round_trip_never_writes_plaintext_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            protector = RecordingProtector()
            cache = KiteDailySessionCache(path, protector=protector)

            expires_at = cache.save(
                KiteCredentials(API_KEY, ACCESS_TOKEN),
                authenticated_at=AUTHENTICATED_AT,
            )
            stored_bytes = path.read_bytes()
            loaded = cache.load(
                API_KEY,
                now=AUTHENTICATED_AT,
            )

        self.assertEqual(expires_at, _session_expiry(AUTHENTICATED_AT))
        self.assertEqual(loaded.api_key(), API_KEY)
        self.assertEqual(loaded.access_token(), ACCESS_TOKEN)
        self.assertNotIn(API_KEY.encode("utf-8"), stored_bytes)
        self.assertNotIn(ACCESS_TOKEN.encode("utf-8"), stored_bytes)
        self.assertNotIn(AUTHENTICATED_AT.isoformat().encode("ascii"), stored_bytes)
        self.assertEqual(
            protector.protect_calls[0][1],
            _entropy_for(API_KEY),
        )
        self.assertEqual(
            protector.unprotect_calls[0][1],
            _entropy_for(API_KEY),
        )

    def test_missing_cache_returns_none_without_calling_protector(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            protector = RecordingProtector()
            cache = KiteDailySessionCache(
                Path(temp_dir) / "missing.json",
                protector=protector,
            )

            result = cache.load(API_KEY, now=AUTHENTICATED_AT)

        self.assertIsNone(result)
        self.assertEqual(protector.unprotect_calls, [])

    def test_expiry_uses_next_ist_cutoff_with_five_minute_margin(self) -> None:
        result = _session_expiry(AUTHENTICATED_AT)

        self.assertEqual(
            result,
            datetime(2026, 7, 27, 0, 25, tzinfo=timezone.utc),
        )

    def test_expired_cache_is_removed_and_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            protector = RecordingProtector()
            cache = KiteDailySessionCache(path, protector=protector)
            expires_at = cache.save(
                KiteCredentials(API_KEY, ACCESS_TOKEN),
                authenticated_at=AUTHENTICATED_AT,
            )

            result = cache.load(API_KEY, now=expires_at)

            self.assertIsNone(result)
            self.assertFalse(path.exists())

    def test_one_microsecond_before_expiry_remains_usable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            protector = RecordingProtector()
            cache = KiteDailySessionCache(path, protector=protector)
            expires_at = cache.save(
                KiteCredentials(API_KEY, ACCESS_TOKEN),
                authenticated_at=AUTHENTICATED_AT,
            )

            result = cache.load(
                API_KEY,
                now=expires_at - timedelta(microseconds=1),
            )

        self.assertEqual(result.access_token(), ACCESS_TOKEN)

    def test_wrong_api_key_is_rejected_without_leaking_either_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            protector = RecordingProtector()
            cache = KiteDailySessionCache(path, protector=protector)
            cache.save(
                KiteCredentials(API_KEY, ACCESS_TOKEN),
                authenticated_at=AUTHENTICATED_AT,
            )
            ciphertext = next(iter(protector._values))
            protector.install(
                ciphertext,
                protector._values[ciphertext],
            )

            with self.assertRaises(KiteSessionCacheError) as raised:
                cache.load("different-api-key", now=AUTHENTICATED_AT)

        rendered = str(raised.exception)
        self.assertNotIn(API_KEY, rendered)
        self.assertNotIn("different-api-key", rendered)
        self.assertNotIn(ACCESS_TOKEN, rendered)

    def test_tampered_ciphertext_is_rejected_and_not_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            protector = RecordingProtector()
            cache = KiteDailySessionCache(path, protector=protector)
            path.write_text(
                json.dumps(
                    {
                        "schema_version": KITE_SESSION_CACHE_SCHEMA_VERSION,
                        "protected_payload": "dGFtcGVyZWQ=",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(KiteSessionCacheError):
                cache.load(API_KEY, now=AUTHENTICATED_AT)

            self.assertTrue(path.exists())

    def test_malformed_duplicate_or_extra_envelope_keys_are_rejected(self) -> None:
        cases = (
            b"not-json",
            b"[]",
            b'{"schema_version":1,"schema_version":1,"protected_payload":"YQ=="}',
            b'{"schema_version":1,"protected_payload":"YQ==","extra":1}',
            b'{"schema_version":2,"protected_payload":"YQ=="}',
            b'{"schema_version":1,"protected_payload":"%%%"}',
        )
        for encoded in cases:
            with self.subTest(encoded=encoded):
                with tempfile.TemporaryDirectory() as temp_dir:
                    path = Path(temp_dir) / "kite-session.json"
                    path.write_bytes(encoded)
                    cache = KiteDailySessionCache(
                        path,
                        protector=RecordingProtector(),
                    )
                    with self.assertRaises(KiteSessionCacheError):
                        cache.load(API_KEY, now=AUTHENTICATED_AT)

    def test_payload_expiry_cannot_be_extended(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            protector = RecordingProtector()
            ciphertext = b"fixed-ciphertext"
            protector.install(
                ciphertext,
                _canonical_json_bytes(
                    {
                        "schema_version": KITE_SESSION_CACHE_SCHEMA_VERSION,
                        "api_key_sha256": hashlib.sha256(
                            API_KEY.encode("utf-8")
                        ).hexdigest(),
                        "access_token": ACCESS_TOKEN,
                        "authenticated_at": AUTHENTICATED_AT.isoformat(),
                        "expires_at": datetime(
                            2099, 1, 1, tzinfo=timezone.utc
                        ).isoformat(),
                    }
                ),
            )
            path.write_text(
                json.dumps(
                    {
                        "schema_version": KITE_SESSION_CACHE_SCHEMA_VERSION,
                        "protected_payload": base64.b64encode(ciphertext).decode(
                            "ascii"
                        ),
                    }
                ),
                encoding="utf-8",
            )
            cache = KiteDailySessionCache(path, protector=protector)

            with self.assertRaises(KiteSessionCacheError):
                cache.load(API_KEY, now=AUTHENTICATED_AT)

    def test_future_authentication_time_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            protector = RecordingProtector()
            cache = KiteDailySessionCache(path, protector=protector)
            cache.save(
                KiteCredentials(API_KEY, ACCESS_TOKEN),
                authenticated_at=AUTHENTICATED_AT,
            )

            with self.assertRaises(KiteSessionCacheError):
                cache.load(
                    API_KEY,
                    now=datetime(2026, 7, 26, 13, 59, tzinfo=timezone.utc),
                )

    def test_clear_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            cache = KiteDailySessionCache(path, protector=RecordingProtector())
            path.write_bytes(b"value")

            cache.clear()
            cache.clear()

            self.assertFalse(path.exists())

    def test_non_windows_dpapi_call_fails_with_sanitized_error(self) -> None:
        protector = WindowsDPAPISecretProtector()
        with patch("india_swing.market_data.kite_session_cache.os.name", "posix"):
            with self.assertRaises(KiteSessionCacheError) as raised:
                protector.protect(b"secret-value", entropy=b"binding")

        self.assertNotIn("secret-value", str(raised.exception))

    @unittest.skipUnless(os.name == "nt", "native DPAPI is Windows-only")
    def test_native_dpapi_cache_round_trip_uses_current_windows_user(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "kite-session.json"
            cache = KiteDailySessionCache(path)

            cache.save(
                KiteCredentials(API_KEY, ACCESS_TOKEN),
                authenticated_at=AUTHENTICATED_AT,
            )
            stored_bytes = path.read_bytes()
            result = cache.load(API_KEY, now=AUTHENTICATED_AT)

        self.assertEqual(result.access_token(), ACCESS_TOKEN)
        self.assertNotIn(API_KEY.encode("utf-8"), stored_bytes)
        self.assertNotIn(ACCESS_TOKEN.encode("utf-8"), stored_bytes)


if __name__ == "__main__":
    unittest.main()
