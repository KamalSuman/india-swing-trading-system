from __future__ import annotations

import base64
import ctypes
import hashlib
import json
import os
from collections.abc import Callable, Mapping
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Protocol

from .config import KiteCredentials


KITE_SESSION_CACHE_SCHEMA_VERSION = 1
KITE_SESSION_CACHE_FILENAME = "kite-session-v1.json"
KITE_SESSION_EXPIRY_HOUR_IST = 6
KITE_SESSION_EXPIRY_MARGIN = timedelta(minutes=5)
_CACHE_DIRECTORY_NAME = "IndiaSwingTradingSystem"
_CACHE_PAYLOAD_KEYS = frozenset(
    {
        "schema_version",
        "api_key_sha256",
        "access_token",
        "authenticated_at",
        "expires_at",
    }
)
_CACHE_ENVELOPE_KEYS = frozenset({"schema_version", "protected_payload"})
_LOWERCASE_SHA256_LENGTH = 64
_IST = timezone(timedelta(hours=5, minutes=30), name="IST")
_DPAPI_ENTROPY_PREFIX = b"india-swing/kite-session-cache/v1/"
_CRYPTPROTECT_UI_FORBIDDEN = 0x1


class KiteSessionCacheError(ValueError):
    """A sanitized failure at the local encrypted-session boundary."""


class SecretProtector(Protocol):
    def protect(self, plaintext: bytes, *, entropy: bytes) -> bytes: ...

    def unprotect(self, ciphertext: bytes, *, entropy: bytes) -> bytes: ...


class _DataBlob(ctypes.Structure):
    _fields_ = [
        ("cbData", ctypes.c_ulong),
        ("pbData", ctypes.POINTER(ctypes.c_ubyte)),
    ]


class WindowsDPAPISecretProtector:
    """Encrypt secrets for the current Windows user via native DPAPI."""

    def protect(self, plaintext: bytes, *, entropy: bytes) -> bytes:
        return self._transform(plaintext, entropy=entropy, decrypt=False)

    def unprotect(self, ciphertext: bytes, *, entropy: bytes) -> bytes:
        return self._transform(ciphertext, entropy=entropy, decrypt=True)

    @staticmethod
    def _transform(value: bytes, *, entropy: bytes, decrypt: bool) -> bytes:
        if os.name != "nt":
            raise KiteSessionCacheError(
                "the encrypted Kite session cache requires Windows DPAPI"
            )
        if type(value) is not bytes or not value:
            raise KiteSessionCacheError("Kite session cache secret is invalid")
        if type(entropy) is not bytes or not entropy:
            raise KiteSessionCacheError("Kite session cache binding is invalid")

        value_buffer = ctypes.create_string_buffer(value)
        entropy_buffer = ctypes.create_string_buffer(entropy)
        input_blob = _DataBlob(
            len(value),
            ctypes.cast(value_buffer, ctypes.POINTER(ctypes.c_ubyte)),
        )
        entropy_blob = _DataBlob(
            len(entropy),
            ctypes.cast(entropy_buffer, ctypes.POINTER(ctypes.c_ubyte)),
        )
        output_blob = _DataBlob()

        try:
            crypt32 = ctypes.windll.crypt32
            kernel32 = ctypes.windll.kernel32
            kernel32.LocalFree.argtypes = [ctypes.c_void_p]
            kernel32.LocalFree.restype = ctypes.c_void_p
            if decrypt:
                operation = crypt32.CryptUnprotectData
                succeeded = operation(
                    ctypes.byref(input_blob),
                    None,
                    ctypes.byref(entropy_blob),
                    None,
                    None,
                    _CRYPTPROTECT_UI_FORBIDDEN,
                    ctypes.byref(output_blob),
                )
            else:
                operation = crypt32.CryptProtectData
                succeeded = operation(
                    ctypes.byref(input_blob),
                    None,
                    ctypes.byref(entropy_blob),
                    None,
                    None,
                    _CRYPTPROTECT_UI_FORBIDDEN,
                    ctypes.byref(output_blob),
                )
            if not succeeded or not output_blob.pbData or output_blob.cbData <= 0:
                raise KiteSessionCacheError(
                    "Kite session cache encryption operation failed"
                )
            try:
                return ctypes.string_at(output_blob.pbData, output_blob.cbData)
            finally:
                kernel32.LocalFree(output_blob.pbData)
        except KiteSessionCacheError:
            raise
        except Exception:
            raise KiteSessionCacheError(
                "Kite session cache encryption operation failed"
            ) from None


def default_kite_session_cache_path(
    environ: Mapping[str, str] | None = None,
) -> Path:
    values = os.environ if environ is None else environ
    local_app_data = values.get("LOCALAPPDATA", "").strip()
    if not local_app_data:
        raise KiteSessionCacheError(
            "LOCALAPPDATA is required for the encrypted Kite session cache"
        )
    return (
        Path(local_app_data)
        / _CACHE_DIRECTORY_NAME
        / "credentials"
        / KITE_SESSION_CACHE_FILENAME
    )


class KiteDailySessionCache:
    """Stores one API-key-bound access token outside the repository.

    The entire credential payload, including its timestamps and API-key
    binding, is authenticated and encrypted by DPAPI. Only a schema marker
    and opaque ciphertext are present in the on-disk JSON envelope.
    """

    def __init__(
        self,
        path: Path,
        *,
        protector: SecretProtector | None = None,
        replace: Callable[[Path, Path], None] = os.replace,
    ) -> None:
        if not isinstance(path, Path):
            raise TypeError("path must be a Path")
        self._path = path
        self._protector = protector or WindowsDPAPISecretProtector()
        self._replace = replace

    @property
    def path(self) -> Path:
        return self._path

    def load(
        self,
        api_key: str,
        *,
        now: datetime,
    ) -> KiteCredentials | None:
        _validate_api_key(api_key)
        now_utc = _aware_utc(now, field_name="now")
        if not self._path.exists():
            return None
        try:
            envelope_bytes = self._path.read_bytes()
        except OSError:
            raise KiteSessionCacheError("Kite session cache could not be read") from None
        envelope = _strict_json_object(envelope_bytes, "cache envelope")
        if frozenset(envelope) != _CACHE_ENVELOPE_KEYS:
            raise KiteSessionCacheError("Kite session cache envelope is invalid")
        if envelope.get("schema_version") != KITE_SESSION_CACHE_SCHEMA_VERSION:
            raise KiteSessionCacheError("Kite session cache schema is unsupported")
        protected_payload = envelope.get("protected_payload")
        if not isinstance(protected_payload, str) or not protected_payload:
            raise KiteSessionCacheError("Kite session cache envelope is invalid")
        try:
            ciphertext = base64.b64decode(protected_payload, validate=True)
        except (ValueError, TypeError):
            raise KiteSessionCacheError("Kite session cache envelope is invalid") from None
        if not ciphertext:
            raise KiteSessionCacheError("Kite session cache envelope is invalid")

        plaintext = self._protector.unprotect(
            ciphertext,
            entropy=_entropy_for(api_key),
        )
        payload = _strict_json_object(plaintext, "cache payload")
        credentials, authenticated_at, expires_at = _validated_payload(
            payload,
            api_key=api_key,
        )
        if authenticated_at > now_utc:
            raise KiteSessionCacheError("Kite session cache timestamps are invalid")
        if now_utc >= expires_at:
            self.clear()
            return None
        return credentials

    def save(
        self,
        credentials: KiteCredentials,
        *,
        authenticated_at: datetime,
    ) -> datetime:
        if type(credentials) is not KiteCredentials:
            raise TypeError("credentials must be exact KiteCredentials")
        authenticated_at_utc = _aware_utc(
            authenticated_at,
            field_name="authenticated_at",
        )
        expires_at = _session_expiry(authenticated_at_utc)
        payload = {
            "schema_version": KITE_SESSION_CACHE_SCHEMA_VERSION,
            "api_key_sha256": _api_key_sha256(credentials.api_key()),
            "access_token": credentials.access_token(),
            "authenticated_at": authenticated_at_utc.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        plaintext = _canonical_json_bytes(payload)
        ciphertext = self._protector.protect(
            plaintext,
            entropy=_entropy_for(credentials.api_key()),
        )
        if type(ciphertext) is not bytes or not ciphertext:
            raise KiteSessionCacheError(
                "Kite session cache encryption operation failed"
            )
        envelope = {
            "schema_version": KITE_SESSION_CACHE_SCHEMA_VERSION,
            "protected_payload": base64.b64encode(ciphertext).decode("ascii"),
        }
        encoded = _canonical_json_bytes(envelope)
        parent = self._path.parent
        temporary = parent / f".{self._path.name}.tmp"
        try:
            parent.mkdir(parents=True, exist_ok=True)
            temporary.write_bytes(encoded)
            self._replace(temporary, self._path)
        except OSError:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise KiteSessionCacheError(
                "Kite session cache could not be written"
            ) from None
        return expires_at

    def clear(self) -> None:
        try:
            self._path.unlink(missing_ok=True)
        except OSError:
            raise KiteSessionCacheError(
                "Kite session cache could not be cleared"
            ) from None


def _validated_payload(
    payload: dict[str, object],
    *,
    api_key: str,
) -> tuple[KiteCredentials, datetime, datetime]:
    if frozenset(payload) != _CACHE_PAYLOAD_KEYS:
        raise KiteSessionCacheError("Kite session cache payload is invalid")
    if payload.get("schema_version") != KITE_SESSION_CACHE_SCHEMA_VERSION:
        raise KiteSessionCacheError("Kite session cache schema is unsupported")
    expected_key_hash = _api_key_sha256(api_key)
    if payload.get("api_key_sha256") != expected_key_hash:
        raise KiteSessionCacheError(
            "Kite session cache does not match the configured API key"
        )
    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token.strip():
        raise KiteSessionCacheError("Kite session cache payload is invalid")
    authenticated_at = _parse_aware_utc(
        payload.get("authenticated_at"),
        field_name="authenticated_at",
    )
    expires_at = _parse_aware_utc(
        payload.get("expires_at"),
        field_name="expires_at",
    )
    if expires_at != _session_expiry(authenticated_at):
        raise KiteSessionCacheError("Kite session cache timestamps are invalid")
    return KiteCredentials(api_key, access_token), authenticated_at, expires_at


def _session_expiry(authenticated_at: datetime) -> datetime:
    authenticated_at_utc = _aware_utc(
        authenticated_at,
        field_name="authenticated_at",
    )
    local_date = authenticated_at_utc.astimezone(_IST).date()
    cutoff_date = date.fromordinal(local_date.toordinal() + 1)
    cutoff = datetime.combine(
        cutoff_date,
        time(hour=KITE_SESSION_EXPIRY_HOUR_IST),
        tzinfo=_IST,
    )
    return (cutoff - KITE_SESSION_EXPIRY_MARGIN).astimezone(timezone.utc)


def _parse_aware_utc(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise KiteSessionCacheError("Kite session cache timestamps are invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise KiteSessionCacheError("Kite session cache timestamps are invalid") from None
    return _aware_utc(parsed, field_name=field_name)


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise KiteSessionCacheError(
            f"Kite session cache {field_name} must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def _validate_api_key(api_key: str) -> None:
    if not isinstance(api_key, str) or not api_key.strip():
        raise KiteSessionCacheError("Kite session cache API key is invalid")


def _api_key_sha256(api_key: str) -> str:
    _validate_api_key(api_key)
    digest = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    if len(digest) != _LOWERCASE_SHA256_LENGTH:
        raise AssertionError("unexpected SHA-256 length")
    return digest


def _entropy_for(api_key: str) -> bytes:
    return _DPAPI_ENTROPY_PREFIX + _api_key_sha256(api_key).encode("ascii")


def _strict_json_object(value: bytes, label: str) -> dict[str, object]:
    if type(value) is not bytes or not value:
        raise KiteSessionCacheError(f"Kite session {label} is invalid")

    def pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise KiteSessionCacheError(f"Kite session {label} is invalid")
            result[key] = item
        return result

    try:
        parsed = json.loads(
            value.decode("utf-8"),
            object_pairs_hook=pairs_hook,
            parse_float=lambda _: _reject_json_number(label),
            parse_constant=lambda _: _reject_json_number(label),
        )
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise KiteSessionCacheError(f"Kite session {label} is invalid") from None
    if type(parsed) is not dict:
        raise KiteSessionCacheError(f"Kite session {label} is invalid")
    return parsed


def _reject_json_number(label: str) -> object:
    raise KiteSessionCacheError(f"Kite session {label} is invalid")


def _canonical_json_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
