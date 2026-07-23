from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id

from .models import NSE_EQUITY_ISIN_PATTERN, SHA256_IDENTIFIER
from .upstox import (
    UpstoxHttpResponse,
    UpstoxHttpTransport,
    UrllibUpstoxHttpTransport,
)


UPSTOX_NSE_INSTRUMENTS_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
)
UPSTOX_INSTRUMENT_CATALOG_SCHEMA_VERSION = "upstox-nse-instrument-catalog/v1"
UPSTOX_INSTRUMENT_CATALOG_POLICY_VERSION = (
    "upstox-nse-eq-current-routing-evidence/v1"
)
UPSTOX_INSTRUMENT_CATALOG_DATASET = "upstox-nse-instrument-catalog"
UPSTOX_INSTRUMENT_CATALOG_CODEC_VERSION = "upstox-nse-instrument-json/v1"

MAXIMUM_UPSTOX_INSTRUMENT_COMPRESSED_BYTES = 64 * 1024 * 1024
MAXIMUM_UPSTOX_INSTRUMENT_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
UPSTOX_INSTRUMENT_HTTP_TIMEOUT_SECONDS = 60.0

RAW_FILENAME = "NSE.json.gz"
NORMALIZED_FILENAME = "catalog.json"
_INSTRUMENT_TYPE = re.compile(r"[A-Z0-9]{1,8}\Z")


class UpstoxInstrumentCatalogError(ValueError):
    pass


class UpstoxInstrumentCatalogIntegrityError(UpstoxInstrumentCatalogError):
    pass


class UpstoxInstrumentCatalogNotFound(UpstoxInstrumentCatalogError):
    pass


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be an exact datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: object, field_name: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 128
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"{field_name} must be bounded canonical text")
    return value


def _positive_integer(value: object, field_name: str) -> int:
    if type(value) is int:
        parsed = value
    elif type(value) is Decimal and value == value.to_integral_value():
        parsed = int(value)
    else:
        raise ValueError(f"{field_name} must be an exact integer")
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _positive_decimal(value: object, field_name: str) -> Decimal:
    if type(value) is int:
        parsed = Decimal(value)
    elif type(value) is Decimal:
        parsed = value
    else:
        raise ValueError(f"{field_name} must be an exact decimal")
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return parsed


def _exchange_token(value: object) -> str:
    if type(value) is int and value > 0:
        return str(value)
    if type(value) is str:
        return _text(value, "exchange_token")
    raise ValueError("exchange_token must be exact text or a positive integer")


@dataclass(frozen=True, slots=True)
class UpstoxNseInstrument:
    instrument_key: str
    exchange_token: str
    trading_symbol: str
    name: str
    isin: str
    instrument_type: str
    lot_size: int
    tick_size: Decimal
    security_type: str | None = None
    segment: str = "NSE_EQ"
    exchange: str = "NSE"
    instrument_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.segment != "NSE_EQ" or self.exchange != "NSE":
            raise ValueError("instrument must belong to the NSE_EQ segment")
        if (
            type(self.isin) is not str
            or NSE_EQUITY_ISIN_PATTERN.fullmatch(self.isin) is None
        ):
            raise ValueError("instrument ISIN must identify an Indian equity")
        if self.instrument_key != f"NSE_EQ|{self.isin}":
            raise ValueError("instrument key disagrees with its ISIN")
        _text(self.exchange_token, "exchange_token")
        _text(self.trading_symbol, "trading_symbol")
        _text(self.name, "instrument name")
        if (
            type(self.instrument_type) is not str
            or _INSTRUMENT_TYPE.fullmatch(self.instrument_type) is None
        ):
            raise ValueError("instrument_type must be canonical uppercase text")
        if type(self.lot_size) is not int or self.lot_size <= 0:
            raise ValueError("lot_size must be a positive exact integer")
        if (
            type(self.tick_size) is not Decimal
            or not self.tick_size.is_finite()
            or self.tick_size <= 0
        ):
            raise ValueError("tick_size must be a positive finite Decimal")
        if self.security_type is not None:
            _text(self.security_type, "security_type")
        object.__setattr__(self, "instrument_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "instrument_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.instrument_id != self._calculated_id():
            raise UpstoxInstrumentCatalogIntegrityError(
                "Upstox instrument identity failed"
            )


def _instrument_sort_key(value: UpstoxNseInstrument) -> tuple[str, ...]:
    return (
        value.isin,
        value.instrument_type,
        value.trading_symbol,
        value.instrument_key,
        value.exchange_token,
        value.instrument_id,
    )


@dataclass(frozen=True, slots=True)
class UpstoxNseInstrumentCatalog:
    observed_at: datetime
    source_url: str
    raw_sha256: str
    compressed_byte_count: int
    uncompressed_sha256: str
    uncompressed_byte_count: int
    source_row_count: int
    instruments: tuple[UpstoxNseInstrument, ...]
    actionable: bool = False
    schema_version: str = UPSTOX_INSTRUMENT_CATALOG_SCHEMA_VERSION
    policy_version: str = UPSTOX_INSTRUMENT_CATALOG_POLICY_VERSION
    catalog_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "observed_at",
            _aware_utc(self.observed_at, "catalog observed_at"),
        )
        if self.source_url != UPSTOX_NSE_INSTRUMENTS_URL:
            raise ValueError("catalog source URL is unsupported")
        for value, name in (
            (self.raw_sha256, "raw_sha256"),
            (self.uncompressed_sha256, "uncompressed_sha256"),
        ):
            if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
                raise ValueError(f"{name} must be lowercase SHA-256")
        for value, name in (
            (self.compressed_byte_count, "compressed_byte_count"),
            (self.uncompressed_byte_count, "uncompressed_byte_count"),
            (self.source_row_count, "source_row_count"),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive exact integer")
        if (
            self.compressed_byte_count > MAXIMUM_UPSTOX_INSTRUMENT_COMPRESSED_BYTES
            or self.uncompressed_byte_count
            > MAXIMUM_UPSTOX_INSTRUMENT_UNCOMPRESSED_BYTES
        ):
            raise ValueError("catalog size exceeds its contract")
        if type(self.instruments) is not tuple or not self.instruments or any(
            type(value) is not UpstoxNseInstrument for value in self.instruments
        ):
            raise TypeError("catalog instruments must be a non-empty exact tuple")
        if self.instruments != tuple(
            sorted(self.instruments, key=_instrument_sort_key)
        ):
            raise ValueError("catalog instruments must be deterministically sorted")
        for value in self.instruments:
            value.verify_content_identity()
        if len({value.instrument_id for value in self.instruments}) != len(
            self.instruments
        ):
            raise ValueError("catalog contains duplicate normalized instruments")
        listing_lanes = {
            (
                value.isin,
                value.instrument_type,
                value.trading_symbol,
            )
            for value in self.instruments
        }
        if len(listing_lanes) != len(self.instruments):
            raise ValueError("catalog contains an ambiguous normalized listing lane")
        if self.actionable is not False:
            raise ValueError("instrument catalog cannot authorize trading")
        if (
            self.schema_version != UPSTOX_INSTRUMENT_CATALOG_SCHEMA_VERSION
            or self.policy_version != UPSTOX_INSTRUMENT_CATALOG_POLICY_VERSION
        ):
            raise ValueError("unsupported instrument catalog contract")
        object.__setattr__(self, "catalog_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "catalog_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.instruments:
            value.verify_content_identity()
        if self.catalog_id != self._calculated_id():
            raise UpstoxInstrumentCatalogIntegrityError(
                "Upstox instrument catalog identity failed"
            )


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _reject_constant(_: str) -> None:
    raise ValueError("non-finite JSON number")


def _decompress(raw_bytes: bytes) -> bytes:
    if type(raw_bytes) is not bytes or not raw_bytes:
        raise UpstoxInstrumentCatalogError(
            "instrument catalog source must be non-empty bytes"
        )
    if len(raw_bytes) > MAXIMUM_UPSTOX_INSTRUMENT_COMPRESSED_BYTES:
        raise UpstoxInstrumentCatalogError(
            "instrument catalog source exceeds the compressed size limit"
        )
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes), mode="rb") as handle:
            value = handle.read(MAXIMUM_UPSTOX_INSTRUMENT_UNCOMPRESSED_BYTES + 1)
    except (EOFError, OSError):
        raise UpstoxInstrumentCatalogError(
            "instrument catalog source is not valid gzip"
        ) from None
    if not value or len(value) > MAXIMUM_UPSTOX_INSTRUMENT_UNCOMPRESSED_BYTES:
        raise UpstoxInstrumentCatalogError(
            "instrument catalog uncompressed content is invalid or oversized"
        )
    return value


def parse_upstox_nse_instrument_catalog(
    raw_bytes: bytes,
    *,
    observed_at: datetime,
    source_url: str = UPSTOX_NSE_INSTRUMENTS_URL,
) -> UpstoxNseInstrumentCatalog:
    uncompressed = _decompress(raw_bytes)
    try:
        rows = json.loads(
            uncompressed.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=Decimal,
            parse_int=int,
            parse_constant=_reject_constant,
        )
        if type(rows) is not list or not rows:
            raise ValueError("catalog root must be a non-empty list")
        instruments: list[UpstoxNseInstrument] = []
        for row in rows:
            if type(row) is not dict:
                raise ValueError("catalog row must be an object")
            if row.get("segment") != "NSE_EQ":
                continue
            if (
                type(row.get("isin")) is not str
                or NSE_EQUITY_ISIN_PATTERN.fullmatch(row["isin"]) is None
            ):
                continue
            required = {
                "segment",
                "exchange",
                "isin",
                "instrument_type",
                "instrument_key",
                "lot_size",
                "exchange_token",
                "tick_size",
                "trading_symbol",
                "name",
            }
            if not required.issubset(row):
                raise ValueError("NSE_EQ catalog row is missing required fields")
            security_type = row.get("security_type")
            instruments.append(
                UpstoxNseInstrument(
                    instrument_key=_text(
                        row["instrument_key"],
                        "instrument_key",
                    ),
                    exchange_token=_exchange_token(row["exchange_token"]),
                    trading_symbol=_text(
                        row["trading_symbol"],
                        "trading_symbol",
                    ),
                    name=_text(row["name"], "instrument name"),
                    isin=row["isin"],
                    instrument_type=row["instrument_type"],
                    lot_size=_positive_integer(row["lot_size"], "lot_size"),
                    tick_size=_positive_decimal(row["tick_size"], "tick_size"),
                    security_type=(
                        None
                        if security_type is None
                        else _text(security_type, "security_type")
                    ),
                    segment=row["segment"],
                    exchange=row["exchange"],
                )
            )
    except UpstoxInstrumentCatalogError:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise UpstoxInstrumentCatalogError(
            "instrument catalog content is malformed"
        ) from None
    if not instruments:
        raise UpstoxInstrumentCatalogError(
            "instrument catalog contains no NSE_EQ instruments"
        )
    return UpstoxNseInstrumentCatalog(
        observed_at=observed_at,
        source_url=source_url,
        raw_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        compressed_byte_count=len(raw_bytes),
        uncompressed_sha256=hashlib.sha256(uncompressed).hexdigest(),
        uncompressed_byte_count=len(uncompressed),
        source_row_count=len(rows),
        instruments=tuple(sorted(instruments, key=_instrument_sort_key)),
    )


def _catalog_value(catalog: UpstoxNseInstrumentCatalog) -> dict[str, object]:
    return {
        "codec_version": UPSTOX_INSTRUMENT_CATALOG_CODEC_VERSION,
        "catalog_id": catalog.catalog_id,
        "schema_version": catalog.schema_version,
        "policy_version": catalog.policy_version,
        "observed_at": catalog.observed_at.isoformat(),
        "source_url": catalog.source_url,
        "raw_sha256": catalog.raw_sha256,
        "compressed_byte_count": catalog.compressed_byte_count,
        "uncompressed_sha256": catalog.uncompressed_sha256,
        "uncompressed_byte_count": catalog.uncompressed_byte_count,
        "source_row_count": catalog.source_row_count,
        "actionable": catalog.actionable,
        "instruments": [
            {
                "instrument_id": value.instrument_id,
                "instrument_key": value.instrument_key,
                "exchange_token": value.exchange_token,
                "trading_symbol": value.trading_symbol,
                "name": value.name,
                "isin": value.isin,
                "instrument_type": value.instrument_type,
                "lot_size": value.lot_size,
                "tick_size": str(value.tick_size),
                "security_type": value.security_type,
                "segment": value.segment,
                "exchange": value.exchange,
            }
            for value in catalog.instruments
        ],
    }


def encode_upstox_nse_instrument_catalog(
    catalog: UpstoxNseInstrumentCatalog,
) -> bytes:
    if type(catalog) is not UpstoxNseInstrumentCatalog:
        raise TypeError("catalog must be an exact UpstoxNseInstrumentCatalog")
    catalog.verify_content_identity()
    return (
        json.dumps(
            _catalog_value(catalog),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def decode_upstox_nse_instrument_catalog(
    payload: bytes,
) -> UpstoxNseInstrumentCatalog:
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
        expected_root = {
            "codec_version",
            "catalog_id",
            "schema_version",
            "policy_version",
            "observed_at",
            "source_url",
            "raw_sha256",
            "compressed_byte_count",
            "uncompressed_sha256",
            "uncompressed_byte_count",
            "source_row_count",
            "actionable",
            "instruments",
        }
        if type(root) is not dict or set(root) != expected_root:
            raise ValueError
        if root["codec_version"] != UPSTOX_INSTRUMENT_CATALOG_CODEC_VERSION:
            raise ValueError
        expected_instrument = {
            "instrument_id",
            "instrument_key",
            "exchange_token",
            "trading_symbol",
            "name",
            "isin",
            "instrument_type",
            "lot_size",
            "tick_size",
            "security_type",
            "segment",
            "exchange",
        }
        values = root["instruments"]
        if type(values) is not list:
            raise ValueError
        instruments: list[UpstoxNseInstrument] = []
        claimed_ids: list[str] = []
        for value in values:
            if type(value) is not dict or set(value) != expected_instrument:
                raise ValueError
            claimed_ids.append(value["instrument_id"])
            instruments.append(
                UpstoxNseInstrument(
                    instrument_key=value["instrument_key"],
                    exchange_token=value["exchange_token"],
                    trading_symbol=value["trading_symbol"],
                    name=value["name"],
                    isin=value["isin"],
                    instrument_type=value["instrument_type"],
                    lot_size=value["lot_size"],
                    tick_size=Decimal(value["tick_size"]),
                    security_type=value["security_type"],
                    segment=value["segment"],
                    exchange=value["exchange"],
                )
            )
        catalog = UpstoxNseInstrumentCatalog(
            observed_at=datetime.fromisoformat(root["observed_at"]),
            source_url=root["source_url"],
            raw_sha256=root["raw_sha256"],
            compressed_byte_count=root["compressed_byte_count"],
            uncompressed_sha256=root["uncompressed_sha256"],
            uncompressed_byte_count=root["uncompressed_byte_count"],
            source_row_count=root["source_row_count"],
            instruments=tuple(instruments),
            actionable=root["actionable"],
            schema_version=root["schema_version"],
            policy_version=root["policy_version"],
        )
        if (
            claimed_ids != [value.instrument_id for value in catalog.instruments]
            or root["catalog_id"] != catalog.catalog_id
        ):
            raise ValueError
        return catalog
    except UpstoxInstrumentCatalogIntegrityError:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise UpstoxInstrumentCatalogIntegrityError(
            "instrument catalog normalized payload is invalid"
        ) from None


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _write_fsynced(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


class LocalUpstoxInstrumentCatalogStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / UPSTOX_INSTRUMENT_CATALOG_DATASET

    def put(
        self,
        raw_bytes: bytes,
        catalog: UpstoxNseInstrumentCatalog,
    ) -> UpstoxNseInstrumentCatalog:
        if type(raw_bytes) is not bytes:
            raise TypeError("raw_bytes must be exact bytes")
        if type(catalog) is not UpstoxNseInstrumentCatalog:
            raise TypeError("catalog must be exact")
        catalog.verify_content_identity()
        if (
            len(raw_bytes) != catalog.compressed_byte_count
            or hashlib.sha256(raw_bytes).hexdigest() != catalog.raw_sha256
        ):
            raise UpstoxInstrumentCatalogIntegrityError(
                "raw catalog content disagrees with normalized lineage"
            )
        replayed = parse_upstox_nse_instrument_catalog(
            raw_bytes,
            observed_at=catalog.observed_at,
            source_url=catalog.source_url,
        )
        if replayed != catalog:
            raise UpstoxInstrumentCatalogIntegrityError(
                "catalog does not replay from its raw source"
            )
        payload = encode_upstox_nse_instrument_catalog(catalog)
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        target = self.dataset_root / catalog.catalog_id
        lock = self.dataset_root / ".catalog.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing = self._read_path(target)
                    if existing != catalog:
                        raise UpstoxInstrumentCatalogIntegrityError(
                            "catalog ID already stores different content"
                        )
                    return existing
                temporary = Path(
                    tempfile.mkdtemp(prefix=".upstox-catalog-", dir=self.dataset_root)
                )
                try:
                    _write_fsynced(temporary / RAW_FILENAME, raw_bytes)
                    _write_fsynced(temporary / NORMALIZED_FILENAME, payload)
                    os.replace(temporary, target)
                except Exception:
                    shutil.rmtree(temporary, ignore_errors=True)
                    raise
        except (FileLockUnavailable, FileSafetyError):
            raise UpstoxInstrumentCatalogIntegrityError(
                "instrument catalog store is unavailable"
            ) from None
        return self._read_path(target)

    def get(self, catalog_id: str) -> UpstoxNseInstrumentCatalog:
        if type(catalog_id) is not str or SHA256_IDENTIFIER.fullmatch(
            catalog_id
        ) is None:
            raise ValueError("catalog_id must be lowercase SHA-256")
        target = self.dataset_root / catalog_id
        if not target.exists():
            raise UpstoxInstrumentCatalogNotFound(
                "instrument catalog was not found"
            )
        return self._read_path(target)

    def _read_path(self, target: Path) -> UpstoxNseInstrumentCatalog:
        try:
            if not target.is_dir() or _is_link_like(target):
                raise UpstoxInstrumentCatalogIntegrityError(
                    "catalog path must be a regular directory"
                )
            children = tuple(target.iterdir())
            if {value.name for value in children} != {
                RAW_FILENAME,
                NORMALIZED_FILENAME,
            } or any(_is_link_like(value) or not value.is_file() for value in children):
                raise UpstoxInstrumentCatalogIntegrityError(
                    "catalog directory contents are invalid"
                )
            raw_bytes = read_stable_regular_file(
                target / RAW_FILENAME,
                maximum_bytes=MAXIMUM_UPSTOX_INSTRUMENT_COMPRESSED_BYTES,
            )
            payload = read_stable_regular_file(
                target / NORMALIZED_FILENAME,
                maximum_bytes=MAXIMUM_UPSTOX_INSTRUMENT_UNCOMPRESSED_BYTES,
            )
            catalog = decode_upstox_nse_instrument_catalog(payload)
            if payload != encode_upstox_nse_instrument_catalog(catalog):
                raise UpstoxInstrumentCatalogIntegrityError(
                    "catalog normalized payload is not canonical"
                )
            if target.name != catalog.catalog_id:
                raise UpstoxInstrumentCatalogIntegrityError(
                    "catalog path disagrees with its identity"
                )
            replayed = parse_upstox_nse_instrument_catalog(
                raw_bytes,
                observed_at=catalog.observed_at,
                source_url=catalog.source_url,
            )
            if replayed != catalog:
                raise UpstoxInstrumentCatalogIntegrityError(
                    "catalog failed raw-source replay"
                )
            return catalog
        except UpstoxInstrumentCatalogIntegrityError:
            raise
        except (FileSafetyError, OSError):
            raise UpstoxInstrumentCatalogIntegrityError(
                "catalog could not be read safely"
            ) from None


def import_upstox_nse_instrument_catalog(
    source: Path,
    *,
    observed_at: datetime,
    store: LocalUpstoxInstrumentCatalogStore,
) -> UpstoxNseInstrumentCatalog:
    try:
        raw_bytes = read_stable_regular_file(
            Path(source),
            maximum_bytes=MAXIMUM_UPSTOX_INSTRUMENT_COMPRESSED_BYTES,
        )
    except FileSafetyError:
        raise UpstoxInstrumentCatalogError(
            "instrument catalog source could not be read safely"
        ) from None
    catalog = parse_upstox_nse_instrument_catalog(
        raw_bytes,
        observed_at=observed_at,
    )
    return store.put(raw_bytes, catalog)


def fetch_upstox_nse_instrument_catalog(
    *,
    store: LocalUpstoxInstrumentCatalogStore,
    transport: UpstoxHttpTransport | None = None,
    clock: Callable[[], datetime] | None = None,
) -> UpstoxNseInstrumentCatalog:
    selected_transport = transport or UrllibUpstoxHttpTransport()
    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    try:
        response = selected_transport.get(
            UPSTOX_NSE_INSTRUMENTS_URL,
            headers={"Accept": "application/gzip, application/octet-stream"},
            timeout_seconds=UPSTOX_INSTRUMENT_HTTP_TIMEOUT_SECONDS,
            maximum_bytes=MAXIMUM_UPSTOX_INSTRUMENT_COMPRESSED_BYTES,
        )
    except Exception as exc:
        raise UpstoxInstrumentCatalogError(
            f"instrument catalog fetch failed ({type(exc).__name__})"
        ) from None
    if type(response) is not UpstoxHttpResponse:
        raise UpstoxInstrumentCatalogError(
            "instrument catalog transport returned an invalid response"
        )
    if response.status_code != 200:
        raise UpstoxInstrumentCatalogError(
            "instrument catalog fetch returned a non-success status"
        )
    if len(response.body) > MAXIMUM_UPSTOX_INSTRUMENT_COMPRESSED_BYTES:
        raise UpstoxInstrumentCatalogError(
            "instrument catalog fetch exceeded the size limit"
        )
    catalog = parse_upstox_nse_instrument_catalog(
        response.body,
        observed_at=selected_clock(),
    )
    return store.put(response.body, catalog)
