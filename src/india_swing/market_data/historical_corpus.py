"""Offline, content-addressed historical evaluation corpus.

This module never calls a provider, never lists or infers an artifact ID, and
never upgrades readiness. It converts one exact, already-sealed
``HistoricalDatasetAdmissionReport`` plus one exact, already-sealed
``HistoricalReconciliationIndex`` into session-partitioned, fully
lineage-bound COLLECTION_ONLY OHLCV evidence: a corpus is research evidence,
not proof that the underlying provider data was known at its historical
decision cutoff.

Every admitted admission entry's provider snapshot and reconciliation
snapshot are independently reloaded and re-verified from disk before a bar is
built; a blocked admission entry contributes no bar but is still preserved in
the sealed corpus index's accounting.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.identity import content_id

from .collection import historical_dataset_name
from .dataset_admission import (
    HistoricalDatasetAdmissionReport,
    LocalHistoricalDatasetAdmissionReportStore,
)
from .models import (
    HistoricalDailyCandleBatch,
    LISTING_KEY_PATTERN,
    MARKET_DATA_PROVIDER_PATTERN,
    NSE_EQUITY_ISIN_PATTERN,
    NSE_SECURITY_SERIES_PATTERN,
    SHA256_IDENTIFIER,
)
from .codec import (
    MARKET_PAYLOAD_CODEC_VERSION,
    encode_market_payload,
    market_payload_record_count,
)
from .reconciliation import (
    HISTORICAL_RECONCILIATION_DATASET,
    HISTORICAL_RECONCILIATION_PROVIDER,
    HistoricalCandleReconciliationReport,
)
from .reconciliation_run import (
    HistoricalReconciliationIndex,
    LocalHistoricalReconciliationIndexStore,
)
from .snapshot_store import (
    PAYLOAD_FILENAME,
    SNAPSHOT_SCHEMA_VERSION,
    LocalMarketSnapshotStore,
    MarketSnapshotManifest,
    StoredMarketSnapshot,
)


HISTORICAL_EVALUATION_CORPUS_SCHEMA_VERSION = "historical-evaluation-corpus/v1"
HISTORICAL_EVALUATION_CORPUS_POLICY_VERSION = (
    "historical-evaluation-corpus-policy/v1"
)
HISTORICAL_EVALUATION_CORPUS_CODEC_VERSION = (
    "historical-evaluation-corpus-json/v1"
)
HISTORICAL_EVALUATION_CORPUS_DATASET = "historical-evaluation-corpora"
INDEX_FILENAME = "index.json"
PARTITIONS_DIRNAME = "partitions"
MAXIMUM_SESSIONS_PER_CORPUS = 5000
MAXIMUM_BARS_PER_SESSION = 20000
MAXIMUM_CORPUS_PARTITION_BYTES = 32 * 1024 * 1024
MAXIMUM_CORPUS_INDEX_BYTES = 8 * 1024 * 1024

ZERO = Decimal("0")


class HistoricalEvaluationCorpusError(ValueError):
    """A corpus input or artifact failed a static safety rule."""


class HistoricalEvaluationCorpusIntegrityError(HistoricalEvaluationCorpusError):
    """Persisted or reloaded corpus evidence failed independent verification."""


def _sha256(value: object, field_name: str) -> None:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise HistoricalEvaluationCorpusError(f"{field_name} must be a lowercase SHA-256")


def _utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise HistoricalEvaluationCorpusError(f"{field_name} must be an exact datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise HistoricalEvaluationCorpusError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _expected_snapshot_id(manifest: MarketSnapshotManifest) -> str:
    """Recompute a snapshot ID from every identity-bound manifest field."""

    return content_id(
        {
            "schema_version": manifest.schema_version,
            "codec_version": manifest.codec_version,
            "dataset": manifest.dataset,
            "selection_key": manifest.selection_key,
            "provider": manifest.provider,
            "provider_version": manifest.provider_version,
            "observed_at": manifest.observed_at,
            "record_count": manifest.record_count,
            "payload_filename": manifest.payload_filename,
            "payload_sha256": manifest.payload_sha256,
        },
        length=64,
    )


@dataclass(frozen=True, slots=True)
class HistoricalEvaluationCorpusBar:
    """One immutable OHLCV bar bound to its exact provider/reconciliation lineage."""

    session: date
    listing_key: str
    series: str
    isin: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    provider: str
    request_id: str
    binding_id: str
    provider_snapshot_id: str
    historical_batch_id: str
    reconciliation_report_id: str
    reconciliation_snapshot_id: str
    observed_at: datetime
    bar_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(
            self, "observed_at", self.observed_at.astimezone(timezone.utc)
        )
        object.__setattr__(self, "bar_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.session) is not date:
            raise TypeError("corpus bar session must be an exact date")
        if (
            type(self.listing_key) is not str
            or LISTING_KEY_PATTERN.fullmatch(self.listing_key) is None
        ):
            raise ValueError(
                "corpus bar listing_key must be canonical NSE:TRADINGSYMBOL text"
            )
        if (
            type(self.series) is not str
            or NSE_SECURITY_SERIES_PATTERN.fullmatch(self.series) is None
        ):
            raise ValueError("corpus bar series must be canonical NSE series text")
        if (
            type(self.isin) is not str
            or NSE_EQUITY_ISIN_PATTERN.fullmatch(self.isin) is None
        ):
            raise ValueError("corpus bar isin must be a canonical Indian equity ISIN")
        for name in ("open", "high", "low", "close"):
            value = getattr(self, name)
            if type(value) is not Decimal or not value.is_finite() or value <= ZERO:
                raise ValueError(f"corpus bar {name} must be a positive finite Decimal")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("corpus bar high is inconsistent with OHLC")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("corpus bar low is inconsistent with OHLC")
        if type(self.volume) is not int or self.volume < 0:
            raise ValueError("corpus bar volume must be a non-negative exact integer")
        if (
            type(self.provider) is not str
            or MARKET_DATA_PROVIDER_PATTERN.fullmatch(self.provider) is None
        ):
            raise ValueError("corpus bar provider must be canonical uppercase provider text")
        for value, name in (
            (self.request_id, "request_id"),
            (self.binding_id, "binding_id"),
            (self.provider_snapshot_id, "provider_snapshot_id"),
            (self.historical_batch_id, "historical_batch_id"),
            (self.reconciliation_report_id, "reconciliation_report_id"),
            (self.reconciliation_snapshot_id, "reconciliation_snapshot_id"),
        ):
            if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
                raise ValueError(f"corpus bar {name} must be a lowercase SHA-256")
        if type(self.observed_at) is not datetime:
            raise TypeError("corpus bar observed_at must be an exact datetime")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("corpus bar observed_at must be timezone-aware")
        observed_session = self.observed_at.astimezone(INDIA_STANDARD_TIME).date()
        if self.session > observed_session:
            raise ValueError("corpus bar session cannot postdate its own observation")

    @property
    def listing_lane(self) -> tuple[str, str]:
        return (self.listing_key, self.series)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "historical-evaluation-corpus-bar/v1",
                "session": self.session,
                "listing_key": self.listing_key,
                "series": self.series,
                "isin": self.isin,
                "open": self.open,
                "high": self.high,
                "low": self.low,
                "close": self.close,
                "volume": self.volume,
                "provider": self.provider,
                "request_id": self.request_id,
                "binding_id": self.binding_id,
                "provider_snapshot_id": self.provider_snapshot_id,
                "historical_batch_id": self.historical_batch_id,
                "reconciliation_report_id": self.reconciliation_report_id,
                "reconciliation_snapshot_id": self.reconciliation_snapshot_id,
                "observed_at": self.observed_at,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.bar_id != self._calculated_id():
            raise HistoricalEvaluationCorpusIntegrityError("corpus bar identity failed")


@dataclass(frozen=True, slots=True)
class HistoricalEvaluationCorpusSessionPartition:
    """A cross-sectional, deduplicated set of bars for exactly one market session."""

    market_session: date
    bars: tuple[HistoricalEvaluationCorpusBar, ...]
    source_snapshot_ids: tuple[str, ...]
    source_report_ids: tuple[str, ...]
    collection_only: bool = True
    actionable: bool = False
    training_eligible: bool = False
    partition_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "partition_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.market_session) is not date:
            raise TypeError("corpus partition market_session must be an exact date")
        if (
            type(self.bars) is not tuple
            or not self.bars
            or len(self.bars) > MAXIMUM_BARS_PER_SESSION
            or any(type(value) is not HistoricalEvaluationCorpusBar for value in self.bars)
        ):
            raise TypeError(
                "corpus partition bars must be a non-empty bounded exact tuple"
            )
        for bar in self.bars:
            bar.verify_content_identity()
            if bar.session != self.market_session:
                raise ValueError("corpus bar belongs to another market session")
        if self.bars != tuple(sorted(self.bars, key=lambda value: value.listing_lane)):
            raise ValueError("corpus partition bars must be listing-lane ordered")
        lanes = [bar.listing_lane for bar in self.bars]
        if len(set(lanes)) != len(lanes):
            raise ValueError("corpus partition contains duplicate symbol/series lanes")
        bindings = [bar.binding_id for bar in self.bars]
        if len(set(bindings)) != len(bindings):
            raise ValueError("corpus partition contains duplicate bindings")
        requests = [bar.request_id for bar in self.bars]
        if len(set(requests)) != len(requests):
            raise ValueError(
                "corpus partition contains duplicate request/session evidence"
            )
        provider_snapshots = [bar.provider_snapshot_id for bar in self.bars]
        if len(set(provider_snapshots)) != len(provider_snapshots):
            raise ValueError(
                "corpus partition contains overlapping provider snapshot evidence"
            )
        reconciliation_snapshots = [bar.reconciliation_snapshot_id for bar in self.bars]
        if len(set(reconciliation_snapshots)) != len(reconciliation_snapshots):
            raise ValueError(
                "corpus partition contains overlapping reconciliation snapshot evidence"
            )
        expected_source_snapshot_ids = tuple(
            sorted({*provider_snapshots, *reconciliation_snapshots})
        )
        if type(self.source_snapshot_ids) is not tuple or tuple(
            self.source_snapshot_ids
        ) != expected_source_snapshot_ids:
            raise ValueError(
                "corpus partition source_snapshot_ids disagree with its bars"
            )
        expected_source_report_ids = tuple(
            sorted({bar.reconciliation_report_id for bar in self.bars})
        )
        if type(self.source_report_ids) is not tuple or tuple(
            self.source_report_ids
        ) != expected_source_report_ids:
            raise ValueError("corpus partition source_report_ids disagree with its bars")
        if self.collection_only is not True:
            raise ValueError("corpus partitions must remain collection-only")
        if self.actionable is not False:
            raise ValueError("corpus partitions cannot authorize trading")
        if self.training_eligible is not False:
            raise ValueError("corpus partitions cannot be training-eligible")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "historical-evaluation-corpus-partition/v1",
                "market_session": self.market_session,
                "bars": self.bars,
                "source_snapshot_ids": self.source_snapshot_ids,
                "source_report_ids": self.source_report_ids,
                "collection_only": self.collection_only,
                "actionable": self.actionable,
                "training_eligible": self.training_eligible,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for bar in self.bars:
            bar.verify_content_identity()
        self._validate()
        if self.partition_id != self._calculated_id():
            raise HistoricalEvaluationCorpusIntegrityError(
                "corpus partition identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalEvaluationCorpusIndex:
    """A sealed, content-addressed corpus binding every admission entry by ID."""

    admission_report_id: str
    reconciliation_index_id: str
    plan_id: str
    progress_id: str
    provider: str
    connector_version: str
    assessed_at: datetime
    built_at: datetime
    partition_ids: tuple[str, ...]
    partition_sessions: tuple[date, ...]
    all_entry_ids: tuple[str, ...]
    admitted_entry_ids: tuple[str, ...]
    blocked_entry_ids: tuple[str, ...]
    disposition_counts: tuple[tuple[str, int], ...]
    safe_requests_complete: bool
    coverage_complete: bool
    collection_only: bool = True
    actionable: bool = False
    training_eligible: bool = False
    schema_version: str = HISTORICAL_EVALUATION_CORPUS_SCHEMA_VERSION
    policy_version: str = HISTORICAL_EVALUATION_CORPUS_POLICY_VERSION
    codec_version: str = HISTORICAL_EVALUATION_CORPUS_CODEC_VERSION
    corpus_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(
            self, "assessed_at", _utc(self.assessed_at, "corpus index assessed_at")
        )
        object.__setattr__(
            self, "built_at", _utc(self.built_at, "corpus index built_at")
        )
        object.__setattr__(self, "corpus_id", self._calculated_id())

    def _validate(self) -> None:
        for value, name in (
            (self.admission_report_id, "admission_report_id"),
            (self.reconciliation_index_id, "reconciliation_index_id"),
            (self.plan_id, "plan_id"),
            (self.progress_id, "progress_id"),
        ):
            if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
                raise ValueError(f"corpus index {name} must be a lowercase SHA-256")
        if (
            type(self.provider) is not str
            or MARKET_DATA_PROVIDER_PATTERN.fullmatch(self.provider) is None
        ):
            raise ValueError(
                "corpus index provider must be canonical uppercase provider text"
            )
        if (
            type(self.connector_version) is not str
            or not self.connector_version
            or len(self.connector_version) > 128
        ):
            raise ValueError("corpus index connector_version must be bounded text")
        assessed_at = _utc(self.assessed_at, "corpus index assessed_at")
        built_at = _utc(self.built_at, "corpus index built_at")
        if built_at < assessed_at:
            raise ValueError("corpus index built_at cannot precede assessed_at")

        if type(self.partition_ids) is not tuple or any(
            type(value) is not str for value in self.partition_ids
        ):
            raise TypeError("corpus index partition_ids must be an exact tuple")
        if len(self.partition_ids) > MAXIMUM_SESSIONS_PER_CORPUS:
            raise ValueError(
                f"corpus index cannot exceed {MAXIMUM_SESSIONS_PER_CORPUS} sessions"
            )
        for value in self.partition_ids:
            if SHA256_IDENTIFIER.fullmatch(value) is None:
                raise ValueError("corpus index partition_ids must be lowercase SHA-256 values")
        if len(set(self.partition_ids)) != len(self.partition_ids):
            raise ValueError("corpus index partition_ids must be unique")
        if type(self.partition_sessions) is not tuple or any(
            type(value) is not date for value in self.partition_sessions
        ):
            raise TypeError("corpus index partition_sessions must be an exact date tuple")
        if len(self.partition_sessions) != len(self.partition_ids):
            raise ValueError("corpus index partition_sessions must align with partition_ids")
        if self.partition_sessions != tuple(sorted(self.partition_sessions)):
            raise ValueError("corpus index partition_sessions must be ascending")
        if len(set(self.partition_sessions)) != len(self.partition_sessions):
            raise ValueError("corpus index partition_sessions must be unique")

        for values, name in (
            (self.all_entry_ids, "all_entry_ids"),
            (self.admitted_entry_ids, "admitted_entry_ids"),
            (self.blocked_entry_ids, "blocked_entry_ids"),
        ):
            if type(values) is not tuple or any(type(value) is not str for value in values):
                raise TypeError(f"corpus index {name} must be an exact tuple")
            for value in values:
                if SHA256_IDENTIFIER.fullmatch(value) is None:
                    raise ValueError(
                        f"corpus index {name} must contain lowercase SHA-256 values"
                    )
            if values != tuple(sorted(values)) or len(set(values)) != len(values):
                raise ValueError(f"corpus index {name} must be sorted and unique")
        if not self.all_entry_ids:
            raise ValueError("corpus index requires at least one admission entry")
        admitted_set = set(self.admitted_entry_ids)
        blocked_set = set(self.blocked_entry_ids)
        if admitted_set & blocked_set:
            raise ValueError("corpus index admitted and blocked entry sets must be disjoint")
        if admitted_set | blocked_set != set(self.all_entry_ids):
            raise ValueError(
                "corpus index admitted/blocked entries must exhaust all_entry_ids"
            )

        if type(self.disposition_counts) is not tuple or any(
            type(value) is not tuple or len(value) != 2 for value in self.disposition_counts
        ):
            raise TypeError("corpus index disposition_counts must be an exact pair tuple")
        if tuple(sorted(self.disposition_counts)) != self.disposition_counts:
            raise ValueError("corpus index disposition_counts must be sorted")
        codes_seen = [value[0] for value in self.disposition_counts]
        if len(set(codes_seen)) != len(codes_seen):
            raise ValueError("corpus index disposition_counts must have unique codes")
        for code, count in self.disposition_counts:
            if type(code) is not str or not code:
                raise ValueError("corpus index disposition_counts code must be text")
            if type(count) is not int or count <= 0:
                raise ValueError("corpus index disposition_counts count must be positive")
        total_count = sum(count for _, count in self.disposition_counts)
        if total_count != len(self.all_entry_ids):
            raise ValueError("corpus index disposition_counts disagree with all_entry_ids")
        admitted_count = sum(
            count for code, count in self.disposition_counts if code == "ADMITTED"
        )
        if admitted_count != len(self.admitted_entry_ids):
            raise ValueError(
                "corpus index disposition_counts disagree with admitted_entry_ids"
            )

        if type(self.safe_requests_complete) is not bool:
            raise TypeError("safe_requests_complete must be bool")
        if type(self.coverage_complete) is not bool:
            raise TypeError("coverage_complete must be bool")
        if self.coverage_complete and not self.safe_requests_complete:
            raise ValueError(
                "corpus index coverage_complete requires safe_requests_complete"
            )
        if self.collection_only is not True:
            raise ValueError("corpus indexes must remain collection-only")
        if self.actionable is not False:
            raise ValueError("corpus indexes cannot authorize trading")
        if self.training_eligible is not False:
            raise ValueError("corpus indexes cannot be training-eligible")
        if (
            self.schema_version != HISTORICAL_EVALUATION_CORPUS_SCHEMA_VERSION
            or self.policy_version != HISTORICAL_EVALUATION_CORPUS_POLICY_VERSION
            or self.codec_version != HISTORICAL_EVALUATION_CORPUS_CODEC_VERSION
        ):
            raise ValueError("unsupported historical evaluation corpus contract")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "corpus_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.corpus_id != self._calculated_id():
            raise HistoricalEvaluationCorpusIntegrityError("corpus index identity failed")


# --- canonical JSON codec -------------------------------------------------

_EXPECTED_BAR_KEYS = {
    "bar_id",
    "session",
    "listing_key",
    "series",
    "isin",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "provider",
    "request_id",
    "binding_id",
    "provider_snapshot_id",
    "historical_batch_id",
    "reconciliation_report_id",
    "reconciliation_snapshot_id",
    "observed_at",
}

_EXPECTED_PARTITION_KEYS = {
    "partition_id",
    "market_session",
    "bars",
    "source_snapshot_ids",
    "source_report_ids",
    "collection_only",
    "actionable",
    "training_eligible",
}

_EXPECTED_INDEX_KEYS = {
    "schema_version",
    "policy_version",
    "codec_version",
    "corpus_id",
    "admission_report_id",
    "reconciliation_index_id",
    "plan_id",
    "progress_id",
    "provider",
    "connector_version",
    "assessed_at",
    "built_at",
    "partition_ids",
    "partition_sessions",
    "all_entry_ids",
    "admitted_entry_ids",
    "blocked_entry_ids",
    "disposition_counts",
    "safe_requests_complete",
    "coverage_complete",
    "collection_only",
    "actionable",
    "training_eligible",
}


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical evaluation corpus artifact contains duplicate JSON keys"
            )
        value[key] = item
    return value


def _canonical_json(value: dict) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _loads_no_floats(payload: bytes) -> object:
    try:
        if type(payload) is not bytes or not payload:
            raise ValueError
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
    except HistoricalEvaluationCorpusIntegrityError:
        raise
    except (AttributeError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise HistoricalEvaluationCorpusIntegrityError(
            "stored historical evaluation corpus artifact is invalid"
        ) from None


def _bar_value(bar: HistoricalEvaluationCorpusBar) -> dict[str, object]:
    return {
        "bar_id": bar.bar_id,
        "session": bar.session.isoformat(),
        "listing_key": bar.listing_key,
        "series": bar.series,
        "isin": bar.isin,
        "open": str(bar.open),
        "high": str(bar.high),
        "low": str(bar.low),
        "close": str(bar.close),
        "volume": bar.volume,
        "provider": bar.provider,
        "request_id": bar.request_id,
        "binding_id": bar.binding_id,
        "provider_snapshot_id": bar.provider_snapshot_id,
        "historical_batch_id": bar.historical_batch_id,
        "reconciliation_report_id": bar.reconciliation_report_id,
        "reconciliation_snapshot_id": bar.reconciliation_snapshot_id,
        "observed_at": bar.observed_at.isoformat(),
    }


def _bar_from_value(value: object) -> HistoricalEvaluationCorpusBar:
    if type(value) is not dict or set(value) != _EXPECTED_BAR_KEYS:
        raise ValueError("invalid encoded corpus bar")
    try:
        bar = HistoricalEvaluationCorpusBar(
            session=date.fromisoformat(value["session"]),
            listing_key=value["listing_key"],
            series=value["series"],
            isin=value["isin"],
            open=Decimal(value["open"]),
            high=Decimal(value["high"]),
            low=Decimal(value["low"]),
            close=Decimal(value["close"]),
            volume=value["volume"],
            provider=value["provider"],
            request_id=value["request_id"],
            binding_id=value["binding_id"],
            provider_snapshot_id=value["provider_snapshot_id"],
            historical_batch_id=value["historical_batch_id"],
            reconciliation_report_id=value["reconciliation_report_id"],
            reconciliation_snapshot_id=value["reconciliation_snapshot_id"],
            observed_at=datetime.fromisoformat(value["observed_at"]),
        )
    except InvalidOperation as exc:
        raise ValueError("invalid encoded corpus bar decimal") from exc
    if bar.bar_id != value["bar_id"]:
        raise ValueError("encoded corpus bar identity disagrees with its content")
    return bar


def encode_historical_evaluation_corpus_partition(
    partition: HistoricalEvaluationCorpusSessionPartition,
) -> bytes:
    if type(partition) is not HistoricalEvaluationCorpusSessionPartition:
        raise TypeError(
            "partition must be an exact HistoricalEvaluationCorpusSessionPartition"
        )
    partition.verify_content_identity()
    value = {
        "partition_id": partition.partition_id,
        "market_session": partition.market_session.isoformat(),
        "bars": [_bar_value(bar) for bar in partition.bars],
        "source_snapshot_ids": list(partition.source_snapshot_ids),
        "source_report_ids": list(partition.source_report_ids),
        "collection_only": partition.collection_only,
        "actionable": partition.actionable,
        "training_eligible": partition.training_eligible,
    }
    return _canonical_json(value)


def decode_historical_evaluation_corpus_partition(
    payload: bytes,
) -> HistoricalEvaluationCorpusSessionPartition:
    root = _loads_no_floats(payload)
    try:
        if len(payload) > MAXIMUM_CORPUS_PARTITION_BYTES:
            raise ValueError
        if type(root) is not dict or set(root) != _EXPECTED_PARTITION_KEYS:
            raise ValueError
        raw_bars = root["bars"]
        if type(raw_bars) is not list:
            raise ValueError
        bars = tuple(_bar_from_value(value) for value in raw_bars)
        raw_snapshot_ids = root["source_snapshot_ids"]
        raw_report_ids = root["source_report_ids"]
        if type(raw_snapshot_ids) is not list or type(raw_report_ids) is not list:
            raise ValueError
        partition = HistoricalEvaluationCorpusSessionPartition(
            market_session=date.fromisoformat(root["market_session"]),
            bars=bars,
            source_snapshot_ids=tuple(raw_snapshot_ids),
            source_report_ids=tuple(raw_report_ids),
            collection_only=root["collection_only"],
            actionable=root["actionable"],
            training_eligible=root["training_eligible"],
        )
        if root["partition_id"] != partition.partition_id:
            raise ValueError
        if payload != encode_historical_evaluation_corpus_partition(partition):
            raise ValueError
        return partition
    except HistoricalEvaluationCorpusIntegrityError:
        raise
    except (KeyError, TypeError, ValueError):
        raise HistoricalEvaluationCorpusIntegrityError(
            "stored historical evaluation corpus partition is invalid"
        ) from None


def encode_historical_evaluation_corpus_index(
    index: HistoricalEvaluationCorpusIndex,
) -> bytes:
    if type(index) is not HistoricalEvaluationCorpusIndex:
        raise TypeError("index must be an exact HistoricalEvaluationCorpusIndex")
    index.verify_content_identity()
    value = {
        "schema_version": index.schema_version,
        "policy_version": index.policy_version,
        "codec_version": index.codec_version,
        "corpus_id": index.corpus_id,
        "admission_report_id": index.admission_report_id,
        "reconciliation_index_id": index.reconciliation_index_id,
        "plan_id": index.plan_id,
        "progress_id": index.progress_id,
        "provider": index.provider,
        "connector_version": index.connector_version,
        "assessed_at": index.assessed_at.isoformat(),
        "built_at": index.built_at.isoformat(),
        "partition_ids": list(index.partition_ids),
        "partition_sessions": [value.isoformat() for value in index.partition_sessions],
        "all_entry_ids": list(index.all_entry_ids),
        "admitted_entry_ids": list(index.admitted_entry_ids),
        "blocked_entry_ids": list(index.blocked_entry_ids),
        "disposition_counts": [[code, count] for code, count in index.disposition_counts],
        "safe_requests_complete": index.safe_requests_complete,
        "coverage_complete": index.coverage_complete,
        "collection_only": index.collection_only,
        "actionable": index.actionable,
        "training_eligible": index.training_eligible,
    }
    return _canonical_json(value)


def decode_historical_evaluation_corpus_index(
    payload: bytes,
) -> HistoricalEvaluationCorpusIndex:
    root = _loads_no_floats(payload)
    try:
        if len(payload) > MAXIMUM_CORPUS_INDEX_BYTES:
            raise ValueError
        if type(root) is not dict or set(root) != _EXPECTED_INDEX_KEYS:
            raise ValueError
        raw_disposition_counts = root["disposition_counts"]
        if type(raw_disposition_counts) is not list:
            raise ValueError
        disposition_counts: list[tuple[str, int]] = []
        for entry in raw_disposition_counts:
            if type(entry) is not list or len(entry) != 2:
                raise ValueError
            disposition_counts.append((entry[0], entry[1]))
        raw_partition_ids = root["partition_ids"]
        raw_partition_sessions = root["partition_sessions"]
        if type(raw_partition_ids) is not list or type(raw_partition_sessions) is not list:
            raise ValueError
        for name in ("all_entry_ids", "admitted_entry_ids", "blocked_entry_ids"):
            if type(root[name]) is not list:
                raise ValueError
        index = HistoricalEvaluationCorpusIndex(
            admission_report_id=root["admission_report_id"],
            reconciliation_index_id=root["reconciliation_index_id"],
            plan_id=root["plan_id"],
            progress_id=root["progress_id"],
            provider=root["provider"],
            connector_version=root["connector_version"],
            assessed_at=datetime.fromisoformat(root["assessed_at"]),
            built_at=datetime.fromisoformat(root["built_at"]),
            partition_ids=tuple(raw_partition_ids),
            partition_sessions=tuple(
                date.fromisoformat(value) for value in raw_partition_sessions
            ),
            all_entry_ids=tuple(root["all_entry_ids"]),
            admitted_entry_ids=tuple(root["admitted_entry_ids"]),
            blocked_entry_ids=tuple(root["blocked_entry_ids"]),
            disposition_counts=tuple(disposition_counts),
            safe_requests_complete=root["safe_requests_complete"],
            coverage_complete=root["coverage_complete"],
            collection_only=root["collection_only"],
            actionable=root["actionable"],
            training_eligible=root["training_eligible"],
            schema_version=root["schema_version"],
            policy_version=root["policy_version"],
            codec_version=root["codec_version"],
        )
        if root["corpus_id"] != index.corpus_id:
            raise ValueError
        if payload != encode_historical_evaluation_corpus_index(index):
            raise ValueError
        return index
    except HistoricalEvaluationCorpusIntegrityError:
        raise
    except (KeyError, TypeError, ValueError):
        raise HistoricalEvaluationCorpusIntegrityError(
            "stored historical evaluation corpus index is invalid"
        ) from None


# --- local store -----------------------------------------------------------


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _write_fsynced(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


class LocalHistoricalEvaluationCorpusStore:
    """Create-once local corpus store; exposes only exact-ID get, never a listing."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / HISTORICAL_EVALUATION_CORPUS_DATASET

    def _verify_dataset_root_boundary(self) -> None:
        """Reject a linked/reparse root or dataset_root before any lock/target use.

        A link check must run before any resolve()-based comparison: resolve()
        itself follows symlinks/junctions, so it can never be the thing that
        proves the boundary is safe -- only that a boundary already found link-free
        is also not silently redirected by some other path-chain inconsistency.
        """

        root = self.root
        dataset_root = self.dataset_root
        if _is_link_like(root) or (root.exists() and not root.is_dir()):
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical evaluation corpus root is not a safe directory"
            )
        if _is_link_like(dataset_root) or (
            dataset_root.exists() and not dataset_root.is_dir()
        ):
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical evaluation corpus dataset root is not a safe directory"
            )
        try:
            resolved_root = root.resolve(strict=False)
            resolved_dataset_root = dataset_root.resolve(strict=False)
        except OSError:
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical evaluation corpus dataset root could not be verified"
            ) from None
        if resolved_dataset_root != resolved_root / HISTORICAL_EVALUATION_CORPUS_DATASET:
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical evaluation corpus dataset root is not a safe directory"
            )

    def put(
        self,
        index: HistoricalEvaluationCorpusIndex,
        partitions: tuple[HistoricalEvaluationCorpusSessionPartition, ...],
    ) -> HistoricalEvaluationCorpusIndex:
        if type(index) is not HistoricalEvaluationCorpusIndex:
            raise TypeError("index must be an exact HistoricalEvaluationCorpusIndex")
        index.verify_content_identity()
        if type(partitions) is not tuple or any(
            type(value) is not HistoricalEvaluationCorpusSessionPartition
            for value in partitions
        ):
            raise TypeError("partitions must be an exact immutable tuple")
        for value in partitions:
            value.verify_content_identity()
        if tuple(value.partition_id for value in partitions) != index.partition_ids:
            raise HistoricalEvaluationCorpusError(
                "partitions do not match the corpus index exactly"
            )
        if tuple(value.market_session for value in partitions) != index.partition_sessions:
            raise HistoricalEvaluationCorpusError(
                "partition sessions do not match the corpus index"
            )

        index_payload = encode_historical_evaluation_corpus_index(index)
        if len(index_payload) > MAXIMUM_CORPUS_INDEX_BYTES:
            raise HistoricalEvaluationCorpusError("corpus index exceeds its size limit")
        partition_payloads: dict[str, bytes] = {}
        for value in partitions:
            payload = encode_historical_evaluation_corpus_partition(value)
            if len(payload) > MAXIMUM_CORPUS_PARTITION_BYTES:
                raise HistoricalEvaluationCorpusError(
                    "corpus partition exceeds its size limit"
                )
            partition_payloads[value.partition_id] = payload

        self._verify_dataset_root_boundary()
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        self._verify_dataset_root_boundary()
        target = self.dataset_root / index.corpus_id
        lock = self.dataset_root / ".historical-evaluation-corpora.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing_index, existing_partitions = self._read_path(target)
                    if existing_index != index or existing_partitions != partitions:
                        raise HistoricalEvaluationCorpusIntegrityError(
                            "corpus ID already stores different content"
                        )
                    return existing_index
                temporary = Path(
                    tempfile.mkdtemp(
                        prefix=".historical-evaluation-corpus-",
                        dir=self.dataset_root,
                    )
                )
                try:
                    _write_fsynced(temporary / INDEX_FILENAME, index_payload)
                    partitions_dir = temporary / PARTITIONS_DIRNAME
                    partitions_dir.mkdir()
                    for partition_id, payload in partition_payloads.items():
                        _write_fsynced(partitions_dir / f"{partition_id}.json", payload)
                    os.replace(temporary, target)
                except Exception:
                    shutil.rmtree(temporary, ignore_errors=True)
                    raise
        except (FileLockUnavailable, FileSafetyError):
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical evaluation corpus store is unavailable"
            ) from None
        stored_index, _stored_partitions = self._read_path(target)
        return stored_index

    def get(
        self, corpus_id: str
    ) -> tuple[
        HistoricalEvaluationCorpusIndex,
        tuple[HistoricalEvaluationCorpusSessionPartition, ...],
    ]:
        if type(corpus_id) is not str or SHA256_IDENTIFIER.fullmatch(corpus_id) is None:
            raise HistoricalEvaluationCorpusError("corpus_id must be a lowercase SHA-256")
        self._verify_dataset_root_boundary()
        target = self.dataset_root / corpus_id
        if not target.exists():
            raise HistoricalEvaluationCorpusError(
                "historical evaluation corpus was not found"
            )
        index, partitions = self._read_path(target)
        if index.corpus_id != corpus_id:
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical evaluation corpus storage identity failed"
            )
        return index, partitions

    def _read_path(
        self, target: Path
    ) -> tuple[
        HistoricalEvaluationCorpusIndex,
        tuple[HistoricalEvaluationCorpusSessionPartition, ...],
    ]:
        try:
            if not target.is_dir() or _is_link_like(target):
                raise HistoricalEvaluationCorpusIntegrityError("corpus path is invalid")
            children = tuple(target.iterdir())
            if {value.name for value in children} != {INDEX_FILENAME, PARTITIONS_DIRNAME}:
                raise HistoricalEvaluationCorpusIntegrityError("corpus directory is invalid")
            index_path = target / INDEX_FILENAME
            partitions_path = target / PARTITIONS_DIRNAME
            if _is_link_like(index_path) or not index_path.is_file():
                raise HistoricalEvaluationCorpusIntegrityError("corpus index path is invalid")
            if _is_link_like(partitions_path) or not partitions_path.is_dir():
                raise HistoricalEvaluationCorpusIntegrityError(
                    "corpus partitions path is invalid"
                )
            index_payload = read_stable_regular_file(
                index_path, maximum_bytes=MAXIMUM_CORPUS_INDEX_BYTES
            )
            index = decode_historical_evaluation_corpus_index(index_payload)
            if (
                target.name != index.corpus_id
                or index_payload != encode_historical_evaluation_corpus_index(index)
            ):
                raise HistoricalEvaluationCorpusIntegrityError(
                    "corpus index storage identity failed"
                )

            partition_entries = tuple(partitions_path.iterdir())
            expected_names = {f"{value}.json" for value in index.partition_ids}
            if {value.name for value in partition_entries} != expected_names:
                raise HistoricalEvaluationCorpusIntegrityError(
                    "corpus partitions do not exactly match its index"
                )
            partitions_by_id: dict[str, HistoricalEvaluationCorpusSessionPartition] = {}
            for partition_id, session in zip(
                index.partition_ids, index.partition_sessions
            ):
                path = partitions_path / f"{partition_id}.json"
                if _is_link_like(path) or not path.is_file():
                    raise HistoricalEvaluationCorpusIntegrityError(
                        "corpus partition path is invalid"
                    )
                payload = read_stable_regular_file(
                    path, maximum_bytes=MAXIMUM_CORPUS_PARTITION_BYTES
                )
                partition = decode_historical_evaluation_corpus_partition(payload)
                if (
                    partition.partition_id != partition_id
                    or partition.market_session != session
                    or payload != encode_historical_evaluation_corpus_partition(partition)
                ):
                    raise HistoricalEvaluationCorpusIntegrityError(
                        "corpus partition storage identity failed"
                    )
                partitions_by_id[partition_id] = partition
            partitions = tuple(
                partitions_by_id[value] for value in index.partition_ids
            )
        except HistoricalEvaluationCorpusIntegrityError:
            raise
        except (FileSafetyError, OSError):
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical evaluation corpus could not be read safely"
            ) from None
        return index, partitions


# --- build service -----------------------------------------------------------


class HistoricalEvaluationCorpusService:
    """Builds one sealed corpus from an exact admission report and reconciliation index.

    Every store is caller-injected and therefore untrusted: the admitted
    provider snapshot and the admitted reconciliation snapshot are each
    independently reloaded and re-verified from their own manifest, canonical
    bytes, recomputed hash, and recomputed content identity before a bar is
    ever built.
    """

    def __init__(
        self,
        *,
        admission_store: LocalHistoricalDatasetAdmissionReportStore,
        reconciliation_index_store: LocalHistoricalReconciliationIndexStore,
        snapshot_store: LocalMarketSnapshotStore,
        corpus_store: LocalHistoricalEvaluationCorpusStore,
    ) -> None:
        self._admission_store = admission_store
        self._reconciliation_index_store = reconciliation_index_store
        self._snapshot_store = snapshot_store
        self._corpus_store = corpus_store

    def build(
        self,
        *,
        admission_report_id: str,
        reconciliation_index_id: str,
        built_at: datetime,
    ) -> HistoricalEvaluationCorpusIndex:
        _sha256(admission_report_id, "admission_report_id")
        _sha256(reconciliation_index_id, "reconciliation_index_id")
        built_at = _utc(built_at, "built_at")

        report = self._load_admission_report(admission_report_id)
        index_evidence = self._load_reconciliation_index(reconciliation_index_id)

        if (
            report.plan_id != index_evidence.plan_id
            or report.progress_id != index_evidence.progress_id
            or report.provider != index_evidence.provider
            or report.connector_version != index_evidence.connector_version
        ):
            raise HistoricalEvaluationCorpusError(
                "admission report and reconciliation index lineage disagree"
            )
        if report.coverage_complete and not index_evidence.complete:
            raise HistoricalEvaluationCorpusError(
                "a complete admission report requires a complete reconciliation index"
            )
        if built_at < report.assessed_at or built_at < index_evidence.updated_at:
            raise HistoricalEvaluationCorpusError(
                "built_at cannot precede the admission report or reconciliation index"
            )

        index_entries_by_request = {
            entry.request_id: entry for entry in index_evidence.entries
        }

        bars_by_session: dict[date, list[HistoricalEvaluationCorpusBar]] = defaultdict(
            list
        )
        seen_lanes: dict[tuple[date, str, str], str] = {}
        for entry in report.entries:
            if not entry.is_admitted:
                continue
            index_entry = index_entries_by_request.get(entry.request_id)
            if index_entry is None:
                raise HistoricalEvaluationCorpusError(
                    "admitted entry is missing matching reconciliation-index evidence"
                )
            if (
                index_entry.provider_snapshot_id != entry.snapshot_id
                or index_entry.historical_batch_id != entry.historical_batch_id
                or index_entry.reconciliation_report_id != entry.reconciliation_report_id
                or index_entry.passed is not True
            ):
                raise HistoricalEvaluationCorpusIntegrityError(
                    "admitted entry disagrees with its reconciliation-index evidence"
                )

            batch = self._load_provider_batch(report=report, entry=entry)
            if batch.request.binding.binding_id != entry.binding_id:
                raise HistoricalEvaluationCorpusIntegrityError(
                    "admitted entry binding disagrees with its provider batch"
                )
            if tuple(candle.session for candle in batch.candles) != entry.sessions:
                raise HistoricalEvaluationCorpusIntegrityError(
                    "admitted entry sessions disagree with its provider batch"
                )

            self._verify_reconciliation_report(index_entry=index_entry, batch=batch)

            binding = batch.request.binding
            for candle in batch.candles:
                bar = HistoricalEvaluationCorpusBar(
                    session=candle.session,
                    listing_key=binding.listing_key,
                    series=binding.security_series,
                    isin=binding.isin,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                    provider=report.provider,
                    request_id=entry.request_id,
                    binding_id=entry.binding_id,
                    provider_snapshot_id=entry.snapshot_id,
                    historical_batch_id=entry.historical_batch_id,
                    reconciliation_report_id=entry.reconciliation_report_id,
                    reconciliation_snapshot_id=index_entry.reconciliation_snapshot_id,
                    observed_at=batch.observed_at,
                )
                lane = (bar.session, bar.listing_key, bar.series)
                existing_request = seen_lanes.get(lane)
                if existing_request is not None and existing_request != entry.request_id:
                    raise HistoricalEvaluationCorpusError(
                        "duplicate session/listing lane across admitted requests"
                    )
                seen_lanes[lane] = entry.request_id
                bars_by_session[bar.session].append(bar)

        partitions = tuple(
            self._build_partition(session, bars_by_session[session])
            for session in sorted(bars_by_session)
        )

        all_entry_ids = tuple(sorted(entry.entry_id for entry in report.entries))
        admitted_entry_ids = tuple(
            sorted(entry.entry_id for entry in report.entries if entry.is_admitted)
        )
        blocked_entry_ids = tuple(sorted(set(all_entry_ids) - set(admitted_entry_ids)))
        disposition_counts = tuple(
            sorted(Counter(entry.disposition.value for entry in report.entries).items())
        )

        index = HistoricalEvaluationCorpusIndex(
            admission_report_id=report.report_id,
            reconciliation_index_id=index_evidence.index_id,
            plan_id=report.plan_id,
            progress_id=report.progress_id,
            provider=report.provider,
            connector_version=report.connector_version,
            assessed_at=report.assessed_at,
            built_at=built_at,
            partition_ids=tuple(value.partition_id for value in partitions),
            partition_sessions=tuple(value.market_session for value in partitions),
            all_entry_ids=all_entry_ids,
            admitted_entry_ids=admitted_entry_ids,
            blocked_entry_ids=blocked_entry_ids,
            disposition_counts=disposition_counts,
            safe_requests_complete=report.safe_requests_complete,
            coverage_complete=report.coverage_complete,
        )
        return self._corpus_store.put(index, partitions)

    def _load_admission_report(
        self, admission_report_id: str
    ) -> HistoricalDatasetAdmissionReport:
        try:
            report = self._admission_store.get(admission_report_id)
        except Exception:
            raise HistoricalEvaluationCorpusError(
                "admission report evidence is unavailable"
            ) from None
        if type(report) is not HistoricalDatasetAdmissionReport:
            raise HistoricalEvaluationCorpusError(
                "admission report must be an exact HistoricalDatasetAdmissionReport"
            )
        try:
            report.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical dataset admission report failed identity verification"
            ) from None
        if report.report_id != admission_report_id:
            raise HistoricalEvaluationCorpusIntegrityError(
                "admission report storage identity failed"
            )
        if (
            report.collection_only is not True
            or report.actionable is not False
            or report.training_eligible is not False
        ):
            raise HistoricalEvaluationCorpusError(
                "admission report safety flags are not intact"
            )
        return report

    def _load_reconciliation_index(
        self, reconciliation_index_id: str
    ) -> HistoricalReconciliationIndex:
        try:
            index_evidence = self._reconciliation_index_store.get(
                reconciliation_index_id
            )
        except Exception:
            raise HistoricalEvaluationCorpusError(
                "reconciliation index evidence is unavailable"
            ) from None
        if type(index_evidence) is not HistoricalReconciliationIndex:
            raise HistoricalEvaluationCorpusError(
                "reconciliation index must be an exact HistoricalReconciliationIndex"
            )
        try:
            index_evidence.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalEvaluationCorpusIntegrityError(
                "historical reconciliation index failed identity verification"
            ) from None
        if index_evidence.index_id != reconciliation_index_id:
            raise HistoricalEvaluationCorpusIntegrityError(
                "reconciliation index storage identity failed"
            )
        if (
            index_evidence.collection_only is not True
            or index_evidence.actionable is not False
            or index_evidence.training_eligible is not False
        ):
            raise HistoricalEvaluationCorpusError(
                "reconciliation index safety flags are not intact"
            )
        return index_evidence

    def _load_provider_batch(
        self, *, report: HistoricalDatasetAdmissionReport, entry
    ) -> HistoricalDailyCandleBatch:
        dataset = historical_dataset_name(report.provider)
        try:
            stored = self._snapshot_store.get(dataset, entry.snapshot_id)
        except Exception:
            raise HistoricalEvaluationCorpusError(
                "admitted provider snapshot evidence is unavailable"
            ) from None
        try:
            if type(stored) is not StoredMarketSnapshot:
                raise TypeError("provider snapshot must be an exact StoredMarketSnapshot")
            if type(stored.manifest) is not MarketSnapshotManifest:
                raise TypeError(
                    "provider snapshot manifest must be an exact MarketSnapshotManifest"
                )
            if type(stored.payload_bytes) is not bytes:
                raise TypeError("provider snapshot payload bytes must be exact bytes")
            batch = stored.normalized_payload
            if type(batch) is not HistoricalDailyCandleBatch:
                raise TypeError(
                    "provider snapshot payload must be an exact HistoricalDailyCandleBatch"
                )
            batch.verify_content_identity()
            manifest = stored.manifest
            if (
                manifest.schema_version != SNAPSHOT_SCHEMA_VERSION
                or manifest.codec_version != MARKET_PAYLOAD_CODEC_VERSION
                or manifest.payload_filename != PAYLOAD_FILENAME
                or manifest.snapshot_id != entry.snapshot_id
                or manifest.dataset != dataset
                or manifest.selection_key != entry.request_id
                or manifest.provider != report.provider
                or manifest.provider_version != report.connector_version
                or manifest.provider_version != batch.provider_version
                or manifest.observed_at != batch.observed_at
                or manifest.record_count != market_payload_record_count(batch)
            ):
                raise ValueError("provider snapshot manifest disagrees with its batch")
            if stored.payload_bytes != encode_market_payload(batch):
                raise ValueError("provider snapshot payload bytes disagree with its batch")
            if (
                manifest.payload_sha256
                != hashlib.sha256(stored.payload_bytes).hexdigest()
            ):
                raise ValueError("provider snapshot payload hash is invalid")
            if manifest.snapshot_id != _expected_snapshot_id(manifest):
                raise ValueError(
                    "provider snapshot identifier does not match its manifest"
                )
            if (
                batch.request.request_id != entry.request_id
                or batch.batch_id != entry.historical_batch_id
            ):
                raise ValueError(
                    "provider snapshot request/batch lineage does not match the "
                    "admission entry"
                )
        except (TypeError, ValueError):
            raise HistoricalEvaluationCorpusIntegrityError(
                "stored provider snapshot failed envelope validation"
            ) from None
        return batch

    def _verify_reconciliation_report(
        self, *, index_entry, batch: HistoricalDailyCandleBatch
    ) -> HistoricalCandleReconciliationReport:
        try:
            stored = self._snapshot_store.get(
                HISTORICAL_RECONCILIATION_DATASET, index_entry.reconciliation_snapshot_id
            )
        except Exception:
            raise HistoricalEvaluationCorpusError(
                "admitted reconciliation snapshot evidence is unavailable"
            ) from None
        try:
            if type(stored) is not StoredMarketSnapshot:
                raise TypeError(
                    "reconciliation snapshot must be an exact StoredMarketSnapshot"
                )
            if type(stored.manifest) is not MarketSnapshotManifest:
                raise TypeError(
                    "reconciliation snapshot manifest must be an exact "
                    "MarketSnapshotManifest"
                )
            if type(stored.payload_bytes) is not bytes:
                raise TypeError(
                    "reconciliation snapshot payload bytes must be exact bytes"
                )
            payload = stored.normalized_payload
            if type(payload) is not HistoricalCandleReconciliationReport:
                raise TypeError(
                    "reconciliation snapshot payload must be an exact "
                    "HistoricalCandleReconciliationReport"
                )
            payload.verify_content_identity()
            manifest = stored.manifest
            if (
                manifest.schema_version != SNAPSHOT_SCHEMA_VERSION
                or manifest.codec_version != MARKET_PAYLOAD_CODEC_VERSION
                or manifest.payload_filename != PAYLOAD_FILENAME
                or manifest.snapshot_id != index_entry.reconciliation_snapshot_id
                or manifest.dataset != HISTORICAL_RECONCILIATION_DATASET
                or manifest.selection_key != payload.historical_batch_id
                or manifest.provider != HISTORICAL_RECONCILIATION_PROVIDER
                or manifest.provider_version != payload.policy_version
                or manifest.observed_at != payload.reconciled_at
                or manifest.record_count != market_payload_record_count(payload)
            ):
                raise ValueError(
                    "reconciliation snapshot manifest disagrees with its payload"
                )
            if stored.payload_bytes != encode_market_payload(payload):
                raise ValueError(
                    "reconciliation snapshot payload bytes disagree with its report"
                )
            if (
                manifest.payload_sha256
                != hashlib.sha256(stored.payload_bytes).hexdigest()
            ):
                raise ValueError("reconciliation snapshot payload hash is invalid")
            if manifest.snapshot_id != _expected_snapshot_id(manifest):
                raise ValueError(
                    "reconciliation snapshot identifier does not match its manifest"
                )
            if (
                payload.historical_batch_id != batch.batch_id
                or payload.historical_request_id != batch.request.request_id
                or payload.actionable is not False
                or payload.report_id != index_entry.reconciliation_report_id
                or payload.reconciled_at != index_entry.reconciled_at
                or payload.passed != index_entry.passed
                or payload.passed is not True
            ):
                raise ValueError(
                    "reconciliation snapshot lineage disagrees with the index entry"
                )
        except (TypeError, ValueError):
            raise HistoricalEvaluationCorpusIntegrityError(
                "stored reconciliation snapshot failed envelope validation"
            ) from None
        return payload

    @staticmethod
    def _build_partition(
        session: date, bars: list[HistoricalEvaluationCorpusBar]
    ) -> HistoricalEvaluationCorpusSessionPartition:
        sorted_bars = tuple(sorted(bars, key=lambda value: value.listing_lane))
        source_snapshot_ids = tuple(
            sorted(
                {
                    *(bar.provider_snapshot_id for bar in sorted_bars),
                    *(bar.reconciliation_snapshot_id for bar in sorted_bars),
                }
            )
        )
        source_report_ids = tuple(
            sorted({bar.reconciliation_report_id for bar in sorted_bars})
        )
        return HistoricalEvaluationCorpusSessionPartition(
            market_session=session,
            bars=sorted_bars,
            source_snapshot_ids=source_snapshot_ids,
            source_report_ids=source_report_ids,
        )
