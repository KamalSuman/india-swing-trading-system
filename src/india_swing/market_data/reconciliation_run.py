"""Offline bulk reconciliation of already-collected provider snapshots.

This module never constructs a connector, authenticates, or calls a provider.
It reads only completions that a previous collection run already pinned into
``HistoricalBackfillProgress``, compares each stored provider batch with exact
NSE EOD artifacts, persists the resulting reconciliation reports, and seals a
cumulative create-once index binding every completion to its evidence.

Resumption is possible only through an explicit prior index ID whose entries
are a verified prefix of the same pinned progress: there is deliberately no
listing, latest, or selection-key lookup anywhere in this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.historical_prices.models import NseEodSessionArtifact
from india_swing.identity import content_id

from .backfill import (
    HistoricalBackfillPlan,
    HistoricalBackfillProgress,
    LocalHistoricalBackfillProgressStore,
)
from .codec import (
    MARKET_PAYLOAD_CODEC_VERSION,
    encode_market_payload,
    market_payload_record_count,
)
from .collection import HistoricalReconciliationCollector, historical_dataset_name
from .models import (
    HistoricalDailyCandleBatch,
    MARKET_DATA_PROVIDER_PATTERN,
    SHA256_IDENTIFIER,
)
from .reconciliation import (
    HISTORICAL_RECONCILIATION_DATASET,
    HISTORICAL_RECONCILIATION_PROVIDER,
    HistoricalCandleReconciliationReport,
    reconcile_historical_batch,
)
from .snapshot_store import (
    PAYLOAD_FILENAME,
    SNAPSHOT_SCHEMA_VERSION,
    LocalMarketSnapshotStore,
    MarketSnapshotManifest,
    StoredMarketSnapshot,
)


HISTORICAL_RECONCILIATION_INDEX_SCHEMA_VERSION = (
    "historical-reconciliation-index/v1"
)
HISTORICAL_RECONCILIATION_INDEX_POLICY_VERSION = (
    "historical-reconciliation-index-policy/v1"
)
HISTORICAL_RECONCILIATION_INDEX_CODEC_VERSION = (
    "historical-reconciliation-index-json/v1"
)
HISTORICAL_RECONCILIATION_INDEX_DATASET = "historical-reconciliation-indexes"
INDEX_FILENAME = "index.json"
MAXIMUM_RECONCILIATION_INDEX_BYTES = 32 * 1024 * 1024
MAXIMUM_RECONCILIATIONS_PER_RUN = 500


class HistoricalBulkReconciliationError(ValueError):
    """A bulk reconciliation input or artifact failed a static safety rule."""


class HistoricalBulkReconciliationIntegrityError(
    HistoricalBulkReconciliationError
):
    """Persisted bulk reconciliation evidence failed independent verification."""


def _sha256(value: object, field_name: str) -> None:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256")


def _utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be an exact datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class HistoricalReconciliationIndexEntry:
    """One completion bound to the exact evidence that reconciled it."""

    request_id: str
    provider_snapshot_id: str
    historical_batch_id: str
    reconciliation_report_id: str
    reconciliation_snapshot_id: str
    reconciled_at: datetime
    passed: bool
    entry_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "entry_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.request_id, "entry request_id")
        _sha256(self.provider_snapshot_id, "entry provider_snapshot_id")
        _sha256(self.historical_batch_id, "entry historical_batch_id")
        _sha256(self.reconciliation_report_id, "entry reconciliation_report_id")
        _sha256(
            self.reconciliation_snapshot_id, "entry reconciliation_snapshot_id"
        )
        object.__setattr__(
            self,
            "reconciled_at",
            _utc(self.reconciled_at, "entry reconciled_at"),
        )
        if type(self.passed) is not bool:
            raise TypeError("entry passed must be an exact bool")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": HISTORICAL_RECONCILIATION_INDEX_SCHEMA_VERSION,
                "request_id": self.request_id,
                "provider_snapshot_id": self.provider_snapshot_id,
                "historical_batch_id": self.historical_batch_id,
                "reconciliation_report_id": self.reconciliation_report_id,
                "reconciliation_snapshot_id": self.reconciliation_snapshot_id,
                "reconciled_at": self.reconciled_at,
                "passed": self.passed,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.entry_id != self._calculated_id():
            raise HistoricalBulkReconciliationIntegrityError(
                "historical reconciliation index entry identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalReconciliationIndex:
    """A cumulative, create-once, collection-only reconciliation index."""

    plan_id: str
    progress_id: str
    provider: str
    connector_version: str
    nse_artifact_ids: tuple[str, ...]
    prior_index_id: str | None
    entries: tuple[HistoricalReconciliationIndexEntry, ...]
    total_completion_count: int
    updated_at: datetime
    complete: bool
    collection_only: bool = True
    actionable: bool = False
    training_eligible: bool = False
    schema_version: str = HISTORICAL_RECONCILIATION_INDEX_SCHEMA_VERSION
    policy_version: str = HISTORICAL_RECONCILIATION_INDEX_POLICY_VERSION
    codec_version: str = HISTORICAL_RECONCILIATION_INDEX_CODEC_VERSION
    index_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "index_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.plan_id, "index plan_id")
        _sha256(self.progress_id, "index progress_id")
        if (
            type(self.provider) is not str
            or MARKET_DATA_PROVIDER_PATTERN.fullmatch(self.provider) is None
        ):
            raise ValueError("index provider must be canonical uppercase text")
        if (
            type(self.connector_version) is not str
            or not self.connector_version
            or len(self.connector_version) > 128
        ):
            raise ValueError("index connector_version must be bounded text")
        if type(self.nse_artifact_ids) is not tuple or not self.nse_artifact_ids:
            raise TypeError("index nse_artifact_ids must be a non-empty tuple")
        for value in self.nse_artifact_ids:
            _sha256(value, "index nse_artifact_id")
        if self.nse_artifact_ids != tuple(sorted(set(self.nse_artifact_ids))):
            raise ValueError("index nse_artifact_ids must be sorted and unique")
        if self.prior_index_id is not None:
            _sha256(self.prior_index_id, "index prior_index_id")

        if type(self.entries) is not tuple or not self.entries or any(
            type(value) is not HistoricalReconciliationIndexEntry
            for value in self.entries
        ):
            raise TypeError("index entries must be a non-empty exact tuple")
        for entry in self.entries:
            entry.verify_content_identity()
        for name in (
            "request_id",
            "provider_snapshot_id",
            "historical_batch_id",
            "reconciliation_report_id",
            "reconciliation_snapshot_id",
            "entry_id",
        ):
            values = tuple(getattr(entry, name) for entry in self.entries)
            if len(set(values)) != len(values):
                raise ValueError(f"index entries must have unique {name}")
        reconciled_times = tuple(entry.reconciled_at for entry in self.entries)
        if reconciled_times != tuple(sorted(reconciled_times)):
            raise ValueError("index entry reconciled times must not regress")

        if (
            type(self.total_completion_count) is not int
            or self.total_completion_count <= 0
        ):
            raise ValueError(
                "index total_completion_count must be a positive exact integer"
            )
        if len(self.entries) > self.total_completion_count:
            raise ValueError(
                "index entries cannot exceed total_completion_count"
            )
        object.__setattr__(
            self,
            "updated_at",
            _utc(self.updated_at, "index updated_at"),
        )
        if any(entry.reconciled_at > self.updated_at for entry in self.entries):
            raise ValueError("index updated_at cannot predate an entry")
        expected_complete = len(self.entries) == self.total_completion_count
        if type(self.complete) is not bool or self.complete != expected_complete:
            raise ValueError("index complete flag disagrees with its entries")

        if self.collection_only is not True:
            raise ValueError(
                "historical reconciliation indexes must remain collection-only"
            )
        if self.actionable is not False:
            raise ValueError(
                "historical reconciliation indexes cannot authorize trading"
            )
        if self.training_eligible is not False:
            raise ValueError(
                "historical reconciliation indexes are not training-eligible"
            )
        if (
            self.schema_version != HISTORICAL_RECONCILIATION_INDEX_SCHEMA_VERSION
            or self.policy_version
            != HISTORICAL_RECONCILIATION_INDEX_POLICY_VERSION
            or self.codec_version != HISTORICAL_RECONCILIATION_INDEX_CODEC_VERSION
        ):
            raise ValueError(
                "unsupported historical reconciliation index contract"
            )

    @property
    def indexed_count(self) -> int:
        return len(self.entries)

    @property
    def remaining_count(self) -> int:
        return self.total_completion_count - len(self.entries)

    @property
    def passed_count(self) -> int:
        return sum(1 for entry in self.entries if entry.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for entry in self.entries if not entry.passed)

    @property
    def reconciliation_snapshot_ids(self) -> tuple[str, ...]:
        return tuple(entry.reconciliation_snapshot_id for entry in self.entries)

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "index_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        for entry in self.entries:
            entry.verify_content_identity()
        if self.index_id != self._calculated_id():
            raise HistoricalBulkReconciliationIntegrityError(
                "historical reconciliation index identity failed"
            )


def reconciliation_index_snapshot_ids(
    index: HistoricalReconciliationIndex,
    *,
    expected_plan_id: str,
    expected_progress_id: str,
) -> tuple[str, ...]:
    """Return the index's ordered reconciliation snapshot IDs after rebinding.

    The index is evidence, not authority: its plan/progress lineage and its
    fixed safety flags must match the caller's own exact expectations before
    any of its snapshot IDs may be admitted downstream.
    """

    if type(index) is not HistoricalReconciliationIndex:
        raise HistoricalBulkReconciliationError(
            "index must be an exact HistoricalReconciliationIndex"
        )
    try:
        index.verify_content_identity()
    except (TypeError, ValueError):
        raise HistoricalBulkReconciliationIntegrityError(
            "historical reconciliation index failed identity verification"
        ) from None
    if (
        index.plan_id != expected_plan_id
        or index.progress_id != expected_progress_id
    ):
        raise HistoricalBulkReconciliationError(
            "historical reconciliation index lineage does not match the "
            "expected plan and progress"
        )
    if (
        index.collection_only is not True
        or index.actionable is not False
        or index.training_eligible is not False
    ):
        raise HistoricalBulkReconciliationError(
            "historical reconciliation index safety flags are not intact"
        )
    return index.reconciliation_snapshot_ids


def _entry_value(entry: HistoricalReconciliationIndexEntry) -> dict[str, object]:
    return {
        "entry_id": entry.entry_id,
        "request_id": entry.request_id,
        "provider_snapshot_id": entry.provider_snapshot_id,
        "historical_batch_id": entry.historical_batch_id,
        "reconciliation_report_id": entry.reconciliation_report_id,
        "reconciliation_snapshot_id": entry.reconciliation_snapshot_id,
        "reconciled_at": entry.reconciled_at.isoformat(),
        "passed": entry.passed,
    }


_EXPECTED_ENTRY_KEYS = {
    "entry_id",
    "request_id",
    "provider_snapshot_id",
    "historical_batch_id",
    "reconciliation_report_id",
    "reconciliation_snapshot_id",
    "reconciled_at",
    "passed",
}

_EXPECTED_INDEX_KEYS = {
    "schema_version",
    "policy_version",
    "codec_version",
    "index_id",
    "plan_id",
    "progress_id",
    "provider",
    "connector_version",
    "nse_artifact_ids",
    "prior_index_id",
    "entries",
    "total_completion_count",
    "updated_at",
    "complete",
    "collection_only",
    "actionable",
    "training_eligible",
}


def encode_historical_reconciliation_index(
    index: HistoricalReconciliationIndex,
) -> bytes:
    if type(index) is not HistoricalReconciliationIndex:
        raise TypeError("index must be an exact HistoricalReconciliationIndex")
    index.verify_content_identity()
    value = {
        "schema_version": index.schema_version,
        "policy_version": index.policy_version,
        "codec_version": index.codec_version,
        "index_id": index.index_id,
        "plan_id": index.plan_id,
        "progress_id": index.progress_id,
        "provider": index.provider,
        "connector_version": index.connector_version,
        "nse_artifact_ids": list(index.nse_artifact_ids),
        "prior_index_id": index.prior_index_id,
        "entries": [_entry_value(entry) for entry in index.entries],
        "total_completion_count": index.total_completion_count,
        "updated_at": index.updated_at.isoformat(),
        "complete": index.complete,
        "collection_only": index.collection_only,
        "actionable": index.actionable,
        "training_eligible": index.training_eligible,
    }
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


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise HistoricalBulkReconciliationIntegrityError(
                "historical reconciliation index contains duplicate JSON keys"
            )
        value[key] = item
    return value


def decode_historical_reconciliation_index(
    payload: bytes,
) -> HistoricalReconciliationIndex:
    try:
        if type(payload) is not bytes:
            raise TypeError
        if not payload:
            raise ValueError
        if len(payload) > MAXIMUM_RECONCILIATION_INDEX_BYTES:
            raise ValueError
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != _EXPECTED_INDEX_KEYS:
            raise ValueError
        raw_entries = root["entries"]
        if type(raw_entries) is not list:
            raise ValueError
        entries: list[HistoricalReconciliationIndexEntry] = []
        for value in raw_entries:
            if type(value) is not dict or set(value) != _EXPECTED_ENTRY_KEYS:
                raise ValueError
            entry = HistoricalReconciliationIndexEntry(
                request_id=value["request_id"],
                provider_snapshot_id=value["provider_snapshot_id"],
                historical_batch_id=value["historical_batch_id"],
                reconciliation_report_id=value["reconciliation_report_id"],
                reconciliation_snapshot_id=value["reconciliation_snapshot_id"],
                reconciled_at=datetime.fromisoformat(value["reconciled_at"]),
                passed=value["passed"],
            )
            if value["entry_id"] != entry.entry_id:
                raise ValueError
            entries.append(entry)
        raw_artifact_ids = root["nse_artifact_ids"]
        if type(raw_artifact_ids) is not list:
            raise ValueError
        index = HistoricalReconciliationIndex(
            plan_id=root["plan_id"],
            progress_id=root["progress_id"],
            provider=root["provider"],
            connector_version=root["connector_version"],
            nse_artifact_ids=tuple(raw_artifact_ids),
            prior_index_id=root["prior_index_id"],
            entries=tuple(entries),
            total_completion_count=root["total_completion_count"],
            updated_at=datetime.fromisoformat(root["updated_at"]),
            complete=root["complete"],
            collection_only=root["collection_only"],
            actionable=root["actionable"],
            training_eligible=root["training_eligible"],
            schema_version=root["schema_version"],
            policy_version=root["policy_version"],
            codec_version=root["codec_version"],
        )
        if root["index_id"] != index.index_id:
            raise ValueError
        if payload != encode_historical_reconciliation_index(index):
            raise ValueError
        return index
    except HistoricalBulkReconciliationIntegrityError:
        raise
    except (
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        raise HistoricalBulkReconciliationIntegrityError(
            "stored historical reconciliation index is invalid"
        ) from None


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _write_fsynced(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


class LocalHistoricalReconciliationIndexStore:
    """Create-once local index store; exposes only exact-ID get, never a listing."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / HISTORICAL_RECONCILIATION_INDEX_DATASET

    def put(
        self, index: HistoricalReconciliationIndex
    ) -> HistoricalReconciliationIndex:
        if type(index) is not HistoricalReconciliationIndex:
            raise TypeError(
                "index must be an exact HistoricalReconciliationIndex"
            )
        index.verify_content_identity()
        payload = encode_historical_reconciliation_index(index)
        if len(payload) > MAXIMUM_RECONCILIATION_INDEX_BYTES:
            raise HistoricalBulkReconciliationError(
                "historical reconciliation index exceeds its size limit"
            )
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        target = self.dataset_root / index.index_id
        lock = self.dataset_root / ".reconciliation-indexes.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing = self._read_path(target)
                    if existing != index:
                        raise HistoricalBulkReconciliationIntegrityError(
                            "index ID already stores different content"
                        )
                    return existing
                temporary = Path(
                    tempfile.mkdtemp(
                        prefix=".reconciliation-index-",
                        dir=self.dataset_root,
                    )
                )
                try:
                    _write_fsynced(temporary / INDEX_FILENAME, payload)
                    os.replace(temporary, target)
                except Exception:
                    shutil.rmtree(temporary, ignore_errors=True)
                    raise
        except (FileLockUnavailable, FileSafetyError):
            raise HistoricalBulkReconciliationIntegrityError(
                "historical reconciliation index store is unavailable"
            ) from None
        return self._read_path(target)

    def get(self, index_id: str) -> HistoricalReconciliationIndex:
        if (
            type(index_id) is not str
            or SHA256_IDENTIFIER.fullmatch(index_id) is None
        ):
            raise HistoricalBulkReconciliationError(
                "index_id must be a lowercase SHA-256"
            )
        target = self.dataset_root / index_id
        if not target.exists():
            raise HistoricalBulkReconciliationError(
                "historical reconciliation index was not found"
            )
        index = self._read_path(target)
        if index.index_id != index_id:
            raise HistoricalBulkReconciliationIntegrityError(
                "historical reconciliation index storage identity failed"
            )
        return index

    def _read_path(self, target: Path) -> HistoricalReconciliationIndex:
        try:
            if not target.is_dir() or _is_link_like(target):
                raise HistoricalBulkReconciliationIntegrityError(
                    "historical reconciliation index path is invalid"
                )
            children = tuple(target.iterdir())
            if {value.name for value in children} != {INDEX_FILENAME} or any(
                _is_link_like(value) or not value.is_file()
                for value in children
            ):
                raise HistoricalBulkReconciliationIntegrityError(
                    "historical reconciliation index directory is invalid"
                )
            payload = read_stable_regular_file(
                target / INDEX_FILENAME,
                maximum_bytes=MAXIMUM_RECONCILIATION_INDEX_BYTES,
            )
            index = decode_historical_reconciliation_index(payload)
            if (
                target.name != index.index_id
                or payload != encode_historical_reconciliation_index(index)
            ):
                raise HistoricalBulkReconciliationIntegrityError(
                    "historical reconciliation index storage identity failed"
                )
            return index
        except HistoricalBulkReconciliationIntegrityError:
            raise
        except (FileSafetyError, OSError):
            raise HistoricalBulkReconciliationIntegrityError(
                "historical reconciliation index could not be read safely"
            ) from None


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


def _require_envelope(stored: object) -> StoredMarketSnapshot:
    if type(stored) is not StoredMarketSnapshot:
        raise TypeError("snapshot must be an exact StoredMarketSnapshot")
    if type(stored.manifest) is not MarketSnapshotManifest:
        raise TypeError("snapshot manifest must be an exact MarketSnapshotManifest")
    if type(stored.payload_bytes) is not bytes:
        raise TypeError("snapshot payload bytes must be exact bytes")
    return stored


class HistoricalBulkReconciliationService:
    """Reconcile already-collected completions; never calls a provider.

    Every store is caller-injected and therefore untrusted: each provider
    snapshot and each persisted reconciliation snapshot is re-verified from
    its manifest, canonical bytes, recomputed hash, and recomputed content
    identity before it is allowed to become index evidence.
    """

    def __init__(
        self,
        *,
        progress_store: LocalHistoricalBackfillProgressStore,
        snapshot_store: LocalMarketSnapshotStore,
        reconciliation_collector: HistoricalReconciliationCollector,
        index_store: LocalHistoricalReconciliationIndexStore,
    ) -> None:
        self._progress_store = progress_store
        self._snapshot_store = snapshot_store
        self._reconciliation_collector = reconciliation_collector
        self._index_store = index_store

    def run(
        self,
        *,
        plan: HistoricalBackfillPlan,
        expected_plan_id: str,
        expected_progress_id: str,
        nse_artifacts: tuple[NseEodSessionArtifact, ...],
        maximum_requests: int,
        reconciled_at: datetime,
        prior_index_id: str | None = None,
    ) -> HistoricalReconciliationIndex:
        if type(plan) is not HistoricalBackfillPlan:
            raise HistoricalBulkReconciliationError(
                "plan must be an exact HistoricalBackfillPlan"
            )
        try:
            plan.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalBulkReconciliationError(
                "historical backfill plan failed identity verification"
            ) from None
        self._require_sha256(expected_plan_id, "expected_plan_id")
        self._require_sha256(expected_progress_id, "expected_progress_id")
        if prior_index_id is not None:
            self._require_sha256(prior_index_id, "prior_index_id")
        if (
            type(maximum_requests) is not int
            or not 0 < maximum_requests <= MAXIMUM_RECONCILIATIONS_PER_RUN
        ):
            raise HistoricalBulkReconciliationError(
                "maximum_requests must be a positive exact integer at or "
                f"below {MAXIMUM_RECONCILIATIONS_PER_RUN}"
            )
        if type(reconciled_at) is not datetime:
            raise HistoricalBulkReconciliationError(
                "reconciled_at must be an exact datetime"
            )
        if reconciled_at.tzinfo is None or reconciled_at.utcoffset() is None:
            raise HistoricalBulkReconciliationError(
                "reconciled_at must be timezone-aware"
            )
        reconciled_at = reconciled_at.astimezone(timezone.utc)
        if plan.plan_id != expected_plan_id:
            raise HistoricalBulkReconciliationError(
                "reconstructed plan does not match expected_plan_id"
            )

        progress = self._load_progress(expected_plan_id, expected_progress_id, plan)
        requests_by_id = {value.request_id: value for value in plan.requests}
        unknown = tuple(
            value.request_id
            for value in progress.completions
            if value.request_id not in requests_by_id
        )
        if unknown:
            raise HistoricalBulkReconciliationError(
                "pinned progress contains a completion outside the plan"
            )
        required_sessions = frozenset(
            session
            for value in progress.completions
            for session in requests_by_id[value.request_id].sessions
        )
        artifacts_by_session = self._validate_nse_artifacts(
            nse_artifacts, required_sessions
        )
        nse_artifact_ids = tuple(
            sorted(value.artifact_id for value in nse_artifacts)
        )
        total_completion_count = len(progress.completions)

        prior_entries = self._load_prior_entries(
            prior_index_id=prior_index_id,
            plan=plan,
            progress=progress,
            requests_by_id=requests_by_id,
            nse_artifact_ids=nse_artifact_ids,
            total_completion_count=total_completion_count,
            reconciled_at=reconciled_at,
        )
        selected = progress.completions[
            len(prior_entries) : len(prior_entries) + maximum_requests
        ]
        if not selected:
            raise HistoricalBulkReconciliationError(
                "no unindexed progress completion remains for this run"
            )

        new_entries: list[HistoricalReconciliationIndexEntry] = []
        for completion in selected:
            batch = self._load_provider_batch(
                completion=completion,
                plan=plan,
                progress=progress,
                request=requests_by_id[completion.request_id],
            )
            matching = tuple(
                artifacts_by_session[session]
                for session in batch.request.sessions
            )
            report = self._reconcile(batch, matching, reconciled_at)
            stored = self._persist_report(report, batch)
            new_entries.append(
                HistoricalReconciliationIndexEntry(
                    request_id=completion.request_id,
                    provider_snapshot_id=completion.snapshot_id,
                    historical_batch_id=batch.batch_id,
                    reconciliation_report_id=report.report_id,
                    reconciliation_snapshot_id=stored.manifest.snapshot_id,
                    reconciled_at=reconciled_at,
                    passed=report.passed,
                )
            )

        entries = prior_entries + tuple(new_entries)
        index = HistoricalReconciliationIndex(
            plan_id=plan.plan_id,
            progress_id=progress.progress_id,
            provider=plan.provider,
            connector_version=progress.connector_version,
            nse_artifact_ids=nse_artifact_ids,
            prior_index_id=prior_index_id,
            entries=entries,
            total_completion_count=total_completion_count,
            updated_at=reconciled_at,
            complete=len(entries) == total_completion_count,
        )
        return self._index_store.put(index)

    @staticmethod
    def _require_sha256(value: object, field_name: str) -> None:
        if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
            raise HistoricalBulkReconciliationError(
                f"{field_name} must be a lowercase SHA-256"
            )

    def _load_progress(
        self,
        expected_plan_id: str,
        expected_progress_id: str,
        plan: HistoricalBackfillPlan,
    ) -> HistoricalBackfillProgress:
        progress = self._progress_store.load(expected_plan_id)
        if progress is None:
            raise HistoricalBulkReconciliationError(
                "no backfill progress exists for the exact plan"
            )
        if type(progress) is not HistoricalBackfillProgress:
            raise HistoricalBulkReconciliationError(
                "progress must be an exact HistoricalBackfillProgress"
            )
        try:
            progress.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalBulkReconciliationIntegrityError(
                "historical backfill progress failed identity verification"
            ) from None
        if progress.progress_id != expected_progress_id:
            raise HistoricalBulkReconciliationError(
                "loaded progress does not match expected_progress_id"
            )
        if progress.plan_id != plan.plan_id or progress.provider != plan.provider:
            raise HistoricalBulkReconciliationError(
                "loaded progress lineage does not match the plan"
            )
        if not progress.completions:
            raise HistoricalBulkReconciliationError(
                "pinned progress contains no completion to reconcile"
            )
        return progress

    @staticmethod
    def _validate_nse_artifacts(
        nse_artifacts: object,
        required_sessions: frozenset,
    ) -> dict:
        if type(nse_artifacts) is not tuple or any(
            type(value) is not NseEodSessionArtifact for value in nse_artifacts
        ):
            raise HistoricalBulkReconciliationError(
                "nse_artifacts must be an exact immutable NseEodSessionArtifact tuple"
            )
        if not nse_artifacts:
            raise HistoricalBulkReconciliationError(
                "bulk reconciliation requires exact NSE session evidence"
            )
        try:
            for artifact in nse_artifacts:
                artifact.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalBulkReconciliationIntegrityError(
                "an NSE session artifact failed identity verification"
            ) from None
        artifact_ids = tuple(value.artifact_id for value in nse_artifacts)
        if len(set(artifact_ids)) != len(artifact_ids):
            raise HistoricalBulkReconciliationError(
                "nse_artifacts must have unique artifact IDs"
            )
        sessions = tuple(value.market_session for value in nse_artifacts)
        if len(set(sessions)) != len(sessions):
            raise HistoricalBulkReconciliationError(
                "nse_artifacts must be session-unique"
            )
        if set(sessions) != required_sessions:
            raise HistoricalBulkReconciliationError(
                "nse_artifacts must exactly cover every pinned progress session"
            )
        return {value.market_session: value for value in nse_artifacts}

    def _load_prior_entries(
        self,
        *,
        prior_index_id: str | None,
        plan: HistoricalBackfillPlan,
        progress: HistoricalBackfillProgress,
        requests_by_id: dict,
        nse_artifact_ids: tuple[str, ...],
        total_completion_count: int,
        reconciled_at: datetime,
    ) -> tuple[HistoricalReconciliationIndexEntry, ...]:
        if prior_index_id is None:
            return ()
        prior = self._index_store.get(prior_index_id)
        if type(prior) is not HistoricalReconciliationIndex:
            raise HistoricalBulkReconciliationError(
                "prior index must be an exact HistoricalReconciliationIndex"
            )
        try:
            prior.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalBulkReconciliationIntegrityError(
                "prior historical reconciliation index failed identity verification"
            ) from None
        if prior.index_id != prior_index_id:
            raise HistoricalBulkReconciliationIntegrityError(
                "prior historical reconciliation index identity does not match "
                "the requested ID"
            )
        if (
            prior.plan_id != plan.plan_id
            or prior.progress_id != progress.progress_id
            or prior.provider != plan.provider
            or prior.connector_version != progress.connector_version
            or prior.total_completion_count != total_completion_count
            or prior.nse_artifact_ids != nse_artifact_ids
        ):
            raise HistoricalBulkReconciliationError(
                "prior historical reconciliation index lineage does not match "
                "the current evidence set"
            )
        if (
            prior.collection_only is not True
            or prior.actionable is not False
            or prior.training_eligible is not False
        ):
            raise HistoricalBulkReconciliationError(
                "prior historical reconciliation index safety flags are not intact"
            )
        if prior.complete:
            raise HistoricalBulkReconciliationError(
                "prior historical reconciliation index is already complete"
            )
        if reconciled_at < prior.updated_at:
            raise HistoricalBulkReconciliationError(
                "reconciled_at cannot predate the prior index update time"
            )
        if len(prior.entries) > total_completion_count:
            raise HistoricalBulkReconciliationError(
                "prior historical reconciliation index is not a progress prefix"
            )
        for entry, completion in zip(prior.entries, progress.completions):
            if (
                entry.request_id != completion.request_id
                or entry.provider_snapshot_id != completion.snapshot_id
            ):
                raise HistoricalBulkReconciliationError(
                    "prior historical reconciliation index is not a progress prefix"
                )
            self._verify_prior_entry(
                entry=entry,
                completion=completion,
                plan=plan,
                progress=progress,
                request=requests_by_id[completion.request_id],
            )
        return prior.entries

    def _verify_prior_entry(
        self,
        *,
        entry: HistoricalReconciliationIndexEntry,
        completion,
        plan: HistoricalBackfillPlan,
        progress: HistoricalBackfillProgress,
        request,
    ) -> None:
        """Re-verify the immutable evidence a prior entry claims to be backed by.

        A prior index is untrusted input: matching request and provider snapshot
        IDs only prove where an entry points, not that its batch, report,
        reconciliation snapshot, reconciliation time, or passed status were ever
        derived from real persisted evidence. Each is reloaded by its exact ID
        and re-verified here; nothing is reconciled or persisted again.
        """

        batch = self._load_provider_batch(
            completion=completion,
            plan=plan,
            progress=progress,
            request=request,
        )
        if batch.batch_id != entry.historical_batch_id:
            raise HistoricalBulkReconciliationError(
                "prior index entry does not match its pinned provider batch"
            )
        try:
            stored = self._snapshot_store.get(
                HISTORICAL_RECONCILIATION_DATASET,
                entry.reconciliation_snapshot_id,
            )
        except Exception:
            raise HistoricalBulkReconciliationError(
                "prior index entry reconciliation snapshot is unavailable"
            ) from None
        try:
            stored, payload = self._verify_reconciliation_snapshot(
                stored, batch=batch
            )
            if (
                stored.manifest.snapshot_id != entry.reconciliation_snapshot_id
                or payload.report_id != entry.reconciliation_report_id
                or payload.historical_batch_id != entry.historical_batch_id
                or payload.reconciled_at != entry.reconciled_at
                or payload.passed != entry.passed
            ):
                raise ValueError(
                    "prior index entry disagrees with its persisted report"
                )
        except (TypeError, ValueError):
            raise HistoricalBulkReconciliationIntegrityError(
                "prior index entry reconciliation evidence failed envelope validation"
            ) from None

    def _load_provider_batch(
        self,
        *,
        completion,
        plan: HistoricalBackfillPlan,
        progress: HistoricalBackfillProgress,
        request,
    ) -> HistoricalDailyCandleBatch:
        dataset = historical_dataset_name(plan.provider)
        try:
            stored = _require_envelope(
                self._snapshot_store.get(dataset, completion.snapshot_id)
            )
            batch = stored.normalized_payload
            if type(batch) is not HistoricalDailyCandleBatch:
                raise TypeError(
                    "provider snapshot payload must be an exact "
                    "HistoricalDailyCandleBatch"
                )
            batch.verify_content_identity()
            manifest = stored.manifest
            if (
                manifest.schema_version != SNAPSHOT_SCHEMA_VERSION
                or manifest.codec_version != MARKET_PAYLOAD_CODEC_VERSION
                or manifest.payload_filename != PAYLOAD_FILENAME
                or manifest.snapshot_id != completion.snapshot_id
                or manifest.dataset != dataset
                or manifest.selection_key != completion.request_id
                or manifest.provider != plan.provider
                or manifest.provider_version != progress.connector_version
                or manifest.provider_version != batch.provider_version
                or manifest.observed_at != batch.observed_at
                or manifest.record_count != market_payload_record_count(batch)
            ):
                raise ValueError(
                    "provider snapshot manifest disagrees with its batch"
                )
            if stored.payload_bytes != encode_market_payload(batch):
                raise ValueError(
                    "provider snapshot payload bytes disagree with its batch"
                )
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
                batch.request.request_id != completion.request_id
                or batch.request != request
            ):
                raise ValueError(
                    "provider snapshot request lineage does not match the plan"
                )
        except (TypeError, ValueError):
            raise HistoricalBulkReconciliationIntegrityError(
                "stored provider snapshot failed envelope validation"
            ) from None
        except Exception:
            raise HistoricalBulkReconciliationError(
                "pinned provider snapshot is unavailable"
            ) from None
        return batch

    @staticmethod
    def _reconcile(
        batch: HistoricalDailyCandleBatch,
        matching_artifacts: tuple[NseEodSessionArtifact, ...],
        reconciled_at: datetime,
    ) -> HistoricalCandleReconciliationReport:
        try:
            return reconcile_historical_batch(
                batch, matching_artifacts, reconciled_at=reconciled_at
            )
        except Exception:
            raise HistoricalBulkReconciliationError(
                "bulk reconciliation of a pinned provider batch failed"
            ) from None

    @staticmethod
    def _verify_reconciliation_snapshot(
        stored: object,
        *,
        batch: HistoricalDailyCandleBatch,
    ) -> tuple[StoredMarketSnapshot, HistoricalCandleReconciliationReport]:
        """Verify one persisted reconciliation envelope against its own batch.

        Shared by newly collected reports and by prior-index-entry evidence, so
        both paths recompute the same manifest lineage, canonical bytes, payload
        hash, and content identity. Raises only TypeError/ValueError; every
        caller converts those into one static sanitized error.
        """

        stored = _require_envelope(stored)
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
        ):
            raise ValueError(
                "reconciliation snapshot lineage does not match its batch"
            )
        return stored, payload

    def _persist_report(
        self,
        report: HistoricalCandleReconciliationReport,
        batch: HistoricalDailyCandleBatch,
    ) -> StoredMarketSnapshot:
        try:
            stored = self._reconciliation_collector.collect(report)
        except Exception:
            raise HistoricalBulkReconciliationError(
                "reconciliation report could not be persisted"
            ) from None
        try:
            stored, payload = self._verify_reconciliation_snapshot(
                stored, batch=batch
            )
            if (
                payload != report
                or payload.report_id != report.report_id
                or payload.reconciled_at != report.reconciled_at
                or payload.passed != report.passed
            ):
                raise ValueError(
                    "reconciliation snapshot lineage does not match its report"
                )
        except (TypeError, ValueError):
            raise HistoricalBulkReconciliationIntegrityError(
                "stored reconciliation snapshot failed envelope validation"
            ) from None
        return stored
