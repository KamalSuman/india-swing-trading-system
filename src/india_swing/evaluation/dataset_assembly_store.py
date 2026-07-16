from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.execution.simulator import SimulationBar
from india_swing.reference.models import ReferenceReadiness

from .baselines import PointInTimeInstrument
from .dataset_assembly import (
    AssembledEvaluationDataset,
    EffectiveTickSize,
    EvaluationDatasetAssemblyError,
    EvaluationSessionEvidence,
)
from .engine import EvaluationDataReadiness, EvaluationDataset


EVALUATION_DATASET_STORE_SCHEMA_VERSION = "local-evaluation-dataset/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_DATASET_BYTES = 512 * 1024 * 1024


class EvaluationDatasetStoreConflict(EvaluationDatasetAssemblyError):
    pass


class EvaluationDatasetNotFound(EvaluationDatasetAssemblyError):
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


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise EvaluationDatasetStoreConflict(
                "evaluation dataset contains a duplicate JSON key"
            )
        result[key] = value
    return result


def _keys(value: object, expected: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise EvaluationDatasetStoreConflict(f"stored {name} has invalid fields")
    return value


def _bar_value(value: SimulationBar) -> dict[str, object]:
    return {
        "bar_id": value.bar_id,
        "close": str(value.close),
        "high": str(value.high),
        "low": str(value.low),
        "lower_circuit_sell_locked": value.lower_circuit_sell_locked,
        "open": str(value.open),
        "session": value.session.isoformat(),
        "symbol": value.symbol,
        "tradable": value.tradable,
        "volume": value.volume,
    }


def _instrument_value(value: PointInTimeInstrument) -> dict[str, object]:
    return {
        "eligibility_bindings": [
            [session.isoformat(), snapshot_id]
            for session, snapshot_id in value.eligibility_bindings
        ],
        "eligible_sessions": [session.isoformat() for session in value.eligible_sessions],
        "instrument_id": value.instrument_id,
        "isin": value.isin,
        "stable_instrument_id": value.stable_instrument_id,
        "symbol": value.symbol,
        "tick_size": str(value.tick_size),
        "universe_snapshot_id": value.universe_snapshot_id,
    }


def _evidence_value(value: EvaluationSessionEvidence) -> dict[str, object]:
    return {
        "actionable_listing_ids": list(value.actionable_listing_ids),
        "calendar_snapshot_id": value.calendar_snapshot_id,
        "cutoff": value.cutoff.isoformat(),
        "evidence_id": value.evidence_id,
        "explicit_nontrading_listing_ids": list(
            value.explicit_nontrading_listing_ids
        ),
        "market_session": value.market_session.isoformat(),
        "price_snapshot_id": value.price_snapshot_id,
        "price_source_artifact_id": value.price_source_artifact_id,
        "price_source_snapshot_ids": list(value.price_source_snapshot_ids),
        "tick_size_specification_ids": list(value.tick_size_specification_ids),
        "universe_snapshot_id": value.universe_snapshot_id,
    }


def _tick_size_value(value: EffectiveTickSize) -> dict[str, object]:
    return {
        "effective_from_session": value.effective_from_session.isoformat(),
        "effective_to_exclusive": (
            value.effective_to_exclusive.isoformat()
            if value.effective_to_exclusive is not None
            else None
        ),
        "instrument_id": value.instrument_id,
        "knowledge_time": value.knowledge_time.isoformat(),
        "listing_id": value.listing_id,
        "readiness": value.readiness.value,
        "schema_version": value.schema_version,
        "source_snapshot_id": value.source_snapshot_id,
        "specification_id": value.specification_id,
        "tick_size": str(value.tick_size),
    }


def encode_evaluation_dataset(value: AssembledEvaluationDataset) -> bytes:
    if type(value) is not AssembledEvaluationDataset:
        raise TypeError("evaluation dataset must be exact")
    value.verify_content_identity()
    payload = {
        "assembly": {
            "assembly_id": value.assembly_id,
            "instruments": [_instrument_value(item) for item in value.instruments],
            "schema_version": value.schema_version,
            "session_evidence": [
                _evidence_value(item) for item in value.session_evidence
            ],
            "tick_sizes": [_tick_size_value(item) for item in value.tick_sizes],
        },
        "dataset": {
            "bars": [_bar_value(item) for item in value.dataset.bars],
            "dataset_id": value.dataset.dataset_id,
            "readiness": value.dataset.readiness.value,
            "sessions": [item.isoformat() for item in value.dataset.sessions],
            "source_snapshot_ids": list(value.dataset.source_snapshot_ids),
            "universe_snapshot_ids": list(value.dataset.universe_snapshot_ids),
        },
        "store_schema_version": EVALUATION_DATASET_STORE_SCHEMA_VERSION,
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


def _decode_bar(raw: object) -> SimulationBar:
    value = _keys(
        raw,
        {
            "bar_id",
            "close",
            "high",
            "low",
            "lower_circuit_sell_locked",
            "open",
            "session",
            "symbol",
            "tradable",
            "volume",
        },
        "simulation bar",
    )
    result = SimulationBar(
        session=date.fromisoformat(value["session"]),
        symbol=value["symbol"],
        open=Decimal(value["open"]),
        high=Decimal(value["high"]),
        low=Decimal(value["low"]),
        close=Decimal(value["close"]),
        volume=value["volume"],
        tradable=value["tradable"],
        lower_circuit_sell_locked=value["lower_circuit_sell_locked"],
    )
    if result.bar_id != value["bar_id"]:
        raise EvaluationDatasetStoreConflict("stored simulation bar identity differs")
    return result


def _decode_instrument(raw: object) -> PointInTimeInstrument:
    value = _keys(
        raw,
        {
            "eligibility_bindings",
            "eligible_sessions",
            "instrument_id",
            "isin",
            "stable_instrument_id",
            "symbol",
            "tick_size",
            "universe_snapshot_id",
        },
        "evaluation instrument",
    )
    if type(value["eligibility_bindings"]) is not list or any(
        type(item) is not list or len(item) != 2
        for item in value["eligibility_bindings"]
    ):
        raise EvaluationDatasetStoreConflict(
            "stored instrument eligibility bindings are invalid"
        )
    result = PointInTimeInstrument(
        symbol=value["symbol"],
        isin=value["isin"],
        universe_snapshot_id=value["universe_snapshot_id"],
        eligible_sessions=tuple(
            date.fromisoformat(item) for item in value["eligible_sessions"]
        ),
        tick_size=Decimal(value["tick_size"]),
        stable_instrument_id=value["stable_instrument_id"],
        eligibility_bindings=tuple(
            (date.fromisoformat(item[0]), item[1])
            for item in value["eligibility_bindings"]
        ),
    )
    if result.instrument_id != value["instrument_id"]:
        raise EvaluationDatasetStoreConflict(
            "stored evaluation instrument identity differs"
        )
    return result


def _decode_evidence(raw: object) -> EvaluationSessionEvidence:
    value = _keys(
        raw,
        {
            "actionable_listing_ids",
            "calendar_snapshot_id",
            "cutoff",
            "evidence_id",
            "explicit_nontrading_listing_ids",
            "market_session",
            "price_snapshot_id",
            "price_source_artifact_id",
            "price_source_snapshot_ids",
            "tick_size_specification_ids",
            "universe_snapshot_id",
        },
        "session evidence",
    )
    result = EvaluationSessionEvidence(
        market_session=date.fromisoformat(value["market_session"]),
        calendar_snapshot_id=value["calendar_snapshot_id"],
        universe_snapshot_id=value["universe_snapshot_id"],
        price_snapshot_id=value["price_snapshot_id"],
        price_source_artifact_id=value["price_source_artifact_id"],
        price_source_snapshot_ids=tuple(value["price_source_snapshot_ids"]),
        cutoff=datetime.fromisoformat(value["cutoff"]),
        actionable_listing_ids=tuple(value["actionable_listing_ids"]),
        explicit_nontrading_listing_ids=tuple(
            value["explicit_nontrading_listing_ids"]
        ),
        tick_size_specification_ids=tuple(value["tick_size_specification_ids"]),
    )
    if result.evidence_id != value["evidence_id"]:
        raise EvaluationDatasetStoreConflict("stored session evidence identity differs")
    return result


def _decode_tick_size(raw: object) -> EffectiveTickSize:
    value = _keys(
        raw,
        {
            "effective_from_session",
            "effective_to_exclusive",
            "instrument_id",
            "knowledge_time",
            "listing_id",
            "readiness",
            "schema_version",
            "source_snapshot_id",
            "specification_id",
            "tick_size",
        },
        "tick-size specification",
    )
    result = EffectiveTickSize(
        instrument_id=value["instrument_id"],
        listing_id=value["listing_id"],
        effective_from_session=date.fromisoformat(value["effective_from_session"]),
        effective_to_exclusive=(
            date.fromisoformat(value["effective_to_exclusive"])
            if value["effective_to_exclusive"] is not None
            else None
        ),
        tick_size=Decimal(value["tick_size"]),
        knowledge_time=datetime.fromisoformat(value["knowledge_time"]),
        source_snapshot_id=value["source_snapshot_id"],
        readiness=ReferenceReadiness(value["readiness"]),
        schema_version=value["schema_version"],
    )
    if result.specification_id != value["specification_id"]:
        raise EvaluationDatasetStoreConflict(
            "stored tick-size specification identity differs"
        )
    return result


def decode_evaluation_dataset(payload: bytes) -> AssembledEvaluationDataset:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root = _keys(
            raw,
            {"assembly", "dataset", "store_schema_version"},
            "evaluation dataset",
        )
        if root["store_schema_version"] != EVALUATION_DATASET_STORE_SCHEMA_VERSION:
            raise EvaluationDatasetStoreConflict(
                "unsupported evaluation-dataset store schema"
            )
        dataset_value = _keys(
            root["dataset"],
            {
                "bars",
                "dataset_id",
                "readiness",
                "sessions",
                "source_snapshot_ids",
                "universe_snapshot_ids",
            },
            "dataset payload",
        )
        dataset = EvaluationDataset(
            sessions=tuple(date.fromisoformat(item) for item in dataset_value["sessions"]),
            bars=tuple(_decode_bar(item) for item in dataset_value["bars"]),
            source_snapshot_ids=tuple(dataset_value["source_snapshot_ids"]),
            universe_snapshot_ids=tuple(dataset_value["universe_snapshot_ids"]),
            readiness=EvaluationDataReadiness(dataset_value["readiness"]),
        )
        if dataset.dataset_id != dataset_value["dataset_id"]:
            raise EvaluationDatasetStoreConflict("stored dataset identity differs")
        assembly_value = _keys(
            root["assembly"],
            {
                "assembly_id",
                "instruments",
                "schema_version",
                "session_evidence",
                "tick_sizes",
            },
            "assembly payload",
        )
        result = AssembledEvaluationDataset(
            dataset=dataset,
            instruments=tuple(
                _decode_instrument(item) for item in assembly_value["instruments"]
            ),
            session_evidence=tuple(
                _decode_evidence(item) for item in assembly_value["session_evidence"]
            ),
            tick_sizes=tuple(
                _decode_tick_size(item) for item in assembly_value["tick_sizes"]
            ),
            schema_version=assembly_value["schema_version"],
        )
        if result.assembly_id != assembly_value["assembly_id"]:
            raise EvaluationDatasetStoreConflict("stored assembly identity differs")
        return result
    except EvaluationDatasetStoreConflict:
        raise
    except EvaluationDatasetAssemblyError as exc:
        raise EvaluationDatasetStoreConflict(
            "stored evaluation dataset violates assembly invariants"
        ) from exc
    except (
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise EvaluationDatasetStoreConflict(
            "stored evaluation dataset is invalid"
        ) from exc


class LocalEvaluationDatasetStore:
    """Create-once content-addressed store for assembled evaluation datasets."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def datasets_root(self) -> Path:
        return self.root / "datasets"

    def path_for(self, assembly_id: str) -> Path:
        if not isinstance(assembly_id, str) or _SHA256.fullmatch(assembly_id) is None:
            raise EvaluationDatasetAssemblyError(
                "assembly_id must be a full lowercase SHA-256"
            )
        return self.datasets_root / f"{assembly_id}.json"

    def put(self, value: AssembledEvaluationDataset) -> AssembledEvaluationDataset:
        if type(value) is not AssembledEvaluationDataset:
            raise TypeError("evaluation dataset must be exact")
        value.verify_content_identity()
        self.datasets_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.datasets_root):
            raise EvaluationDatasetStoreConflict("dataset root cannot be a link")
        target = self.path_for(value.assembly_id)
        payload = encode_evaluation_dataset(value)
        try:
            with advisory_file_lock(self.datasets_root / ".datasets.lock"):
                if target.exists():
                    stored = self.get(value.assembly_id)
                    if stored != value:
                        raise EvaluationDatasetStoreConflict(
                            "assembly ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".dataset-", suffix=".tmp", dir=self.datasets_root
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
        except (FileLockUnavailable, FileSafetyError) as exc:
            raise EvaluationDatasetStoreConflict("dataset store unavailable") from exc
        return self.get(value.assembly_id)

    def get(self, assembly_id: str) -> AssembledEvaluationDataset:
        path = self.path_for(assembly_id)
        if not path.exists():
            raise EvaluationDatasetNotFound(assembly_id)
        if not path.is_file() or _is_link_like(path):
            raise EvaluationDatasetStoreConflict(
                "evaluation dataset must be a regular file"
            )
        try:
            value = decode_evaluation_dataset(
                read_stable_regular_file(path, maximum_bytes=_MAX_DATASET_BYTES)
            )
        except FileSafetyError as exc:
            raise EvaluationDatasetStoreConflict(
                "evaluation dataset could not be read safely"
            ) from exc
        if value.assembly_id != assembly_id:
            raise EvaluationDatasetStoreConflict(
                "evaluation dataset differs from its path"
            )
        return value

    def list_datasets(self) -> tuple[AssembledEvaluationDataset, ...]:
        if not self.datasets_root.exists():
            return ()
        if not self.datasets_root.is_dir() or _is_link_like(self.datasets_root):
            raise EvaluationDatasetStoreConflict("dataset root must be a directory")
        values = []
        for path in sorted(self.datasets_root.iterdir(), key=lambda item: item.name):
            if path.name == ".datasets.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise EvaluationDatasetStoreConflict("dataset file set is invalid")
            values.append(self.get(path.stem))
        return tuple(values)
