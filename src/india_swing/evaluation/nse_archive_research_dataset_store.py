"""Canonical codec and exact-ID local store for NseArchiveResearchDataset.

This module never lists, discovers, or selects a latest artifact: ``get`` is
exact-ID only, and ``put`` is create-once and idempotent -- an artifact that
already exists is reloaded and byte/content compared, never overwritten. The
codec accepts only the exact, already-self-verifying ``NseArchiveResearchDataset``
domain type on encode, and on decode independently reconstructs every nested
object through the same domain constructors (which re-derive and check their
own content-addressed identities), then requires byte-for-byte canonical
re-encoding equality before trusting anything read back from disk.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from datetime import date
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .nse_archive_research_dataset import (
    NseArchiveResearchDataset,
    NseArchiveResearchDatasetError,
    NseArchiveResearchDatasetSplitPartition,
    NseArchiveResearchRangeBinding,
    ResearchArchiveExclusion,
    ResearchArchiveExclusionReason,
    ResearchArchiveSplitPolicy,
    ResearchSplitRole,
)


NSE_ARCHIVE_RESEARCH_DATASET_STORE_SCHEMA_VERSION = (
    "nse-archive-research-dataset-store/v1"
)
NSE_ARCHIVE_RESEARCH_DATASET_STORE_DIRECTORY = "nse-archive-research-datasets"
MAXIMUM_MANIFEST_BYTES = 2 * 1024 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_ROOT_KEYS = {
    "store_schema_version",
    "dataset_id",
    "index_snapshot_ids",
    "range_bindings",
    "accepted_sessions",
    "session_snapshot_ids",
    "exclusions",
    "partitions",
    "record_count",
    "identity_issue_count",
    "identity_quarantined_session_count",
    "incomplete_evidence_session_count",
    "evidence_profile_counts",
    "split_policy",
    "split_policy_id",
    "collection_only",
    "actionable",
    "training_eligible",
    "feature_eligible",
    "label_eligible",
    "alert_eligible",
    "execution_eligible",
    "identity_resolution_complete",
    "corporate_action_adjustment_complete",
    "coverage_complete",
}
_BINDING_KEYS = {
    "index_snapshot_id",
    "range_start",
    "range_end",
    "session_snapshot_ids",
    "accepted_sessions",
    "record_count",
    "identity_issue_count",
    "identity_quarantined_session_count",
    "incomplete_evidence_session_count",
    "evidence_profile_counts",
    "binding_id",
}
_EXCLUSION_KEYS = {"session", "reason", "exclusion_id"}
_PARTITION_KEYS = {
    "role",
    "sessions",
    "candidate_label_origin_sessions",
    "unavailable_label_tail_sessions",
    "maximum_forward_label_horizon_sessions",
    "partition_id",
}
_POLICY_KEYS = {
    "train_end",
    "validation_start",
    "validation_end",
    "test_start",
    "maximum_forward_label_horizon_sessions",
    "policy_id",
}
_SAFETY_FLAG_NAMES = (
    "collection_only",
    "actionable",
    "training_eligible",
    "feature_eligible",
    "label_eligible",
    "alert_eligible",
    "execution_eligible",
    "identity_resolution_complete",
    "corporate_action_adjustment_complete",
    "coverage_complete",
)


class NseArchiveResearchDatasetStoreError(NseArchiveResearchDatasetError):
    """A research dataset store input or artifact failed a static safety rule."""


class NseArchiveResearchDatasetStoreConflict(NseArchiveResearchDatasetStoreError):
    """A stored or requested artifact is missing, tampered, or in conflict."""


class NseArchiveResearchDatasetStoreNotFound(NseArchiveResearchDatasetStoreError):
    """No artifact exists for the requested exact dataset ID."""


# --- canonical codec ---------------------------------------------------------


def _profile_counts_value(value: tuple[tuple[str, int], ...]) -> list[list[object]]:
    return [[profile, count] for profile, count in value]


def _binding_value(value: NseArchiveResearchRangeBinding) -> dict[str, object]:
    return {
        "index_snapshot_id": value.index_snapshot_id,
        "range_start": value.range_start.isoformat(),
        "range_end": value.range_end.isoformat(),
        "session_snapshot_ids": list(value.session_snapshot_ids),
        "accepted_sessions": [item.isoformat() for item in value.accepted_sessions],
        "record_count": value.record_count,
        "identity_issue_count": value.identity_issue_count,
        "identity_quarantined_session_count": value.identity_quarantined_session_count,
        "incomplete_evidence_session_count": value.incomplete_evidence_session_count,
        "evidence_profile_counts": _profile_counts_value(value.evidence_profile_counts),
        "binding_id": value.binding_id,
    }


def _exclusion_value(value: ResearchArchiveExclusion) -> dict[str, object]:
    return {
        "session": value.session.isoformat(),
        "reason": value.reason.value,
        "exclusion_id": value.exclusion_id,
    }


def _partition_value(
    value: NseArchiveResearchDatasetSplitPartition,
) -> dict[str, object]:
    return {
        "role": value.role.value,
        "sessions": [item.isoformat() for item in value.sessions],
        "candidate_label_origin_sessions": [
            item.isoformat() for item in value.candidate_label_origin_sessions
        ],
        "unavailable_label_tail_sessions": [
            item.isoformat() for item in value.unavailable_label_tail_sessions
        ],
        "maximum_forward_label_horizon_sessions": (
            value.maximum_forward_label_horizon_sessions
        ),
        "partition_id": value.partition_id,
    }


def _policy_value(value: ResearchArchiveSplitPolicy) -> dict[str, object]:
    return {
        "train_end": value.train_end.isoformat(),
        "validation_start": value.validation_start.isoformat(),
        "validation_end": value.validation_end.isoformat(),
        "test_start": value.test_start.isoformat(),
        "maximum_forward_label_horizon_sessions": (
            value.maximum_forward_label_horizon_sessions
        ),
        "policy_id": value.policy_id,
    }


def encode_nse_archive_research_dataset(value: NseArchiveResearchDataset) -> bytes:
    if type(value) is not NseArchiveResearchDataset:
        raise TypeError("research dataset must be exact")
    value.verify_content_identity()
    payload = {
        "store_schema_version": NSE_ARCHIVE_RESEARCH_DATASET_STORE_SCHEMA_VERSION,
        "dataset_id": value.dataset_id,
        "index_snapshot_ids": list(value.index_snapshot_ids),
        "range_bindings": [_binding_value(item) for item in value.range_bindings],
        "accepted_sessions": [item.isoformat() for item in value.accepted_sessions],
        "session_snapshot_ids": list(value.session_snapshot_ids),
        "exclusions": [_exclusion_value(item) for item in value.exclusions],
        "partitions": [_partition_value(item) for item in value.partitions],
        "record_count": value.record_count,
        "identity_issue_count": value.identity_issue_count,
        "identity_quarantined_session_count": value.identity_quarantined_session_count,
        "incomplete_evidence_session_count": value.incomplete_evidence_session_count,
        "evidence_profile_counts": _profile_counts_value(value.evidence_profile_counts),
        "split_policy": _policy_value(value.split_policy),
        "split_policy_id": value.split_policy_id,
        "collection_only": value.collection_only,
        "actionable": value.actionable,
        "training_eligible": value.training_eligible,
        "feature_eligible": value.feature_eligible,
        "label_eligible": value.label_eligible,
        "alert_eligible": value.alert_eligible,
        "execution_eligible": value.execution_eligible,
        "identity_resolution_complete": value.identity_resolution_complete,
        "corporate_action_adjustment_complete": (
            value.corporate_action_adjustment_complete
        ),
        "coverage_complete": value.coverage_complete,
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


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _keys(value: object, expected: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise ValueError("unexpected field set")
    return value


def _date_value(value: object) -> date:
    if type(value) is not str:
        raise ValueError("date must be a string")
    return date.fromisoformat(value)


def _str_tuple(value: object) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str for item in value):
        raise ValueError("expected a list of strings")
    return tuple(value)


def _date_tuple(value: object) -> tuple[date, ...]:
    if type(value) is not list:
        raise ValueError("expected a list of dates")
    return tuple(_date_value(item) for item in value)


def _profile_counts(value: object) -> tuple[tuple[str, int], ...]:
    if type(value) is not list:
        raise ValueError("expected an evidence profile count list")
    seen: dict[str, int] = {}
    for item in value:
        if (
            type(item) is not list
            or len(item) != 2
            or type(item[0]) is not str
            or type(item[1]) is not int
        ):
            raise ValueError("invalid evidence profile count entry")
        profile, count = item
        if profile in seen:
            raise ValueError("duplicate evidence profile count entry")
        seen[profile] = count
    return tuple(sorted(seen.items()))


def _decode_binding(raw: object) -> NseArchiveResearchRangeBinding:
    value = _keys(raw, _BINDING_KEYS)
    result = NseArchiveResearchRangeBinding(
        index_snapshot_id=value["index_snapshot_id"],
        range_start=_date_value(value["range_start"]),
        range_end=_date_value(value["range_end"]),
        session_snapshot_ids=_str_tuple(value["session_snapshot_ids"]),
        accepted_sessions=_date_tuple(value["accepted_sessions"]),
        record_count=value["record_count"],
        identity_issue_count=value["identity_issue_count"],
        identity_quarantined_session_count=value["identity_quarantined_session_count"],
        incomplete_evidence_session_count=value["incomplete_evidence_session_count"],
        evidence_profile_counts=_profile_counts(value["evidence_profile_counts"]),
    )
    if result.binding_id != value["binding_id"]:
        raise ValueError("claimed binding identity differs")
    return result


def _decode_exclusion(raw: object) -> ResearchArchiveExclusion:
    value = _keys(raw, _EXCLUSION_KEYS)
    if type(value["reason"]) is not str:
        raise ValueError("exclusion reason must be a string")
    result = ResearchArchiveExclusion(
        session=_date_value(value["session"]),
        reason=ResearchArchiveExclusionReason(value["reason"]),
    )
    if result.exclusion_id != value["exclusion_id"]:
        raise ValueError("claimed exclusion identity differs")
    return result


def _decode_partition(raw: object) -> NseArchiveResearchDatasetSplitPartition:
    value = _keys(raw, _PARTITION_KEYS)
    if type(value["role"]) is not str:
        raise ValueError("partition role must be a string")
    result = NseArchiveResearchDatasetSplitPartition(
        role=ResearchSplitRole(value["role"]),
        sessions=_date_tuple(value["sessions"]),
        candidate_label_origin_sessions=_date_tuple(
            value["candidate_label_origin_sessions"]
        ),
        unavailable_label_tail_sessions=_date_tuple(
            value["unavailable_label_tail_sessions"]
        ),
        maximum_forward_label_horizon_sessions=value[
            "maximum_forward_label_horizon_sessions"
        ],
    )
    if result.partition_id != value["partition_id"]:
        raise ValueError("claimed partition identity differs")
    return result


def _decode_policy(raw: object) -> ResearchArchiveSplitPolicy:
    value = _keys(raw, _POLICY_KEYS)
    result = ResearchArchiveSplitPolicy(
        train_end=_date_value(value["train_end"]),
        validation_start=_date_value(value["validation_start"]),
        validation_end=_date_value(value["validation_end"]),
        test_start=_date_value(value["test_start"]),
        maximum_forward_label_horizon_sessions=value[
            "maximum_forward_label_horizon_sessions"
        ],
    )
    if result.policy_id != value["policy_id"]:
        raise ValueError("claimed split policy identity differs")
    return result


def decode_nse_archive_research_dataset(payload: bytes) -> NseArchiveResearchDataset:
    """Reconstruct and fully re-verify a research dataset from stored bytes.

    Every failure mode -- malformed JSON, duplicate keys, floats/NaN/Infinity,
    a wrong/missing/extra field, an invalid enum/date/int/SHA/schema value, a
    claimed ID that does not match its reconstructed value, a claimed safety
    flag that does not match its always-hard-coded reconstructed value, or a
    canonical re-encoding that does not reproduce the input bytes exactly --
    collapses to the same single static, sanitized error. The error is always
    raised after this function's own try/except has fully exited, so it never
    carries a nested cause/context, even though `from None` alone would not
    be enough for that (raising a fresh exception from inside an active
    except clause still attaches the caught exception as __context__).
    """

    if type(payload) is not bytes or not payload:
        raise NseArchiveResearchDatasetStoreConflict(
            "stored research dataset is invalid"
        )
    if len(payload) > MAXIMUM_MANIFEST_BYTES:
        raise NseArchiveResearchDatasetStoreConflict(
            "stored research dataset exceeds its size limit"
        )

    malformed = False
    dataset: NseArchiveResearchDataset | None = None
    try:
        text = payload.decode("utf-8")
        raw = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root = _keys(raw, _ROOT_KEYS)
        if (
            root["store_schema_version"]
            != NSE_ARCHIVE_RESEARCH_DATASET_STORE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported store schema version")

        bindings = tuple(_decode_binding(item) for item in root["range_bindings"])
        exclusions = tuple(_decode_exclusion(item) for item in root["exclusions"])
        partitions = tuple(_decode_partition(item) for item in root["partitions"])
        policy = _decode_policy(root["split_policy"])

        dataset = NseArchiveResearchDataset(
            index_snapshot_ids=_str_tuple(root["index_snapshot_ids"]),
            range_bindings=bindings,
            accepted_sessions=_date_tuple(root["accepted_sessions"]),
            session_snapshot_ids=_str_tuple(root["session_snapshot_ids"]),
            exclusions=exclusions,
            partitions=partitions,
            record_count=root["record_count"],
            identity_issue_count=root["identity_issue_count"],
            identity_quarantined_session_count=root[
                "identity_quarantined_session_count"
            ],
            incomplete_evidence_session_count=root[
                "incomplete_evidence_session_count"
            ],
            evidence_profile_counts=_profile_counts(root["evidence_profile_counts"]),
            split_policy=policy,
        )

        for name in _SAFETY_FLAG_NAMES:
            if getattr(dataset, name) is not root[name]:
                raise ValueError("claimed safety flag differs")
        if dataset.dataset_id != root["dataset_id"]:
            raise ValueError("claimed dataset identity differs")
        if dataset.split_policy_id != root["split_policy_id"]:
            raise ValueError("claimed split policy identity differs")
        if encode_nse_archive_research_dataset(dataset) != payload:
            raise ValueError("canonical re-encoding differs from stored bytes")
    except Exception:
        malformed = True
    if malformed or dataset is None:
        raise NseArchiveResearchDatasetStoreConflict(
            "stored research dataset is invalid"
        )
    return dataset


# --- exact-ID local store -----------------------------------------------------


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


class LocalNseArchiveResearchDatasetStore:
    """Create-once local store; exposes only exact-ID put/get, never a listing."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / NSE_ARCHIVE_RESEARCH_DATASET_STORE_DIRECTORY

    def _verify_dataset_root_boundary(self) -> None:
        """Reject a linked/reparse root or dataset_root before any lock/target use.

        A link check must run before any resolve()-based comparison: resolve()
        itself follows symlinks/junctions, so it can never be the thing that
        proves the boundary is safe -- only that a boundary already found
        link-free is also not silently redirected by some other path-chain
        inconsistency.

        Every filesystem call this method makes (`exists`/`is_dir`/`resolve`)
        is wrapped in one try/except so a raw OSError from any of them -- not
        just `resolve()` -- can never escape uncaught to a caller such as
        `get()`, which calls this method before it has any try block of its
        own. The sanitized error is always raised only after that try/except
        has fully exited, so it never carries a nested cause/context (see the
        module docstring on decode_nse_archive_research_dataset for why
        `from None` alone would not be enough).
        """

        root = self.root
        dataset_root = self.dataset_root
        boundary_failed = False
        invalid = False
        try:
            if _is_link_like(root) or (root.exists() and not root.is_dir()):
                invalid = True
            elif _is_link_like(dataset_root) or (
                dataset_root.exists() and not dataset_root.is_dir()
            ):
                invalid = True
            else:
                resolved_root = root.resolve(strict=False)
                resolved_dataset_root = dataset_root.resolve(strict=False)
                if (
                    resolved_dataset_root
                    != resolved_root / NSE_ARCHIVE_RESEARCH_DATASET_STORE_DIRECTORY
                ):
                    invalid = True
        except OSError:
            boundary_failed = True
        if boundary_failed:
            raise NseArchiveResearchDatasetStoreConflict(
                "research dataset store dataset root could not be verified"
            )
        if invalid:
            raise NseArchiveResearchDatasetStoreConflict(
                "research dataset store dataset root is not a safe directory"
            )

    def _path_for(self, dataset_id: str) -> Path:
        if type(dataset_id) is not str or _SHA256.fullmatch(dataset_id) is None:
            raise NseArchiveResearchDatasetError(
                "dataset_id must be a full lowercase SHA-256"
            )
        return self.dataset_root / f"{dataset_id}.json"

    def put(self, dataset: NseArchiveResearchDataset) -> NseArchiveResearchDataset:
        """Publish a dataset exactly once, never overwriting an existing artifact.

        Publication uses ``os.link`` (never ``os.replace``): a hard link can
        only be created when the target name is absent, so a target that
        appears between our own absence check and the link call -- a genuine
        write-write race, not merely a hypothetical -- makes the link call
        itself fail instead of silently overwriting whatever is already
        there. Every OS/lock failure along the way (boundary checks, mkdir,
        lock acquisition, temp creation/write/fsync/link, cleanup) is
        collected into a flag and only translated into one static sanitized
        error after this method's own try/except has fully exited, so the
        error never carries a nested cause/context and never repeats a raw
        path, errno string, or planted byte content.
        """

        if type(dataset) is not NseArchiveResearchDataset:
            raise TypeError("research dataset must be an exact NseArchiveResearchDataset")
        dataset.verify_content_identity()
        payload = encode_nse_archive_research_dataset(dataset)
        if len(payload) > MAXIMUM_MANIFEST_BYTES:
            raise NseArchiveResearchDatasetError(
                "research dataset manifest exceeds its size limit"
            )

        store_unavailable = False
        try:
            self._verify_dataset_root_boundary()
            self.dataset_root.mkdir(parents=True, exist_ok=True)
            self._verify_dataset_root_boundary()
            target = self._path_for(dataset.dataset_id)
            lock = (
                self.dataset_root
                / f".{NSE_ARCHIVE_RESEARCH_DATASET_STORE_DIRECTORY}.lock"
            )
            with advisory_file_lock(lock):
                if not target.exists():
                    descriptor, temporary_name = tempfile.mkstemp(
                        prefix=".nse-archive-research-dataset-",
                        suffix=".tmp",
                        dir=self.dataset_root,
                    )
                    temporary = Path(temporary_name)
                    try:
                        with os.fdopen(descriptor, "wb") as handle:
                            handle.write(payload)
                            handle.flush()
                            os.fsync(handle.fileno())
                        if _is_link_like(temporary):
                            raise OSError("temporary artifact is unsafe")
                        try:
                            os.link(temporary, target)
                        except FileExistsError:
                            # Another writer published this exact target
                            # between our absence check and this call. Never
                            # overwrite it -- fall through to the reload and
                            # content-equality check below, exactly as if
                            # target.exists() had been true from the start.
                            pass
                    finally:
                        temporary.unlink(missing_ok=True)
        except (FileLockUnavailable, FileSafetyError, OSError):
            store_unavailable = True
        if store_unavailable:
            raise NseArchiveResearchDatasetStoreConflict(
                "research dataset store is unavailable"
            )

        stored = self.get(dataset.dataset_id)
        if stored != dataset:
            raise NseArchiveResearchDatasetStoreConflict(
                "dataset ID already stores different content"
            )
        return stored

    def get(self, dataset_id: str) -> NseArchiveResearchDataset:
        target = self._path_for(dataset_id)
        self._verify_dataset_root_boundary()

        existence_failed = False
        exists = False
        try:
            exists = target.exists()
        except OSError:
            existence_failed = True
        if existence_failed:
            raise NseArchiveResearchDatasetStoreConflict(
                "research dataset store is unavailable"
            )
        if not exists:
            raise NseArchiveResearchDatasetStoreNotFound(
                "research dataset was not found"
            )

        stat_failed = False
        is_regular = False
        try:
            is_regular = not _is_link_like(target) and stat.S_ISREG(
                target.stat().st_mode
            )
        except OSError:
            stat_failed = True
        if stat_failed:
            raise NseArchiveResearchDatasetStoreConflict(
                "research dataset store is unavailable"
            )
        if not is_regular:
            raise NseArchiveResearchDatasetStoreConflict(
                "research dataset artifact must be a regular file"
            )

        read_failed = False
        payload = None
        try:
            payload = read_stable_regular_file(
                target, maximum_bytes=MAXIMUM_MANIFEST_BYTES
            )
        except (FileSafetyError, OSError):
            read_failed = True
        if read_failed:
            raise NseArchiveResearchDatasetStoreConflict(
                "research dataset artifact could not be read safely"
            )

        dataset = decode_nse_archive_research_dataset(payload)
        if dataset.dataset_id != dataset_id:
            raise NseArchiveResearchDatasetStoreConflict(
                "research dataset artifact identity failed"
            )
        return dataset
