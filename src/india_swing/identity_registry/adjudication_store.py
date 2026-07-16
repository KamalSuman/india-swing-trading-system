from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from datetime import date, datetime
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.reference.models import ReferenceReadiness

from .adjudication import (
    IDENTITY_ADJUDICATION_POLICY_VERSION,
    IDENTITY_ADJUDICATION_SCHEMA_VERSION,
    IdentityAdjudicationCase,
    IdentityAdjudicationError,
    IdentityAdjudicationQueue,
    IdentityAdjudicationRequirement,
    IdentityAdjudicationState,
    build_identity_adjudication_queue,
)
from .artifact_store import LocalIdentityRegistryStore
from .models import IdentityCandidateBasis, IdentityCandidateStatus


IDENTITY_ADJUDICATION_STORE_SCHEMA_VERSION = "local-identity-adjudication-queue/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_QUEUE_BYTES = 256 * 1024 * 1024


class IdentityAdjudicationStoreConflict(IdentityAdjudicationError):
    pass


class IdentityAdjudicationQueueNotFound(IdentityAdjudicationError):
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
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise IdentityAdjudicationStoreConflict(
                "adjudication queue contains a duplicate JSON key"
            )
        value[key] = item
    return value


def _keys(value: object, expected: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise IdentityAdjudicationStoreConflict(f"stored {name} has invalid fields")
    return value


def _case_value(value: IdentityAdjudicationCase) -> dict[str, object]:
    return {
        "basis": value.basis.value,
        "candidate_id": value.candidate_id,
        "candidate_status": value.candidate_status.value,
        "case_id": value.case_id,
        "conflict_ids": list(value.conflict_ids),
        "observation_claims": [
            [observation_id, claimed_date.isoformat()]
            for observation_id, claimed_date in value.observation_claims
        ],
        "requirements": [item.value for item in value.requirements],
        "state": value.state.value,
        "transition_ids": list(value.transition_ids),
    }


def encode_identity_adjudication_queue(value: IdentityAdjudicationQueue) -> bytes:
    if type(value) is not IdentityAdjudicationQueue:
        raise TypeError("adjudication queue must be exact")
    value.verify_content_identity()
    payload = {
        "queue": {
            "actionable": value.actionable,
            "cases": [_case_value(item) for item in value.cases],
            "policy_version": value.policy_version,
            "queue_id": value.queue_id,
            "readiness": value.readiness.value,
            "schema_version": value.schema_version,
            "source_artifact_ids": list(value.source_artifact_ids),
            "source_cutoff": value.source_cutoff.isoformat(),
            "source_knowledge_time": value.source_knowledge_time.isoformat(),
            "source_manifest_ids": list(value.source_manifest_ids),
            "source_registry_id": value.source_registry_id,
            "stable_identity_assigned": value.stable_identity_assigned,
        },
        "store_schema_version": IDENTITY_ADJUDICATION_STORE_SCHEMA_VERSION,
    }
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _decode_case(raw: object) -> IdentityAdjudicationCase:
    value = _keys(
        raw,
        {
            "basis",
            "candidate_id",
            "candidate_status",
            "case_id",
            "conflict_ids",
            "observation_claims",
            "requirements",
            "state",
            "transition_ids",
        },
        "adjudication case",
    )
    if type(value["observation_claims"]) is not list or any(
        type(item) is not list or len(item) != 2
        for item in value["observation_claims"]
    ):
        raise IdentityAdjudicationStoreConflict(
            "stored adjudication observation claims are invalid"
        )
    result = IdentityAdjudicationCase(
        candidate_id=value["candidate_id"],
        basis=IdentityCandidateBasis(value["basis"]),
        candidate_status=IdentityCandidateStatus(value["candidate_status"]),
        observation_claims=tuple(
            (item[0], date.fromisoformat(item[1]))
            for item in value["observation_claims"]
        ),
        transition_ids=tuple(value["transition_ids"]),
        conflict_ids=tuple(value["conflict_ids"]),
        requirements=tuple(
            IdentityAdjudicationRequirement(item) for item in value["requirements"]
        ),
        state=IdentityAdjudicationState(value["state"]),
    )
    if result.case_id != value["case_id"]:
        raise IdentityAdjudicationStoreConflict(
            "stored adjudication case identity differs"
        )
    return result


def decode_identity_adjudication_queue(payload: bytes) -> IdentityAdjudicationQueue:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root = _keys(
            raw,
            {"queue", "store_schema_version"},
            "adjudication queue",
        )
        if root["store_schema_version"] != IDENTITY_ADJUDICATION_STORE_SCHEMA_VERSION:
            raise IdentityAdjudicationStoreConflict(
                "unsupported adjudication store schema"
            )
        value = _keys(
            root["queue"],
            {
                "actionable",
                "cases",
                "policy_version",
                "queue_id",
                "readiness",
                "schema_version",
                "source_artifact_ids",
                "source_cutoff",
                "source_knowledge_time",
                "source_manifest_ids",
                "source_registry_id",
                "stable_identity_assigned",
            },
            "adjudication queue payload",
        )
        result = IdentityAdjudicationQueue(
            source_registry_id=value["source_registry_id"],
            source_cutoff=datetime.fromisoformat(value["source_cutoff"]),
            source_knowledge_time=datetime.fromisoformat(
                value["source_knowledge_time"]
            ),
            source_artifact_ids=tuple(value["source_artifact_ids"]),
            source_manifest_ids=tuple(value["source_manifest_ids"]),
            cases=tuple(_decode_case(item) for item in value["cases"]),
            readiness=ReferenceReadiness(value["readiness"]),
            actionable=value["actionable"],
            stable_identity_assigned=value["stable_identity_assigned"],
            schema_version=value["schema_version"],
            policy_version=value["policy_version"],
        )
        if result.queue_id != value["queue_id"]:
            raise IdentityAdjudicationStoreConflict(
                "stored adjudication queue identity differs"
            )
        return result
    except IdentityAdjudicationStoreConflict:
        raise
    except IdentityAdjudicationError as exc:
        raise IdentityAdjudicationStoreConflict(
            "stored adjudication queue violates its invariants"
        ) from exc
    except (
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise IdentityAdjudicationStoreConflict(
            "stored adjudication queue is invalid"
        ) from exc


class LocalIdentityAdjudicationQueueStore:
    """Create-once queues that replay from one sealed identity registry."""

    def __init__(
        self,
        root: Path,
        registry_store: LocalIdentityRegistryStore,
    ) -> None:
        self.root = Path(root)
        if type(registry_store) is not LocalIdentityRegistryStore:
            raise TypeError("registry_store must be exact")
        if self.root.resolve() != registry_store.root.resolve():
            raise ValueError("queue and registry stores must share one identity root")
        self.registry_store = registry_store

    @property
    def queues_root(self) -> Path:
        return self.root / "identity-adjudication-queues"

    def path_for(self, registry_id: str) -> Path:
        if not isinstance(registry_id, str) or _SHA256.fullmatch(registry_id) is None:
            raise IdentityAdjudicationError(
                "registry_id must be a full lowercase SHA-256"
            )
        return self.queues_root / f"{registry_id}.json"

    @staticmethod
    def _read_file(path: Path) -> IdentityAdjudicationQueue:
        if not path.is_file() or _is_link_like(path):
            raise IdentityAdjudicationStoreConflict(
                "adjudication queue must be a regular file"
            )
        try:
            return decode_identity_adjudication_queue(
                read_stable_regular_file(path, maximum_bytes=_MAX_QUEUE_BYTES)
            )
        except FileSafetyError as exc:
            raise IdentityAdjudicationStoreConflict(
                "adjudication queue could not be read safely"
            ) from exc

    def publish(
        self,
        queue: IdentityAdjudicationQueue,
        *,
        registry_id: str,
    ) -> IdentityAdjudicationQueue:
        if type(queue) is not IdentityAdjudicationQueue:
            raise TypeError("adjudication queue must be exact")
        queue.verify_content_identity()
        registry = self.registry_store.get(registry_id).registry
        expected = build_identity_adjudication_queue(registry)
        if queue != expected or queue.source_registry_id != registry_id:
            raise IdentityAdjudicationStoreConflict(
                "adjudication queue differs from its persisted registry"
            )
        self.queues_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.queues_root):
            raise IdentityAdjudicationStoreConflict("adjudication root cannot be a link")
        target = self.path_for(registry_id)
        payload = encode_identity_adjudication_queue(queue)
        try:
            with advisory_file_lock(self.queues_root / ".adjudication-queues.lock"):
                if target.exists():
                    stored = self._read_file(target)
                    if stored != queue:
                        raise IdentityAdjudicationStoreConflict(
                            "registry already stores a different adjudication queue"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".adjudication-", suffix=".tmp", dir=self.queues_root
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
            raise IdentityAdjudicationStoreConflict(
                "adjudication queue store unavailable"
            ) from exc
        stored = self._read_file(target)
        if stored != expected:
            raise IdentityAdjudicationStoreConflict(
                "published adjudication queue differs from its registry"
            )
        return stored

    def get(self, registry_id: str) -> IdentityAdjudicationQueue:
        path = self.path_for(registry_id)
        if not path.exists():
            raise IdentityAdjudicationQueueNotFound(registry_id)
        queue = self._read_file(path)
        registry = self.registry_store.get(registry_id).registry
        expected = build_identity_adjudication_queue(registry)
        if queue != expected or queue.source_registry_id != registry_id:
            raise IdentityAdjudicationStoreConflict(
                "stored adjudication queue does not replay from its registry"
            )
        return queue

    def list_queues(self) -> tuple[IdentityAdjudicationQueue, ...]:
        if not self.queues_root.exists():
            return ()
        if not self.queues_root.is_dir() or _is_link_like(self.queues_root):
            raise IdentityAdjudicationStoreConflict(
                "adjudication queue root must be a directory"
            )
        values = []
        for path in sorted(self.queues_root.iterdir(), key=lambda item: item.name):
            if path.name == ".adjudication-queues.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise IdentityAdjudicationStoreConflict(
                    "adjudication queue file set is invalid"
                )
            values.append(self.get(path.stem))
        return tuple(values)
