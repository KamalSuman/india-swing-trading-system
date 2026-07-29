"""Durable manifest for a fully persisted promoted walk-forward run."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id

from .baseline_store import LocalDeterministicComparisonRunStore
from .promoted_intent_store import LocalPromotedResearchIntentStore
from .promoted_walk_forward import (
    PROMOTED_WALK_FORWARD_POLICY_VERSION,
    PromotedFoldCrossSectionBinding,
    PromotedWalkForwardEvaluationRun,
    PromotedWalkForwardStrategyRun,
)


class PromotedWalkForwardStoreError(ValueError):
    pass


class PromotedWalkForwardRunConflict(PromotedWalkForwardStoreError):
    pass


class PromotedWalkForwardRunNotFound(PromotedWalkForwardStoreError):
    pass


PROMOTED_WALK_FORWARD_RUN_STORE_SCHEMA_VERSION = (
    "promoted-walk-forward-run-store/v1"
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_BYTES = 8 * 1024 * 1024


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _unique_object(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedWalkForwardRunConflict(
                "promoted run manifest has duplicate keys"
            )
        result[key] = value
    return result


def _object(
    value: object,
    expected: set[str],
    name: str,
) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise PromotedWalkForwardRunConflict(
            f"promoted run {name} has invalid fields"
        )
    return value


def _iso_date(value: object) -> date:
    if type(value) is not str:
        raise PromotedWalkForwardRunConflict(
            "promoted run signal session is invalid"
        )
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise PromotedWalkForwardRunConflict(
            "promoted run signal session is invalid"
        ) from None
    if result.isoformat() != value:
        raise PromotedWalkForwardRunConflict(
            "promoted run signal session is noncanonical"
        )
    return result


@dataclass(frozen=True, slots=True)
class PromotedWalkForwardRunManifest:
    trial_id: str
    promoted_run_id: str
    strategy_run_id: str
    deterministic_run_id: str
    strategy_batch_id: str
    comparison_id: str
    binding_ids: tuple[str, ...]
    research_batch_ids: tuple[str, ...]
    bindings: tuple[PromotedFoldCrossSectionBinding, ...]
    manifest_id: str = field(init=False)

    def __post_init__(self) -> None:
        identifiers = (
            self.trial_id,
            self.promoted_run_id,
            self.strategy_run_id,
            self.deterministic_run_id,
            self.strategy_batch_id,
            self.comparison_id,
        )
        if (
            any(
                type(value) is not str
                or _SHA256.fullmatch(value) is None
                for value in identifiers
            )
            or type(self.binding_ids) is not tuple
            or not self.binding_ids
            or self.binding_ids
            != tuple(sorted(set(self.binding_ids)))
            or type(self.research_batch_ids) is not tuple
            or not self.research_batch_ids
            or len(self.research_batch_ids) != len(self.bindings)
            or len(set(self.research_batch_ids))
            != len(self.research_batch_ids)
            or any(
                type(value) is not str
                or _SHA256.fullmatch(value) is None
                for value in (
                    self.binding_ids + self.research_batch_ids
                )
            )
            or type(self.bindings) is not tuple
            or len(self.bindings) != len(self.binding_ids)
            or any(
                type(value) is not PromotedFoldCrossSectionBinding
                for value in self.bindings
            )
            or tuple(
                sorted(value.binding_id for value in self.bindings)
            )
            != self.binding_ids
        ):
            raise PromotedWalkForwardRunConflict(
                "promoted run manifest graph is invalid"
            )
        for value in self.bindings:
            value.verify_content_identity()
        object.__setattr__(self, "manifest_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": (
                    PROMOTED_WALK_FORWARD_RUN_STORE_SCHEMA_VERSION
                ),
                "trial_id": self.trial_id,
                "promoted_run_id": self.promoted_run_id,
                "strategy_run_id": self.strategy_run_id,
                "deterministic_run_id": self.deterministic_run_id,
                "strategy_batch_id": self.strategy_batch_id,
                "comparison_id": self.comparison_id,
                "binding_ids": self.binding_ids,
                "research_batch_ids": self.research_batch_ids,
                "bindings": self.bindings,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.manifest_id != self._calculated_id():
            raise PromotedWalkForwardRunConflict(
                "promoted run manifest identity failed"
            )
        for value in self.bindings:
            value.verify_content_identity()


def _manifest_from_run(
    run: PromotedWalkForwardEvaluationRun,
) -> PromotedWalkForwardRunManifest:
    deterministic = run.deterministic_run
    strategy = run.strategy_run
    return PromotedWalkForwardRunManifest(
        trial_id=deterministic.comparison.trial_id,
        promoted_run_id=run.run_id,
        strategy_run_id=strategy.run_id,
        deterministic_run_id=deterministic.run_id,
        strategy_batch_id=strategy.strategy_batch.batch_id,
        comparison_id=deterministic.comparison.comparison_id,
        binding_ids=tuple(
            sorted(value.binding_id for value in strategy.bindings)
        ),
        research_batch_ids=tuple(
            value.batch_id for value in strategy.research_batches
        ),
        bindings=strategy.bindings,
    )


def encode_promoted_walk_forward_manifest(
    manifest: PromotedWalkForwardRunManifest,
) -> bytes:
    if type(manifest) is not PromotedWalkForwardRunManifest:
        raise TypeError("manifest must be exact")
    manifest.verify_content_identity()
    return (
        json.dumps(
            {
                "store_schema_version": (
                    PROMOTED_WALK_FORWARD_RUN_STORE_SCHEMA_VERSION
                ),
                "trial_id": manifest.trial_id,
                "promoted_run_id": manifest.promoted_run_id,
                "strategy_run_id": manifest.strategy_run_id,
                "deterministic_run_id": (
                    manifest.deterministic_run_id
                ),
                "strategy_batch_id": manifest.strategy_batch_id,
                "comparison_id": manifest.comparison_id,
                "binding_ids": list(manifest.binding_ids),
                "research_batch_ids": list(
                    manifest.research_batch_ids
                ),
                "bindings": [
                    {
                        "fold_id": value.fold_id,
                        "signal_session": (
                            value.signal_session.isoformat()
                        ),
                        "cross_section_panel_id": (
                            value.cross_section_panel_id
                        ),
                        "binding_id": value.binding_id,
                    }
                    for value in manifest.bindings
                ],
                "manifest_id": manifest.manifest_id,
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def decode_promoted_walk_forward_manifest(
    payload: bytes,
) -> PromotedWalkForwardRunManifest:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        expected = {
            "store_schema_version",
            "trial_id",
            "promoted_run_id",
            "strategy_run_id",
            "deterministic_run_id",
            "strategy_batch_id",
            "comparison_id",
            "binding_ids",
            "research_batch_ids",
            "bindings",
            "manifest_id",
        }
        value = _object(raw, expected, "manifest")
        if (
            value["store_schema_version"]
            != PROMOTED_WALK_FORWARD_RUN_STORE_SCHEMA_VERSION
        ):
            raise PromotedWalkForwardRunConflict(
                "promoted run store schema is unsupported"
            )
        if any(
            type(value[name]) is not list
            for name in (
                "binding_ids",
                "research_batch_ids",
                "bindings",
            )
        ):
            raise PromotedWalkForwardRunConflict(
                "promoted run manifest collections are invalid"
            )
        bindings = []
        for item in value["bindings"]:
            raw_binding = _object(
                item,
                {
                    "fold_id",
                    "signal_session",
                    "cross_section_panel_id",
                    "binding_id",
                },
                "fold binding",
            )
            binding = PromotedFoldCrossSectionBinding(
                fold_id=raw_binding["fold_id"],
                signal_session=_iso_date(
                    raw_binding["signal_session"]
                ),
                cross_section_panel_id=raw_binding[
                    "cross_section_panel_id"
                ],
            )
            if binding.binding_id != raw_binding["binding_id"]:
                raise PromotedWalkForwardRunConflict(
                    "promoted fold binding ID differs from content"
                )
            bindings.append(binding)
        manifest = PromotedWalkForwardRunManifest(
            trial_id=value["trial_id"],
            promoted_run_id=value["promoted_run_id"],
            strategy_run_id=value["strategy_run_id"],
            deterministic_run_id=value["deterministic_run_id"],
            strategy_batch_id=value["strategy_batch_id"],
            comparison_id=value["comparison_id"],
            binding_ids=tuple(value["binding_ids"]),
            research_batch_ids=tuple(value["research_batch_ids"]),
            bindings=tuple(bindings),
        )
        if manifest.manifest_id != value["manifest_id"]:
            raise PromotedWalkForwardRunConflict(
                "promoted run manifest ID differs from content"
            )
        return manifest
    except PromotedWalkForwardRunConflict:
        raise
    except (
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        raise PromotedWalkForwardRunConflict(
            "stored promoted run manifest is invalid"
        ) from None


class LocalPromotedWalkForwardRunStore:
    """One promoted run per trial; full replay requires research storage."""

    def __init__(
        self,
        root: Path,
        deterministic_store: LocalDeterministicComparisonRunStore,
        research_store: LocalPromotedResearchIntentStore | None = None,
    ) -> None:
        self.root = Path(root)
        if (
            type(deterministic_store)
            is not LocalDeterministicComparisonRunStore
        ):
            raise TypeError("deterministic_store must be exact")
        if (
            research_store is not None
            and type(research_store)
            is not LocalPromotedResearchIntentStore
        ):
            raise TypeError("research_store must be exact when supplied")
        if self.root.resolve() != deterministic_store.root.resolve():
            raise ValueError(
                "promoted and deterministic stores must share a root"
            )
        if (
            research_store is not None
            and self.root.resolve() != research_store.root.resolve()
        ):
            raise ValueError(
                "promoted and research stores must share a root"
            )
        self.deterministic_store = deterministic_store
        self.research_store = research_store

    @property
    def runs_root(self) -> Path:
        return self.root / "promoted_walk_forward_runs"

    def path_for(self, trial_id: str) -> Path:
        if (
            type(trial_id) is not str
            or _SHA256.fullmatch(trial_id) is None
        ):
            raise PromotedWalkForwardStoreError(
                "trial_id must be a full lowercase SHA-256"
            )
        return self.runs_root / f"{trial_id}.json"

    def publish(
        self,
        run: PromotedWalkForwardEvaluationRun,
    ) -> PromotedWalkForwardEvaluationRun:
        if type(run) is not PromotedWalkForwardEvaluationRun:
            raise TypeError("run must be exact")
        if self.research_store is None:
            raise PromotedWalkForwardStoreError(
                "research store is required to publish a promoted run"
            )
        run.verify_content_identity()
        for batch in run.strategy_run.research_batches:
            self.research_store.require_persisted(batch)
        deterministic = self.deterministic_store.publish(
            run.deterministic_run
        )
        if deterministic != run.deterministic_run:
            raise PromotedWalkForwardRunConflict(
                "persisted deterministic run differs"
            )
        manifest = _manifest_from_run(run)
        payload = encode_promoted_walk_forward_manifest(manifest)
        target = self.path_for(manifest.trial_id)
        self.runs_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.runs_root):
            raise PromotedWalkForwardRunConflict(
                "promoted run store path cannot be a link"
            )
        try:
            with advisory_file_lock(
                self.runs_root / ".promoted-runs.lock"
            ):
                if target.exists():
                    stored = self.get(manifest.trial_id)
                    if stored != run:
                        raise PromotedWalkForwardRunConflict(
                            "trial already stores a different promoted run"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".promoted-run-",
                    suffix=".tmp",
                    dir=self.runs_root,
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
            raise PromotedWalkForwardRunConflict(
                "promoted run store unavailable"
            ) from exc
        return self.get(manifest.trial_id)

    def get_manifest(
        self,
        trial_id: str,
    ) -> PromotedWalkForwardRunManifest:
        path = self.path_for(trial_id)
        if not path.exists():
            raise PromotedWalkForwardRunNotFound(trial_id)
        if not path.is_file() or _is_link_like(path):
            raise PromotedWalkForwardRunConflict(
                "promoted run manifest must be a regular file"
            )
        try:
            payload = read_stable_regular_file(
                path,
                maximum_bytes=_MAXIMUM_BYTES,
            )
        except FileSafetyError as exc:
            raise PromotedWalkForwardRunConflict(
                "promoted run manifest could not be read safely"
            ) from exc
        manifest = decode_promoted_walk_forward_manifest(payload)
        if manifest.trial_id != trial_id:
            raise PromotedWalkForwardRunConflict(
                "promoted run path differs from content"
            )
        if encode_promoted_walk_forward_manifest(manifest) != payload:
            raise PromotedWalkForwardRunConflict(
                "promoted run manifest is noncanonical"
            )
        return manifest

    def get(
        self,
        trial_id: str,
    ) -> PromotedWalkForwardEvaluationRun:
        if self.research_store is None:
            raise PromotedWalkForwardStoreError(
                "research store is required to replay a promoted run"
            )
        manifest = self.get_manifest(trial_id)
        deterministic = self.deterministic_store.get(trial_id)
        research_batches = tuple(
            self.research_store.get(batch_id)
            for batch_id in manifest.research_batch_ids
        )
        strategy = PromotedWalkForwardStrategyRun(
            policy_version=PROMOTED_WALK_FORWARD_POLICY_VERSION,
            split_plan_id=deterministic.strategy_batch.split_plan_id,
            config_id=deterministic.strategy_batch.generator_id,
            bindings=manifest.bindings,
            research_batches=research_batches,
            strategy_batch=deterministic.strategy_batch,
        )
        run = PromotedWalkForwardEvaluationRun(
            strategy_run=strategy,
            deterministic_run=deterministic,
        )
        expected = _manifest_from_run(run)
        if expected != manifest:
            raise PromotedWalkForwardRunConflict(
                "promoted run replay differs from manifest"
            )
        return run
