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

from .landing_lineage import (
    LANDING_INPUT_LINEAGE_SCHEMA_VERSION,
    LEGACY_LANDING_INPUT_LINEAGE_SCHEMA_VERSION,
    AcquisitionFileType,
    LandingInputLineage,
    LandingLineageError,
    LandingManifestSourceLineage,
    LandingObjectLineage,
)
from .models import (
    DailyPipelineError,
    DailyPipelineRun,
)


DAILY_PIPELINE_RUN_STORE_SCHEMA_VERSION = "local-daily-pipeline-run/v2"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_RUN_BYTES = 2 * 1024 * 1024

_LANDING_OBJECT_LINEAGE_FIELDS = {
    "file_type", "bucket", "object_name", "generation", "target_session", "sha256_hash",
}
_LANDING_MANIFEST_SOURCE_LINEAGE_FIELDS = {
    "bucket", "object_name", "generation", "target_session",
}
_LANDING_INPUT_LINEAGE_BASE_FIELDS = {
    "schema_version", "manifest_sha256", "manifest_knowledge_time", "binding_not_before",
    "binding_cutoff", "target_session", "security_master", "daily_bundle", "lineage_id",
}
_LANDING_INPUT_LINEAGE_V2_FIELDS = _LANDING_INPUT_LINEAGE_BASE_FIELDS | {"manifest_source"}


class DailyPipelineRunConflict(DailyPipelineError):
    pass


class DailyPipelineRunNotFound(DailyPipelineError):
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
            raise DailyPipelineRunConflict("daily run contains a duplicate JSON key")
        result[key] = value
    return result


def _landing_object_lineage_data(value: LandingObjectLineage) -> dict[str, object]:
    return {
        "file_type": value.file_type.value,
        "bucket": value.bucket,
        "object_name": value.object_name,
        "generation": value.generation,
        "target_session": value.target_session.isoformat(),
        "sha256_hash": value.sha256_hash,
    }


def _landing_manifest_source_lineage_data(value: LandingManifestSourceLineage) -> dict[str, object]:
    return {
        "bucket": value.bucket,
        "object_name": value.object_name,
        "generation": value.generation,
        "target_session": value.target_session.isoformat(),
    }


def _landing_input_lineage_data(value: LandingInputLineage | None) -> object:
    if value is None:
        return None
    data: dict[str, object] = {
        "schema_version": value.schema_version,
        "manifest_sha256": value.manifest_sha256,
        "manifest_knowledge_time": value.manifest_knowledge_time.isoformat(),
        "binding_not_before": value.binding_not_before.isoformat(),
        "binding_cutoff": value.binding_cutoff.isoformat(),
        "target_session": value.target_session.isoformat(),
        "security_master": _landing_object_lineage_data(value.security_master),
        "daily_bundle": _landing_object_lineage_data(value.daily_bundle),
        "lineage_id": value.lineage_id,
    }
    if value.manifest_source is not None:
        data["manifest_source"] = _landing_manifest_source_lineage_data(value.manifest_source)
    return data


def _run_data(run: DailyPipelineRun) -> dict[str, object]:
    return {
        "schema_version": run.schema_version,
        "run_id": run.run_id,
        "market_session": run.market_session.isoformat(),
        "cutoff": run.cutoff.isoformat(),
        "calendar_materialization_id": run.calendar_materialization_id,
        "calendar_snapshot_id": run.calendar_snapshot_id,
        "previous_run_id": run.previous_run_id,
        "security_master_artifact_ids": list(run.security_master_artifact_ids),
        "daily_bundle_artifact_ids": list(run.daily_bundle_artifact_ids),
        "current_security_master_artifact_id": run.current_security_master_artifact_id,
        "current_daily_bundle_artifact_id": run.current_daily_bundle_artifact_id,
        "observed_date_artifact_id": run.observed_date_artifact_id,
        "observed_dates": [value.isoformat() for value in run.observed_dates],
        "historical_price_artifact_id": run.historical_price_artifact_id,
        "historical_price_manifest_id": run.historical_price_manifest_id,
        "bar_count": run.bar_count,
        "reconciliation_snapshot_id": run.reconciliation_snapshot_id,
        "reconciliation_global_reason_codes": list(
            run.reconciliation_global_reason_codes
        ),
        "retained_row_count": run.retained_row_count,
        "main_scope_count": run.main_scope_count,
        "sme_scope_count": run.sme_scope_count,
        "unsupported_series_count": run.unsupported_series_count,
        "unresolved_count": run.unresolved_count,
        "traded_row_count": run.traded_row_count,
        "orphan_report_key_count": run.orphan_report_key_count,
        "identity_registry_id": run.identity_registry_id,
        "identity_registry_manifest_id": run.identity_registry_manifest_id,
        "identity_observation_count": run.identity_observation_count,
        "identity_candidate_count": run.identity_candidate_count,
        "identity_transition_count": run.identity_transition_count,
        "identity_conflict_count": run.identity_conflict_count,
        "adjudication_queue_id": run.adjudication_queue_id,
        "adjudication_case_count": run.adjudication_case_count,
        "adjudication_requirement_counts": [
            [name, count] for name, count in run.adjudication_requirement_counts
        ],
        "completeness_issues": list(run.completeness_issues),
        "landing_input_lineage": _landing_input_lineage_data(run.landing_input_lineage),
        "readiness": run.readiness.value,
        "actionable": run.actionable,
        "stable_identity_assigned": run.stable_identity_assigned,
    }


_RUN_FIELDS = {
    "schema_version", "run_id", "market_session", "cutoff",
    "calendar_materialization_id", "calendar_snapshot_id", "previous_run_id",
    "security_master_artifact_ids", "daily_bundle_artifact_ids",
    "current_security_master_artifact_id", "current_daily_bundle_artifact_id",
    "observed_date_artifact_id", "observed_dates",
    "historical_price_artifact_id", "historical_price_manifest_id", "bar_count",
    "reconciliation_snapshot_id", "reconciliation_global_reason_codes",
    "retained_row_count", "main_scope_count", "sme_scope_count",
    "unsupported_series_count", "unresolved_count", "traded_row_count",
    "orphan_report_key_count", "identity_registry_id",
    "identity_registry_manifest_id", "identity_observation_count",
    "identity_candidate_count", "identity_transition_count",
    "identity_conflict_count", "adjudication_queue_id", "adjudication_case_count",
    "adjudication_requirement_counts", "completeness_issues", "landing_input_lineage",
    "readiness", "actionable", "stable_identity_assigned",
}


def _decode_landing_object_lineage(value: object) -> LandingObjectLineage:
    if type(value) is not dict or set(value) != _LANDING_OBJECT_LINEAGE_FIELDS:
        raise DailyPipelineRunConflict("stored landing object lineage has an invalid shape")

    file_type_raw = value["file_type"]
    bucket = value["bucket"]
    object_name = value["object_name"]
    generation = value["generation"]
    target_session_raw = value["target_session"]
    sha256_hash = value["sha256_hash"]
    if (
        type(file_type_raw) is not str
        or type(bucket) is not str
        or type(object_name) is not str
        or type(generation) is not int
        or type(target_session_raw) is not str
        or type(sha256_hash) is not str
    ):
        raise DailyPipelineRunConflict("stored landing object lineage is invalid")

    try:
        file_type = AcquisitionFileType(file_type_raw)
    except ValueError:
        raise DailyPipelineRunConflict("stored landing object lineage is invalid") from None
    try:
        target_session = date.fromisoformat(target_session_raw)
    except ValueError:
        raise DailyPipelineRunConflict("stored landing object lineage is invalid") from None

    try:
        return LandingObjectLineage(
            file_type=file_type,
            bucket=bucket,
            object_name=object_name,
            generation=generation,
            target_session=target_session,
            sha256_hash=sha256_hash,
        )
    except LandingLineageError:
        raise DailyPipelineRunConflict("stored landing object lineage is invalid") from None


def _decode_landing_manifest_source_lineage(value: object) -> LandingManifestSourceLineage:
    if type(value) is not dict or set(value) != _LANDING_MANIFEST_SOURCE_LINEAGE_FIELDS:
        raise DailyPipelineRunConflict("stored landing manifest source lineage has an invalid shape")

    bucket = value["bucket"]
    object_name = value["object_name"]
    generation = value["generation"]
    target_session_raw = value["target_session"]
    if (
        type(bucket) is not str
        or type(object_name) is not str
        or type(generation) is not int
        or type(target_session_raw) is not str
    ):
        raise DailyPipelineRunConflict("stored landing manifest source lineage is invalid")

    try:
        target_session = date.fromisoformat(target_session_raw)
    except ValueError:
        raise DailyPipelineRunConflict("stored landing manifest source lineage is invalid") from None

    try:
        return LandingManifestSourceLineage(
            bucket=bucket,
            object_name=object_name,
            generation=generation,
            target_session=target_session,
        )
    except LandingLineageError:
        raise DailyPipelineRunConflict("stored landing manifest source lineage is invalid") from None


def _decode_landing_input_lineage(value: object) -> LandingInputLineage | None:
    if value is None:
        return None
    if type(value) is not dict:
        raise DailyPipelineRunConflict("stored landing input lineage has an invalid shape")

    schema_version_raw = value.get("schema_version")
    if schema_version_raw == LEGACY_LANDING_INPUT_LINEAGE_SCHEMA_VERSION:
        expected_fields = _LANDING_INPUT_LINEAGE_BASE_FIELDS
    elif schema_version_raw == LANDING_INPUT_LINEAGE_SCHEMA_VERSION:
        expected_fields = _LANDING_INPUT_LINEAGE_V2_FIELDS
    else:
        raise DailyPipelineRunConflict("stored landing input lineage has an invalid shape")
    if set(value) != expected_fields:
        raise DailyPipelineRunConflict("stored landing input lineage has an invalid shape")

    schema_version = value["schema_version"]
    manifest_sha256 = value["manifest_sha256"]
    manifest_knowledge_time_raw = value["manifest_knowledge_time"]
    binding_not_before_raw = value["binding_not_before"]
    binding_cutoff_raw = value["binding_cutoff"]
    target_session_raw = value["target_session"]
    stored_lineage_id = value["lineage_id"]
    if (
        type(schema_version) is not str
        or type(manifest_sha256) is not str
        or type(manifest_knowledge_time_raw) is not str
        or type(binding_not_before_raw) is not str
        or type(binding_cutoff_raw) is not str
        or type(target_session_raw) is not str
        or type(stored_lineage_id) is not str
    ):
        raise DailyPipelineRunConflict("stored landing input lineage is invalid")

    try:
        manifest_knowledge_time = datetime.fromisoformat(manifest_knowledge_time_raw)
        binding_not_before = datetime.fromisoformat(binding_not_before_raw)
        binding_cutoff = datetime.fromisoformat(binding_cutoff_raw)
        target_session = date.fromisoformat(target_session_raw)
    except ValueError:
        raise DailyPipelineRunConflict("stored landing input lineage is invalid") from None
    for candidate in (manifest_knowledge_time, binding_not_before, binding_cutoff):
        if candidate.tzinfo is None or candidate.utcoffset() is None:
            raise DailyPipelineRunConflict("stored landing input lineage is invalid")

    security_master = _decode_landing_object_lineage(value["security_master"])
    daily_bundle = _decode_landing_object_lineage(value["daily_bundle"])
    manifest_source = (
        _decode_landing_manifest_source_lineage(value["manifest_source"])
        if "manifest_source" in value
        else None
    )

    try:
        lineage = LandingInputLineage(
            schema_version=schema_version,
            manifest_sha256=manifest_sha256,
            manifest_knowledge_time=manifest_knowledge_time,
            binding_not_before=binding_not_before,
            binding_cutoff=binding_cutoff,
            target_session=target_session,
            security_master=security_master,
            daily_bundle=daily_bundle,
            manifest_source=manifest_source,
        )
    except LandingLineageError:
        raise DailyPipelineRunConflict("stored landing input lineage is invalid") from None
    if lineage.lineage_id != stored_lineage_id:
        raise DailyPipelineRunConflict("stored landing input lineage ID differs from content")
    return lineage


def _decode_run(value: object) -> DailyPipelineRun:
    if type(value) is not dict or set(value) != _RUN_FIELDS:
        raise DailyPipelineRunConflict("stored daily run has an invalid shape")
    try:
        run = DailyPipelineRun(
            market_session=date.fromisoformat(value["market_session"]),
            cutoff=datetime.fromisoformat(value["cutoff"]),
            calendar_materialization_id=value["calendar_materialization_id"],
            calendar_snapshot_id=value["calendar_snapshot_id"],
            previous_run_id=value["previous_run_id"],
            security_master_artifact_ids=tuple(value["security_master_artifact_ids"]),
            daily_bundle_artifact_ids=tuple(value["daily_bundle_artifact_ids"]),
            current_security_master_artifact_id=value[
                "current_security_master_artifact_id"
            ],
            current_daily_bundle_artifact_id=value["current_daily_bundle_artifact_id"],
            observed_date_artifact_id=value["observed_date_artifact_id"],
            observed_dates=tuple(date.fromisoformat(item) for item in value["observed_dates"]),
            historical_price_artifact_id=value["historical_price_artifact_id"],
            historical_price_manifest_id=value["historical_price_manifest_id"],
            bar_count=value["bar_count"],
            reconciliation_snapshot_id=value["reconciliation_snapshot_id"],
            reconciliation_global_reason_codes=tuple(
                value["reconciliation_global_reason_codes"]
            ),
            retained_row_count=value["retained_row_count"],
            main_scope_count=value["main_scope_count"],
            sme_scope_count=value["sme_scope_count"],
            unsupported_series_count=value["unsupported_series_count"],
            unresolved_count=value["unresolved_count"],
            traded_row_count=value["traded_row_count"],
            orphan_report_key_count=value["orphan_report_key_count"],
            identity_registry_id=value["identity_registry_id"],
            identity_registry_manifest_id=value["identity_registry_manifest_id"],
            identity_observation_count=value["identity_observation_count"],
            identity_candidate_count=value["identity_candidate_count"],
            identity_transition_count=value["identity_transition_count"],
            identity_conflict_count=value["identity_conflict_count"],
            adjudication_queue_id=value["adjudication_queue_id"],
            adjudication_case_count=value["adjudication_case_count"],
            adjudication_requirement_counts=tuple(
                (item[0], item[1]) for item in value["adjudication_requirement_counts"]
            ),
            completeness_issues=tuple(value["completeness_issues"]),
            landing_input_lineage=_decode_landing_input_lineage(value["landing_input_lineage"]),
            readiness=ReferenceReadiness(value["readiness"]),
            actionable=value["actionable"],
            stable_identity_assigned=value["stable_identity_assigned"],
            schema_version=value["schema_version"],
        )
        if run.run_id != value["run_id"]:
            raise DailyPipelineRunConflict("stored daily run ID differs from content")
        return run
    except DailyPipelineError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise DailyPipelineRunConflict("stored daily run is invalid") from exc


class LocalDailyPipelineRunStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def runs_root(self) -> Path:
        return self.root / "runs"

    def path_for(self, run_id: str) -> Path:
        if not isinstance(run_id, str) or _SHA256.fullmatch(run_id) is None:
            raise DailyPipelineRunConflict("run_id must be a full lowercase SHA-256")
        return self.runs_root / f"{run_id}.json"

    @staticmethod
    def _payload(run: DailyPipelineRun) -> bytes:
        return (
            json.dumps(
                {
                    "store_schema_version": DAILY_PIPELINE_RUN_STORE_SCHEMA_VERSION,
                    "run": _run_data(run),
                },
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")

    def publish(self, run: DailyPipelineRun) -> DailyPipelineRun:
        if type(run) is not DailyPipelineRun:
            raise TypeError("run must be exact")
        run.verify_content_identity()
        self.runs_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.runs_root):
            raise DailyPipelineRunConflict("daily-run root cannot be a link")
        target = self.path_for(run.run_id)
        payload = self._payload(run)
        try:
            with advisory_file_lock(self.runs_root / ".daily-runs.lock"):
                if target.exists():
                    stored = self.get(run.run_id)
                    if stored != run:
                        raise DailyPipelineRunConflict(
                            "run ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".daily-run-", suffix=".tmp", dir=self.runs_root
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
            raise DailyPipelineRunConflict("daily-run store unavailable") from exc
        return self.get(run.run_id)

    def get(self, run_id: str) -> DailyPipelineRun:
        path = self.path_for(run_id)
        if not path.exists():
            raise DailyPipelineRunNotFound(run_id)
        if not path.is_file() or _is_link_like(path):
            raise DailyPipelineRunConflict("daily run must be a regular file")
        try:
            raw = json.loads(
                read_stable_regular_file(path, maximum_bytes=_MAX_RUN_BYTES).decode(
                    "utf-8"
                ),
                object_pairs_hook=_unique_object,
                parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
            )
            if (
                type(raw) is not dict
                or set(raw) != {"store_schema_version", "run"}
                or raw["store_schema_version"]
                != DAILY_PIPELINE_RUN_STORE_SCHEMA_VERSION
            ):
                raise DailyPipelineRunConflict("stored daily-run envelope is invalid")
            run = _decode_run(raw["run"])
            if run.run_id != run_id:
                raise DailyPipelineRunConflict("stored daily run differs from path")
            return run
        except DailyPipelineError:
            raise
        except (
            FileSafetyError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise DailyPipelineRunConflict("stored daily run is invalid") from exc

    def list_runs(self) -> tuple[DailyPipelineRun, ...]:
        if not self.runs_root.exists():
            return ()
        if not self.runs_root.is_dir() or _is_link_like(self.runs_root):
            raise DailyPipelineRunConflict("daily-run root must be a directory")
        runs = []
        for path in sorted(self.runs_root.iterdir(), key=lambda value: value.name):
            if path.name == ".daily-runs.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise DailyPipelineRunConflict("daily-run file set is invalid")
            runs.append(self.get(path.stem))
        return tuple(sorted(runs, key=lambda value: (value.market_session, value.run_id)))
