from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


DAILY_PIPELINE_RUN_SCHEMA_VERSION = "nse-cm-daily-pipeline-run/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]*\Z")


class DailyPipelineError(Exception):
    pass


class DailyPipelineIntegrityError(DailyPipelineError):
    pass


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DailyPipelineIntegrityError(
            f"{field_name} must be a full lowercase SHA-256"
        )


def _require_count(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise DailyPipelineIntegrityError(
            f"{field_name} must be a non-negative integer"
        )


@dataclass(frozen=True, slots=True)
class DailyPipelineRun:
    market_session: date
    cutoff: datetime
    calendar_materialization_id: str
    calendar_snapshot_id: str
    previous_run_id: str | None
    security_master_artifact_ids: tuple[str, ...]
    daily_bundle_artifact_ids: tuple[str, ...]
    current_security_master_artifact_id: str
    current_daily_bundle_artifact_id: str
    observed_date_artifact_id: str
    observed_dates: tuple[date, ...]
    historical_price_artifact_id: str
    historical_price_manifest_id: str
    bar_count: int
    reconciliation_snapshot_id: str
    reconciliation_global_reason_codes: tuple[str, ...]
    retained_row_count: int
    main_scope_count: int
    sme_scope_count: int
    unsupported_series_count: int
    unresolved_count: int
    traded_row_count: int
    orphan_report_key_count: int
    identity_registry_id: str
    identity_registry_manifest_id: str
    identity_observation_count: int
    identity_candidate_count: int
    identity_transition_count: int
    identity_conflict_count: int
    adjudication_queue_id: str
    adjudication_case_count: int
    adjudication_requirement_counts: tuple[tuple[str, int], ...]
    completeness_issues: tuple[str, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    stable_identity_assigned: bool = False
    schema_version: str = DAILY_PIPELINE_RUN_SCHEMA_VERSION
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise TypeError("market_session must be a date")
        if not isinstance(self.cutoff, datetime):
            raise TypeError("cutoff must be a datetime")
        if self.cutoff.tzinfo is None or self.cutoff.utcoffset() is None:
            raise DailyPipelineIntegrityError("cutoff must be timezone-aware")
        object.__setattr__(self, "cutoff", self.cutoff.astimezone(timezone.utc))
        if self.schema_version != DAILY_PIPELINE_RUN_SCHEMA_VERSION:
            raise DailyPipelineIntegrityError("unsupported daily-pipeline schema")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY:
            raise DailyPipelineIntegrityError("daily runs must remain collection-only")
        if self.actionable or self.stable_identity_assigned:
            raise DailyPipelineIntegrityError(
                "daily runs cannot assign actionable or stable identity state"
            )

        for value, name in (
            (self.calendar_materialization_id, "calendar_materialization_id"),
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
            (
                self.current_security_master_artifact_id,
                "current_security_master_artifact_id",
            ),
            (self.current_daily_bundle_artifact_id, "current_daily_bundle_artifact_id"),
            (self.observed_date_artifact_id, "observed_date_artifact_id"),
            (self.historical_price_artifact_id, "historical_price_artifact_id"),
            (self.historical_price_manifest_id, "historical_price_manifest_id"),
            (self.reconciliation_snapshot_id, "reconciliation_snapshot_id"),
            (self.identity_registry_id, "identity_registry_id"),
            (self.identity_registry_manifest_id, "identity_registry_manifest_id"),
            (self.adjudication_queue_id, "adjudication_queue_id"),
        ):
            _require_sha256(value, name)
        if self.previous_run_id is not None:
            _require_sha256(self.previous_run_id, "previous_run_id")

        for values, name, current in (
            (
                self.security_master_artifact_ids,
                "security_master_artifact_ids",
                self.current_security_master_artifact_id,
            ),
            (
                self.daily_bundle_artifact_ids,
                "daily_bundle_artifact_ids",
                self.current_daily_bundle_artifact_id,
            ),
        ):
            if type(values) is not tuple or not values:
                raise DailyPipelineIntegrityError(f"{name} must be a non-empty tuple")
            if len(set(values)) != len(values) or values[-1] != current:
                raise DailyPipelineIntegrityError(
                    f"{name} must be unique and end with the current artifact"
                )
            for value in values:
                _require_sha256(value, name)

        if (
            type(self.observed_dates) is not tuple
            or tuple(sorted(set(self.observed_dates))) != self.observed_dates
            or self.market_session not in self.observed_dates
        ):
            raise DailyPipelineIntegrityError(
                "observed_dates must be unique, sorted, and include the market session"
            )

        for value, name in (
            (self.bar_count, "bar_count"),
            (self.retained_row_count, "retained_row_count"),
            (self.main_scope_count, "main_scope_count"),
            (self.sme_scope_count, "sme_scope_count"),
            (self.unsupported_series_count, "unsupported_series_count"),
            (self.unresolved_count, "unresolved_count"),
            (self.traded_row_count, "traded_row_count"),
            (self.orphan_report_key_count, "orphan_report_key_count"),
            (self.identity_observation_count, "identity_observation_count"),
            (self.identity_candidate_count, "identity_candidate_count"),
            (self.identity_transition_count, "identity_transition_count"),
            (self.identity_conflict_count, "identity_conflict_count"),
            (self.adjudication_case_count, "adjudication_case_count"),
        ):
            _require_count(value, name)
        if self.retained_row_count != (
            self.main_scope_count + self.sme_scope_count + self.unsupported_series_count
        ):
            raise DailyPipelineIntegrityError(
                "reconciliation scope counts must cover every retained row"
            )
        if self.adjudication_case_count != self.identity_candidate_count:
            raise DailyPipelineIntegrityError(
                "adjudication cases must cover every identity candidate"
            )

        for values, name in (
            (
                self.reconciliation_global_reason_codes,
                "reconciliation_global_reason_codes",
            ),
            (self.completeness_issues, "completeness_issues"),
        ):
            if (
                type(values) is not tuple
                or tuple(sorted(set(values))) != values
                or any(not isinstance(value, str) or _REASON.fullmatch(value) is None for value in values)
            ):
                raise DailyPipelineIntegrityError(f"{name} must be sorted reason codes")
        if not self.completeness_issues:
            raise DailyPipelineIntegrityError(
                "collection-only daily runs require completeness issues"
            )
        if tuple(sorted(set(self.adjudication_requirement_counts))) != (
            self.adjudication_requirement_counts
        ):
            raise DailyPipelineIntegrityError(
                "adjudication requirement counts must be unique and sorted"
            )
        for name, count in self.adjudication_requirement_counts:
            if not isinstance(name, str) or _REASON.fullmatch(name) is None:
                raise DailyPipelineIntegrityError("invalid adjudication requirement name")
            if type(count) is not int or count <= 0:
                raise DailyPipelineIntegrityError(
                    "adjudication requirement counts must be positive"
                )

        object.__setattr__(self, "run_id", self._calculated_run_id())

    def _identity_material(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "run_id"
        }

    def _calculated_run_id(self) -> str:
        return content_id(self._identity_material(), length=64)

    def verify_content_identity(self) -> None:
        if self.run_id != self._calculated_run_id():
            raise DailyPipelineIntegrityError(
                "daily-pipeline run content identity verification failed"
            )
