from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timezone

from india_swing.historical_prices.models import NseEodSessionArtifact
from india_swing.identity import content_id

from .backfill import HistoricalBackfillPlan, HistoricalBackfillRunner
from .collection import (
    HistoricalReconciliationCollector,
    historical_dataset_name,
)
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


HISTORICAL_BACKFILL_PILOT_SCHEMA_VERSION = "historical-backfill-pilot/v1"
HISTORICAL_BACKFILL_PILOT_POLICY_VERSION = "historical-backfill-pilot-policy/v1"
MAXIMUM_PILOT_TOTAL_REQUESTS = 50


class HistoricalBackfillPilotError(ValueError):
    pass


class HistoricalBackfillPilotIntegrityError(HistoricalBackfillPilotError):
    pass


def _utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: object, field_name: str) -> None:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256")


def _provider(value: object, field_name: str) -> None:
    if type(value) is not str or MARKET_DATA_PROVIDER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be canonical uppercase provider text")


@dataclass(frozen=True, slots=True)
class HistoricalBackfillPilotCompletion:
    """One independently verified collection completion within the pilot prefix."""

    request_id: str
    snapshot_id: str
    row_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha256(self.request_id, "pilot completion request_id")
        _sha256(self.snapshot_id, "pilot completion snapshot_id")
        object.__setattr__(self, "row_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": HISTORICAL_BACKFILL_PILOT_SCHEMA_VERSION,
                "request_id": self.request_id,
                "snapshot_id": self.snapshot_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.row_id != self._calculated_id():
            raise HistoricalBackfillPilotIntegrityError(
                "pilot completion row identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillPilotReconciliation:
    """One independently verified persisted reconciliation report within the pilot."""

    request_id: str
    report_id: str
    report_snapshot_id: str
    passed: bool
    row_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha256(self.request_id, "pilot reconciliation request_id")
        _sha256(self.report_id, "pilot reconciliation report_id")
        _sha256(self.report_snapshot_id, "pilot reconciliation report_snapshot_id")
        if type(self.passed) is not bool:
            raise TypeError("pilot reconciliation passed must be bool")
        object.__setattr__(self, "row_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": HISTORICAL_BACKFILL_PILOT_SCHEMA_VERSION,
                "request_id": self.request_id,
                "report_id": self.report_id,
                "report_snapshot_id": self.report_snapshot_id,
                "passed": self.passed,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.row_id != self._calculated_id():
            raise HistoricalBackfillPilotIntegrityError(
                "pilot reconciliation row identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillPilotResult:
    """A bounded, content-addressed, research-only collect+reconcile pilot outcome."""

    plan_id: str
    progress_id: str
    provider: str
    connector_version: str
    maximum_total_requests: int
    selected_request_ids: tuple[str, ...]
    completions: tuple[HistoricalBackfillPilotCompletion, ...]
    reconciliations: tuple[HistoricalBackfillPilotReconciliation, ...]
    reconciled_at: datetime
    passed: bool
    collection_only: bool = True
    actionable: bool = False
    schema_version: str = HISTORICAL_BACKFILL_PILOT_SCHEMA_VERSION
    policy_version: str = HISTORICAL_BACKFILL_PILOT_POLICY_VERSION
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "result_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.plan_id, "pilot plan_id")
        _sha256(self.progress_id, "pilot progress_id")
        _provider(self.provider, "pilot provider")
        if (
            type(self.connector_version) is not str
            or not self.connector_version
            or len(self.connector_version) > 128
        ):
            raise ValueError("pilot connector_version must be bounded text")
        if (
            type(self.maximum_total_requests) is not int
            or not 0 < self.maximum_total_requests <= MAXIMUM_PILOT_TOTAL_REQUESTS
        ):
            raise ValueError(
                "pilot maximum_total_requests must be a positive exact integer "
                f"at or below {MAXIMUM_PILOT_TOTAL_REQUESTS}"
            )
        if type(self.selected_request_ids) is not tuple or not self.selected_request_ids:
            raise TypeError(
                "pilot selected_request_ids must be a non-empty exact tuple"
            )
        for value in self.selected_request_ids:
            _sha256(value, "pilot selected request_id")
        if len(set(self.selected_request_ids)) != len(self.selected_request_ids):
            raise ValueError("pilot selected_request_ids must be unique")
        if len(self.selected_request_ids) > self.maximum_total_requests:
            raise ValueError(
                "pilot selected_request_ids cannot exceed maximum_total_requests"
            )

        if type(self.completions) is not tuple or any(
            type(value) is not HistoricalBackfillPilotCompletion
            for value in self.completions
        ):
            raise TypeError("pilot completions must be an exact immutable tuple")
        for value in self.completions:
            value.verify_content_identity()
        if tuple(value.request_id for value in self.completions) != self.selected_request_ids:
            raise ValueError(
                "pilot completions must exactly cover the selected prefix in order"
            )

        if type(self.reconciliations) is not tuple or any(
            type(value) is not HistoricalBackfillPilotReconciliation
            for value in self.reconciliations
        ):
            raise TypeError("pilot reconciliations must be an exact immutable tuple")
        for value in self.reconciliations:
            value.verify_content_identity()
        if (
            tuple(value.request_id for value in self.reconciliations)
            != self.selected_request_ids
        ):
            raise ValueError(
                "pilot reconciliations must exactly cover the selected prefix in order"
            )

        object.__setattr__(
            self,
            "reconciled_at",
            _utc(self.reconciled_at, "pilot reconciled_at"),
        )
        expected_passed = all(value.passed for value in self.reconciliations)
        if type(self.passed) is not bool or self.passed != expected_passed:
            raise ValueError("pilot passed flag disagrees with reconciliation rows")
        if self.collection_only is not True:
            raise ValueError("historical backfill pilots must remain collection-only")
        if self.actionable is not False:
            raise ValueError("historical backfill pilots cannot authorize trading")
        if (
            self.schema_version != HISTORICAL_BACKFILL_PILOT_SCHEMA_VERSION
            or self.policy_version != HISTORICAL_BACKFILL_PILOT_POLICY_VERSION
        ):
            raise ValueError("unsupported historical backfill pilot contract")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "result_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        for value in self.completions:
            value.verify_content_identity()
        for value in self.reconciliations:
            value.verify_content_identity()
        if self.result_id != self._calculated_id():
            raise HistoricalBackfillPilotIntegrityError(
                "historical backfill pilot result identity failed"
            )


class HistoricalBackfillPilotService:
    """Bind an existing runner/collector to one capped, reconciled plan prefix."""

    def __init__(
        self,
        runner: HistoricalBackfillRunner,
        reconciliation_collector: HistoricalReconciliationCollector,
    ) -> None:
        if type(runner) is not HistoricalBackfillRunner:
            raise TypeError(
                "runner must be an exact HistoricalBackfillRunner"
            )
        if type(reconciliation_collector) is not HistoricalReconciliationCollector:
            raise TypeError(
                "reconciliation_collector must be an exact HistoricalReconciliationCollector"
            )
        self.runner = runner
        self.reconciliation_collector = reconciliation_collector

    def run(
        self,
        plan: HistoricalBackfillPlan,
        nse_artifacts: tuple[NseEodSessionArtifact, ...],
        maximum_total_requests: int,
        *,
        reconciled_at: datetime,
    ) -> HistoricalBackfillPilotResult:
        if type(plan) is not HistoricalBackfillPlan:
            raise HistoricalBackfillPilotError(
                "plan must be an exact HistoricalBackfillPlan"
            )
        try:
            plan.verify_content_identity()
        except (TypeError, ValueError) as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill plan failed identity verification"
            ) from exc
        if not plan.requests:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot requires a non-empty plan"
            )
        if plan.has_blocking_issues:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot cannot run against a blocking plan"
            )
        if (
            type(maximum_total_requests) is not int
            or not 0 < maximum_total_requests <= MAXIMUM_PILOT_TOTAL_REQUESTS
        ):
            raise HistoricalBackfillPilotError(
                "maximum_total_requests must be a positive exact integer at or "
                f"below {MAXIMUM_PILOT_TOTAL_REQUESTS}"
            )
        reconciled_at = _reconciled_at(reconciled_at)
        try:
            connector_provider = self.runner.connector.provider
            connector_version = self.runner.connector.provider_version
        except Exception as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot connector lineage is unavailable"
            ) from exc
        if (
            connector_provider != plan.provider
            or type(connector_version) is not str
            or not connector_version
            or len(connector_version) > 128
        ):
            raise HistoricalBackfillPilotError(
                "historical backfill pilot connector lineage mismatch"
            )

        prefix = plan.requests[: min(maximum_total_requests, len(plan.requests))]
        selected_request_ids = tuple(value.request_id for value in prefix)
        selected_ids = set(selected_request_ids)
        expected_session_union = frozenset(
            session for request in prefix for session in request.sessions
        )
        artifacts_by_session = self._validate_nse_artifacts(
            nse_artifacts, expected_session_union
        )

        try:
            progress = self.runner.progress_store.load(plan.plan_id)
        except Exception as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot progress is unavailable"
            ) from exc
        if progress is not None:
            try:
                progress.verify_content_identity()
            except (TypeError, ValueError) as exc:
                raise HistoricalBackfillPilotError(
                    "historical backfill pilot progress failed identity verification"
                ) from exc
            if (
                progress.plan_id != plan.plan_id
                or progress.provider != plan.provider
                or progress.connector_version != connector_version
            ):
                raise HistoricalBackfillPilotError(
                    "historical backfill pilot progress lineage mismatch"
                )
            out_of_prefix = tuple(
                value.request_id
                for value in progress.completions
                if value.request_id not in selected_ids
            )
            if out_of_prefix:
                raise HistoricalBackfillPilotError(
                    "historical backfill pilot progress contains a completion "
                    "outside the selected prefix"
                )
            already_completed = sum(
                1
                for value in progress.completions
                if value.request_id in selected_ids
            )
        else:
            already_completed = 0

        remaining_capacity = len(selected_request_ids) - already_completed
        if remaining_capacity > 0:
            try:
                progress = self.runner.run(
                    plan, maximum_requests=remaining_capacity
                )
            except Exception as exc:
                raise HistoricalBackfillPilotError(
                    "historical backfill pilot collection failed"
                ) from exc
        if progress is None:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot has no durable progress"
            )
        try:
            progress.verify_content_identity()
        except (TypeError, ValueError) as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot progress failed identity verification"
            ) from exc
        completed_ids = {value.request_id for value in progress.completions}
        if completed_ids != selected_ids:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot did not collect exactly the "
                "selected prefix"
            )

        completions_by_id = {
            value.request_id: value for value in progress.completions
        }
        completions: list[HistoricalBackfillPilotCompletion] = []
        reconciliations: list[HistoricalBackfillPilotReconciliation] = []
        for request_id in selected_request_ids:
            completion = completions_by_id[request_id]
            batch = self._verify_completed_batch(
                request_id,
                completion.snapshot_id,
                plan.provider,
                progress.connector_version,
            )
            completions.append(
                HistoricalBackfillPilotCompletion(
                    request_id=request_id,
                    snapshot_id=completion.snapshot_id,
                )
            )
            matching_artifacts = tuple(
                artifacts_by_session[session] for session in batch.request.sessions
            )
            report = self._reconcile(batch, matching_artifacts, reconciled_at)
            report_snapshot = self._persist_report(report)
            reconciliations.append(
                HistoricalBackfillPilotReconciliation(
                    request_id=request_id,
                    report_id=report.report_id,
                    report_snapshot_id=report_snapshot.manifest.snapshot_id,
                    passed=report.passed,
                )
            )

        return HistoricalBackfillPilotResult(
            plan_id=plan.plan_id,
            progress_id=progress.progress_id,
            provider=plan.provider,
            connector_version=progress.connector_version,
            maximum_total_requests=maximum_total_requests,
            selected_request_ids=selected_request_ids,
            completions=tuple(completions),
            reconciliations=tuple(reconciliations),
            reconciled_at=reconciled_at,
            passed=all(value.passed for value in reconciliations),
        )

    @staticmethod
    def _validate_nse_artifacts(
        nse_artifacts: object,
        expected_session_union: frozenset,
    ) -> dict:
        if type(nse_artifacts) is not tuple or any(
            type(value) is not NseEodSessionArtifact for value in nse_artifacts
        ):
            raise HistoricalBackfillPilotError(
                "nse_artifacts must be an exact immutable NseEodSessionArtifact tuple"
            )
        if not nse_artifacts:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot requires exact NSE session evidence"
            )
        try:
            for artifact in nse_artifacts:
                artifact.verify_content_identity()
        except (TypeError, ValueError) as exc:
            raise HistoricalBackfillPilotError(
                "an NSE session artifact failed identity verification"
            ) from exc
        sessions = tuple(value.market_session for value in nse_artifacts)
        if len(set(sessions)) != len(sessions):
            raise HistoricalBackfillPilotError(
                "nse_artifacts must be session-unique"
            )
        if set(sessions) != expected_session_union:
            raise HistoricalBackfillPilotError(
                "nse_artifacts must exactly cover the selected prefix sessions"
            )
        return {value.market_session: value for value in nse_artifacts}

    def _verify_completed_batch(
        self,
        request_id: str,
        snapshot_id: str,
        provider: str,
        connector_version: str,
    ) -> HistoricalDailyCandleBatch:
        dataset = historical_dataset_name(provider)
        try:
            stored = self.runner.snapshot_store.get(dataset, snapshot_id)
        except Exception as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot completion snapshot is unavailable"
            ) from exc
        payload = stored.normalized_payload
        if type(payload) is not HistoricalDailyCandleBatch:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot completion snapshot has the wrong "
                "payload type"
            )
        try:
            payload.verify_content_identity()
        except (TypeError, ValueError) as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot completion snapshot failed identity "
                "verification"
            ) from exc
        if (
            stored.manifest.selection_key != request_id
            or stored.manifest.provider != provider
            or stored.manifest.provider_version != connector_version
            or payload.request.request_id != request_id
            or payload.provider != provider
            or payload.provider_version != connector_version
        ):
            raise HistoricalBackfillPilotError(
                "historical backfill pilot completion snapshot lineage mismatch"
            )
        return payload

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
        except Exception as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot reconciliation failed"
            ) from exc

    def _persist_report(
        self, report: HistoricalCandleReconciliationReport
    ):
        try:
            stored = self.reconciliation_collector.collect(report)
        except Exception as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot reconciliation report could not be "
                "persisted"
            ) from exc
        payload = stored.normalized_payload
        if type(payload) is not HistoricalCandleReconciliationReport:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot reconciliation snapshot has the "
                "wrong payload type"
            )
        try:
            payload.verify_content_identity()
        except (TypeError, ValueError) as exc:
            raise HistoricalBackfillPilotError(
                "historical backfill pilot reconciliation snapshot failed "
                "identity verification"
            ) from exc
        if (
            stored.manifest.dataset != HISTORICAL_RECONCILIATION_DATASET
            or stored.manifest.selection_key != report.historical_batch_id
            or stored.manifest.provider != HISTORICAL_RECONCILIATION_PROVIDER
            or stored.manifest.provider_version != report.policy_version
            or payload.report_id != report.report_id
            or payload.historical_batch_id != report.historical_batch_id
            or payload.historical_request_id != report.historical_request_id
            or payload.passed != report.passed
            or payload.actionable != report.actionable
        ):
            raise HistoricalBackfillPilotError(
                "historical backfill pilot reconciliation snapshot lineage "
                "mismatch"
            )
        return stored


def _reconciled_at(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise HistoricalBackfillPilotError("reconciled_at must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise HistoricalBackfillPilotError("reconciled_at must be timezone-aware")
    return value.astimezone(timezone.utc)
