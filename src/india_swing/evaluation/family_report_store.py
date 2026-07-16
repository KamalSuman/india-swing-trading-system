from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .baselines import DeterministicComparisonRun
from .family_aggregate_store import LocalTrialFamilyAggregateStore
from .family_aggregation import (
    TrialFamilyAggregationError,
    TrialFamilyEvaluationAggregate,
)
from .family_report import (
    TrialFamilyEvaluationReport,
    build_trial_family_evaluation_report,
)


TRIAL_FAMILY_REPORT_STORE_SCHEMA_VERSION = "local-trial-family-report/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_REPORT_BYTES = 64 * 1024 * 1024


class TrialFamilyReportConflict(TrialFamilyAggregationError):
    pass


class TrialFamilyReportNotFound(TrialFamilyAggregationError):
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
            raise TrialFamilyReportConflict("report contains a duplicate JSON key")
        result[key] = value
    return result


class LocalTrialFamilyReportStore:
    """One create-once human-readable report per persisted family aggregate."""

    def __init__(
        self,
        root: Path,
        aggregate_store: LocalTrialFamilyAggregateStore,
    ) -> None:
        self.root = Path(root)
        if type(aggregate_store) is not LocalTrialFamilyAggregateStore:
            raise TypeError("aggregate_store must be exact")
        if self.root.resolve() != aggregate_store.root.resolve():
            raise ValueError("report and aggregate stores must share one evidence root")
        self.aggregate_store = aggregate_store

    @property
    def reports_root(self) -> Path:
        return self.root / "family_reports"

    def path_for(self, aggregate_id: str) -> Path:
        if not isinstance(aggregate_id, str) or _SHA256.fullmatch(aggregate_id) is None:
            raise TrialFamilyAggregationError("aggregate_id must be a full lowercase SHA-256")
        return self.reports_root / f"{aggregate_id}.json"

    @staticmethod
    def _payload(
        report: TrialFamilyEvaluationReport,
        aggregate: TrialFamilyEvaluationAggregate,
    ) -> bytes:
        return (
            json.dumps(
                {
                    "store_schema_version": TRIAL_FAMILY_REPORT_STORE_SCHEMA_VERSION,
                    "strategy_family_id": aggregate.strategy_family_id,
                    "registered_trial_ids": list(aggregate.registered_trial_ids),
                    "report": {
                        "aggregate_id": report.aggregate_id,
                        "markdown": report.markdown,
                        "report_id": report.report_id,
                    },
                },
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")

    def publish(
        self,
        report: TrialFamilyEvaluationReport,
        *,
        aggregate: TrialFamilyEvaluationAggregate,
        runs: tuple[DeterministicComparisonRun, ...],
    ) -> TrialFamilyEvaluationReport:
        if type(report) is not TrialFamilyEvaluationReport:
            raise TypeError("report must be exact")
        report.verify_content_identity()
        self.aggregate_store.require_persisted(aggregate)
        expected = build_trial_family_evaluation_report(
            aggregate=aggregate,
            runs=runs,
        )
        if expected != report:
            raise TrialFamilyReportConflict(
                "family report differs from persisted aggregate and run evidence"
            )
        self.reports_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.reports_root):
            raise TrialFamilyReportConflict("family-report root cannot be a link")
        target = self.path_for(aggregate.aggregate_id)
        payload = self._payload(report, aggregate)
        try:
            with advisory_file_lock(self.reports_root / ".family-reports.lock"):
                if target.exists():
                    stored = self.get(aggregate.aggregate_id)
                    if stored != report:
                        raise TrialFamilyReportConflict(
                            "aggregate already stores a different family report"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".family-report-", suffix=".tmp", dir=self.reports_root
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
            raise TrialFamilyReportConflict("family-report store unavailable") from exc
        return self.get(aggregate.aggregate_id)

    def _read(
        self, aggregate_id: str
    ) -> tuple[TrialFamilyEvaluationReport, str, tuple[str, ...]]:
        path = self.path_for(aggregate_id)
        if not path.exists():
            raise TrialFamilyReportNotFound(aggregate_id)
        if not path.is_file() or _is_link_like(path):
            raise TrialFamilyReportConflict("family report must be a regular file")
        try:
            raw = json.loads(
                read_stable_regular_file(path, maximum_bytes=_MAX_REPORT_BYTES).decode(
                    "utf-8"
                ),
                object_pairs_hook=_unique_object,
                parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
            )
            if (
                type(raw) is not dict
                or set(raw)
                != {
                    "store_schema_version",
                    "strategy_family_id",
                    "registered_trial_ids",
                    "report",
                }
                or raw["store_schema_version"]
                != TRIAL_FAMILY_REPORT_STORE_SCHEMA_VERSION
                or type(raw["strategy_family_id"]) is not str
                or type(raw["registered_trial_ids"]) is not list
                or type(raw["report"]) is not dict
                or set(raw["report"]) != {"aggregate_id", "markdown", "report_id"}
            ):
                raise ValueError
            value = raw["report"]
            report = TrialFamilyEvaluationReport(
                aggregate_id=value["aggregate_id"],
                markdown=value["markdown"],
            )
            if report.report_id != value["report_id"] or report.aggregate_id != aggregate_id:
                raise TrialFamilyReportConflict("stored report differs from content or path")
            trial_ids = tuple(raw["registered_trial_ids"])
            aggregate = self.aggregate_store.get(raw["strategy_family_id"], trial_ids)
            if aggregate.aggregate_id != report.aggregate_id:
                raise TrialFamilyReportConflict("report aggregate reference differs")
            return report, raw["strategy_family_id"], trial_ids
        except TrialFamilyAggregationError:
            raise
        except (
            FileSafetyError,
            KeyError,
            TypeError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise TrialFamilyReportConflict("stored family report is invalid") from exc

    def get(self, aggregate_id: str) -> TrialFamilyEvaluationReport:
        report, _, _ = self._read(aggregate_id)
        return report

    def list_reports(self) -> tuple[TrialFamilyEvaluationReport, ...]:
        if not self.reports_root.exists():
            return ()
        if not self.reports_root.is_dir() or _is_link_like(self.reports_root):
            raise TrialFamilyReportConflict("family-report root must be a directory")
        reports = []
        for path in sorted(self.reports_root.iterdir(), key=lambda value: value.name):
            if path.name == ".family-reports.lock":
                continue
            if (
                not path.name.endswith(".json")
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise TrialFamilyReportConflict("family-report file set is invalid")
            reports.append(self.get(path.stem))
        return tuple(reports)
