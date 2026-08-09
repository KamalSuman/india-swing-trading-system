from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from enum import Enum

from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.identity import content_id
from india_swing.market_data.models import InstrumentBatch

from .canonical_response import (
    MAXIMUM_CATALOG_INSTRUMENTS,
    PILOT_PROTOCOL_SHA256,
    ResponseClassification,
    ScheduledWindowKind,
)
from .capture_runner import (
    CONFIRMED_SESSION_COUNT,
    QualityPilotCampaignSpec,
    QualityPilotCaptureRunResult,
    QualityPilotCaptureSpec,
)


QUALITY_PILOT_CAMPAIGN_PLAN_SCHEMA_VERSION = "quality_pilot_campaign_plan_v1"
QUALITY_PILOT_COMPLETENESS_LEDGER_SCHEMA_VERSION = (
    "quality_pilot_completeness_ledger_v1"
)
MAXIMUM_EXPECTED_CAPTURE_SPECS = 700_000

_BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
_WINDOW_ORDER = {
    ScheduledWindowKind.CATALOG_PREOPEN: 0,
    ScheduledWindowKind.QUOTE_0920: 1,
    ScheduledWindowKind.QUOTE_CLOSE: 2,
    ScheduledWindowKind.OHLCV_CLOSE: 3,
}
_EXPECTED_WINDOW_KINDS = tuple(_WINDOW_ORDER)
_POSTURE_NAMES = (
    "quality_only",
    "counts_toward_o0",
    "counts_toward_clean_accumulation",
    "research_partition_eligible",
    "training_eligible",
    "feature_eligible",
    "label_eligible",
    "signal_eligible",
    "paper_trade_eligible",
    "notification_eligible",
    "execution_eligible",
    "capital_eligible",
)


class QualityPilotCampaignLedgerError(ValueError):
    """A campaign plan or completeness artifact failed a static trust rule."""


def _fail(message: str) -> None:
    raise QualityPilotCampaignLedgerError(message)


class CampaignCompletenessStatus(Enum):
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    DUE_INCOMPLETE = "DUE_INCOMPLETE"
    OUTCOMES_COMPLETE = "OUTCOMES_COMPLETE"


class _FixedPostureMixin:
    @property
    def quality_only(self) -> bool:
        return True

    @property
    def counts_toward_o0(self) -> bool:
        return False

    @property
    def counts_toward_clean_accumulation(self) -> bool:
        return False

    @property
    def research_partition_eligible(self) -> bool:
        return False

    @property
    def training_eligible(self) -> bool:
        return False

    @property
    def feature_eligible(self) -> bool:
        return False

    @property
    def label_eligible(self) -> bool:
        return False

    @property
    def signal_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False

    @property
    def capital_eligible(self) -> bool:
        return False


def _posture_tree(value: object) -> dict[str, bool]:
    return {name: getattr(value, name) for name in _POSTURE_NAMES}


def _verify_campaign(value: object) -> QualityPilotCampaignSpec:
    if type(value) is not QualityPilotCampaignSpec:
        _fail("campaign must be an exact QualityPilotCampaignSpec")
    failed = False
    try:
        value.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("campaign failed independent verification")
    return value


def _verify_capture_spec(value: object) -> QualityPilotCaptureSpec:
    if type(value) is not QualityPilotCaptureSpec:
        _fail("capture plan contains an invalid capture spec type")
    failed = False
    try:
        value.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("capture plan contains an unverifiable capture spec")
    return value


def _verify_run_result(value: object) -> QualityPilotCaptureRunResult:
    if type(value) is not QualityPilotCaptureRunResult:
        _fail("completeness ledger contains an invalid run result type")
    failed = False
    try:
        value.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("completeness ledger contains an unverifiable run result")
    return value


def _route_key(spec: QualityPilotCaptureSpec) -> tuple[date, int, int]:
    return (
        spec.window.market_session,
        _WINDOW_ORDER[spec.window.window_kind],
        spec.chunk_index,
    )


def _window_is_inside_authorized_schedule(spec: QualityPilotCaptureSpec) -> bool:
    valid = False
    try:
        opens_at = spec.window.opens_at.astimezone(INDIA_STANDARD_TIME)
        closes_at = spec.window.closes_at.astimezone(INDIA_STANDARD_TIME)
        opens = opens_at.time().replace(tzinfo=None)
        closes = closes_at.time().replace(tzinfo=None)
        if not opens < closes:
            return False
        kind = spec.window.window_kind
        if kind is ScheduledWindowKind.CATALOG_PREOPEN:
            valid = time(8, 45) <= opens and closes <= time(9, 0)
        elif kind is ScheduledWindowKind.QUOTE_0920:
            valid = opens == time(9, 20) and closes <= time(9, 25)
        elif kind is ScheduledWindowKind.QUOTE_CLOSE:
            valid = time(15, 40) <= opens and closes <= time(16, 0)
        else:
            valid = time(16, 15) <= opens and closes <= time(18, 0)
    except Exception:
        valid = False
    return valid


def _group_specs(
    specs: tuple[QualityPilotCaptureSpec, ...],
) -> dict[tuple[date, ScheduledWindowKind], list[QualityPilotCaptureSpec]]:
    groups: dict[tuple[date, ScheduledWindowKind], list[QualityPilotCaptureSpec]] = {}
    for spec in specs:
        groups.setdefault(
            (spec.window.market_session, spec.window.window_kind), []
        ).append(spec)
    return groups


def _validate_complete_route(group: list[QualityPilotCaptureSpec]) -> None:
    chunk_counts = {spec.chunk_count for spec in group}
    window_ids = {spec.window.window_id for spec in group}
    if len(chunk_counts) != 1 or len(window_ids) != 1:
        _fail("capture plan route does not share one window and chunk count")
    chunk_count = group[0].chunk_count
    if len(group) != chunk_count:
        _fail("capture plan route does not contain its declared number of chunks")
    if tuple(spec.chunk_index for spec in group) != tuple(range(1, chunk_count + 1)):
        _fail("capture plan route chunk indices are incomplete or out of order")


def _quote_universe(group: list[QualityPilotCaptureSpec]) -> tuple[str, ...]:
    keys = tuple(key for spec in group for key in spec.requested_keys)
    if (
        not keys
        or len(keys) > MAXIMUM_CATALOG_INSTRUMENTS
        or keys != tuple(sorted(set(keys)))
    ):
        _fail("capture plan quote route does not define one canonical full universe")
    return keys


def _ohlcv_universe(
    group: list[QualityPilotCaptureSpec],
) -> tuple[tuple[str, ...], tuple[int, ...]]:
    keys = tuple(spec.requested_keys[0] for spec in group)
    tokens = tuple(spec.provider_instrument_token for spec in group)
    if (
        not keys
        or len(keys) > MAXIMUM_CATALOG_INSTRUMENTS
        or keys != tuple(sorted(set(keys)))
    ):
        _fail("capture plan OHLCV route does not define one canonical full universe")
    if (
        any(type(token) is not int or token <= 0 for token in tokens)
        or len(tokens) != len(set(tokens))
    ):
        _fail("capture plan OHLCV route has invalid or duplicate provider tokens")
    return keys, tokens  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class QualityPilotCampaignPlan(_FixedPostureMixin):
    """Append-only canonical prefix of fully planned campaign sessions.

    A future session's exact universe is unknowable until its pre-open catalog
    arrives. Therefore the plan may contain zero through twenty complete
    session routes, but planned sessions must be a prefix of the campaign and
    every included session must already contain all four full-universe routes.
    """

    campaign: QualityPilotCampaignSpec
    capture_specs: tuple[QualityPilotCaptureSpec, ...]
    planned_sessions: tuple[date, ...] = field(init=False)
    plan_id: str = field(init=False)

    def __post_init__(self) -> None:
        planned_sessions = self._validate()
        object.__setattr__(self, "planned_sessions", planned_sessions)
        object.__setattr__(self, "plan_id", self._calculated_id())

    def _validate(self) -> tuple[date, ...]:
        campaign = _verify_campaign(self.campaign)
        if type(self.capture_specs) is not tuple or len(
            self.capture_specs
        ) > MAXIMUM_EXPECTED_CAPTURE_SPECS:
            _fail("capture plan specs must be a bounded exact tuple")
        specs = tuple(_verify_capture_spec(value) for value in self.capture_specs)
        if len({spec.capture_spec_id for spec in specs}) != len(specs):
            _fail("capture plan contains duplicate capture spec identities")
        if tuple(_route_key(spec) for spec in specs) != tuple(
            sorted(_route_key(spec) for spec in specs)
        ):
            _fail("capture plan specs are not in canonical route order")
        if any(
            spec.campaign.campaign_id != campaign.campaign_id
            or spec.protocol_sha256 != PILOT_PROTOCOL_SHA256
            for spec in specs
        ):
            _fail("capture plan spec lineage disagrees with its campaign")
        if specs and len({(spec.provider, spec.provider_version) for spec in specs}) != 1:
            _fail("capture plan must use one exact provider identity")
        if any(not _window_is_inside_authorized_schedule(spec) for spec in specs):
            _fail("capture plan contains a window outside its authorized schedule")

        groups = _group_specs(specs)
        planned_sessions = tuple(
            session
            for session in campaign.confirmed_sessions
            if any(group_session == session for group_session, _ in groups)
        )
        if planned_sessions != campaign.confirmed_sessions[: len(planned_sessions)]:
            _fail("capture plan sessions must be a canonical campaign prefix")
        expected_group_keys = {
            (session, kind)
            for session in planned_sessions
            for kind in _EXPECTED_WINDOW_KINDS
        }
        if set(groups) != expected_group_keys:
            _fail("capture plan does not contain all four windows for every session")

        for session in planned_sessions:
            catalog = groups[(session, ScheduledWindowKind.CATALOG_PREOPEN)]
            quote_0920 = groups[(session, ScheduledWindowKind.QUOTE_0920)]
            quote_close = groups[(session, ScheduledWindowKind.QUOTE_CLOSE)]
            ohlcv = groups[(session, ScheduledWindowKind.OHLCV_CLOSE)]
            for group in (catalog, quote_0920, quote_close, ohlcv):
                _validate_complete_route(group)
            if len(catalog) != 1 or catalog[0].chunk_index != 1 or catalog[0].chunk_count != 1:
                _fail("capture plan catalog route must be exactly one chunk")
            morning_keys = _quote_universe(quote_0920)
            close_keys = _quote_universe(quote_close)
            ohlcv_keys, _ = _ohlcv_universe(ohlcv)
            if morning_keys != close_keys or morning_keys != ohlcv_keys:
                _fail("capture plan universes disagree across quote and OHLCV routes")
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("capture plan safety posture is invalid")
        return planned_sessions

    @property
    def expected_capture_count(self) -> int:
        return len(self.capture_specs)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": QUALITY_PILOT_CAMPAIGN_PLAN_SCHEMA_VERSION,
                "campaign_id": self.campaign.campaign_id,
                "capture_spec_ids": tuple(
                    spec.capture_spec_id for spec in self.capture_specs
                ),
                "planned_sessions": self.planned_sessions,
                "protocol_sha256": self.campaign.protocol_sha256,
                "posture": _posture_tree(self),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        planned_sessions = self._validate()
        if self.planned_sessions != planned_sessions:
            _fail("capture plan planned sessions failed independent verification")
        failed = False
        calculated = ""
        try:
            calculated = self._calculated_id()
        except Exception:
            failed = True
        if failed or self.plan_id != calculated:
            _fail("capture plan identity failed")


@dataclass(frozen=True, slots=True)
class QualityPilotClassificationCount:
    classification: ResponseClassification
    count: int

    def __post_init__(self) -> None:
        if type(self.classification) is not ResponseClassification:
            _fail("classification count has an invalid classification")
        if type(self.count) is not int or self.count < 0:
            _fail("classification count must be a non-negative exact integer")


@dataclass(frozen=True, slots=True)
class QualityPilotCampaignCompletenessLedger(_FixedPostureMixin):
    """Deterministic progress snapshot over one exact campaign plan.

    The ledger never claims that HYP-002 passed. ``OUTCOMES_COMPLETE`` means
    only that every planned request has an immutable success or classified-gap
    outcome and is ready for aggregate quality review.
    """

    plan: QualityPilotCampaignPlan
    run_results: tuple[QualityPilotCaptureRunResult, ...]
    evaluated_at: datetime
    bucket: str
    status: CampaignCompletenessStatus = field(init=False)
    completed_capture_spec_ids: tuple[str, ...] = field(init=False)
    missing_due_capture_spec_ids: tuple[str, ...] = field(init=False)
    pending_capture_spec_ids: tuple[str, ...] = field(init=False)
    classification_counts: tuple[QualityPilotClassificationCount, ...] = field(
        init=False
    )
    completed_session_count: int = field(init=False)
    fully_due_session_count: int = field(init=False)
    unplanned_due_sessions: tuple[date, ...] = field(init=False)
    future_unplanned_sessions: tuple[date, ...] = field(init=False)
    successful_record_count: int = field(init=False)
    published_object_count: int = field(init=False)
    published_encoded_byte_count: int = field(init=False)
    ledger_id: str = field(init=False)

    def __post_init__(self) -> None:
        derived = self._validate_and_derive()
        for name, value in derived.items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "ledger_id", self._calculated_id())

    def _validate_and_derive(self) -> dict[str, object]:
        if type(self.plan) is not QualityPilotCampaignPlan:
            _fail("completeness ledger plan type is invalid")
        plan_failed = False
        try:
            self.plan.verify_content_identity()
        except Exception:
            plan_failed = True
        if plan_failed:
            _fail("completeness ledger plan failed independent verification")
        if type(self.run_results) is not tuple:
            _fail("completeness ledger run results must be an exact tuple")
        if type(self.evaluated_at) is not datetime:
            _fail("completeness ledger evaluated_at is invalid")
        time_failed = False
        offset = None
        try:
            offset = self.evaluated_at.utcoffset()
        except Exception:
            time_failed = True
        if time_failed or offset is None:
            _fail("completeness ledger evaluated_at must be timezone-aware")
        if (
            type(self.bucket) is not str
            or _BUCKET_PATTERN.fullmatch(self.bucket) is None
        ):
            _fail("completeness ledger bucket is invalid")

        verified_runs = tuple(_verify_run_result(value) for value in self.run_results)
        expected_specs = self.plan.capture_specs
        planned_groups = _group_specs(expected_specs)
        planned_universe_by_session = {
            session: _quote_universe(
                planned_groups[(session, ScheduledWindowKind.QUOTE_0920)]
            )
            for session in self.plan.planned_sessions
        }
        expected_by_id = {spec.capture_spec_id: spec for spec in expected_specs}
        expected_index = {
            spec.capture_spec_id: index for index, spec in enumerate(expected_specs)
        }
        run_spec_ids = tuple(run.capture_spec_id for run in verified_runs)
        if len(set(run_spec_ids)) != len(run_spec_ids):
            _fail("completeness ledger contains duplicate outcomes for one capture spec")
        if any(value not in expected_by_id for value in run_spec_ids):
            _fail("completeness ledger contains an outcome outside its exact plan")
        if tuple(expected_index[value] for value in run_spec_ids) != tuple(
            sorted(expected_index[value] for value in run_spec_ids)
        ):
            _fail("completeness ledger outcomes are not in canonical plan order")

        evaluation_failed = False
        evaluated_utc: datetime | None = None
        try:
            evaluated_utc = self.evaluated_at.astimezone(timezone.utc)
        except Exception:
            evaluation_failed = True
        if evaluation_failed or evaluated_utc is None:
            _fail("completeness ledger evaluated_at could not be normalized")

        for run in verified_runs:
            expected = expected_by_id[run.capture_spec_id]
            if (
                run.campaign_id != self.plan.campaign.campaign_id
                or run.capture_spec.capture_spec_id != expected.capture_spec_id
                or run.requested_bucket != self.bucket
                or run.published.bucket != self.bucket
            ):
                _fail("completeness ledger run lineage disagrees with its plan")
            future_failed = False
            future_observation = False
            try:
                future_observation = run.observation.request.request_ended_at > evaluated_utc
            except Exception:
                future_failed = True
            if future_failed or future_observation:
                _fail("completeness ledger contains an outcome not known by evaluated_at")
            if (
                expected.window.window_kind is ScheduledWindowKind.CATALOG_PREOPEN
                and run.observation.request.response_classification
                is ResponseClassification.SUCCESS
            ):
                payload = run.observation.payload
                catalog_matches = False
                try:
                    catalog_matches = (
                        type(payload) is InstrumentBatch
                        and tuple(
                            sorted(item.listing_key for item in payload.instruments)
                        )
                        == planned_universe_by_session[expected.window.market_session]
                    )
                except Exception:
                    catalog_matches = False
                if not catalog_matches:
                    _fail("successful catalog universe disagrees with its session plan")

        completed_set = set(run_spec_ids)
        due_ids: list[str] = []
        pending_ids: list[str] = []
        for spec in expected_specs:
            due_failed = False
            due = False
            try:
                due = spec.window.closes_at <= evaluated_utc
            except Exception:
                due_failed = True
            if due_failed:
                _fail("completeness ledger could not evaluate a capture deadline")
            if due:
                due_ids.append(spec.capture_spec_id)
            else:
                pending_ids.append(spec.capture_spec_id)
        missing_due = tuple(value for value in due_ids if value not in completed_set)
        pending = tuple(value for value in pending_ids if value not in completed_set)
        due_set = set(due_ids)

        unplanned_due: list[date] = []
        future_unplanned: list[date] = []
        planned_session_set = set(self.plan.planned_sessions)
        for session in self.plan.campaign.confirmed_sessions:
            if session in planned_session_set:
                continue
            deadline = datetime.combine(
                session, time(9, 25), tzinfo=INDIA_STANDARD_TIME
            ).astimezone(timezone.utc)
            if deadline <= evaluated_utc:
                unplanned_due.append(session)
            else:
                future_unplanned.append(session)

        counts = {classification: 0 for classification in ResponseClassification}
        successful_record_count = 0
        published_encoded_byte_count = 0
        for run in verified_runs:
            classification = run.observation.request.response_classification
            counts[classification] += 1
            successful_record_count += run.observation.record_count
            published_encoded_byte_count += run.published.encoded_byte_count
        classification_counts = tuple(
            QualityPilotClassificationCount(classification, counts[classification])
            for classification in ResponseClassification
        )

        completed_by_session = {
            session: 0 for session in self.plan.campaign.confirmed_sessions
        }
        expected_by_session = {
            session: 0 for session in self.plan.campaign.confirmed_sessions
        }
        due_by_session = {
            session: 0 for session in self.plan.campaign.confirmed_sessions
        }
        for spec in expected_specs:
            session = spec.window.market_session
            expected_by_session[session] += 1
            if spec.capture_spec_id in completed_set:
                completed_by_session[session] += 1
            if spec.capture_spec_id in due_set:
                due_by_session[session] += 1
        completed_session_count = sum(
            completed_by_session[session] == expected_by_session[session]
            for session in self.plan.planned_sessions
        )
        fully_due_session_count = sum(
            due_by_session[session] == expected_by_session[session]
            for session in self.plan.planned_sessions
        )

        if missing_due or unplanned_due:
            status = CampaignCompletenessStatus.DUE_INCOMPLETE
        elif (
            len(self.plan.planned_sessions) == CONFIRMED_SESSION_COUNT
            and len(verified_runs) == len(expected_specs)
        ):
            status = CampaignCompletenessStatus.OUTCOMES_COMPLETE
        elif not verified_runs:
            status = CampaignCompletenessStatus.NOT_STARTED
        else:
            status = CampaignCompletenessStatus.IN_PROGRESS

        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("completeness ledger safety posture is invalid")
        return {
            "status": status,
            "completed_capture_spec_ids": run_spec_ids,
            "missing_due_capture_spec_ids": missing_due,
            "pending_capture_spec_ids": pending,
            "classification_counts": classification_counts,
            "completed_session_count": completed_session_count,
            "fully_due_session_count": fully_due_session_count,
            "unplanned_due_sessions": tuple(unplanned_due),
            "future_unplanned_sessions": tuple(future_unplanned),
            "successful_record_count": successful_record_count,
            "published_object_count": len(verified_runs),
            "published_encoded_byte_count": published_encoded_byte_count,
        }

    @property
    def expected_capture_count(self) -> int:
        return self.plan.expected_capture_count

    @property
    def completed_capture_count(self) -> int:
        return len(self.completed_capture_spec_ids)

    @property
    def missing_due_capture_count(self) -> int:
        return len(self.missing_due_capture_spec_ids)

    @property
    def pending_capture_count(self) -> int:
        return len(self.pending_capture_spec_ids)

    @property
    def planned_session_count(self) -> int:
        return len(self.plan.planned_sessions)

    @property
    def unplanned_session_count(self) -> int:
        return len(self.unplanned_due_sessions) + len(self.future_unplanned_sessions)

    @property
    def classified_gap_count(self) -> int:
        return sum(
            value.count
            for value in self.classification_counts
            if value.classification is not ResponseClassification.SUCCESS
        )

    @property
    def ready_for_aggregate_quality_review(self) -> bool:
        return self.status is CampaignCompletenessStatus.OUTCOMES_COMPLETE

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": QUALITY_PILOT_COMPLETENESS_LEDGER_SCHEMA_VERSION,
                "plan_id": self.plan.plan_id,
                "evaluated_at": self.evaluated_at.astimezone(timezone.utc),
                "bucket": self.bucket,
                "status": self.status.value,
                "completed_capture_spec_ids": self.completed_capture_spec_ids,
                "missing_due_capture_spec_ids": self.missing_due_capture_spec_ids,
                "pending_capture_spec_ids": self.pending_capture_spec_ids,
                "classification_counts": tuple(
                    (value.classification.value, value.count)
                    for value in self.classification_counts
                ),
                "completed_session_count": self.completed_session_count,
                "fully_due_session_count": self.fully_due_session_count,
                "unplanned_due_sessions": self.unplanned_due_sessions,
                "future_unplanned_sessions": self.future_unplanned_sessions,
                "successful_record_count": self.successful_record_count,
                "published_object_count": self.published_object_count,
                "published_encoded_byte_count": self.published_encoded_byte_count,
                "run_result_ids": tuple(run.run_result_id for run in self.run_results),
                "posture": _posture_tree(self),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        failed = False
        reconstructed: QualityPilotCampaignCompletenessLedger | None = None
        calculated = ""
        try:
            reconstructed = QualityPilotCampaignCompletenessLedger(
                plan=self.plan,
                run_results=self.run_results,
                evaluated_at=self.evaluated_at,
                bucket=self.bucket,
            )
            calculated = self._calculated_id()
        except Exception:
            failed = True
        if (
            failed
            or reconstructed is None
            or self.ledger_id != calculated
            or self.ledger_id != reconstructed.ledger_id
        ):
            _fail("completeness ledger identity failed independent verification")
