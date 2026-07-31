"""Restart-safe local publication artifacts for the accepted promoted
operational runner.

Defines two compact, content-addressed, JSON-persisted record types --
``PromotedOperationalAdvisoryRecord`` (a human-review advisory for every
``COMPLETE``/``FAILED`` runner result) and ``PromotedOperationalTerminalRecord``
(a compact terminal summary referencing the advisory and, for a singular
``COMPLETE`` ``PAPER_BUY``, an exact ``PaperTradeRegistration``) -- plus the
strict codecs and hardened create-once local stores that publish them. It
never modifies the accepted runner/decision/allocation/quote-gate/
preparation types, the paper-ledger, or any legacy operations/recommendation
module. Local advisory creation is not Telegram delivery and grants no
notification or execution authority: every record here is
``paper_only=True`` and permanently ``notification_eligible=False``/
``execution_eligible=False``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.paper_trades.models import PaperTradeRegistration
from india_swing.promoted_operational_decision import (
    PAPER_RESEARCH_WARNING,
    PromotedOperationalDecisionAction,
)
from india_swing.promoted_operational_runner import (
    PromotedOperationalRunFailureCode,
    PromotedOperationalRunResult,
    PromotedOperationalRunSpec,
    PromotedOperationalRunStatus,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_RUNNER_FAILURE_CODE_VALUES = frozenset(
    value.value for value in PromotedOperationalRunFailureCode
)
_MAXIMUM_ADVISORY_TEXT_BYTES = 128 * 1024
_MAXIMUM_ADVISORY_FILE_BYTES = 128 * 1024
_MAXIMUM_TERMINAL_FILE_BYTES = 512 * 1024

_ADVISORY_SCHEMA_VERSION = "promoted-operational-advisory/v1"
_TERMINAL_SCHEMA_VERSION = "promoted-operational-terminal/v1"
_ADVISORY_CODEC_SCHEMA_VERSION = "promoted-operational-advisory-json/v1"
_TERMINAL_CODEC_SCHEMA_VERSION = "promoted-operational-terminal-json/v1"


class PromotedOperationalPersistenceError(ValueError):
    pass


class PromotedOperationalStoreError(PromotedOperationalPersistenceError):
    pass


class PromotedOperationalArtifactNotFound(PromotedOperationalStoreError):
    pass


_ERR_TYPE = "promoted operational persistence type is invalid"
_ERR_RECORD = "promoted operational persistence record is invalid"
_ERR_AUTHORITY = "promoted operational persistence authority flags are invalid"
_ERR_TEXT = "promoted operational persistence text is invalid"


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _require_sha(value: object, message: str) -> str:
    if not _sha(value):
        raise PromotedOperationalPersistenceError(message)
    return value  # type: ignore[return-value]


def _require_optional_sha(value: object, message: str) -> str | None:
    if value is None:
        return None
    return _require_sha(value, message)


def _require_aware_utc(value: object, message: str) -> datetime:
    if type(value) is not datetime:
        raise PromotedOperationalPersistenceError(message)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedOperationalPersistenceError(message) from None
    if value.tzinfo is None or offset is None:
        raise PromotedOperationalPersistenceError(message)
    return value.astimezone(timezone.utc)


def _public_text(value: object, name: str) -> None:
    if (
        type(value) is not str
        or not value.strip()
        or len(value.encode("utf-8")) > _MAXIMUM_ADVISORY_TEXT_BYTES
        or any(ord(character) < 32 and character not in "\n\t" for character in value)
    ):
        raise PromotedOperationalPersistenceError(f"{name} must be safe non-empty text")


def _failure_codes_tuple(value: object) -> None:
    if type(value) is not tuple or any(
        type(item) is not str or item not in _RUNNER_FAILURE_CODE_VALUES for item in value
    ):
        raise PromotedOperationalPersistenceError(_ERR_RECORD)
    if value != tuple(sorted(set(value))):
        raise PromotedOperationalPersistenceError(_ERR_RECORD)


def _failed_advisory_text(
    *,
    spec_id: str,
    result_id: str,
    target_session: date,
    failure_codes: tuple[str, ...],
) -> str:
    lines = [
        PAPER_RESEARCH_WARNING,
        "Status: FAILED",
        f"Target session: {target_session.isoformat()}",
        f"Spec ID: {spec_id}",
        f"Result ID: {result_id}",
        "Failure codes:",
        *[f"- {code}" for code in failure_codes],
        "This package cannot place an order, notify, or grant execution authority.",
    ]
    return "\n".join(lines) + "\n"


@dataclass(frozen=True, slots=True)
class PromotedOperationalAdvisoryRecord:
    """One compact, content-addressed human-review advisory for exactly one
    ``PromotedOperationalRunResult``.

    For a ``COMPLETE`` result the advisory text/hash/action/evaluated_at/
    decision_id/package_id are an exact, unedited copy of the retained
    ``PromotedOperationalDecisionPackage``'s own advisory. For a ``FAILED``
    result the text is deterministically derived from nothing but this
    record's own retained scalar fields, so ``verify_content_identity`` can
    genuinely re-derive and compare it byte-for-byte even without the live
    result.
    """

    spec_id: str
    result_id: str
    target_session: date
    status: PromotedOperationalRunStatus
    action: PromotedOperationalDecisionAction
    evaluated_at: datetime | None
    decision_id: str | None
    package_id: str | None
    failure_codes: tuple[str, ...]
    advisory_text: str
    advisory_sha256: str
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    schema_version: str = _ADVISORY_SCHEMA_VERSION
    advisory_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.evaluated_at is not None:
            object.__setattr__(
                self, "evaluated_at", _require_aware_utc(self.evaluated_at, _ERR_RECORD)
            )
        self._verify()
        object.__setattr__(self, "advisory_id", self._calculated_id())

    def _verify(self) -> None:
        _require_sha(self.spec_id, _ERR_RECORD)
        _require_sha(self.result_id, _ERR_RECORD)
        if type(self.target_session) is not date:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if type(self.status) is not PromotedOperationalRunStatus:
            raise PromotedOperationalPersistenceError(_ERR_TYPE)
        if type(self.action) is not PromotedOperationalDecisionAction:
            raise PromotedOperationalPersistenceError(_ERR_TYPE)
        if self.evaluated_at is not None:
            _require_aware_utc(self.evaluated_at, _ERR_RECORD)
        _require_optional_sha(self.decision_id, _ERR_RECORD)
        _require_optional_sha(self.package_id, _ERR_RECORD)
        _failure_codes_tuple(self.failure_codes)
        _public_text(self.advisory_text, "advisory text")
        if not self.advisory_text.startswith(PAPER_RESEARCH_WARNING + "\n"):
            raise PromotedOperationalPersistenceError(_ERR_TEXT)
        _require_sha(self.advisory_sha256, _ERR_TEXT)
        if hashlib.sha256(self.advisory_text.encode("utf-8")).hexdigest() != self.advisory_sha256:
            raise PromotedOperationalPersistenceError(_ERR_TEXT)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalPersistenceError(_ERR_AUTHORITY)
        if self.schema_version != _ADVISORY_SCHEMA_VERSION:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)

        if self.status is PromotedOperationalRunStatus.COMPLETE:
            if (
                self.failure_codes
                or self.decision_id is None
                or self.package_id is None
                or self.evaluated_at is None
            ):
                raise PromotedOperationalPersistenceError(_ERR_RECORD)
        else:
            if (
                not self.failure_codes
                or self.decision_id is not None
                or self.package_id is not None
                or self.action is not PromotedOperationalDecisionAction.NO_TRADE
            ):
                raise PromotedOperationalPersistenceError(_ERR_RECORD)
            expected_text = _failed_advisory_text(
                spec_id=self.spec_id,
                result_id=self.result_id,
                target_session=self.target_session,
                failure_codes=self.failure_codes,
            )
            if self.advisory_text != expected_text:
                raise PromotedOperationalPersistenceError(_ERR_TEXT)

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "advisory_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.advisory_id != self._calculated_id():
            raise PromotedOperationalPersistenceError(_ERR_RECORD)


def build_promoted_operational_advisory(
    result: PromotedOperationalRunResult,
) -> PromotedOperationalAdvisoryRecord:
    """Pure builder: derive one exact advisory from one already-verified
    ``PromotedOperationalRunResult``. Never reformats or edits a COMPLETE
    result's retained decision-package advisory text."""

    if type(result) is not PromotedOperationalRunResult:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    result.verify_content_identity()
    target_session = result.spec.quote_gate_spec.preparation.manifest.target_session

    if result.status is PromotedOperationalRunStatus.COMPLETE:
        package = result.decision_package
        decision = package.decision
        return PromotedOperationalAdvisoryRecord(
            spec_id=result.spec.spec_id,
            result_id=result.result_id,
            target_session=target_session,
            status=result.status,
            action=decision.action,
            evaluated_at=result.evaluated_at,
            decision_id=decision.decision_id,
            package_id=package.package_id,
            failure_codes=(),
            advisory_text=package.advisory_text,
            advisory_sha256=package.advisory_sha256,
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )

    text = _failed_advisory_text(
        spec_id=result.spec.spec_id,
        result_id=result.result_id,
        target_session=target_session,
        failure_codes=result.failure_codes,
    )
    return PromotedOperationalAdvisoryRecord(
        spec_id=result.spec.spec_id,
        result_id=result.result_id,
        target_session=target_session,
        status=result.status,
        action=PromotedOperationalDecisionAction.NO_TRADE,
        evaluated_at=result.evaluated_at,
        decision_id=None,
        package_id=None,
        failure_codes=result.failure_codes,
        advisory_text=text,
        advisory_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )


def promoted_paper_registration_from_result(
    result: PromotedOperationalRunResult,
    advisory: PromotedOperationalAdvisoryRecord,
) -> PaperTradeRegistration | None:
    """Pure adapter: ``None`` unless ``result`` is an exact COMPLETE run whose
    decision action is ``PAPER_BUY``. Every mapped value comes only from
    retained result/advisory fields; nothing is invented or widened."""

    if type(result) is not PromotedOperationalRunResult:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    result.verify_content_identity()
    if type(advisory) is not PromotedOperationalAdvisoryRecord:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    advisory.verify_content_identity()
    if advisory.spec_id != result.spec.spec_id or advisory.result_id != result.result_id:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)
    if result.status is not PromotedOperationalRunStatus.COMPLETE:
        return None
    decision = result.decision_package.decision
    if decision.action is not PromotedOperationalDecisionAction.PAPER_BUY:
        return None
    recommendation = decision.recommendation
    if recommendation is None:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)

    outcome = recommendation.outcome
    candidate = outcome.quote_outcome.candidate
    evaluation_intent = candidate.research_intent.evaluation_intent
    entry_order = evaluation_intent.entry_order
    quote_gate_spec = outcome.quote_outcome.spec
    preparation_id = quote_gate_spec.preparation.manifest.preparation_id

    return PaperTradeRegistration(
        alert_id=advisory.advisory_id,
        source_run_id=result.result_id,
        source_pipeline_integrity_hash=preparation_id,
        source_decision_integrity_hash=decision.decision_id,
        signal_id=candidate.candidate_id,
        symbol=recommendation.symbol,
        quantity=recommendation.quantity,
        decision_time=result.evaluated_at,
        earliest_entry_at=quote_gate_spec.decision_not_before,
        entry_expires_at=quote_gate_spec.decision_deadline,
        entry_low=outcome.reference_entry_price,
        entry_high=entry_order.limit_price,
        stop=evaluation_intent.stop_price,
        target=evaluation_intent.target_price,
        max_holding_sessions=evaluation_intent.max_holding_sessions,
        estimated_round_trip_cost=outcome.estimated_round_trip_cost,
    )


@dataclass(frozen=True, slots=True)
class PromotedOperationalTerminalRecord:
    """One compact, content-addressed publication summary for exactly one
    ``PromotedOperationalRunResult``. Not independent provenance and not a
    serialization of the entire runner graph -- every retained identifier is
    cross-checked against its exact retained parent before first write."""

    spec_id: str
    result_id: str
    target_session: date
    status: PromotedOperationalRunStatus
    action: PromotedOperationalDecisionAction
    started_at: datetime
    evaluated_at: datetime | None
    completed_at: datetime
    quote_source_id: str | None
    portfolio_source_id: str | None
    preparation_id: str
    quote_batch_id: str | None
    portfolio_context_id: str | None
    portfolio_snapshot_id: str | None
    quote_gate_batch_id: str | None
    allocation_batch_id: str | None
    decision_id: str | None
    package_id: str | None
    advisory_id: str
    advisory_sha256: str
    paper_registration_id: str | None
    failure_codes: tuple[str, ...]
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    schema_version: str = _TERMINAL_SCHEMA_VERSION
    terminal_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "started_at", _require_aware_utc(self.started_at, _ERR_RECORD)
        )
        object.__setattr__(
            self, "completed_at", _require_aware_utc(self.completed_at, _ERR_RECORD)
        )
        if self.evaluated_at is not None:
            object.__setattr__(
                self, "evaluated_at", _require_aware_utc(self.evaluated_at, _ERR_RECORD)
            )
        self._verify()
        object.__setattr__(self, "terminal_id", self._calculated_id())

    def _verify(self) -> None:
        _require_sha(self.spec_id, _ERR_RECORD)
        _require_sha(self.result_id, _ERR_RECORD)
        _require_sha(self.preparation_id, _ERR_RECORD)
        _require_sha(self.advisory_id, _ERR_RECORD)
        _require_sha(self.advisory_sha256, _ERR_RECORD)
        for value in (
            self.quote_source_id,
            self.portfolio_source_id,
            self.quote_batch_id,
            self.portfolio_context_id,
            self.portfolio_snapshot_id,
            self.quote_gate_batch_id,
            self.allocation_batch_id,
            self.decision_id,
            self.package_id,
            self.paper_registration_id,
        ):
            _require_optional_sha(value, _ERR_RECORD)
        if type(self.target_session) is not date:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if type(self.status) is not PromotedOperationalRunStatus:
            raise PromotedOperationalPersistenceError(_ERR_TYPE)
        if type(self.action) is not PromotedOperationalDecisionAction:
            raise PromotedOperationalPersistenceError(_ERR_TYPE)

        started_at = _require_aware_utc(self.started_at, _ERR_RECORD)
        completed_at = _require_aware_utc(self.completed_at, _ERR_RECORD)
        if completed_at < started_at:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if self.evaluated_at is not None:
            evaluated_at = _require_aware_utc(self.evaluated_at, _ERR_RECORD)
            clock_violation = (
                PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value
                in self.failure_codes
            )
            if completed_at < evaluated_at:
                raise PromotedOperationalPersistenceError(_ERR_RECORD)
            # CLOCK_NON_MONOTONIC exists precisely to retain, for audit, an
            # evaluated_at that violated ordering against started_at -- so
            # that specific ordering is not re-enforced here when the code
            # is present, mirroring the accepted runner's own result-level
            # relaxation for the identical failure code.
            if not clock_violation and evaluated_at < started_at:
                raise PromotedOperationalPersistenceError(_ERR_RECORD)

        _failure_codes_tuple(self.failure_codes)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalPersistenceError(_ERR_AUTHORITY)
        if self.schema_version != _TERMINAL_SCHEMA_VERSION:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)

        # Prefix-closedness at the ID level, mirroring the runner's own
        # exact stage-prefix depth invariant (a terminal never claims a
        # deeper lineage than its own retained result actually produced).
        # portfolio_context_id/portfolio_snapshot_id must always be present
        # or absent together -- this pairing is spec-independent, unlike
        # the quote_batch_id contiguity rule (which depends on whether the
        # spec's preparation has any candidates at all and is therefore
        # enforced in _verify_terminal_matches_spec instead).
        if (self.portfolio_context_id is None) != (self.portfolio_snapshot_id is None):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if self.allocation_batch_id is not None and self.quote_gate_batch_id is None:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if self.quote_gate_batch_id is not None and (
            self.portfolio_context_id is None or self.portfolio_snapshot_id is None
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if (self.decision_id is None) != (self.package_id is None):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if self.decision_id is not None and self.allocation_batch_id is None:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)

        if self.status is PromotedOperationalRunStatus.FAILED:
            if (
                self.action is not PromotedOperationalDecisionAction.NO_TRADE
                or not self.failure_codes
                or self.decision_id is not None
                or self.package_id is not None
                or self.paper_registration_id is not None
            ):
                raise PromotedOperationalPersistenceError(_ERR_RECORD)
        else:
            if (
                self.failure_codes
                or self.evaluated_at is None
                or self.decision_id is None
                or self.package_id is None
                or self.portfolio_context_id is None
                or self.portfolio_snapshot_id is None
                or self.quote_gate_batch_id is None
                or self.allocation_batch_id is None
            ):
                raise PromotedOperationalPersistenceError(_ERR_RECORD)
            if self.action is PromotedOperationalDecisionAction.PAPER_BUY:
                if self.paper_registration_id is None:
                    raise PromotedOperationalPersistenceError(_ERR_RECORD)
            elif self.paper_registration_id is not None:
                raise PromotedOperationalPersistenceError(_ERR_RECORD)

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "terminal_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.terminal_id != self._calculated_id():
            raise PromotedOperationalPersistenceError(_ERR_RECORD)


def _terminal_stage_depth(terminal: PromotedOperationalTerminalRecord) -> int:
    """Mirror the accepted runner's own artifact-prefix depth ladder
    (quote batch -> portfolio context -> quote-gate batch -> allocation
    batch -> decision package) using only the terminal's compact ID
    fields."""

    depth = 0
    if terminal.quote_batch_id is not None:
        depth = 1
    if terminal.portfolio_context_id is not None:
        depth = 2
    if terminal.quote_gate_batch_id is not None:
        depth = 3
    if terminal.allocation_batch_id is not None:
        depth = 4
    if terminal.decision_id is not None:
        depth = 5
    return depth


def _verify_terminal_matches_spec(
    terminal: PromotedOperationalTerminalRecord, spec: PromotedOperationalRunSpec
) -> None:
    """Bind a persisted compact terminal's status/action/time/depth/
    failure-code shape to the exact live run spec, replaying
    ``PromotedOperationalRunResult``'s own stage-prefix truth table against
    the terminal's compact fields. A self-consistent terminal-file rewrite
    that disagrees with what this exact spec could ever structurally
    produce is rejected here even though the terminal's own per-field hash
    still recomputes correctly."""

    if type(terminal) is not PromotedOperationalTerminalRecord:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    terminal.verify_content_identity()
    if type(spec) is not PromotedOperationalRunSpec:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    spec.verify_content_identity()
    if terminal.spec_id != spec.spec_id:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)
    manifest = spec.quote_gate_spec.preparation.manifest
    if (
        terminal.target_session != manifest.target_session
        or terminal.preparation_id != manifest.preparation_id
    ):
        raise PromotedOperationalPersistenceError(_ERR_RECORD)

    not_before = spec.quote_gate_spec.decision_not_before
    deadline = spec.quote_gate_spec.decision_deadline
    started_in_window = not_before <= terminal.started_at <= deadline
    both_sources_pinned = (
        terminal.quote_source_id == spec.expected_quote_source_id
        and terminal.portfolio_source_id == spec.expected_portfolio_source_id
    )
    candidates_present = bool(spec.quote_gate_spec.preparation.candidates)

    # Quote-batch contiguity depends on the spec's own candidate presence,
    # which the terminal's own standalone _verify() cannot see: for a spec
    # WITH candidates, the runner always acquires the quote batch strictly
    # before portfolio context (or anything deeper), on every status, so
    # any terminal that reached portfolio-or-deeper without quote_batch_id
    # is structurally impossible. For a ZERO-candidate spec, the quote
    # source is never touched at all, on every status, so quote_batch_id
    # must always be absent. Enforcing this directly (rather than only
    # through the deepest-truthy-field depth ladder below) closes the gap
    # where a forged terminal could drop quote_batch_id while keeping every
    # deeper ID present and still compute the same apparent depth.
    reached_portfolio_or_deeper = (
        terminal.portfolio_context_id is not None
        or terminal.quote_gate_batch_id is not None
        or terminal.allocation_batch_id is not None
        or terminal.decision_id is not None
    )
    if candidates_present:
        if reached_portfolio_or_deeper and terminal.quote_batch_id is None:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif terminal.quote_batch_id is not None:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)

    depth = _terminal_stage_depth(terminal)

    if terminal.status is PromotedOperationalRunStatus.COMPLETE:
        if (
            terminal.failure_codes
            or depth != 5
            or terminal.evaluated_at is None
            or terminal.evaluated_at > deadline
            or not started_in_window
            or not both_sources_pinned
            or terminal.completed_at > deadline
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        return

    F = PromotedOperationalRunFailureCode
    codes = set(terminal.failure_codes)
    clock_violation = F.CLOCK_NON_MONOTONIC.value in codes
    primary_codes = codes - {F.CLOCK_NON_MONOTONIC.value}
    if len(primary_codes) > 1:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)
    primary = next(iter(primary_codes), None)

    clock_compatible_primary_codes = {
        F.QUOTE_ACQUISITION_FAILED.value,
        F.QUOTE_COVERAGE_INVALID.value,
        F.PORTFOLIO_ACQUISITION_FAILED.value,
        F.QUOTE_GATE_FAILED.value,
        F.ALLOCATION_FAILED.value,
        F.DECISION_ASSEMBLY_FAILED.value,
    }
    if (
        clock_violation
        and primary is not None
        and primary not in clock_compatible_primary_codes
    ):
        raise PromotedOperationalPersistenceError(_ERR_RECORD)
    if clock_violation:
        if primary in {
            F.QUOTE_ACQUISITION_FAILED.value,
            F.QUOTE_COVERAGE_INVALID.value,
            F.PORTFOLIO_ACQUISITION_FAILED.value,
        }:
            expected_clock_failure_completion = terminal.started_at
        elif primary in {
            F.QUOTE_GATE_FAILED.value,
            F.ALLOCATION_FAILED.value,
            F.DECISION_ASSEMBLY_FAILED.value,
        }:
            expected_clock_failure_completion = terminal.evaluated_at
        elif depth == 2:
            expected_clock_failure_completion = max(
                terminal.started_at, terminal.evaluated_at or terminal.started_at
            )
        else:
            expected_clock_failure_completion = terminal.evaluated_at
        if (
            expected_clock_failure_completion is None
            or terminal.completed_at != expected_clock_failure_completion
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)

    if primary == F.START_BEFORE_WINDOW.value:
        if (
            clock_violation
            or depth != 0
            or terminal.evaluated_at is not None
            or terminal.quote_source_id is not None
            or terminal.portfolio_source_id is not None
            or terminal.started_at >= not_before
            or terminal.completed_at != terminal.started_at
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif primary == F.START_AFTER_DEADLINE.value:
        if (
            clock_violation
            or depth != 0
            or terminal.evaluated_at is not None
            or terminal.quote_source_id is not None
            or terminal.portfolio_source_id is not None
            or terminal.started_at <= deadline
            or terminal.completed_at != terminal.started_at
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif primary == F.SOURCE_IDENTITY_INVALID.value:
        if (
            clock_violation
            or depth != 0
            or terminal.evaluated_at is not None
            or not started_in_window
            or both_sources_pinned
            or terminal.completed_at != terminal.started_at
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif primary in (F.QUOTE_ACQUISITION_FAILED.value, F.QUOTE_COVERAGE_INVALID.value):
        if (
            depth != 0
            or terminal.evaluated_at is not None
            or not started_in_window
            or not both_sources_pinned
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif primary == F.PORTFOLIO_ACQUISITION_FAILED.value:
        expected_depth = 1 if candidates_present else 0
        if (
            depth != expected_depth
            or terminal.evaluated_at is not None
            or not started_in_window
            or not both_sources_pinned
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif primary in (F.EVALUATION_AFTER_DEADLINE.value, F.QUOTE_GATE_FAILED.value):
        if (
            depth != 2
            or terminal.evaluated_at is None
            or not started_in_window
            or not both_sources_pinned
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if primary == F.EVALUATION_AFTER_DEADLINE.value and terminal.evaluated_at <= deadline:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if (
            primary == F.EVALUATION_AFTER_DEADLINE.value
            and terminal.completed_at != terminal.evaluated_at
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if primary == F.QUOTE_GATE_FAILED.value and terminal.evaluated_at > deadline:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif primary == F.ALLOCATION_FAILED.value:
        if (
            depth != 3
            or terminal.evaluated_at is None
            or terminal.evaluated_at > deadline
            or not started_in_window
            or not both_sources_pinned
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif primary in (F.DECISION_ASSEMBLY_FAILED.value, F.COMPLETION_AFTER_DEADLINE.value):
        if (
            depth != 4
            or terminal.evaluated_at is None
            or terminal.evaluated_at > deadline
            or not started_in_window
            or not both_sources_pinned
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if primary == F.COMPLETION_AFTER_DEADLINE.value and terminal.completed_at <= deadline:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    elif primary is None:
        # CLOCK_NON_MONOTONIC alone: only the evaluation prefix (depth 2)
        # or the completion prefix (depth 4) are structurally reachable.
        if (
            not clock_violation
            or depth not in (2, 4)
            or not started_in_window
            or not both_sources_pinned
        ):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        if depth == 4 and (terminal.evaluated_at is None or terminal.evaluated_at > deadline):
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    else:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)


def verify_promoted_operational_published_bundle(
    terminal: PromotedOperationalTerminalRecord,
    advisory: PromotedOperationalAdvisoryRecord,
    registration: PaperTradeRegistration | None,
) -> None:
    """Centralized cross-artifact verifier: a terminal record is not
    independent provenance of its own referenced advisory/registration --
    every mutually checkable field must agree, or a self-consistent rewrite
    of one artifact against an untouched sibling (each individually still
    hashing to its own recomputed ID) would silently pass."""

    if type(terminal) is not PromotedOperationalTerminalRecord:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    terminal.verify_content_identity()
    if type(advisory) is not PromotedOperationalAdvisoryRecord:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    advisory.verify_content_identity()

    if (
        terminal.advisory_id != advisory.advisory_id
        or terminal.advisory_sha256 != advisory.advisory_sha256
        or terminal.spec_id != advisory.spec_id
        or terminal.result_id != advisory.result_id
        or terminal.target_session != advisory.target_session
        or terminal.status != advisory.status
        or terminal.action != advisory.action
        or terminal.evaluated_at != advisory.evaluated_at
        or terminal.decision_id != advisory.decision_id
        or terminal.package_id != advisory.package_id
        or terminal.failure_codes != advisory.failure_codes
        or terminal.paper_only != advisory.paper_only
        or terminal.notification_eligible != advisory.notification_eligible
        or terminal.execution_eligible != advisory.execution_eligible
    ):
        raise PromotedOperationalPersistenceError(_ERR_RECORD)

    if terminal.paper_registration_id is None:
        if registration is not None:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        return

    if type(registration) is not PaperTradeRegistration:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)
    registration.verify_content_identity()
    if (
        registration.registration_id != terminal.paper_registration_id
        or registration.alert_id != terminal.advisory_id
        or registration.source_run_id != terminal.result_id
        or registration.source_pipeline_integrity_hash != terminal.preparation_id
        or registration.source_decision_integrity_hash != terminal.decision_id
        or registration.decision_time != terminal.evaluated_at
        or terminal.action is not PromotedOperationalDecisionAction.PAPER_BUY
    ):
        raise PromotedOperationalPersistenceError(_ERR_RECORD)


def build_promoted_operational_terminal_record(
    result: PromotedOperationalRunResult,
    advisory: PromotedOperationalAdvisoryRecord,
    registration: PaperTradeRegistration | None,
) -> PromotedOperationalTerminalRecord:
    """Pure builder: derive one exact terminal record from one already-
    verified result, its exact advisory, and its optional exact paper
    registration -- replaying the registration mapping rather than trusting
    a supplied one."""

    if type(result) is not PromotedOperationalRunResult:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    result.verify_content_identity()
    if type(advisory) is not PromotedOperationalAdvisoryRecord:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    advisory.verify_content_identity()
    if advisory.spec_id != result.spec.spec_id or advisory.result_id != result.result_id:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)

    expected_registration = promoted_paper_registration_from_result(result, advisory)
    if expected_registration is None:
        if registration is not None:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
    else:
        if type(registration) is not PaperTradeRegistration:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)
        registration.verify_content_identity()
        if registration.registration_id != expected_registration.registration_id:
            raise PromotedOperationalPersistenceError(_ERR_RECORD)

    preparation_id = result.spec.quote_gate_spec.preparation.manifest.preparation_id
    quote_batch = result.quote_batch
    portfolio_context = result.portfolio_context
    quote_gate_batch = result.quote_gate_batch
    allocation_batch = result.allocation_batch

    terminal = PromotedOperationalTerminalRecord(
        spec_id=result.spec.spec_id,
        result_id=result.result_id,
        target_session=advisory.target_session,
        status=result.status,
        action=advisory.action,
        started_at=result.started_at,
        evaluated_at=result.evaluated_at,
        completed_at=result.completed_at,
        quote_source_id=result.quote_source_id,
        portfolio_source_id=result.portfolio_source_id,
        preparation_id=preparation_id,
        quote_batch_id=None if quote_batch is None else quote_batch.batch_id,
        portfolio_context_id=(
            None if portfolio_context is None else portfolio_context.context_id
        ),
        portfolio_snapshot_id=(
            None
            if portfolio_context is None
            else portfolio_context.portfolio.portfolio_snapshot_id
        ),
        quote_gate_batch_id=None if quote_gate_batch is None else quote_gate_batch.batch_id,
        allocation_batch_id=(
            None if allocation_batch is None else allocation_batch.allocation_batch_id
        ),
        decision_id=advisory.decision_id,
        package_id=advisory.package_id,
        advisory_id=advisory.advisory_id,
        advisory_sha256=advisory.advisory_sha256,
        paper_registration_id=None if registration is None else registration.registration_id,
        failure_codes=result.failure_codes,
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )
    # Defense-in-depth: a builder-produced terminal must itself satisfy the
    # exact same spec-binding and cross-artifact rules a later sealed replay
    # will re-check against the persisted bytes.
    _verify_terminal_matches_spec(terminal, result.spec)
    verify_promoted_operational_published_bundle(terminal, advisory, registration)
    return terminal


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
            raise PromotedOperationalStoreError("stored JSON has duplicate keys")
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalStoreError("stored JSON contains a rejected numeric literal")


def _load_envelope(payload: bytes, *, maximum_bytes: int) -> dict[str, object]:
    if type(payload) is not bytes or not payload or len(payload) > maximum_bytes:
        raise PromotedOperationalStoreError("stored bytes are invalid")
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise PromotedOperationalStoreError("stored bytes are not valid UTF-8") from None
    try:
        value = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedOperationalStoreError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise PromotedOperationalStoreError("stored JSON is malformed") from None
    if type(value) is not dict:
        raise PromotedOperationalStoreError("stored envelope is invalid")
    return value


_ADVISORY_FIELDS = frozenset(
    {
        "spec_id",
        "result_id",
        "target_session",
        "status",
        "action",
        "evaluated_at",
        "decision_id",
        "package_id",
        "failure_codes",
        "advisory_text",
        "advisory_sha256",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
        "schema_version",
        "advisory_id",
    }
)


def encode_promoted_operational_advisory(value: PromotedOperationalAdvisoryRecord) -> bytes:
    if type(value) is not PromotedOperationalAdvisoryRecord:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    value.verify_content_identity()
    data = {
        "spec_id": value.spec_id,
        "result_id": value.result_id,
        "target_session": value.target_session.isoformat(),
        "status": value.status.value,
        "action": value.action.value,
        "evaluated_at": None if value.evaluated_at is None else value.evaluated_at.isoformat(),
        "decision_id": value.decision_id,
        "package_id": value.package_id,
        "failure_codes": list(value.failure_codes),
        "advisory_text": value.advisory_text,
        "advisory_sha256": value.advisory_sha256,
        "paper_only": value.paper_only,
        "notification_eligible": value.notification_eligible,
        "execution_eligible": value.execution_eligible,
        "schema_version": value.schema_version,
        "advisory_id": value.advisory_id,
    }
    payload = (
        json.dumps(
            {"codec_schema_version": _ADVISORY_CODEC_SCHEMA_VERSION, "advisory": data},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_ADVISORY_FILE_BYTES:
        raise PromotedOperationalPersistenceError(_ERR_TEXT)
    return payload


def decode_promoted_operational_advisory(payload: bytes) -> PromotedOperationalAdvisoryRecord:
    root = _load_envelope(payload, maximum_bytes=_MAXIMUM_ADVISORY_FILE_BYTES)
    if (
        set(root) != {"codec_schema_version", "advisory"}
        or root["codec_schema_version"] != _ADVISORY_CODEC_SCHEMA_VERSION
    ):
        raise PromotedOperationalStoreError("stored advisory envelope is invalid")
    raw = root["advisory"]
    if type(raw) is not dict or set(raw) != _ADVISORY_FIELDS:
        raise PromotedOperationalStoreError("stored advisory fields are invalid")
    try:
        evaluated_at = raw["evaluated_at"]
        value = PromotedOperationalAdvisoryRecord(
            spec_id=raw["spec_id"],
            result_id=raw["result_id"],
            target_session=date.fromisoformat(raw["target_session"]),
            status=PromotedOperationalRunStatus(raw["status"]),
            action=PromotedOperationalDecisionAction(raw["action"]),
            evaluated_at=None if evaluated_at is None else datetime.fromisoformat(evaluated_at),
            decision_id=raw["decision_id"],
            package_id=raw["package_id"],
            failure_codes=tuple(raw["failure_codes"]),
            advisory_text=raw["advisory_text"],
            advisory_sha256=raw["advisory_sha256"],
            paper_only=raw["paper_only"],
            notification_eligible=raw["notification_eligible"],
            execution_eligible=raw["execution_eligible"],
            schema_version=raw["schema_version"],
        )
    except Exception:
        raise PromotedOperationalStoreError("stored advisory is invalid") from None
    if value.advisory_id != raw["advisory_id"]:
        raise PromotedOperationalStoreError("stored advisory identity differs")
    if encode_promoted_operational_advisory(value) != payload:
        raise PromotedOperationalStoreError("stored advisory is not canonically encoded")
    return value


_TERMINAL_FIELDS = frozenset(
    {
        "spec_id",
        "result_id",
        "target_session",
        "status",
        "action",
        "started_at",
        "evaluated_at",
        "completed_at",
        "quote_source_id",
        "portfolio_source_id",
        "preparation_id",
        "quote_batch_id",
        "portfolio_context_id",
        "portfolio_snapshot_id",
        "quote_gate_batch_id",
        "allocation_batch_id",
        "decision_id",
        "package_id",
        "advisory_id",
        "advisory_sha256",
        "paper_registration_id",
        "failure_codes",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
        "schema_version",
        "terminal_id",
    }
)


def encode_promoted_operational_terminal_record(
    value: PromotedOperationalTerminalRecord,
) -> bytes:
    if type(value) is not PromotedOperationalTerminalRecord:
        raise PromotedOperationalPersistenceError(_ERR_TYPE)
    value.verify_content_identity()
    data = {
        "spec_id": value.spec_id,
        "result_id": value.result_id,
        "target_session": value.target_session.isoformat(),
        "status": value.status.value,
        "action": value.action.value,
        "started_at": value.started_at.isoformat(),
        "evaluated_at": None if value.evaluated_at is None else value.evaluated_at.isoformat(),
        "completed_at": value.completed_at.isoformat(),
        "quote_source_id": value.quote_source_id,
        "portfolio_source_id": value.portfolio_source_id,
        "preparation_id": value.preparation_id,
        "quote_batch_id": value.quote_batch_id,
        "portfolio_context_id": value.portfolio_context_id,
        "portfolio_snapshot_id": value.portfolio_snapshot_id,
        "quote_gate_batch_id": value.quote_gate_batch_id,
        "allocation_batch_id": value.allocation_batch_id,
        "decision_id": value.decision_id,
        "package_id": value.package_id,
        "advisory_id": value.advisory_id,
        "advisory_sha256": value.advisory_sha256,
        "paper_registration_id": value.paper_registration_id,
        "failure_codes": list(value.failure_codes),
        "paper_only": value.paper_only,
        "notification_eligible": value.notification_eligible,
        "execution_eligible": value.execution_eligible,
        "schema_version": value.schema_version,
        "terminal_id": value.terminal_id,
    }
    payload = (
        json.dumps(
            {"codec_schema_version": _TERMINAL_CODEC_SCHEMA_VERSION, "terminal": data},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_TERMINAL_FILE_BYTES:
        raise PromotedOperationalPersistenceError(_ERR_RECORD)
    return payload


def decode_promoted_operational_terminal_record(
    payload: bytes,
) -> PromotedOperationalTerminalRecord:
    root = _load_envelope(payload, maximum_bytes=_MAXIMUM_TERMINAL_FILE_BYTES)
    if (
        set(root) != {"codec_schema_version", "terminal"}
        or root["codec_schema_version"] != _TERMINAL_CODEC_SCHEMA_VERSION
    ):
        raise PromotedOperationalStoreError("stored terminal envelope is invalid")
    raw = root["terminal"]
    if type(raw) is not dict or set(raw) != _TERMINAL_FIELDS:
        raise PromotedOperationalStoreError("stored terminal fields are invalid")
    try:
        evaluated_at = raw["evaluated_at"]
        value = PromotedOperationalTerminalRecord(
            spec_id=raw["spec_id"],
            result_id=raw["result_id"],
            target_session=date.fromisoformat(raw["target_session"]),
            status=PromotedOperationalRunStatus(raw["status"]),
            action=PromotedOperationalDecisionAction(raw["action"]),
            started_at=datetime.fromisoformat(raw["started_at"]),
            evaluated_at=None if evaluated_at is None else datetime.fromisoformat(evaluated_at),
            completed_at=datetime.fromisoformat(raw["completed_at"]),
            quote_source_id=raw["quote_source_id"],
            portfolio_source_id=raw["portfolio_source_id"],
            preparation_id=raw["preparation_id"],
            quote_batch_id=raw["quote_batch_id"],
            portfolio_context_id=raw["portfolio_context_id"],
            portfolio_snapshot_id=raw["portfolio_snapshot_id"],
            quote_gate_batch_id=raw["quote_gate_batch_id"],
            allocation_batch_id=raw["allocation_batch_id"],
            decision_id=raw["decision_id"],
            package_id=raw["package_id"],
            advisory_id=raw["advisory_id"],
            advisory_sha256=raw["advisory_sha256"],
            paper_registration_id=raw["paper_registration_id"],
            failure_codes=tuple(raw["failure_codes"]),
            paper_only=raw["paper_only"],
            notification_eligible=raw["notification_eligible"],
            execution_eligible=raw["execution_eligible"],
            schema_version=raw["schema_version"],
        )
    except Exception:
        raise PromotedOperationalStoreError("stored terminal is invalid") from None
    if value.terminal_id != raw["terminal_id"]:
        raise PromotedOperationalStoreError("stored terminal identity differs")
    if encode_promoted_operational_terminal_record(value) != payload:
        raise PromotedOperationalStoreError("stored terminal is not canonically encoded")
    return value


def _create_once(target: Path, payload: bytes, directory: Path, *, prefix: str) -> None:
    descriptor, name = tempfile.mkstemp(prefix=prefix, suffix=".tmp", dir=directory)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


class LocalPromotedOperationalAdvisoryOutbox:
    """Create-once local advisory handoff keyed by the immutable advisory ID.

    This is a local human-review artifact only: it grants no notification or
    execution authority. A later, explicitly authorized Telegram adapter may
    consume a sealed advisory after its own delivery policy.
    """

    _DIRECTORY = "advisories"
    _LOCK_FILENAME = ".promoted-operational-advisory.lock"

    def __init__(self, root: Path) -> None:
        self.root = Path(root) / self._DIRECTORY

    def path_for(self, advisory_id: str) -> Path:
        return self.root / f"{_require_sha(advisory_id, _ERR_RECORD)}.json"

    def put(
        self, value: PromotedOperationalAdvisoryRecord
    ) -> PromotedOperationalAdvisoryRecord:
        if type(value) is not PromotedOperationalAdvisoryRecord:
            raise PromotedOperationalStoreError("advisory must be exact")
        try:
            payload = encode_promoted_operational_advisory(value)
        except PromotedOperationalPersistenceError:
            raise PromotedOperationalStoreError("advisory is invalid") from None
        target = self.path_for(value.advisory_id)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.root):
                raise PromotedOperationalStoreError("advisory root cannot be a link")
            with advisory_file_lock(self.root / self._LOCK_FILENAME):
                if target.exists():
                    stored = self.get(value.advisory_id)
                    if stored.advisory_id != value.advisory_id or stored != value:
                        raise PromotedOperationalStoreError(
                            "advisory ID already stores different content"
                        )
                    return stored
                _create_once(target, payload, self.root, prefix=".promoted-advisory-")
        except PromotedOperationalStoreError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PromotedOperationalStoreError("advisory outbox is unavailable") from None
        return self.get(value.advisory_id)

    def get(self, advisory_id: str) -> PromotedOperationalAdvisoryRecord:
        path = self.path_for(advisory_id)
        if not path.exists():
            raise PromotedOperationalArtifactNotFound("advisory was not found")
        if not path.is_file() or _is_link_like(path):
            raise PromotedOperationalStoreError("advisory must be a regular file")
        try:
            value = decode_promoted_operational_advisory(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_ADVISORY_FILE_BYTES)
            )
        except PromotedOperationalStoreError:
            raise
        except FileSafetyError:
            raise PromotedOperationalStoreError("advisory could not be read safely") from None
        if value.advisory_id != advisory_id:
            raise PromotedOperationalStoreError("advisory differs from its path")
        return value


class LocalPromotedOperationalTerminalStore:
    """Create-once local terminal record keyed by the immutable run spec ID.

    ``get_optional`` returns ``None`` only for exact, unambiguous absence at
    the canonical path; every other unsafe or malformed condition raises
    rather than being silently treated as absence.
    """

    _DIRECTORY = "terminals"
    _LOCK_FILENAME = ".promoted-operational-terminal.lock"

    def __init__(self, root: Path) -> None:
        self.root = Path(root) / self._DIRECTORY

    def path_for(self, spec_id: str) -> Path:
        return self.root / f"{_require_sha(spec_id, _ERR_RECORD)}.json"

    def put(self, value: PromotedOperationalTerminalRecord) -> PromotedOperationalTerminalRecord:
        if type(value) is not PromotedOperationalTerminalRecord:
            raise PromotedOperationalStoreError("terminal record must be exact")
        try:
            payload = encode_promoted_operational_terminal_record(value)
        except PromotedOperationalPersistenceError:
            raise PromotedOperationalStoreError("terminal record is invalid") from None
        target = self.path_for(value.spec_id)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.root):
                raise PromotedOperationalStoreError("terminal root cannot be a link")
            with advisory_file_lock(self.root / self._LOCK_FILENAME):
                if target.exists():
                    stored = self.get(value.spec_id)
                    if stored.terminal_id != value.terminal_id or stored != value:
                        raise PromotedOperationalStoreError(
                            "run spec already stores a different terminal record"
                        )
                    return stored
                _create_once(target, payload, self.root, prefix=".promoted-terminal-")
        except PromotedOperationalStoreError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PromotedOperationalStoreError("terminal store is unavailable") from None
        return self.get(value.spec_id)

    def get(self, spec_id: str) -> PromotedOperationalTerminalRecord:
        path = self.path_for(spec_id)
        if not path.exists():
            raise PromotedOperationalArtifactNotFound("terminal record was not found")
        if not path.is_file() or _is_link_like(path):
            raise PromotedOperationalStoreError("terminal record must be a regular file")
        try:
            value = decode_promoted_operational_terminal_record(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_TERMINAL_FILE_BYTES)
            )
        except PromotedOperationalStoreError:
            raise
        except FileSafetyError:
            raise PromotedOperationalStoreError(
                "terminal record could not be read safely"
            ) from None
        if value.spec_id != spec_id:
            raise PromotedOperationalStoreError("terminal record differs from its path")
        return value

    def get_optional(self, spec_id: str) -> PromotedOperationalTerminalRecord | None:
        path = self.path_for(spec_id)
        if not path.exists() and not path.is_symlink():
            return None
        return self.get(spec_id)
