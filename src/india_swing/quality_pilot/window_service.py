"""HYP-002 quality pilot: bounded restart-safe window service.

Processes exactly one scheduled window (one confirmed session's one
``ScheduledWindowKind``) per invocation. Walks the deterministic
completion-receipt chain starting from that window's published entry:

- A sealed completion is reused with zero collector/write calls -- but
  only after independently loading its exact claim, outcome plan,
  outcome transition, and outcome snapshot evidence, deriving the
  canonical successor from that evidence and the current runbook (the
  same derivation the fresh path itself uses), and requiring the
  receipt's recorded successor/next-window-entry pins to equal that
  derived route exactly. A foreign, coordinated, or self-consistent but
  non-canonical replay is rejected before any collector call.
- An unresolved existing claim fails the whole invocation closed with
  zero collector calls.
- A fresh action first loads and verifies its exact plan/predecessor-
  transition lineage (for resumable actions), then claims strictly once,
  executes it through the already-accepted ``QualityPilotCaptureRunner``/
  ``QualityPilotSessionBootstrapService``/``QualityPilotResumableCaptureService``
  composition (never their financial/data-integrity logic reproduced
  here), and seals a completion receipt naming its exact successor.

Processing stops the moment the successor would belong to a different
window or session -- crossing windows publishes the successor binding and
a new window entry and returns; it is never followed within the same
invocation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.identity import content_id

from .canonical_response import MAXIMUM_CATALOG_INSTRUMENTS, ScheduledWindowKind
from .capture_runner import QualityPilotCampaignSpec, QualityPilotCaptureRunner, QualityPilotCollector
from .control_plane_store import (
    QualityPilotCampaignPlan,
    QualityPilotCompletenessSnapshot,
    pinned_quality_pilot_control_artifact_request,
    pinned_quality_pilot_ledger_transition_request,
    read_pinned_quality_pilot_control_artifact,
    read_pinned_quality_pilot_ledger_transition,
)
from .invocation_control_plane import (
    QualityPilotActionBinding,
    QualityPilotActionClaim,
    QualityPilotActionKind,
    QualityPilotClaimConflictError,
    QualityPilotClaimWriter,
    QualityPilotCompletionReceipt,
    QualityPilotCurrentObjectReader,
    QualityPilotInvocationControlPlaneError,
    QualityPilotWindowEntry,
    catalog_capture_spec_for_session,
    load_current_quality_pilot_window_entry,
    load_optional_quality_pilot_completion_receipt,
    pinned_quality_pilot_action_binding_request,
    pinned_quality_pilot_action_claim_request,
    pinned_quality_pilot_window_entry_request,
    publish_quality_pilot_action_binding,
    publish_quality_pilot_action_claim,
    publish_quality_pilot_completion_receipt,
    publish_quality_pilot_window_entry,
    read_pinned_quality_pilot_action_binding,
    read_pinned_quality_pilot_action_claim,
    read_pinned_quality_pilot_window_entry,
)
from .resumable_service import QualityPilotResumableCaptureRequest, QualityPilotResumableCaptureService
from .session_bootstrap import QualityPilotSessionBootstrapRequest, QualityPilotSessionBootstrapService

_SHA256_LENGTH = 64
MAXIMUM_ACTIONS_PER_INVOCATION_CEILING = MAXIMUM_CATALOG_INSTRUMENTS
QUALITY_PILOT_WINDOW_SERVICE_RESULT_SCHEMA_VERSION = "quality_pilot_window_service_result_v1"


class QualityPilotWindowServiceError(ValueError):
    """A window-service input, claim, or restart-safety invariant failed."""


class QualityPilotIndeterminateClaimedActionError(QualityPilotWindowServiceError):
    """An unresolved claim already exists for the next action to process.

    A prior process may have crashed after issuing the provider request;
    recollecting would violate the one-scheduled-request protocol. The
    invocation must stop here with zero collector/API/write calls.
    """


def _fail(message: str) -> None:
    raise QualityPilotWindowServiceError(message)


def _indeterminate(message: str) -> None:
    raise QualityPilotIndeterminateClaimedActionError(message)


def _is_sha256(value: object) -> bool:
    return type(value) is str and len(value) == _SHA256_LENGTH and all(c in "0123456789abcdef" for c in value)


class _InvocationLocalCachingReader:
    """Wraps one injected ``GCSObjectReader`` with a per-invocation, never-
    expiring cache keyed by the exact ``(bucket, object_name, generation,
    maximum_bytes)`` quadruple -- ``maximum_bytes`` is part of the key so a
    payload fetched under one caller's bound can never be handed back to a
    later caller requesting a smaller bound. Every non-identical request
    delegates to the injected reader unchanged -- this never widens,
    weakens, or alters the wrapped reader's own generation-pinned contract,
    and preserves ``QualityPilotResumableCaptureService``'s own read counts
    exactly (repeated reads of the identical plan/transition/snapshot
    generation under the SAME ceiling, across same-window steps or across
    the pre-claim verification and the domain service's own internal
    reload, are served from cache instead of a second remote call)."""

    def __init__(self, reader: GCSObjectReader) -> None:
        if reader is None:
            _fail("invocation-local caching reader requires an injected reader")
        self._reader = reader
        self._cache: dict[tuple[str, str, int, int], GCSObjectPayload] = {}

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        key = (bucket, object_name, generation, maximum_bytes)
        if key in self._cache:
            return self._cache[key]
        result = self._reader.read_generation(
            bucket=bucket, object_name=object_name, generation=generation, maximum_bytes=maximum_bytes
        )
        if type(result) is not GCSObjectPayload:
            _fail("invocation-local caching reader received an invalid payload")
        self._cache[key] = result
        return result


# ---------------------------------------------------------------------------
# QualityPilotWindowServiceResult
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotWindowServiceResult:
    """One immutable, independently re-verifiable record of one bounded
    window-service invocation. ``window_complete`` is true only when this
    invocation crossed into a different window/session or reached exact
    campaign completion; a maximum-actions stop that never crossed leaves
    it false (``WINDOW_PARTIAL``) with an exact continuation available
    through the already-sealed completion receipts."""

    pilot_run_id: str
    market_session: date
    window_kind: ScheduledWindowKind
    actions_processed: int
    actions_reused: int
    final_transition_id: str
    campaign_complete: bool
    window_complete: bool
    next_window_session: date | None
    next_window_kind: ScheduledWindowKind | None
    service_result_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "service_result_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.pilot_run_id):
            _fail("window service result pilot run id is invalid")
        if type(self.market_session) is not date:
            _fail("window service result session is invalid")
        if type(self.window_kind) is not ScheduledWindowKind:
            _fail("window service result window kind is invalid")
        if type(self.actions_processed) is not int or self.actions_processed <= 0:
            _fail("window service result actions_processed is invalid")
        if type(self.actions_reused) is not int or self.actions_reused < 0 or self.actions_reused > self.actions_processed:
            _fail("window service result actions_reused is invalid")
        if not _is_sha256(self.final_transition_id):
            _fail("window service result final transition id is invalid")
        if type(self.campaign_complete) is not bool:
            _fail("window service result campaign-complete flag is invalid")
        if type(self.window_complete) is not bool:
            _fail("window service result window-complete flag is invalid")
        if (self.next_window_session is None) != (self.next_window_kind is None):
            _fail("window service result next window fields must both be present or both be absent")
        if self.campaign_complete and self.next_window_session is not None:
            _fail("window service result at campaign completion must not name a next window")
        crossed_or_complete = self.campaign_complete or self.next_window_session is not None
        if self.window_complete != crossed_or_complete:
            _fail("window service result window-complete flag disagrees with crossing/completion")

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_WINDOW_SERVICE_RESULT_SCHEMA_VERSION,
                    "pilot_run_id": self.pilot_run_id,
                    "market_session": self.market_session,
                    "window_kind": self.window_kind.value,
                    "actions_processed": self.actions_processed,
                    "actions_reused": self.actions_reused,
                    "final_transition_id": self.final_transition_id,
                    "campaign_complete": self.campaign_complete,
                    "window_complete": self.window_complete,
                    "next_window_session": self.next_window_session,
                    "next_window_kind": self.next_window_kind.value if self.next_window_kind is not None else None,
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("window service result identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.service_result_id != self._calculated_id():
            _fail("window service result identity failed")


# ---------------------------------------------------------------------------
# Shared successor derivation (used by BOTH the fresh-execution path and the
# replay-verification path, so both can never drift from one another).
# ---------------------------------------------------------------------------


def _next_session_after(campaign: QualityPilotCampaignSpec, market_session: date) -> date | None:
    index_failed = False
    index = -1
    try:
        index = campaign.confirmed_sessions.index(market_session)
    except ValueError:
        index_failed = True
    if index_failed:
        _fail("window service session is not part of the campaign")
    if index + 1 >= len(campaign.confirmed_sessions):
        return None
    return campaign.confirmed_sessions[index + 1]


def _window_for_session(runbook, market_session: date, window_kind: ScheduledWindowKind):
    session_index = runbook.campaign.confirmed_sessions.index(market_session)
    kind_order = (
        ScheduledWindowKind.CATALOG_PREOPEN,
        ScheduledWindowKind.QUOTE_0920,
        ScheduledWindowKind.QUOTE_CLOSE,
        ScheduledWindowKind.OHLCV_CLOSE,
    )
    kind_index = kind_order.index(window_kind)
    return runbook.windows[session_index * len(kind_order) + kind_index]


def _current_capture_spec_id(current_binding: QualityPilotActionBinding) -> str:
    if current_binding.action_kind is QualityPilotActionKind.CATALOG_BOOTSTRAP:
        spec = catalog_capture_spec_for_session(current_binding.runbook, current_binding.market_session)
        return spec.capture_spec_id
    return current_binding.target_capture_spec_id


def _index_of_capture_spec(plan: QualityPilotCampaignPlan, capture_spec_id: str) -> int:
    index_failed = False
    index = -1
    try:
        index = next(i for i, spec in enumerate(plan.capture_specs) if spec.capture_spec_id == capture_spec_id)
    except StopIteration:
        index_failed = True
    if index_failed:
        _fail("current capture spec is not part of the loaded plan")
    return index


def _derive_successor_binding(
    *,
    current_binding: QualityPilotActionBinding,
    plan: QualityPilotCampaignPlan,
    outcome_plan_pin,
    outcome_transition_pin,
) -> tuple[QualityPilotActionBinding | None, bool, bool]:
    """Pure derivation: given the current action, the exact outcome plan it
    just completed against, and the pins of that plan/transition, derive
    the exact canonical successor action binding (or ``None`` at true
    campaign completion) plus whether it crosses into a different
    window/session. Never publishes or reads anything -- callers pass in
    already-loaded/already-verified evidence.
    """

    runbook = current_binding.runbook
    campaign = runbook.campaign
    current_spec_id = _current_capture_spec_id(current_binding)
    current_index = _index_of_capture_spec(plan, current_spec_id)

    if current_index + 1 < len(plan.capture_specs):
        next_spec = plan.capture_specs[current_index + 1]
        next_window_kind = next_spec.window.window_kind
        crosses_window = next_window_kind is not current_binding.window_kind

        construct_failed = False
        successor_binding: QualityPilotActionBinding | None = None
        try:
            successor_binding = QualityPilotActionBinding(
                runbook=runbook,
                action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
                market_session=current_binding.market_session,
                window_kind=next_window_kind,
                prior_plan_pin=None,
                prior_transition_pin=None,
                plan_pin=outcome_plan_pin,
                predecessor_transition_pin=outcome_transition_pin,
                target_capture_spec_id=next_spec.capture_spec_id,
            )
        except Exception:
            construct_failed = True
        if construct_failed or successor_binding is None:
            _fail("resumable successor action binding could not be constructed")
        return successor_binding, crosses_window, False

    next_session = _next_session_after(campaign, current_binding.market_session)
    if next_session is None:
        return None, False, True

    construct_failed = False
    successor_binding = None
    try:
        successor_binding = QualityPilotActionBinding(
            runbook=runbook,
            action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=next_session,
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=outcome_plan_pin,
            prior_transition_pin=outcome_transition_pin,
            plan_pin=None,
            predecessor_transition_pin=None,
            target_capture_spec_id=None,
        )
    except Exception:
        construct_failed = True
    if construct_failed or successor_binding is None:
        _fail("next-session catalog successor action binding could not be constructed")
    return successor_binding, True, False


class QualityPilotWindowService:
    """Stateless orchestration: one deterministic window entry in, at most
    ``maximum_actions_per_invocation`` capture steps sequentially claimed,
    executed, and sealed, stopping before a different window or session.
    """

    def run(
        self,
        *,
        pilot_run_id: str,
        market_session: date,
        window_kind: ScheduledWindowKind,
        bucket: str,
        maximum_actions_per_invocation: int,
        code_sha256: str,
        environment_sha256: str,
        clock: Callable[[], datetime],
        current_reader: QualityPilotCurrentObjectReader,
        pinned_reader: GCSObjectReader,
        writer: StateObjectWriter,
        claim_writer: QualityPilotClaimWriter,
        collector: QualityPilotCollector,
    ) -> QualityPilotWindowServiceResult:
        if not _is_sha256(pilot_run_id):
            _fail("window service pilot run id is invalid")
        if type(market_session) is not date:
            _fail("window service session is invalid")
        if type(window_kind) is not ScheduledWindowKind:
            _fail("window service window kind is invalid")
        if type(bucket) is not str or not bucket:
            _fail("window service bucket is invalid")
        if (
            type(maximum_actions_per_invocation) is not int
            or not (1 <= maximum_actions_per_invocation <= MAXIMUM_ACTIONS_PER_INVOCATION_CEILING)
        ):
            _fail("window service maximum actions per invocation is invalid")
        if not _is_sha256(code_sha256) or not _is_sha256(environment_sha256):
            _fail("window service code/environment digests are invalid")
        if not callable(clock):
            _fail("window service clock is required")

        cached_reader = _InvocationLocalCachingReader(pinned_reader)

        entry_load_failed = False
        entry: object = None
        try:
            entry = load_current_quality_pilot_window_entry(
                pilot_run_id=pilot_run_id, market_session=market_session, window_kind=window_kind,
                bucket=bucket, reader=current_reader,
            )
        except Exception:
            entry_load_failed = True
        if entry_load_failed:
            _fail("window entry could not be loaded for this session/window")

        binding_load_failed = False
        loaded_binding: object = None
        try:
            loaded_binding = read_pinned_quality_pilot_action_binding(entry.action_binding_pin, pinned_reader)
        except Exception:
            binding_load_failed = True
        if binding_load_failed:
            _fail("window entry action binding could not be loaded")

        current_binding: QualityPilotActionBinding = loaded_binding.binding
        actions_processed = 0
        actions_reused = 0
        final_transition_id: str | None = None
        campaign_complete = False
        next_window_session: date | None = None
        next_window_kind: ScheduledWindowKind | None = None
        crossed = False

        while not crossed and actions_processed < maximum_actions_per_invocation:
            if current_binding.market_session != market_session or current_binding.window_kind is not window_kind:
                _fail("window service action binding disagrees with the requested window")

            receipt_load_failed = False
            receipt: object = None
            try:
                receipt = load_optional_quality_pilot_completion_receipt(
                    pilot_run_id=pilot_run_id, action_id=current_binding.action_id, bucket=bucket, reader=current_reader,
                )
            except Exception:
                receipt_load_failed = True
            if receipt_load_failed:
                _fail("completion receipt could not be checked for the current action")

            if receipt is not None:
                actions_processed += 1
                actions_reused += 1

                successor, crosses_window = self._verify_replay(
                    current_binding=current_binding, receipt=receipt,
                    code_sha256=code_sha256, environment_sha256=environment_sha256,
                    pinned_reader=pinned_reader, cached_reader=cached_reader,
                )
                final_transition_id = receipt.final_transition_id

                if receipt.campaign_complete:
                    campaign_complete = True
                    crossed = True
                    break
                if crosses_window:
                    next_window_session = successor.market_session
                    next_window_kind = successor.window_kind
                    crossed = True
                    break
                current_binding = successor
                continue

            self._verify_pre_claim_lineage(current_binding=current_binding, cached_reader=cached_reader)

            claim = None
            claim_construct_failed = False
            try:
                claim = QualityPilotActionClaim(
                    pilot_run_id=pilot_run_id,
                    action_id=current_binding.action_id,
                    invocation_at=clock(),
                    code_sha256=code_sha256,
                    environment_sha256=environment_sha256,
                )
            except Exception:
                claim_construct_failed = True
            if claim_construct_failed or claim is None:
                _fail("action claim could not be constructed")

            claim_conflict = False
            claim_failed = False
            published_claim: object = None
            try:
                published_claim = publish_quality_pilot_action_claim(claim, bucket, claim_writer)
            except QualityPilotClaimConflictError:
                claim_conflict = True
            except Exception:
                claim_failed = True
            if claim_conflict:
                _indeterminate("an unresolved claim already exists for the next action")
            if claim_failed:
                _fail("action claim could not be published")

            claim_pin_failed = False
            claim_pin: object = None
            try:
                claim_pin = pinned_quality_pilot_action_claim_request(published_claim)
            except Exception:
                claim_pin_failed = True
            if claim_pin_failed:
                _fail("action claim publication could not be pinned")

            successor_binding, next_entry_needed, is_campaign_complete, sealed_transition_id = self._execute_and_seal(
                current_binding=current_binding,
                claim_pin=claim_pin,
                invocation_at=claim.invocation_at,
                bucket=bucket,
                cached_reader=cached_reader,
                writer=writer,
                collector=collector,
            )
            actions_processed += 1
            final_transition_id = sealed_transition_id
            crossed = True
            if is_campaign_complete:
                campaign_complete = True
            elif next_entry_needed:
                next_window_session = successor_binding.market_session
                next_window_kind = successor_binding.window_kind
            else:
                # Same-window continuation: keep processing in this invocation.
                crossed = False
                current_binding = successor_binding

        window_complete = campaign_complete or next_window_session is not None

        result_failed = False
        result: QualityPilotWindowServiceResult | None = None
        try:
            result = QualityPilotWindowServiceResult(
                pilot_run_id=pilot_run_id,
                market_session=market_session,
                window_kind=window_kind,
                actions_processed=actions_processed,
                actions_reused=actions_reused,
                final_transition_id=final_transition_id,
                campaign_complete=campaign_complete,
                window_complete=window_complete,
                next_window_session=next_window_session,
                next_window_kind=next_window_kind,
            )
        except Exception:
            result_failed = True
        if result_failed or result is None:
            _fail("window service result could not be constructed")
        return result

    def _load_completed_plan_and_snapshot_from_transition_pin(
        self, *, plan_pin, transition_pin, cached_reader: _InvocationLocalCachingReader, error_prefix: str
    ):
        """Load one exact plan pin and its terminal transition pin, plus the
        completeness snapshot the transition's own ``next_snapshot``
        references. Returns ``(plan, transition, snapshot)``. Every read
        goes through the invocation-local cache."""

        transition_load_failed = False
        loaded_transition: object = None
        try:
            loaded_transition = read_pinned_quality_pilot_ledger_transition(transition_pin, cached_reader)
        except Exception:
            transition_load_failed = True
        if transition_load_failed:
            _fail(f"{error_prefix} transition could not be loaded for pre-claim verification")
        transition = loaded_transition.transition

        plan_load_failed = False
        loaded_plan: object = None
        try:
            loaded_plan = read_pinned_quality_pilot_control_artifact(plan_pin, cached_reader)
        except Exception:
            plan_load_failed = True
        if plan_load_failed or type(loaded_plan.artifact) is not QualityPilotCampaignPlan:
            _fail(f"{error_prefix} plan could not be loaded for pre-claim verification")
        plan: QualityPilotCampaignPlan = loaded_plan.artifact
        if transition.plan_id != plan.plan_id:
            _fail(f"{error_prefix} transition disagrees with its own plan")

        snapshot_pin_failed = False
        snapshot_pin: object = None
        try:
            snapshot_pin = pinned_quality_pilot_control_artifact_request(transition.next_snapshot)
        except Exception:
            snapshot_pin_failed = True
        if snapshot_pin_failed:
            _fail(f"{error_prefix} next snapshot metadata could not be pinned for pre-claim verification")

        snapshot_load_failed = False
        loaded_snapshot: object = None
        try:
            loaded_snapshot = read_pinned_quality_pilot_control_artifact(snapshot_pin, cached_reader)
        except Exception:
            snapshot_load_failed = True
        if snapshot_load_failed or type(loaded_snapshot.artifact) is not QualityPilotCompletenessSnapshot:
            _fail(f"{error_prefix} completeness snapshot could not be loaded for pre-claim verification")
        snapshot = loaded_snapshot.artifact
        if snapshot.plan_id != plan.plan_id:
            _fail(f"{error_prefix} completeness snapshot disagrees with its own plan")

        return plan, transition, snapshot

    def _verify_pre_claim_lineage(
        self, *, current_binding: QualityPilotActionBinding, cached_reader: _InvocationLocalCachingReader
    ) -> None:
        """Before claiming ANY fresh action -- catalog genesis, catalog
        extension, or resumable -- independently load and verify its exact
        predecessor evidence, entirely before any collector call. Every
        read goes through the invocation-local cache, so a downstream
        domain service's own internal reload of an identical generation
        costs zero additional remote reads."""

        runbook = current_binding.runbook
        campaign = runbook.campaign

        if current_binding.action_kind is QualityPilotActionKind.CATALOG_BOOTSTRAP:
            if current_binding.prior_plan_pin is None:
                if current_binding.market_session != campaign.confirmed_sessions[0]:
                    _fail("catalog genesis action target is not the campaign's first confirmed session")
                return

            plan, transition, snapshot = self._load_completed_plan_and_snapshot_from_transition_pin(
                plan_pin=current_binding.prior_plan_pin, transition_pin=current_binding.prior_transition_pin,
                cached_reader=cached_reader, error_prefix="catalog extension predecessor",
            )
            if plan.campaign.campaign_id != campaign.campaign_id:
                _fail("catalog extension predecessor plan campaign disagrees with the runbook")
            if (
                not plan.planned_sessions
                or plan.planned_sessions != campaign.confirmed_sessions[: len(plan.planned_sessions)]
                or len(plan.planned_sessions) >= len(campaign.confirmed_sessions)
                or campaign.confirmed_sessions[len(plan.planned_sessions)] != current_binding.market_session
            ):
                _fail("catalog extension predecessor plan is not the canonical prefix immediately before the target session")
            final_spec = plan.capture_specs[-1]
            if transition.capture_spec_id != final_spec.capture_spec_id:
                _fail("catalog extension predecessor transition does not resolve the predecessor plan's final capture spec")
            expected_completed_ids = tuple(spec.capture_spec_id for spec in plan.capture_specs)
            if (
                snapshot.completed_capture_spec_ids != expected_completed_ids
                or snapshot.missing_due_capture_spec_ids != ()
                or snapshot.pending_capture_spec_ids != ()
            ):
                _fail("catalog extension predecessor snapshot is not the fully completed exact prefix")
            if (
                snapshot.campaign_id != plan.campaign.campaign_id
                or snapshot.pilot_run_id != plan.campaign.pilot_run_id
                or snapshot.protocol_sha256 != plan.campaign.protocol_sha256
                or snapshot.bucket != runbook.bucket
            ):
                _fail("catalog extension predecessor snapshot lineage disagrees with the predecessor plan/runbook branch")
            final_index = len(plan.capture_specs) - 1
            if transition.run_result_id != snapshot.run_result_ids[final_index]:
                _fail("catalog extension predecessor transition run result disagrees with the predecessor snapshot's final capture")
            return

        # RESUMABLE_CAPTURE
        plan_load_failed = False
        loaded_plan: object = None
        try:
            loaded_plan = read_pinned_quality_pilot_control_artifact(current_binding.plan_pin, cached_reader)
        except Exception:
            plan_load_failed = True
        if plan_load_failed or type(loaded_plan.artifact) is not QualityPilotCampaignPlan:
            _fail("resumable action plan could not be loaded for pre-claim verification")
        plan: QualityPilotCampaignPlan = loaded_plan.artifact
        if plan.campaign.campaign_id != campaign.campaign_id:
            _fail("resumable action plan campaign disagrees with the runbook")

        transition_load_failed = False
        loaded_transition: object = None
        try:
            loaded_transition = read_pinned_quality_pilot_ledger_transition(
                current_binding.predecessor_transition_pin, cached_reader
            )
        except Exception:
            transition_load_failed = True
        if transition_load_failed:
            _fail("resumable action predecessor transition could not be loaded for pre-claim verification")
        predecessor_transition = loaded_transition.transition
        if predecessor_transition.plan_id != plan.plan_id:
            _fail("resumable action predecessor transition disagrees with the plan")

        target_spec = None
        for spec in plan.capture_specs:
            if spec.capture_spec_id == current_binding.target_capture_spec_id:
                target_spec = spec
                break
        if target_spec is None:
            _fail("resumable action target capture spec is not part of the loaded plan")
        if (
            target_spec.window.market_session != current_binding.market_session
            or target_spec.window.window_kind is not current_binding.window_kind
            or target_spec.provider_version != runbook.provider_version
        ):
            _fail("resumable action target spec disagrees with the action's own session/window/provider")

    def _expected_previous_snapshot_id(
        self, *, current_binding: QualityPilotActionBinding, cached_reader: _InvocationLocalCachingReader
    ) -> str | None:
        """Independently derive the exact predecessor completeness-snapshot
        id the current action's own outcome transition must advance from,
        by loading the current action's own predecessor transition pin
        (catalog extension's prior_transition_pin, resumable's
        predecessor_transition_pin, or None for catalog genesis) and
        reading its own sealed ``next_snapshot``. Never trusts the outcome
        transition's own previous_snapshot_id field as self-attesting."""

        if current_binding.action_kind is QualityPilotActionKind.CATALOG_BOOTSTRAP:
            if current_binding.prior_transition_pin is None:
                return None
            predecessor_pin = current_binding.prior_transition_pin
        else:
            predecessor_pin = current_binding.predecessor_transition_pin

        load_failed = False
        loaded: object = None
        try:
            loaded = read_pinned_quality_pilot_ledger_transition(predecessor_pin, cached_reader)
        except Exception:
            load_failed = True
        if load_failed:
            _fail("current action predecessor transition could not be loaded to derive the expected previous snapshot")
        return loaded.transition.next_snapshot.artifact_id

    def _load_outcome_evidence(
        self,
        *,
        current_binding: QualityPilotActionBinding,
        receipt: QualityPilotCompletionReceipt,
        cached_reader: _InvocationLocalCachingReader,
    ):
        runbook = current_binding.runbook

        plan_load_failed = False
        loaded_plan: object = None
        try:
            loaded_plan = read_pinned_quality_pilot_control_artifact(receipt.outcome_plan_pin, cached_reader)
        except Exception:
            plan_load_failed = True
        if plan_load_failed or type(loaded_plan.artifact) is not QualityPilotCampaignPlan:
            _fail("completion receipt outcome plan could not be loaded")
        plan: QualityPilotCampaignPlan = loaded_plan.artifact
        if plan.campaign.campaign_id != runbook.campaign.campaign_id:
            _fail("completion receipt outcome plan disagrees with the runbook campaign/provider branch")

        transition_load_failed = False
        loaded_transition: object = None
        try:
            loaded_transition = read_pinned_quality_pilot_ledger_transition(
                receipt.outcome_transition_pin, cached_reader
            )
        except Exception:
            transition_load_failed = True
        if transition_load_failed:
            _fail("completion receipt outcome transition could not be loaded")
        transition = loaded_transition.transition
        if transition.plan_id != plan.plan_id:
            _fail("completion receipt outcome transition disagrees with the outcome plan")

        current_spec_id = _current_capture_spec_id(current_binding)
        if transition.capture_spec_id != current_spec_id:
            _fail("completion receipt outcome transition disagrees with the current action's own capture spec")

        expected_previous_snapshot_id = self._expected_previous_snapshot_id(
            current_binding=current_binding, cached_reader=cached_reader
        )
        if transition.previous_snapshot_id != expected_previous_snapshot_id:
            _fail("completion receipt outcome transition does not advance from the expected predecessor snapshot")

        snapshot_load_failed = False
        loaded_snapshot: object = None
        try:
            loaded_snapshot = read_pinned_quality_pilot_control_artifact(receipt.outcome_snapshot_pin, cached_reader)
        except Exception:
            snapshot_load_failed = True
        if snapshot_load_failed:
            _fail("completion receipt outcome snapshot could not be loaded")
        snapshot = loaded_snapshot.artifact
        if (
            snapshot.snapshot_id != transition.next_snapshot.artifact_id
            or snapshot.plan_id != plan.plan_id
        ):
            _fail("completion receipt outcome snapshot disagrees with the outcome transition")

        current_index = _index_of_capture_spec(plan, current_spec_id)
        expected_completed_ids = tuple(spec.capture_spec_id for spec in plan.capture_specs[: current_index + 1])
        if snapshot.completed_capture_spec_ids != expected_completed_ids:
            _fail("completion receipt outcome snapshot is not the exact canonical completed prefix through the current capture")

        if (
            snapshot.campaign_id != plan.campaign.campaign_id
            or snapshot.pilot_run_id != plan.campaign.pilot_run_id
            or snapshot.protocol_sha256 != plan.campaign.protocol_sha256
            or snapshot.bucket != runbook.bucket
        ):
            _fail("completion receipt outcome snapshot lineage disagrees with the outcome plan/runbook branch")
        if snapshot.expected_capture_count != len(plan.capture_specs):
            _fail("completion receipt outcome snapshot expected capture count disagrees with the outcome plan")
        if transition.run_result_id != snapshot.run_result_ids[current_index]:
            _fail("completion receipt outcome transition run result disagrees with the outcome snapshot at the current capture")

        current_spec = plan.capture_specs[current_index]
        observation_pin = snapshot.pinned_observations[current_index]
        if (
            observation_pin.pilot_run_id != current_spec.window.pilot_run_id
            or observation_pin.market_session != current_spec.window.market_session
            or observation_pin.window_kind is not current_spec.window.window_kind
            or observation_pin.endpoint_family is not current_spec.window.endpoint_family
            or observation_pin.chunk_index != current_spec.chunk_index
            or observation_pin.chunk_count != current_spec.chunk_count
            or observation_pin.bucket != runbook.bucket
        ):
            _fail("completion receipt outcome snapshot observation pin disagrees with the current capture's exact route")

        return plan, transition, snapshot

    def _verify_replay(
        self,
        *,
        current_binding: QualityPilotActionBinding,
        receipt: QualityPilotCompletionReceipt,
        code_sha256: str,
        environment_sha256: str,
        pinned_reader: GCSObjectReader,
        cached_reader: _InvocationLocalCachingReader,
    ) -> tuple[QualityPilotActionBinding | None, bool]:
        """Independently reload and verify the exact claim, outcome plan,
        outcome transition, and outcome snapshot a sealed completion
        receipt claims to be backed by; derive the canonical successor
        from that evidence and the current runbook using the identical
        derivation the fresh-execution path uses; and require the
        receipt's own recorded successor/next-window-entry pins to equal
        that derived route by full field reconstruction, not action_id
        alone. Returns ``(loaded_successor_or_None, crosses_window)``.
        Makes zero collector or write calls.
        """

        if receipt.pilot_run_id != current_binding.runbook.campaign.pilot_run_id:
            _fail("completion receipt disagrees with the current action's pilot run id")
        if receipt.action_id != current_binding.action_id:
            _fail("completion receipt disagrees with the current action id")

        claim_load_failed = False
        loaded_claim: object = None
        try:
            loaded_claim = read_pinned_quality_pilot_action_claim(receipt.claim_pin, pinned_reader)
        except Exception:
            claim_load_failed = True
        if claim_load_failed:
            _fail("completion receipt claim could not be loaded")
        if (
            loaded_claim.claim.action_id != current_binding.action_id
            or loaded_claim.claim.pilot_run_id != current_binding.runbook.campaign.pilot_run_id
            or loaded_claim.claim.code_sha256 != code_sha256
            or loaded_claim.claim.environment_sha256 != environment_sha256
        ):
            _fail("completion receipt claim disagrees with the current action or invocation identity")

        plan, transition, _snapshot = self._load_outcome_evidence(
            current_binding=current_binding, receipt=receipt, cached_reader=cached_reader
        )

        if transition.transition_id != receipt.final_transition_id:
            _fail("completion receipt final transition id disagrees with its own outcome transition")

        derived_successor, derived_crosses, derived_campaign_complete = _derive_successor_binding(
            current_binding=current_binding, plan=plan,
            outcome_plan_pin=receipt.outcome_plan_pin, outcome_transition_pin=receipt.outcome_transition_pin,
        )

        if derived_campaign_complete != receipt.campaign_complete:
            _fail("completion receipt campaign-complete flag disagrees with the derived canonical outcome")

        if receipt.campaign_complete:
            if receipt.successor_action_binding_pin is not None:
                _fail("completion receipt at campaign completion must not carry a successor")
            return None, False

        if derived_successor is None or receipt.successor_action_binding_pin is None:
            _fail("completion receipt successor disagrees with the derived canonical successor")

        successor_load_failed = False
        loaded_successor: object = None
        try:
            loaded_successor = read_pinned_quality_pilot_action_binding(
                receipt.successor_action_binding_pin, pinned_reader
            )
        except Exception:
            successor_load_failed = True
        if successor_load_failed:
            _fail("completion receipt successor action binding could not be loaded")
        successor = loaded_successor.binding

        # Field-level reconstruction equality, not action_id-only: a
        # content identity remains useful as a fast pre-check but must
        # never substitute for comparing the reconstructed structured
        # value at this trust boundary.
        if successor != derived_successor:
            _fail("completion receipt successor is not the canonical next action")

        if derived_crosses != (receipt.next_window_entry_pin is not None):
            _fail("completion receipt window-crossing marker disagrees with the derived canonical outcome")

        if receipt.next_window_entry_pin is not None:
            entry_load_failed = False
            loaded_entry: object = None
            try:
                loaded_entry = read_pinned_quality_pilot_window_entry(receipt.next_window_entry_pin, pinned_reader)
            except Exception:
                entry_load_failed = True
            if entry_load_failed:
                _fail("completion receipt next window entry could not be loaded")

            expected_entry_failed = False
            expected_entry: object = None
            try:
                expected_entry = QualityPilotWindowEntry(
                    pilot_run_id=successor.runbook.campaign.pilot_run_id,
                    market_session=successor.market_session,
                    window_kind=successor.window_kind,
                    action_binding_pin=receipt.successor_action_binding_pin,
                )
            except Exception:
                expected_entry_failed = True
            if expected_entry_failed or expected_entry is None:
                _fail("completion receipt next window entry could not be reconstructed for comparison")
            if loaded_entry.entry != expected_entry:
                _fail("completion receipt next window entry does not point at the canonical successor's exact route")

        return successor, derived_crosses

    def _execute_and_seal(
        self,
        *,
        current_binding: QualityPilotActionBinding,
        claim_pin,
        invocation_at: datetime,
        bucket: str,
        cached_reader: _InvocationLocalCachingReader,
        writer: StateObjectWriter,
        collector: QualityPilotCollector,
    ) -> tuple[QualityPilotActionBinding | None, bool, bool, str]:
        """Execute one freshly-claimed action through the accepted domain
        services, derive and publish its exact successor (or seal campaign
        completion), and return ``(successor_binding,
        crosses_into_new_window, campaign_complete, sealed_transition_id)``.
        """

        runbook = current_binding.runbook
        campaign = runbook.campaign

        if current_binding.action_kind is QualityPilotActionKind.CATALOG_BOOTSTRAP:
            catalog_spec = catalog_capture_spec_for_session(runbook, current_binding.market_session)
            catalog_run_result_failed = False
            catalog_run_result: object = None
            try:
                catalog_run_result = QualityPilotCaptureRunner().run(catalog_spec, collector, bucket, writer)
            except Exception:
                catalog_run_result_failed = True
            if catalog_run_result_failed:
                _fail("catalog capture could not be run")

            quote_0920_window = _window_for_session(runbook, current_binding.market_session, ScheduledWindowKind.QUOTE_0920)
            quote_close_window = _window_for_session(runbook, current_binding.market_session, ScheduledWindowKind.QUOTE_CLOSE)
            ohlcv_close_window = _window_for_session(runbook, current_binding.market_session, ScheduledWindowKind.OHLCV_CLOSE)

            bootstrap_request_failed = False
            bootstrap_request: object = None
            try:
                bootstrap_request = QualityPilotSessionBootstrapRequest(
                    campaign=campaign,
                    catalog_run_result=catalog_run_result,
                    quote_0920_window=quote_0920_window,
                    quote_close_window=quote_close_window,
                    ohlcv_close_window=ohlcv_close_window,
                    bucket=bucket,
                    prior_plan_pin=current_binding.prior_plan_pin,
                    prior_transition_pin=current_binding.prior_transition_pin,
                )
            except Exception:
                bootstrap_request_failed = True
            if bootstrap_request_failed:
                _fail("session bootstrap request could not be constructed")

            bootstrap_failed = False
            bootstrap_result: object = None
            try:
                bootstrap_result = QualityPilotSessionBootstrapService().run(bootstrap_request, cached_reader, writer)
            except Exception:
                bootstrap_failed = True
            if bootstrap_failed:
                _fail("session bootstrap could not be run")

            outcome_plan_pin = pinned_quality_pilot_control_artifact_request(bootstrap_result.published_plan)
            outcome_transition_pin = pinned_quality_pilot_ledger_transition_request(bootstrap_result.published_transition)
            outcome_snapshot_pin = pinned_quality_pilot_control_artifact_request(bootstrap_result.published_snapshot)

            successor_binding, crosses_window, is_campaign_complete = _derive_successor_binding(
                current_binding=current_binding, plan=bootstrap_result.plan,
                outcome_plan_pin=outcome_plan_pin, outcome_transition_pin=outcome_transition_pin,
            )

            self._publish_successor_and_seal(
                current_binding=current_binding,
                current_action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
                claim_pin=claim_pin,
                outcome_plan_pin=outcome_plan_pin,
                outcome_transition_pin=outcome_transition_pin,
                outcome_snapshot_pin=outcome_snapshot_pin,
                successor_binding=successor_binding,
                bucket=bucket,
                writer=writer,
                crosses_window=crosses_window,
                campaign_complete=is_campaign_complete,
            )
            return successor_binding, crosses_window, is_campaign_complete, outcome_transition_pin.transition_id

        # RESUMABLE_CAPTURE
        resumable_request_failed = False
        resumable_request: object = None
        try:
            resumable_request = QualityPilotResumableCaptureRequest(
                plan_pin=current_binding.plan_pin,
                predecessor_transition_pin=current_binding.predecessor_transition_pin,
                target_capture_spec_id=current_binding.target_capture_spec_id,
                invocation_at=invocation_at,
            )
        except Exception:
            resumable_request_failed = True
        if resumable_request_failed:
            _fail("resumable capture request could not be constructed")

        resumable_failed = False
        resumable_result: object = None
        try:
            resumable_result = QualityPilotResumableCaptureService().run(
                resumable_request, cached_reader, collector, writer
            )
        except Exception:
            resumable_failed = True
        if resumable_failed:
            _fail("resumable capture could not be run")

        plan_load_failed = False
        loaded_plan: object = None
        try:
            loaded_plan = read_pinned_quality_pilot_control_artifact(current_binding.plan_pin, cached_reader)
        except Exception:
            plan_load_failed = True
        if plan_load_failed or type(loaded_plan.artifact) is not QualityPilotCampaignPlan:
            _fail("campaign plan could not be loaded to derive the successor")
        plan: QualityPilotCampaignPlan = loaded_plan.artifact

        outcome_plan_pin = current_binding.plan_pin
        outcome_transition_pin = pinned_quality_pilot_ledger_transition_request(resumable_result.published_transition)
        outcome_snapshot_pin = pinned_quality_pilot_control_artifact_request(resumable_result.published_snapshot)

        successor_binding, crosses_window, is_campaign_complete = _derive_successor_binding(
            current_binding=current_binding, plan=plan,
            outcome_plan_pin=outcome_plan_pin, outcome_transition_pin=outcome_transition_pin,
        )

        self._publish_successor_and_seal(
            current_binding=current_binding,
            current_action_kind=QualityPilotActionKind.RESUMABLE_CAPTURE,
            claim_pin=claim_pin,
            outcome_plan_pin=outcome_plan_pin,
            outcome_transition_pin=outcome_transition_pin,
            outcome_snapshot_pin=outcome_snapshot_pin,
            successor_binding=successor_binding,
            bucket=bucket,
            writer=writer,
            crosses_window=crosses_window,
            campaign_complete=is_campaign_complete,
        )
        return successor_binding, crosses_window, is_campaign_complete, outcome_transition_pin.transition_id

    def _publish_successor_and_seal(
        self,
        *,
        current_binding: QualityPilotActionBinding,
        current_action_kind: QualityPilotActionKind,
        claim_pin,
        outcome_plan_pin,
        outcome_transition_pin,
        outcome_snapshot_pin,
        successor_binding: QualityPilotActionBinding | None,
        bucket: str,
        writer: StateObjectWriter,
        crosses_window: bool,
        campaign_complete: bool,
    ) -> None:
        successor_pin = None
        next_window_entry_pin = None

        if successor_binding is not None:
            publish_failed = False
            published_successor: object = None
            try:
                published_successor = publish_quality_pilot_action_binding(successor_binding, writer)
            except Exception:
                publish_failed = True
            if publish_failed:
                _fail("successor action binding could not be published")
            successor_pin = pinned_quality_pilot_action_binding_request(published_successor)

            if crosses_window:
                entry_failed = False
                entry: object = None
                try:
                    entry = QualityPilotWindowEntry(
                        pilot_run_id=successor_binding.runbook.campaign.pilot_run_id,
                        market_session=successor_binding.market_session,
                        window_kind=successor_binding.window_kind,
                        action_binding_pin=successor_pin,
                    )
                except Exception:
                    entry_failed = True
                if entry_failed or entry is None:
                    _fail("next window entry could not be constructed")
                publish_entry_failed = False
                published_entry: object = None
                try:
                    published_entry = publish_quality_pilot_window_entry(entry, bucket, writer)
                except Exception:
                    publish_entry_failed = True
                if publish_entry_failed:
                    _fail("next window entry could not be published")
                next_window_entry_pin = pinned_quality_pilot_window_entry_request(entry, bucket, published_entry)

        receipt_failed = False
        receipt: QualityPilotCompletionReceipt | None = None
        try:
            receipt = QualityPilotCompletionReceipt(
                pilot_run_id=current_binding.runbook.campaign.pilot_run_id,
                action_id=current_binding.action_id,
                action_kind=current_action_kind,
                claim_pin=claim_pin,
                outcome_plan_pin=outcome_plan_pin,
                outcome_transition_pin=outcome_transition_pin,
                outcome_snapshot_pin=outcome_snapshot_pin,
                successor_action_binding_pin=successor_pin,
                next_window_entry_pin=next_window_entry_pin,
                campaign_complete=campaign_complete,
            )
        except Exception:
            receipt_failed = True
        if receipt_failed or receipt is None:
            _fail("completion receipt could not be constructed")

        publish_receipt_failed = False
        try:
            publish_quality_pilot_completion_receipt(receipt, bucket, writer)
        except Exception:
            publish_receipt_failed = True
        if publish_receipt_failed:
            _fail("completion receipt could not be published")
