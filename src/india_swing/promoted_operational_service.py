"""Publication orchestration around the accepted promoted operational
runner.

Contains only pure adapters and idempotent publication orchestration: it
never modifies the accepted runner/decision/allocation/quote-gate/
preparation types, the paper-ledger, or any legacy operations/recommendation
module, and it never imports Telegram, an HTTP/network client, a Kite
adapter, GCP, environment variables, credentials, subprocess, or a broker
order API. Filesystem access is limited to the two local stores and the
caller-supplied ``LocalPaperTradeLedger``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from india_swing.paper_trades.models import PaperTradeRegistration
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    PromotedOperationalAdvisoryRecord,
    PromotedOperationalPersistenceError,
    PromotedOperationalTerminalRecord,
    _verify_terminal_matches_spec,
    build_promoted_operational_advisory,
    build_promoted_operational_terminal_record,
    promoted_paper_registration_from_result,
    verify_promoted_operational_published_bundle,
)
from india_swing.promoted_operational_runner import (
    PromotedOperationalPortfolioSource,
    PromotedOperationalQuoteSource,
    PromotedOperationalRunResult,
    PromotedOperationalRunSpec,
    execute_promoted_operational_run,
)


class PromotedOperationalServiceError(PromotedOperationalPersistenceError):
    pass


_ERR_SERVICE = "promoted operational service call is invalid"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _require_sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedOperationalServiceError(_ERR_SERVICE)
    return value


@dataclass(frozen=True, slots=True)
class TrustedPromotedOperationalTerminalBinding:
    """A caller-supplied trust anchor from an independent, durably-retained
    control plane -- NOT content-addressed provenance, and never derived by
    this service from ``terminal_store``.

    Individual content hashes cannot authenticate a *coordinated* rewrite
    where both the local terminal and its referenced advisory are rewritten
    together, self-consistently, so that they still agree with each other.
    This binding exists to anchor sealed replay to a ``terminal_id`` the
    caller retained independently of the local terminal file -- e.g. the
    future GCP control plane, which durably records the terminal_id at the
    moment a fresh run completes, before ever trusting a local replay.

    Constructing this binding from the very local terminal being replayed
    (``TrustedPromotedOperationalTerminalBinding(spec_id=terminal.spec_id,
    expected_terminal_id=terminal.terminal_id)``) defeats the trust
    boundary this type exists to enforce: it would make the anchor and the
    thing it is meant to authenticate the same untrusted local artifact.
    The exact ``expected_terminal_id`` must come from storage independent
    of the local terminal file this service is about to read.
    """

    spec_id: str
    expected_terminal_id: str

    def __post_init__(self) -> None:
        _require_sha(self.spec_id)
        _require_sha(self.expected_terminal_id)


@dataclass(frozen=True, slots=True)
class PromotedOperationalPublishedState:
    """Fixed-shape return value of the promoted publication service.

    ``reused_existing_terminal`` is operational metadata only -- it is not
    part of content identity, and a fresh publish versus a sealed-terminal
    replay always return equal ``terminal``/``advisory``/``paper_registration``
    artifacts for the same run spec.
    """

    terminal: PromotedOperationalTerminalRecord
    advisory: PromotedOperationalAdvisoryRecord
    paper_registration: PaperTradeRegistration | None
    reused_existing_terminal: bool

    def __post_init__(self) -> None:
        try:
            verify_promoted_operational_published_bundle(
                self.terminal, self.advisory, self.paper_registration
            )
        except PromotedOperationalPersistenceError:
            raise PromotedOperationalServiceError(_ERR_SERVICE) from None
        if type(self.reused_existing_terminal) is not bool:
            raise PromotedOperationalServiceError(_ERR_SERVICE)


def publish_promoted_operational_result(
    *,
    result: PromotedOperationalRunResult,
    advisory_outbox: LocalPromotedOperationalAdvisoryOutbox,
    terminal_store: LocalPromotedOperationalTerminalStore,
    paper_ledger: LocalPaperTradeLedger | None = None,
) -> PromotedOperationalPublishedState:
    """Publish idempotent side effects in order -- advisory, then paper
    registration (only for a singular COMPLETE PAPER_BUY), then the
    terminal record last. If advisory or required registration publication
    fails, no terminal record is written."""

    if type(result) is not PromotedOperationalRunResult:
        raise PromotedOperationalServiceError(_ERR_SERVICE)
    result.verify_content_identity()
    if type(advisory_outbox) is not LocalPromotedOperationalAdvisoryOutbox:
        raise PromotedOperationalServiceError(_ERR_SERVICE)
    if type(terminal_store) is not LocalPromotedOperationalTerminalStore:
        raise PromotedOperationalServiceError(_ERR_SERVICE)

    advisory = build_promoted_operational_advisory(result)
    stored_advisory = advisory_outbox.put(advisory)
    if stored_advisory.advisory_id != advisory.advisory_id:
        raise PromotedOperationalServiceError(_ERR_SERVICE)

    expected_registration = promoted_paper_registration_from_result(result, stored_advisory)
    stored_registration: PaperTradeRegistration | None = None
    if expected_registration is not None:
        if type(paper_ledger) is not LocalPaperTradeLedger:
            raise PromotedOperationalServiceError(
                "a singular COMPLETE PAPER_BUY requires an exact paper ledger"
            )
        stored_registration = paper_ledger.register_value(expected_registration)
        if stored_registration.registration_id != expected_registration.registration_id:
            raise PromotedOperationalServiceError(_ERR_SERVICE)

    terminal = build_promoted_operational_terminal_record(
        result, stored_advisory, stored_registration
    )
    stored_terminal = terminal_store.put(terminal)
    if stored_terminal.terminal_id != terminal.terminal_id:
        raise PromotedOperationalServiceError(_ERR_SERVICE)

    return PromotedOperationalPublishedState(
        terminal=stored_terminal,
        advisory=stored_advisory,
        paper_registration=stored_registration,
        reused_existing_terminal=False,
    )


def run_and_publish_promoted_operational_service(
    *,
    spec: PromotedOperationalRunSpec,
    quote_source: PromotedOperationalQuoteSource,
    portfolio_source: PromotedOperationalPortfolioSource,
    clock: Callable[[], datetime],
    advisory_outbox: LocalPromotedOperationalAdvisoryOutbox,
    terminal_store: LocalPromotedOperationalTerminalStore,
    paper_ledger: LocalPaperTradeLedger | None = None,
    terminal_binding: TrustedPromotedOperationalTerminalBinding | None = None,
) -> PromotedOperationalPublishedState:
    """Single schedulable service call.

    Checks ``terminal_store.get_optional(spec.spec_id)`` before ever
    reading either source_id property or invoking the clock.

    If no local terminal exists, ``terminal_binding`` must be ``None`` --
    a supplied binding means the independently anchored terminal is
    missing, and this fails closed before any source property, acquisition,
    clock, advisory, or ledger access. The accepted
    ``execute_promoted_operational_run`` then runs exactly once and its
    result is published terminal-last; the caller must durably anchor the
    returned terminal's ``terminal_id`` outside this local bundle before
    acknowledging the run.

    If a local terminal exists, an exact ``terminal_binding`` is mandatory:
    ``binding.spec_id`` must equal both the supplied ``spec.spec_id`` and
    the existing terminal's own ``spec_id``, and
    ``binding.expected_terminal_id`` must equal the existing terminal's own
    ``terminal_id`` -- checked before ``advisory_outbox.get`` or any
    ``paper_ledger`` access. This service never constructs the binding
    itself from ``terminal_store``; individual content hashes cannot
    authenticate a coordinated rewrite of both the terminal and its
    referenced advisory, so only an independently supplied anchor can. On
    an exact match, no clock read, source property read, or acquisition
    call happens at all -- the referenced advisory and optional paper
    registration are loaded, cross-checked, and returned instead of
    re-running the market pipeline.
    """

    if type(spec) is not PromotedOperationalRunSpec:
        raise PromotedOperationalServiceError(_ERR_SERVICE)
    spec.verify_content_identity()
    if type(terminal_store) is not LocalPromotedOperationalTerminalStore:
        raise PromotedOperationalServiceError(_ERR_SERVICE)
    if (
        terminal_binding is not None
        and type(terminal_binding) is not TrustedPromotedOperationalTerminalBinding
    ):
        raise PromotedOperationalServiceError(_ERR_SERVICE)

    existing = terminal_store.get_optional(spec.spec_id)
    if existing is None:
        if terminal_binding is not None:
            raise PromotedOperationalServiceError(
                "a trusted terminal binding was supplied but no local terminal exists"
                " for this spec"
            )
        result = execute_promoted_operational_run(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
        )
        return publish_promoted_operational_result(
            result=result,
            advisory_outbox=advisory_outbox,
            terminal_store=terminal_store,
            paper_ledger=paper_ledger,
        )

    if (
        terminal_binding is None
        or terminal_binding.spec_id != spec.spec_id
        or terminal_binding.spec_id != existing.spec_id
        or terminal_binding.expected_terminal_id != existing.terminal_id
    ):
        raise PromotedOperationalServiceError(
            "a local terminal exists but the supplied trusted terminal binding is"
            " missing or does not match"
        )
    try:
        _verify_terminal_matches_spec(existing, spec)
    except PromotedOperationalPersistenceError:
        raise PromotedOperationalServiceError(
            "existing terminal record does not match the live run spec"
        ) from None
    if type(advisory_outbox) is not LocalPromotedOperationalAdvisoryOutbox:
        raise PromotedOperationalServiceError(_ERR_SERVICE)

    advisory = advisory_outbox.get(existing.advisory_id)

    registration: PaperTradeRegistration | None = None
    if existing.paper_registration_id is not None:
        if type(paper_ledger) is not LocalPaperTradeLedger:
            raise PromotedOperationalServiceError(
                "existing terminal references a paper registration but no ledger was supplied"
            )
        try:
            registration = paper_ledger.get_registration(existing.paper_registration_id)
        except Exception:
            raise PromotedOperationalServiceError(
                "existing terminal's referenced paper registration could not be verified"
            ) from None
        if registration.registration_id != existing.paper_registration_id:
            raise PromotedOperationalServiceError(
                "existing terminal's paper registration cross-link is invalid"
            )
        if (
            registration.earliest_entry_at != spec.quote_gate_spec.decision_not_before
            or registration.entry_expires_at != spec.quote_gate_spec.decision_deadline
        ):
            raise PromotedOperationalServiceError(
                "existing terminal's paper registration entry window does not match the live spec"
            )

    return PromotedOperationalPublishedState(
        terminal=existing,
        advisory=advisory,
        paper_registration=registration,
        reused_existing_terminal=True,
    )
