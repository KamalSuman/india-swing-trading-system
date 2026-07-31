"""Thin orchestration joining the local publication service to the
independent trusted-terminal-binding control plane behind one schedulable
entry point.

Routes solely on the remote anchor: ``load_optional_trusted_promoted_-
operational_terminal_binding`` is called exactly once, and its result --
and nothing else -- becomes the ``terminal_binding`` argument to the
already-accepted ``run_and_publish_promoted_operational_service``, which
alone implements the complete truth table over local-terminal presence and
binding presence. This module never reads ``terminal_store`` itself, never
branches on local terminal presence, never touches the clock or either
source directly, and never constructs a
``TrustedPromotedOperationalTerminalBinding`` itself. Terminal-last local
publication is preserved unmodified; the binding is sealed only after a
genuinely fresh publication (``reused_existing_terminal is False``), never
before and never on replay. A seal failure after a fresh publication fails
closed without deleting, overwriting, retrying, or repairing anything
already published locally -- that state (local terminal present, binding
unsealed) is the deliberate manual-recovery state the accepted revision-3
contract already establishes, and every subsequent retry with the same
spec fails closed rather than rerunning the market pipeline.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
)
from india_swing.promoted_operational_runner import (
    PromotedOperationalPortfolioSource,
    PromotedOperationalQuoteSource,
    PromotedOperationalRunSpec,
)
from india_swing.promoted_operational_service import (
    PromotedOperationalPublishedState,
    run_and_publish_promoted_operational_service,
)
from india_swing.promoted_terminal_binding import PromotedOperationalTerminalBindingRecord
from india_swing.promoted_terminal_binding_control_plane import (
    LoadedPromotedOperationalTerminalBinding,
    PromotedTerminalBindingObjectReader,
    SealedPromotedOperationalTerminalBinding,
    load_optional_trusted_promoted_operational_terminal_binding,
    seal_promoted_operational_terminal_binding,
)


class PromotedOperationalAnchoredSessionError(ValueError):
    pass


_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_ERR_SESSION = "promoted operational anchored session call is invalid"


def _validate_binding_bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
    return value


@dataclass(frozen=True, slots=True)
class AnchoredPromotedOperationalSessionState:
    """Fixed-shape return value of the anchored session orchestrator.

    Not content-addressed: it is a plain result wrapper and never alters
    any retained artifact identity or authority from ``published`` or
    ``binding_record``.
    """

    published: PromotedOperationalPublishedState
    binding_record: PromotedOperationalTerminalBindingRecord
    binding_bucket: str
    binding_object_name: str
    binding_generation: int
    reused_existing_terminal: bool

    def __post_init__(self) -> None:
        if type(self.published) is not PromotedOperationalPublishedState:
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
        if type(self.binding_record) is not PromotedOperationalTerminalBindingRecord:
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
        self.binding_record.verify_content_identity()
        if type(self.binding_bucket) is not str or not self.binding_bucket:
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
        if type(self.binding_object_name) is not str or not self.binding_object_name:
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
        if (
            type(self.binding_generation) is bool
            or type(self.binding_generation) is not int
            or self.binding_generation <= 0
        ):
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
        if type(self.reused_existing_terminal) is not bool:
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
        if self.reused_existing_terminal is not self.published.reused_existing_terminal:
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
        if (
            self.binding_record.spec_id != self.published.terminal.spec_id
            or self.binding_record.expected_terminal_id != self.published.terminal.terminal_id
        ):
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)


def run_publish_and_anchor_promoted_operational_session(
    *,
    spec: PromotedOperationalRunSpec,
    quote_source: PromotedOperationalQuoteSource,
    portfolio_source: PromotedOperationalPortfolioSource,
    clock: Callable[[], datetime],
    advisory_outbox: LocalPromotedOperationalAdvisoryOutbox,
    terminal_store: LocalPromotedOperationalTerminalStore,
    paper_ledger: LocalPaperTradeLedger | None = None,
    binding_bucket: str,
    binding_writer: StateObjectWriter,
    binding_reader: PromotedTerminalBindingObjectReader,
) -> AnchoredPromotedOperationalSessionState:
    """Single schedulable entry point joining local publication to the
    independent binding control plane.

    Routes solely on the remote anchor: ``load_optional_trusted_promoted_-
    operational_terminal_binding`` is called exactly once, before
    ``run_and_publish_promoted_operational_service`` (which alone owns the
    local-terminal/binding truth table) is called exactly once with that
    result as ``terminal_binding``. If and only if the service reports a
    genuinely fresh run (``reused_existing_terminal is False``), the exact
    terminal it just returned is sealed exactly once. On replay, the
    already-loaded binding is re-checked against the returned terminal at
    this boundary and no control-plane write happens at all.
    """

    if type(spec) is not PromotedOperationalRunSpec:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
    spec.verify_content_identity()
    if type(advisory_outbox) is not LocalPromotedOperationalAdvisoryOutbox:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
    if type(terminal_store) is not LocalPromotedOperationalTerminalStore:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
    if paper_ledger is not None and type(paper_ledger) is not LocalPaperTradeLedger:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)

    bucket_failed = False
    try:
        _validate_binding_bucket(binding_bucket)
    except Exception:
        bucket_failed = True
    if bucket_failed:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)

    load_failed = False
    loaded: LoadedPromotedOperationalTerminalBinding | None = None
    try:
        loaded = load_optional_trusted_promoted_operational_terminal_binding(
            spec=spec, bucket=binding_bucket, reader=binding_reader
        )
    except Exception:
        load_failed = True
    if load_failed:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)

    terminal_binding = None if loaded is None else loaded.binding

    run_failed = False
    published: PromotedOperationalPublishedState | None = None
    try:
        published = run_and_publish_promoted_operational_service(
            spec=spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=advisory_outbox,
            terminal_store=terminal_store,
            paper_ledger=paper_ledger,
            terminal_binding=terminal_binding,
        )
    except Exception:
        run_failed = True
    if run_failed or published is None:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)

    if published.reused_existing_terminal:
        if (
            loaded is None
            or loaded.binding.expected_terminal_id != published.terminal.terminal_id
            or loaded.binding.spec_id != spec.spec_id
        ):
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)

        replay_construct_failed = False
        replay_state: AnchoredPromotedOperationalSessionState | None = None
        try:
            replay_state = AnchoredPromotedOperationalSessionState(
                published=published,
                binding_record=loaded.record,
                binding_bucket=loaded.bucket,
                binding_object_name=loaded.object_name,
                binding_generation=loaded.generation,
                reused_existing_terminal=True,
            )
        except Exception:
            replay_construct_failed = True
        if replay_construct_failed or replay_state is None:
            raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
        return replay_state

    seal_failed = False
    sealed: SealedPromotedOperationalTerminalBinding | None = None
    try:
        sealed = seal_promoted_operational_terminal_binding(
            terminal=published.terminal,
            spec=spec,
            bucket=binding_bucket,
            writer=binding_writer,
        )
    except Exception:
        seal_failed = True
    if seal_failed or sealed is None:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)

    fresh_construct_failed = False
    fresh_state: AnchoredPromotedOperationalSessionState | None = None
    try:
        fresh_state = AnchoredPromotedOperationalSessionState(
            published=published,
            binding_record=sealed.record,
            binding_bucket=sealed.bucket,
            binding_object_name=sealed.published.object_name,
            binding_generation=sealed.published.generation,
            reused_existing_terminal=False,
        )
    except Exception:
        fresh_construct_failed = True
    if fresh_construct_failed or fresh_state is None:
        raise PromotedOperationalAnchoredSessionError(_ERR_SESSION)
    return fresh_state
