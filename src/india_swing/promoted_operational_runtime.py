"""First production-shaped, fully dependency-injected runtime boundary for
the promoted paper engine.

Defines a strict content-addressed job-level artifact
(``PromotedOperationalRuntimeJobSpec``) that binds an already-verified
``PromotedOperationalRunSpec`` to the two pinned source identities, the
target session, the decision window, and the terminal-binding bucket,
plus a strict canonical JSON codec and a hardened local file loader for
it. Also defines an exact pinned portfolio-context source
(``PinnedPromotedOperationalPortfolioSource``) and one thin orchestrator
(``run_promoted_operational_runtime_job``) that independently cross-checks
the job spec against the caller's exact ``PromotedOperationalRunSpec`` and
injected source identities before calling
``run_publish_and_anchor_promoted_operational_session`` exactly once.

This module never constructs a real storage/broker/Telegram client, never
reads an environment variable or credential, never makes a network call,
and never reads the current time. It never reranks a candidate, mutates a
price, widens a policy, resizes a quantity, infers open listings, or
treats a veto as a failure -- all of that remains entirely owned by the
already-accepted preparation/quote-gate/allocation/decision/runner/
service/anchored-session chain this module only binds together and
cross-checks. Loading the promoted run spec from upstream stores,
constructing real Kite/GCS clients from credentials, Telegram delivery,
CLI/Cloud Run wiring, scheduling, and a real shadow run are explicitly out
of scope for this increment.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.identity import content_id
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_allocation import PromotedOperationalPortfolioContext
from india_swing.promoted_operational_anchored_session import (
    AnchoredPromotedOperationalSessionState,
    run_publish_and_anchor_promoted_operational_session,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
)
from india_swing.promoted_operational_runner import (
    PromotedOperationalPortfolioSource,
    PromotedOperationalQuoteSource,
    PromotedOperationalRunSpec,
)
from india_swing.promoted_terminal_binding_control_plane import (
    PromotedTerminalBindingControlPlanePreflight,
    PromotedTerminalBindingObjectReader,
)


class PromotedOperationalRuntimeError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_ERR_RUNTIME = "promoted operational runtime call is invalid"

PROMOTED_OPERATIONAL_RUNTIME_JOB_SPEC_SCHEMA_VERSION = "promoted-operational-runtime-job-spec/v1"
_RUNTIME_JOB_SPEC_CODEC_SCHEMA_VERSION = "promoted-operational-runtime-job-spec-json/v1"
MAXIMUM_RUNTIME_JOB_SPEC_BYTES = 64 * 1024

_CONCRETE_PATH_TYPE = type(Path())


def _require_literal_utc(value: object) -> datetime:
    """Require an exact ``datetime`` with a literal zero UTC offset.

    Unlike sibling modules' ``_require_aware_utc`` helpers, this never
    normalizes a non-UTC-but-aware representation (e.g. IST) via
    ``astimezone`` -- decision timestamps must already be canonically
    represented in UTC, so an equivalent instant under a different offset
    is rejected outright rather than silently coerced."""

    if type(value) is not datetime:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    offset_failed = False
    offset = None
    try:
        offset = value.utcoffset()
    except Exception:
        offset_failed = True
    if offset_failed:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    if value.tzinfo is None or offset != timedelta(0):
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    return value


@dataclass(frozen=True, slots=True)
class PromotedOperationalRuntimeJobSpec:
    """One exact, content-addressed binding of a live run spec's identity
    to its target session, decision window, and the two pinned source
    identities plus the terminal-binding bucket.

    This is a binding, not a second engine spec: it never encodes or
    rebuilds the deeply nested ``PromotedOperationalRunSpec`` -- it only
    retains the exact run-spec ID plus redundant safety-critical
    cross-links so a caller-injected run spec can be independently
    re-verified before anything else runs. Grants no authority:
    ``paper_only`` is always true and both
    ``notification_eligible``/``execution_eligible`` are always false.
    """

    operational_run_spec_id: str
    preparation_id: str
    target_session: date
    decision_not_before: datetime
    decision_deadline: datetime
    expected_quote_source_id: str
    expected_portfolio_source_id: str
    expected_portfolio_context_id: str
    binding_bucket: str
    paper_only: bool = True
    notification_eligible: bool = False
    execution_eligible: bool = False
    schema_version: str = PROMOTED_OPERATIONAL_RUNTIME_JOB_SPEC_SCHEMA_VERSION
    job_spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_RUNTIME_JOB_SPEC_SCHEMA_VERSION:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        for value in (
            self.operational_run_spec_id,
            self.preparation_id,
            self.expected_quote_source_id,
            self.expected_portfolio_source_id,
            self.expected_portfolio_context_id,
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        if type(self.target_session) is not date:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

        not_before = _require_literal_utc(self.decision_not_before)
        deadline = _require_literal_utc(self.decision_deadline)
        if not_before >= deadline:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        if (
            not_before.astimezone(INDIA_STANDARD_TIME).date() != self.target_session
            or deadline.astimezone(INDIA_STANDARD_TIME).date() != self.target_session
        ):
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

        if type(self.binding_bucket) is not str or _BUCKET.fullmatch(self.binding_bucket) is None:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

        object.__setattr__(self, "job_spec_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            item.name: getattr(self, item.name)
            for item in fields(self)
            if item.name != "job_spec_id"
        }

    def _calculated_id(self) -> str:
        return content_id(self._identity(), length=64)

    def verify_content_identity(self) -> None:
        """Reconstruct a fresh instance from this object's own retained
        field values -- rerunning every validation in ``__post_init__`` --
        and require its re-derived ID to match. Never merely compares a
        caller-supplied hash."""

        if type(self) is not PromotedOperationalRuntimeJobSpec:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        reconstruct_failed = False
        reraise: PromotedOperationalRuntimeError | None = None
        fresh: PromotedOperationalRuntimeJobSpec | None = None
        try:
            fresh = PromotedOperationalRuntimeJobSpec(**self._identity())
        except PromotedOperationalRuntimeError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        if self.job_spec_id != fresh.job_spec_id:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)


def build_promoted_operational_runtime_job_spec(
    *,
    run_spec: PromotedOperationalRunSpec,
    portfolio_context: PromotedOperationalPortfolioContext,
    binding_bucket: str,
) -> PromotedOperationalRuntimeJobSpec:
    """Pure builder: derive every retained field from an exact verified
    run spec, an exact verified portfolio context, and a bucket name --
    without caller duplication. Never reads a clock, source, file,
    environment, GCS object, or store."""

    if type(run_spec) is not PromotedOperationalRunSpec:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    if type(portfolio_context) is not PromotedOperationalPortfolioContext:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    verify_failed = False
    try:
        run_spec.verify_content_identity()
        portfolio_context.verify_content_identity()
    except Exception:
        verify_failed = True
    if verify_failed:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    if portfolio_context.source_portfolio_artifact_id != run_spec.expected_portfolio_source_id:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    manifest = run_spec.quote_gate_spec.preparation.manifest

    build_failed = False
    job_spec: PromotedOperationalRuntimeJobSpec | None = None
    try:
        job_spec = PromotedOperationalRuntimeJobSpec(
            operational_run_spec_id=run_spec.spec_id,
            preparation_id=manifest.preparation_id,
            target_session=manifest.target_session,
            decision_not_before=run_spec.quote_gate_spec.decision_not_before,
            decision_deadline=run_spec.quote_gate_spec.decision_deadline,
            expected_quote_source_id=run_spec.expected_quote_source_id,
            expected_portfolio_source_id=run_spec.expected_portfolio_source_id,
            expected_portfolio_context_id=portfolio_context.context_id,
            binding_bucket=binding_bucket,
        )
    except Exception:
        build_failed = True
    if build_failed or job_spec is None:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    return job_spec


_ENVELOPE_KEYS = frozenset({"codec_schema_version", "runtime_job_spec"})
_BODY_KEYS = frozenset(
    {
        "schema_version",
        "operational_run_spec_id",
        "preparation_id",
        "target_session",
        "decision_not_before",
        "decision_deadline",
        "expected_quote_source_id",
        "expected_portfolio_source_id",
        "expected_portfolio_context_id",
        "binding_bucket",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
        "job_spec_id",
    }
)


def _job_spec_body(value: PromotedOperationalRuntimeJobSpec) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "operational_run_spec_id": value.operational_run_spec_id,
        "preparation_id": value.preparation_id,
        "target_session": value.target_session.isoformat(),
        "decision_not_before": value.decision_not_before.isoformat(),
        "decision_deadline": value.decision_deadline.isoformat(),
        "expected_quote_source_id": value.expected_quote_source_id,
        "expected_portfolio_source_id": value.expected_portfolio_source_id,
        "expected_portfolio_context_id": value.expected_portfolio_context_id,
        "binding_bucket": value.binding_bucket,
        "paper_only": value.paper_only,
        "notification_eligible": value.notification_eligible,
        "execution_eligible": value.execution_eligible,
        "job_spec_id": value.job_spec_id,
    }


def encode_promoted_operational_runtime_job_spec(value: PromotedOperationalRuntimeJobSpec) -> bytes:
    if type(value) is not PromotedOperationalRuntimeJobSpec:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": _RUNTIME_JOB_SPEC_CODEC_SCHEMA_VERSION,
                "runtime_job_spec": _job_spec_body(value),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_RUNTIME_JOB_SPEC_BYTES:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalRuntimeError(_ERR_RUNTIME)


def decode_promoted_operational_runtime_job_spec(payload: bytes) -> PromotedOperationalRuntimeJobSpec:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > MAXIMUM_RUNTIME_JOB_SPEC_BYTES
    ):
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    decode_failed = False
    text = ""
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        decode_failed = True
    if decode_failed:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    parse_failed = False
    reraise: PromotedOperationalRuntimeError | None = None
    root: object = None
    try:
        root = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedOperationalRuntimeError as error:
        reraise = error
    except (json.JSONDecodeError, RecursionError):
        parse_failed = True
    if reraise is not None:
        raise reraise
    if parse_failed:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    if (
        type(root) is not dict
        or set(root) != _ENVELOPE_KEYS
        or root["codec_schema_version"] != _RUNTIME_JOB_SPEC_CODEC_SCHEMA_VERSION
    ):
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    raw = root["runtime_job_spec"]
    if type(raw) is not dict or set(raw) != _BODY_KEYS:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    construction_failed = False
    construction_reraise: PromotedOperationalRuntimeError | None = None
    value: PromotedOperationalRuntimeJobSpec | None = None
    try:
        value = PromotedOperationalRuntimeJobSpec(
            schema_version=raw["schema_version"],
            operational_run_spec_id=raw["operational_run_spec_id"],
            preparation_id=raw["preparation_id"],
            target_session=date.fromisoformat(raw["target_session"]),
            decision_not_before=datetime.fromisoformat(raw["decision_not_before"]),
            decision_deadline=datetime.fromisoformat(raw["decision_deadline"]),
            expected_quote_source_id=raw["expected_quote_source_id"],
            expected_portfolio_source_id=raw["expected_portfolio_source_id"],
            expected_portfolio_context_id=raw["expected_portfolio_context_id"],
            binding_bucket=raw["binding_bucket"],
            paper_only=raw["paper_only"],
            notification_eligible=raw["notification_eligible"],
            execution_eligible=raw["execution_eligible"],
        )
    except PromotedOperationalRuntimeError as error:
        construction_reraise = error
    except Exception:
        construction_failed = True
    if construction_reraise is not None:
        raise construction_reraise
    if construction_failed or value is None:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    if value.job_spec_id != raw["job_spec_id"]:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    if encode_promoted_operational_runtime_job_spec(value) != payload:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    return value


def load_promoted_operational_runtime_job_spec_file(path: Path) -> PromotedOperationalRuntimeJobSpec:
    """Load and strictly decode one runtime job spec from a concrete,
    absolute, non-traversing local path. ``path`` must be exactly the
    platform's concrete ``pathlib.Path`` subclass (``type(path) is
    type(Path())``, e.g. ``WindowsPath`` on Windows) -- an arbitrary
    ``Path`` subclass instance is rejected before any of its (possibly
    overridden) path methods or filesystem behavior can be used. No
    filesystem discovery, list/latest/nearest/glob behavior, symlink
    following, fallback, or default path exists here --
    ``read_stable_regular_file`` already rejects symlinks/reparse points
    and concurrent mutation; every failure collapses to one static
    sanitized error."""

    if type(path) is not _CONCRETE_PATH_TYPE:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    read_failed = False
    payload = b""
    try:
        payload = read_stable_regular_file(path, maximum_bytes=MAXIMUM_RUNTIME_JOB_SPEC_BYTES)
    except Exception:
        read_failed = True
    if read_failed:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    decode_failed = False
    value: PromotedOperationalRuntimeJobSpec | None = None
    try:
        value = decode_promoted_operational_runtime_job_spec(payload)
    except Exception:
        decode_failed = True
    if decode_failed or value is None:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    return value


class PinnedPromotedOperationalPortfolioSource:
    """Read-only ``PromotedOperationalPortfolioSource`` port that retains
    exactly one already-verified ``PromotedOperationalPortfolioContext``.

    Never infers open listings, queries a broker, mutates the context, or
    synthesizes a replacement -- the context is exact, external evidence
    supplied at construction. Independently re-verifies content identity
    at construction and on every read, so later in-place tampering (or
    passing an already-corrupted context) is rejected with one static
    sanitized error rather than silently trusted.
    """

    def __init__(self, context: PromotedOperationalPortfolioContext) -> None:
        if type(context) is not PromotedOperationalPortfolioContext:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        verify_failed = False
        try:
            context.verify_content_identity()
        except Exception:
            verify_failed = True
        if verify_failed:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        self._context = context

    def _verified_context(self) -> PromotedOperationalPortfolioContext:
        verify_failed = False
        try:
            self._context.verify_content_identity()
        except Exception:
            verify_failed = True
        if verify_failed:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        return self._context

    @property
    def source_id(self) -> str:
        return self._verified_context().source_portfolio_artifact_id

    @property
    def context_id(self) -> str:
        return self._verified_context().context_id

    def read_portfolio_context(self) -> PromotedOperationalPortfolioContext:
        return self._verified_context()


@dataclass(frozen=True, slots=True)
class PromotedOperationalRuntimeState:
    """Fixed-shape return value of the runtime orchestrator: the exact
    runtime job spec plus the anchored session state it produced.

    Independently re-verifies both and requires every cross-link mutually
    available between them to agree -- job/run-spec ID, preparation ID,
    target session, portfolio source/context IDs when present on the
    anchored result's retained terminal record, the binding bucket, and
    paper-only authority. The decision window is pinned and cross-checked
    against the caller's exact nested run spec earlier, by
    ``run_promoted_operational_runtime_job``, before this state is ever
    constructed -- neither the anchored result nor the terminal record it
    retains carries a decision-window field to re-derive it from here.
    Never copies or invents any trade value.
    """

    job_spec: PromotedOperationalRuntimeJobSpec
    anchored: AnchoredPromotedOperationalSessionState

    def __post_init__(self) -> None:
        if type(self.job_spec) is not PromotedOperationalRuntimeJobSpec:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        job_spec_verify_failed = False
        try:
            self.job_spec.verify_content_identity()
        except Exception:
            job_spec_verify_failed = True
        if job_spec_verify_failed:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
        if type(self.anchored) is not AnchoredPromotedOperationalSessionState:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

        # Defensively collapse any malformed/tampered nested attribute
        # access or comparison into the static runtime error, rather than
        # leaking a foreign exception type or context -- ``self.anchored``
        # is frozen and already validated at its own construction, but
        # nothing here trusts that a post-construction ``object.__setattr__``
        # tamper could not have altered its nested structure.
        cross_check_failed = False
        mismatch = True
        try:
            terminal = self.anchored.published.terminal
            mismatch = (
                self.job_spec.operational_run_spec_id != terminal.spec_id
                or self.job_spec.preparation_id != terminal.preparation_id
                or self.job_spec.target_session != terminal.target_session
                or self.job_spec.binding_bucket != self.anchored.binding_bucket
                or self.job_spec.paper_only is not terminal.paper_only
                or self.job_spec.notification_eligible is not terminal.notification_eligible
                or self.job_spec.execution_eligible is not terminal.execution_eligible
                or (
                    terminal.portfolio_source_id is not None
                    and terminal.portfolio_source_id != self.job_spec.expected_portfolio_source_id
                )
                or (
                    terminal.portfolio_context_id is not None
                    and terminal.portfolio_context_id
                    != self.job_spec.expected_portfolio_context_id
                )
            )
        except Exception:
            cross_check_failed = True
        if cross_check_failed or mismatch:
            raise PromotedOperationalRuntimeError(_ERR_RUNTIME)


def run_promoted_operational_runtime_job(
    *,
    job_spec: PromotedOperationalRuntimeJobSpec,
    run_spec: PromotedOperationalRunSpec,
    quote_source: PromotedOperationalQuoteSource,
    portfolio_source: PinnedPromotedOperationalPortfolioSource,
    clock: Callable[[], datetime],
    advisory_outbox: LocalPromotedOperationalAdvisoryOutbox,
    terminal_store: LocalPromotedOperationalTerminalStore,
    paper_ledger: LocalPaperTradeLedger | None = None,
    binding_writer: StateObjectWriter,
    binding_reader: PromotedTerminalBindingObjectReader,
    binding_preflight: PromotedTerminalBindingControlPlanePreflight,
) -> PromotedOperationalRuntimeState:
    """Thin orchestrator: independently cross-check the job spec against
    the caller's exact run spec and injected source identities, then call
    ``run_publish_and_anchor_promoted_operational_session`` exactly once.

    Before that single call -- and before touching the clock, either
    source's acquisition methods, either local store, or the control
    plane -- ``job_spec`` and ``run_spec`` are independently re-verified
    (any nested failure, including a tampered ``run_spec`` raising its own
    ``PromotedOperationalRunnerError``, is caught and converted to the
    static sanitized ``PromotedOperationalRuntimeError`` here rather than
    escaping under a foreign exception type), ``advisory_outbox``/
    ``terminal_store``/``paper_ledger``/``clock`` are exact-type/callable
    checked without ever calling them, and every retained job-spec
    cross-link is required to agree with the live ``run_spec`` (spec ID,
    preparation ID, target session, decision window, both expected source
    IDs) and with the injected sources' own pinned identities
    (``quote_source.source_id`` read only through a sanitized safe
    boundary; ``portfolio_source.source_id``/``.context_id``, which the
    exact ``PinnedPromotedOperationalPortfolioSource`` type re-verifies on
    every read). On any disagreement this fails before any side effect.
    This function never calls either source's acquisition method itself
    and never re-implements the accepted runner's ordering or call counts
    -- ``run_publish_and_anchor_promoted_operational_session`` remains the
    sole caller of ``run_and_publish_promoted_operational_service``. No
    retry, fallback, reconstruction, deletion, or repair exists here.
    """

    if type(job_spec) is not PromotedOperationalRuntimeJobSpec:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    job_spec_verify_failed = False
    try:
        job_spec.verify_content_identity()
    except Exception:
        job_spec_verify_failed = True
    if job_spec_verify_failed:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    if type(run_spec) is not PromotedOperationalRunSpec:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    run_spec_verify_failed = False
    try:
        run_spec.verify_content_identity()
    except Exception:
        run_spec_verify_failed = True
    if run_spec_verify_failed:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    if type(portfolio_source) is not PinnedPromotedOperationalPortfolioSource:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    if (
        not callable(clock)
        or type(advisory_outbox) is not LocalPromotedOperationalAdvisoryOutbox
        or type(terminal_store) is not LocalPromotedOperationalTerminalStore
        or (paper_ledger is not None and type(paper_ledger) is not LocalPaperTradeLedger)
    ):
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    quote_source_id_failed = False
    quote_source_id: object = None
    try:
        quote_source_id = quote_source.source_id
    except Exception:
        quote_source_id_failed = True
    if quote_source_id_failed or type(quote_source_id) is not str:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    portfolio_identity_failed = False
    portfolio_source_id: object = None
    portfolio_context_id: object = None
    try:
        portfolio_source_id = portfolio_source.source_id
        portfolio_context_id = portfolio_source.context_id
    except Exception:
        portfolio_identity_failed = True
    if (
        portfolio_identity_failed
        or type(portfolio_source_id) is not str
        or type(portfolio_context_id) is not str
    ):
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    manifest = run_spec.quote_gate_spec.preparation.manifest
    if (
        job_spec.operational_run_spec_id != run_spec.spec_id
        or job_spec.preparation_id != manifest.preparation_id
        or job_spec.target_session != manifest.target_session
        or job_spec.decision_not_before != run_spec.quote_gate_spec.decision_not_before
        or job_spec.decision_deadline != run_spec.quote_gate_spec.decision_deadline
        or job_spec.expected_quote_source_id != run_spec.expected_quote_source_id
        or job_spec.expected_quote_source_id != quote_source_id
        or job_spec.expected_portfolio_source_id != run_spec.expected_portfolio_source_id
        or job_spec.expected_portfolio_source_id != portfolio_source_id
        or job_spec.expected_portfolio_context_id != portfolio_context_id
    ):
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    run_failed = False
    anchored: AnchoredPromotedOperationalSessionState | None = None
    try:
        anchored = run_publish_and_anchor_promoted_operational_session(
            spec=run_spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=clock,
            advisory_outbox=advisory_outbox,
            terminal_store=terminal_store,
            paper_ledger=paper_ledger,
            binding_bucket=job_spec.binding_bucket,
            binding_writer=binding_writer,
            binding_reader=binding_reader,
            binding_preflight=binding_preflight,
        )
    except Exception:
        run_failed = True
    if run_failed or anchored is None:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)

    state_failed = False
    state: PromotedOperationalRuntimeState | None = None
    try:
        state = PromotedOperationalRuntimeState(job_spec=job_spec, anchored=anchored)
    except Exception:
        state_failed = True
    if state_failed or state is None:
        raise PromotedOperationalRuntimeError(_ERR_RUNTIME)
    return state
