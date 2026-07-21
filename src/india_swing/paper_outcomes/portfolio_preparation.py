from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.paper_trades import LocalPaperTradeLedger, PaperTradeStatus

from .models import PaperOutcomePolicy
from .operational import PaperOutcomeEvidenceSource, prepare_paper_outcome_job_spec
from .portfolio import (
    LocalPaperPortfolioStateStore,
    PaperPortfolioBatchSpec,
    PaperPortfolioError,
    decode_paper_portfolio_batch_spec,
    encode_paper_portfolio_batch_spec,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"IN[A-Z0-9]{9}[0-9]\Z")
_PREPARATION_SCHEMA = "paper-portfolio-preparation-spec/v1"
_PREPARATION_CODEC = "paper-portfolio-preparation-spec-json/v1"
_MAXIMUM_BYTES = 16 * 1024 * 1024
_ACTIVE_LEDGER_STATUSES = frozenset({PaperTradeStatus.ALERTED, PaperTradeStatus.OPEN})


class PaperPortfolioPreparationError(PaperPortfolioError):
    pass


class PaperPortfolioBatchNotFound(PaperPortfolioPreparationError):
    pass


class PaperPortfolioBatchConflict(PaperPortfolioPreparationError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperPortfolioPreparationError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise PaperPortfolioPreparationError("preparation cutoff must be timezone-aware")
    return value.astimezone(timezone.utc)


def _identity(value: object, omitted: set[str]) -> str:
    return content_id(
        {
            item.name: getattr(value, item.name)
            for item in fields(value)
            if item.name not in omitted
        },
        length=64,
    )


@dataclass(frozen=True, slots=True)
class PaperRegistrationListing:
    registration_id: str
    tick_snapshot_id: str
    series: str
    validated_isin: str
    listing_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.registration_id, "registration_id")
        _sha(self.tick_snapshot_id, "tick_snapshot_id")
        if (
            type(self.series) is not str
            or not self.series
            or self.series != self.series.strip().upper()
        ):
            raise PaperPortfolioPreparationError("paper listing series is invalid")
        if type(self.validated_isin) is not str or _ISIN.fullmatch(self.validated_isin) is None:
            raise PaperPortfolioPreparationError("paper listing ISIN is invalid")
        object.__setattr__(self, "listing_id", _identity(self, {"listing_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperRegistrationListing(
                registration_id=self.registration_id,
                tick_snapshot_id=self.tick_snapshot_id,
                series=self.series,
                validated_isin=self.validated_isin,
            )
        except Exception:
            raise PaperPortfolioPreparationError("paper listing identity failed") from None
        if fresh.listing_id != self.listing_id:
            raise PaperPortfolioPreparationError("paper listing identity failed")


@dataclass(frozen=True, slots=True)
class PaperPortfolioPreparationSpec:
    as_of: datetime
    calendar_materialization_id: str
    historical_artifact_ids: tuple[str, ...]
    listings: tuple[PaperRegistrationListing, ...]
    policy: PaperOutcomePolicy
    previous_batch_id: str | None = None
    expected_previous_state_id: str | None = None
    daily_loss_limit: Decimal = Decimal("1000")
    cumulative_loss_limit: Decimal = Decimal("2000")
    schema_version: str = _PREPARATION_SCHEMA
    preparation_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _utc(self.as_of))
        _sha(self.calendar_materialization_id, "calendar_materialization_id")
        if (
            type(self.historical_artifact_ids) is not tuple
            or not self.historical_artifact_ids
            or len(set(self.historical_artifact_ids)) != len(self.historical_artifact_ids)
        ):
            raise PaperPortfolioPreparationError("historical evidence IDs are invalid")
        for value in self.historical_artifact_ids:
            _sha(value, "historical_artifact_id")
        if type(self.listings) is not tuple or any(
            type(value) is not PaperRegistrationListing for value in self.listings
        ):
            raise PaperPortfolioPreparationError("paper listings must be an exact tuple")
        for value in self.listings:
            value.verify_content_identity()
        registration_ids = tuple(value.registration_id for value in self.listings)
        if registration_ids != tuple(sorted(set(registration_ids))):
            raise PaperPortfolioPreparationError("paper listings must be unique and ordered")
        if type(self.policy) is not PaperOutcomePolicy:
            raise PaperPortfolioPreparationError("paper outcome policy must be exact")
        self.policy.verify_content_identity()
        object.__setattr__(
            self,
            "policy",
            PaperOutcomePolicy(
                slippage_bps=self.policy.slippage_bps,
                maximum_participation=self.policy.maximum_participation,
                policy_version=self.policy.policy_version,
            ),
        )
        if (self.previous_batch_id is None) != (self.expected_previous_state_id is None):
            raise PaperPortfolioPreparationError("preparation predecessor binding is incomplete")
        if self.previous_batch_id is not None:
            _sha(self.previous_batch_id, "previous_batch_id")
            _sha(self.expected_previous_state_id, "expected_previous_state_id")
        for value, name in (
            (self.daily_loss_limit, "daily_loss_limit"),
            (self.cumulative_loss_limit, "cumulative_loss_limit"),
        ):
            if type(value) is not Decimal or not value.is_finite() or value <= 0:
                raise PaperPortfolioPreparationError(f"{name} must be positive")
        if self.schema_version != _PREPARATION_SCHEMA:
            raise PaperPortfolioPreparationError("unsupported portfolio preparation spec")
        object.__setattr__(self, "preparation_id", _identity(self, {"preparation_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperPortfolioPreparationSpec(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "preparation_id"
                }
            )
        except Exception:
            raise PaperPortfolioPreparationError("preparation identity failed") from None
        if fresh.preparation_id != self.preparation_id:
            raise PaperPortfolioPreparationError("preparation identity failed")


def _listing_body(value: PaperRegistrationListing) -> dict[str, object]:
    return {
        "listing_id": value.listing_id,
        "registration_id": value.registration_id,
        "series": value.series,
        "tick_snapshot_id": value.tick_snapshot_id,
        "validated_isin": value.validated_isin,
    }


def encode_paper_portfolio_preparation_spec(value: PaperPortfolioPreparationSpec) -> bytes:
    if type(value) is not PaperPortfolioPreparationSpec:
        raise PaperPortfolioPreparationError("preparation spec must be exact")
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": _PREPARATION_CODEC,
                "spec": {
                    "as_of": value.as_of.isoformat(),
                    "calendar_materialization_id": value.calendar_materialization_id,
                    "cumulative_loss_limit": str(value.cumulative_loss_limit),
                    "daily_loss_limit": str(value.daily_loss_limit),
                    "expected_previous_state_id": value.expected_previous_state_id,
                    "historical_artifact_ids": list(value.historical_artifact_ids),
                    "listings": [_listing_body(item) for item in value.listings],
                    "policy": {
                        "maximum_participation": str(value.policy.maximum_participation),
                        "policy_id": value.policy.policy_id,
                        "policy_version": value.policy.policy_version,
                        "slippage_bps": str(value.policy.slippage_bps),
                    },
                    "preparation_id": value.preparation_id,
                    "previous_batch_id": value.previous_batch_id,
                    "schema_version": value.schema_version,
                },
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_BYTES:
        raise PaperPortfolioPreparationError("preparation spec is too large")
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def decode_paper_portfolio_preparation_spec(payload: bytes) -> PaperPortfolioPreparationSpec:
    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_BYTES:
        raise PaperPortfolioPreparationError("preparation spec bytes are invalid")
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != {"codec_schema_version", "spec"}:
            raise ValueError
        if root["codec_schema_version"] != _PREPARATION_CODEC:
            raise ValueError
        raw = root["spec"]
        expected = {
            "as_of", "calendar_materialization_id", "cumulative_loss_limit",
            "daily_loss_limit", "expected_previous_state_id", "historical_artifact_ids",
            "listings", "policy", "preparation_id", "previous_batch_id",
            "schema_version",
        }
        if type(raw) is not dict or set(raw) != expected or type(raw["listings"]) is not list:
            raise ValueError
        listings = []
        for item in raw["listings"]:
            if type(item) is not dict or set(item) != {
                "listing_id", "registration_id", "series", "tick_snapshot_id",
                "validated_isin"
            }:
                raise ValueError
            listing = PaperRegistrationListing(
                registration_id=item["registration_id"],
                tick_snapshot_id=item["tick_snapshot_id"],
                series=item["series"],
                validated_isin=item["validated_isin"],
            )
            if listing.listing_id != item["listing_id"]:
                raise ValueError
            listings.append(listing)
        policy_raw = raw["policy"]
        if type(policy_raw) is not dict or set(policy_raw) != {
            "maximum_participation", "policy_id", "policy_version", "slippage_bps"
        }:
            raise ValueError
        policy = PaperOutcomePolicy(
            slippage_bps=Decimal(policy_raw["slippage_bps"]),
            maximum_participation=Decimal(policy_raw["maximum_participation"]),
            policy_version=policy_raw["policy_version"],
        )
        if policy.policy_id != policy_raw["policy_id"]:
            raise ValueError
        stored_id = raw["preparation_id"]
        value = PaperPortfolioPreparationSpec(
            as_of=datetime.fromisoformat(raw["as_of"]),
            calendar_materialization_id=raw["calendar_materialization_id"],
            historical_artifact_ids=tuple(raw["historical_artifact_ids"]),
            listings=tuple(listings),
            policy=policy,
            previous_batch_id=raw["previous_batch_id"],
            expected_previous_state_id=raw["expected_previous_state_id"],
            daily_loss_limit=Decimal(raw["daily_loss_limit"]),
            cumulative_loss_limit=Decimal(raw["cumulative_loss_limit"]),
            schema_version=raw["schema_version"],
        )
        if value.preparation_id != stored_id or encode_paper_portfolio_preparation_spec(value) != payload:
            raise ValueError
        return value
    except Exception:
        raise PaperPortfolioPreparationError("preparation spec is invalid") from None


def load_paper_portfolio_preparation_spec_file(path: Path) -> PaperPortfolioPreparationSpec:
    try:
        payload = read_stable_regular_file(path, maximum_bytes=_MAXIMUM_BYTES)
    except Exception:
        raise PaperPortfolioPreparationError("preparation spec file is unavailable") from None
    return decode_paper_portfolio_preparation_spec(payload)


def prepare_paper_portfolio_batch(
    *,
    spec: PaperPortfolioPreparationSpec,
    ledger: LocalPaperTradeLedger,
    evidence_source: PaperOutcomeEvidenceSource,
    portfolio_store: LocalPaperPortfolioStateStore,
) -> PaperPortfolioBatchSpec:
    if type(spec) is not PaperPortfolioPreparationSpec:
        raise PaperPortfolioPreparationError("preparation spec must be exact")
    if type(ledger) is not LocalPaperTradeLedger:
        raise PaperPortfolioPreparationError("paper ledger must be exact")
    if type(portfolio_store) is not LocalPaperPortfolioStateStore:
        raise PaperPortfolioPreparationError("paper portfolio store must be exact")
    spec.verify_content_identity()
    try:
        registrations = ledger.list_registrations()
        active_ids = tuple(
            value.registration_id
            for value in registrations
            if ledger.summary(value.registration_id).status in _ACTIVE_LEDGER_STATUSES
        )
        listing_ids = tuple(value.registration_id for value in spec.listings)
        if active_ids != listing_ids:
            raise PaperPortfolioPreparationError(
                "preparation listing coverage differs from active paper registrations"
            )
        if not active_ids:
            raise PaperPortfolioPreparationError("no active paper registrations exist")
        stored_states = portfolio_store.list_states()
        if spec.previous_batch_id is None:
            if stored_states:
                raise PaperPortfolioPreparationError("preparation predecessor is required")
        else:
            previous = portfolio_store.get(spec.previous_batch_id)
            if previous.state_id != spec.expected_previous_state_id or previous.as_of >= spec.as_of:
                raise PaperPortfolioPreparationError("preparation predecessor differs")
            predecessor_ids = {
                value.previous_state_id
                for value in stored_states
                if value.previous_state_id is not None
            }
            leaves = tuple(
                value for value in stored_states if value.state_id not in predecessor_ids
            )
            if len(leaves) != 1 or leaves[0].state_id != previous.state_id:
                raise PaperPortfolioPreparationError(
                    "preparation predecessor is not the unique portfolio leaf"
                )
        jobs = tuple(
            prepare_paper_outcome_job_spec(
                registration_id=listing.registration_id,
                calendar_materialization_id=spec.calendar_materialization_id,
                tick_snapshot_id=listing.tick_snapshot_id,
                historical_artifact_ids=spec.historical_artifact_ids,
                series=listing.series,
                validated_isin=listing.validated_isin,
                as_of=spec.as_of,
                policy=spec.policy,
                evidence_source=evidence_source,
            )
            for listing in spec.listings
        )
        return PaperPortfolioBatchSpec(
            as_of=spec.as_of,
            outcome_jobs=jobs,
            previous_batch_id=spec.previous_batch_id,
            expected_previous_state_id=spec.expected_previous_state_id,
            daily_loss_limit=spec.daily_loss_limit,
            cumulative_loss_limit=spec.cumulative_loss_limit,
        )
    except PaperPortfolioPreparationError:
        raise
    except Exception:
        raise PaperPortfolioPreparationError("portfolio preparation failed safely") from None


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalPaperPortfolioBatchStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def batches_root(self) -> Path:
        return self.root / "batches"

    def path_for(self, batch_id: str) -> Path:
        _sha(batch_id, "batch_id")
        return self.batches_root / f"{batch_id}.json"

    def get(self, batch_id: str) -> PaperPortfolioBatchSpec:
        path = self.path_for(batch_id)
        if not path.exists() or not path.is_file() or _is_link_like(path):
            raise PaperPortfolioBatchNotFound("prepared paper portfolio batch was not found")
        try:
            value = decode_paper_portfolio_batch_spec(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_BYTES)
            )
        except PaperPortfolioError:
            raise
        except Exception:
            raise PaperPortfolioPreparationError("prepared batch could not be read") from None
        if value.batch_id != batch_id:
            raise PaperPortfolioPreparationError("prepared batch differs from its path")
        return value

    def put(self, value: PaperPortfolioBatchSpec) -> PaperPortfolioBatchSpec:
        if type(value) is not PaperPortfolioBatchSpec:
            raise PaperPortfolioPreparationError("prepared batch must be exact")
        payload = encode_paper_portfolio_batch_spec(value)
        try:
            self.batches_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.batches_root):
                raise PaperPortfolioPreparationError("prepared batch root is unsafe")
            target = self.path_for(value.batch_id)
            with advisory_file_lock(self.batches_root / ".paper-portfolio-batches.lock"):
                if target.exists():
                    stored = self.get(value.batch_id)
                    if stored != value:
                        raise PaperPortfolioBatchConflict(
                            "batch ID already has different prepared content"
                        )
                    return stored
                descriptor, name = tempfile.mkstemp(
                    prefix=".paper-portfolio-batch-", suffix=".tmp", dir=self.batches_root
                )
                temporary = Path(name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PaperPortfolioBatchConflict("prepared batch store is unavailable") from None
        return self.get(value.batch_id)


class LocalPaperPortfolioPreparationStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def specifications_root(self) -> Path:
        return self.root / "specifications"

    def path_for(self, preparation_id: str) -> Path:
        _sha(preparation_id, "preparation_id")
        return self.specifications_root / f"{preparation_id}.json"

    def get(self, preparation_id: str) -> PaperPortfolioPreparationSpec:
        path = self.path_for(preparation_id)
        if not path.exists() or not path.is_file() or _is_link_like(path):
            raise PaperPortfolioPreparationError("stored preparation was not found safely")
        try:
            value = decode_paper_portfolio_preparation_spec(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_BYTES)
            )
        except PaperPortfolioPreparationError:
            raise
        except Exception:
            raise PaperPortfolioPreparationError("stored preparation is invalid") from None
        if value.preparation_id != preparation_id:
            raise PaperPortfolioPreparationError("stored preparation differs from its path")
        return value

    def put(self, value: PaperPortfolioPreparationSpec) -> PaperPortfolioPreparationSpec:
        if type(value) is not PaperPortfolioPreparationSpec:
            raise PaperPortfolioPreparationError("stored preparation must be exact")
        payload = encode_paper_portfolio_preparation_spec(value)
        try:
            self.specifications_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.specifications_root):
                raise PaperPortfolioPreparationError("preparation store root is unsafe")
            target = self.path_for(value.preparation_id)
            with advisory_file_lock(
                self.specifications_root / ".paper-portfolio-preparations.lock"
            ):
                if target.exists():
                    stored = self.get(value.preparation_id)
                    if stored != value:
                        raise PaperPortfolioBatchConflict(
                            "preparation ID already has different content"
                        )
                    return stored
                descriptor, name = tempfile.mkstemp(
                    prefix=".paper-portfolio-preparation-",
                    suffix=".tmp",
                    dir=self.specifications_root,
                )
                temporary = Path(name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PaperPortfolioBatchConflict("preparation store is unavailable") from None
        return self.get(value.preparation_id)
