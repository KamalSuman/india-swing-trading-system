"""One-time checkpoint materialization and bounded price-stream restoration.

This module is the only bridge between the persisted checkpoint artifact and
the pure replay/identity/price-stream layers.  It verifies the exact dataset
binding before supplying restored latest-observation maps to the private
same-call-chain derivation path.  No latest selection, discovery, trading
authority, or mutable global state is introduced.
"""

from __future__ import annotations

from datetime import date
from typing import Iterator

from . import nse_archive_research_identity as identity_module
from . import nse_archive_research_price_stream as price_stream_module
from .nse_archive_research_dataset import NseArchiveResearchDataset
from .nse_archive_research_identity_checkpoint import (
    NseArchiveResearchIdentityCheckpoint,
    NseArchiveResearchIdentityListingState,
    NseArchiveResearchIdentityState,
    verify_nse_archive_research_identity_checkpoint_for_dataset,
)
from .nse_archive_research_price_stream import NseArchiveResearchPriceStreamSession
from .nse_archive_research_replay import NseHistoricalArchiveSnapshotReader


class NseArchiveResearchIdentityCheckpointRuntimeError(ValueError):
    """Checkpoint construction or bounded restoration failed safely."""


def _fail(message: str) -> None:
    raise NseArchiveResearchIdentityCheckpointRuntimeError(message)


def _verified_dataset(dataset: object) -> NseArchiveResearchDataset:
    failed = False
    try:
        if type(dataset) is not NseArchiveResearchDataset:
            raise ValueError
        dataset.verify_content_identity()
        identity_module._verify_admission_dataset_safety_posture(dataset)
    except Exception:
        failed = True
    if failed:
        _fail("research identity checkpoint dataset is invalid")
    return dataset


def build_nse_archive_research_identity_checkpoint(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
    *,
    checkpoint_session: date,
) -> NseArchiveResearchIdentityCheckpoint:
    """Fully authenticate one exact prefix and seal its identity state once."""

    dataset = _verified_dataset(dataset)
    if (
        reader is None
        or type(checkpoint_session) is not date
        or checkpoint_session not in dataset.accepted_sessions
    ):
        _fail("research identity checkpoint request is invalid")

    failed = False
    result = None
    try:
        latest_by_listing_key: dict[str, identity_module._PriorObservation] = {}
        latest_by_identity: dict[str, identity_module._PriorObservation] = {}
        freshly_verified = callable(
            getattr(type(reader), "get_hash_verified_from_date_partition", None)
        )
        sessions = (
            identity_module._iter_freshly_verified_nse_archive_research_sessions(
                dataset,
                reader,
            )
            if freshly_verified
            else identity_module.iter_verified_nse_archive_research_sessions(
                dataset,
                reader,
            )
        )
        reached = False
        for session in sessions:
            admission = identity_module._build_admission_session(
                session,
                latest_by_listing_key,
                latest_by_identity,
                freshly_verified=freshly_verified,
            )
            for decision in admission.decisions:
                if decision.admission_status in identity_module._ADMITTED_STATUSES:
                    latest_by_listing_key[decision.listing_key] = (
                        decision.research_identity_id,
                        decision.source_isin,
                        decision.symbol,
                        decision.record_id,
                        decision.market_session,
                    )
                    latest_by_identity[decision.research_identity_id] = (
                        decision.listing_key,
                        decision.source_isin,
                        decision.symbol,
                        decision.record_id,
                        decision.market_session,
                    )
            if session.market_session == checkpoint_session:
                reached = True
                break
        if not reached:
            raise ValueError

        position = dataset.accepted_sessions.index(checkpoint_session)
        result = NseArchiveResearchIdentityCheckpoint(
            dataset_id=dataset.dataset_id,
            checkpoint_session=checkpoint_session,
            checkpoint_session_snapshot_id=dataset.session_snapshot_ids[position],
            latest_by_listing_key=tuple(
                NseArchiveResearchIdentityListingState(
                    listing_key=listing_key,
                    research_identity_id=value[0],
                    source_isin=value[1],
                    symbol=value[2],
                    record_id=value[3],
                    market_session=value[4],
                )
                for listing_key, value in sorted(latest_by_listing_key.items())
            ),
            latest_by_identity=tuple(
                NseArchiveResearchIdentityState(
                    research_identity_id=research_identity_id,
                    listing_key=value[0],
                    source_isin=value[1],
                    symbol=value[2],
                    record_id=value[3],
                    market_session=value[4],
                )
                for research_identity_id, value in sorted(latest_by_identity.items())
            ),
        )
        result.verify_content_identity()
    except Exception:
        failed = True
    if failed or result is None:
        _fail("research identity checkpoint could not be reconstructed")
    return result


def _sanitize_price_stream(
    iterator: Iterator[NseArchiveResearchPriceStreamSession],
) -> Iterator[NseArchiveResearchPriceStreamSession]:
    while True:
        failed = False
        value = None
        try:
            value = next(iterator)
        except StopIteration:
            return
        except Exception:
            failed = True
        if failed or value is None:
            _fail("research identity checkpoint suffix could not be reconstructed")
        yield value


def iter_nse_archive_research_price_stream_sessions_from_checkpoint(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
    *,
    start_session: date,
    checkpoint: NseArchiveResearchIdentityCheckpoint,
) -> Iterator[NseArchiveResearchPriceStreamSession]:
    """Restore exact state and derive only sessions after the sealed prefix."""

    dataset = _verified_dataset(dataset)
    failed = False
    paired = None
    try:
        if (
            reader is None
            or type(start_session) is not date
            or start_session not in dataset.accepted_sessions
            or type(checkpoint) is not NseArchiveResearchIdentityCheckpoint
            or checkpoint.checkpoint_session >= start_session
        ):
            raise ValueError
        latest_by_listing_key, latest_by_identity = (
            verify_nse_archive_research_identity_checkpoint_for_dataset(
                checkpoint,
                dataset,
            )
        )
        paired = identity_module._iter_paired_sessions(
            dataset,
            reader,
            yield_from_session=start_session,
            latest_by_listing_key=latest_by_listing_key,
            latest_by_identity=latest_by_identity,
            replay_after_session=checkpoint.checkpoint_session,
        )
    except Exception:
        failed = True
    if failed or paired is None:
        _fail("research identity checkpoint is invalid for the requested boundary")

    freshly_verified = callable(
        getattr(type(reader), "get_hash_verified_from_date_partition", None)
    )
    return _sanitize_price_stream(
        price_stream_module._iter_price_stream_sessions(
            paired,
            freshly_verified=freshly_verified,
        )
    )
