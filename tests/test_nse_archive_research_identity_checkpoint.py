from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.evaluation import nse_archive_research_identity as identity_module
from india_swing.evaluation import (
    nse_archive_research_identity_checkpoint_runtime as checkpoint_runtime,
)
from india_swing.evaluation.nse_archive_research_identity import (
    NseArchiveResearchIdentityTransitionKind,
    research_identity_id_for_isin,
)
from india_swing.evaluation.nse_archive_research_identity_checkpoint_runtime import (
    build_nse_archive_research_identity_checkpoint,
)
from india_swing.evaluation.nse_archive_research_identity_checkpoint import (
    MAXIMUM_NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_BYTES,
    NseArchiveResearchIdentityCheckpoint,
    NseArchiveResearchIdentityCheckpointError,
    NseArchiveResearchIdentityListingState,
    NseArchiveResearchIdentityState,
    PinnedNseArchiveResearchIdentityCheckpointRequest,
    decode_nse_archive_research_identity_checkpoint,
    encode_nse_archive_research_identity_checkpoint,
    publish_nse_archive_research_identity_checkpoint,
    read_pinned_nse_archive_research_identity_checkpoint,
)
from india_swing.market_data.nse_archive_range import (
    stream_verified_nse_historical_archive_range,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore

from tests.test_nse_archive_research_dataset import (
    _baseline_dataset,
    _fake_sha256,
    _import_range,
    _stage_sessions,
)
from tests.test_nse_archive_research_identity import (
    _FixedSessionsIterator,
    _record,
    _session,
)


_ISIN_A = "INE009A01021"
_ISIN_B = "INE467B01029"


def _checkpoint(dataset, *, session: date, symbol: str = "AAA"):
    source_isin = _ISIN_A
    identity_id = research_identity_id_for_isin(source_isin)
    record_id = _fake_sha256(f"checkpoint-{session}-{symbol}")
    listing_key = f"NSE:{symbol}"
    position = dataset.accepted_sessions.index(session)
    return NseArchiveResearchIdentityCheckpoint(
        dataset_id=dataset.dataset_id,
        checkpoint_session=session,
        checkpoint_session_snapshot_id=dataset.session_snapshot_ids[position],
        latest_by_listing_key=(
            NseArchiveResearchIdentityListingState(
                listing_key=listing_key,
                research_identity_id=identity_id,
                source_isin=source_isin,
                symbol=symbol,
                record_id=record_id,
                market_session=session,
            ),
        ),
        latest_by_identity=(
            NseArchiveResearchIdentityState(
                research_identity_id=identity_id,
                listing_key=listing_key,
                source_isin=source_isin,
                symbol=symbol,
                record_id=record_id,
                market_session=session,
            ),
        ),
    )


class IdentityCheckpointArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.value = _checkpoint(
            self.dataset,
            session=self.dataset.accepted_sessions[0],
        )

    def test_canonical_round_trip_and_exact_pinned_read(self) -> None:
        payload = encode_nse_archive_research_identity_checkpoint(self.value)
        self.assertEqual(
            decode_nse_archive_research_identity_checkpoint(payload).checkpoint_id,
            self.value.checkpoint_id,
        )

        class Reader:
            def __init__(self) -> None:
                self.calls = []

            def read_generation(self, **kwargs):
                self.calls.append(kwargs)
                return GCSObjectPayload(content_bytes=payload, generation=17)

        reader = Reader()
        restored = read_pinned_nse_archive_research_identity_checkpoint(
            request=PinnedNseArchiveResearchIdentityCheckpointRequest(
                bucket="india-swing-research-data",
                checkpoint_id=self.value.checkpoint_id,
                generation=17,
                expected_sha256=hashlib.sha256(payload).hexdigest(),
            ),
            reader=reader,
        )
        self.assertEqual(restored.checkpoint_id, self.value.checkpoint_id)
        self.assertEqual(len(reader.calls), 1)
        self.assertEqual(
            reader.calls[0]["maximum_bytes"],
            MAXIMUM_NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_BYTES,
        )

    def test_nested_state_tampering_with_cached_checkpoint_id_fails(self) -> None:
        raw = json.loads(
            encode_nse_archive_research_identity_checkpoint(self.value).decode("utf-8")
        )
        raw["latest_by_listing_key"][0]["record_id"] = _fake_sha256("tampered")
        payload = (
            json.dumps(raw, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")
        with self.assertRaises(NseArchiveResearchIdentityCheckpointError) as context:
            decode_nse_archive_research_identity_checkpoint(payload)
        self.assertIsNone(context.exception.__cause__)
        self.assertIsNone(context.exception.__context__)

    def test_publication_is_create_once_and_independently_rechecked(self) -> None:
        payload = encode_nse_archive_research_identity_checkpoint(self.value)

        class Writer:
            def __init__(self) -> None:
                self.calls = []

            def create_or_verify(self, **kwargs):
                self.calls.append(kwargs)
                return PublishedStateObject(
                    object_name=kwargs["object_name"],
                    generation=23,
                    byte_count=len(kwargs["content_bytes"]),
                    sha256=hashlib.sha256(kwargs["content_bytes"]).hexdigest(),
                )

        writer = Writer()
        published = publish_nse_archive_research_identity_checkpoint(
            checkpoint=self.value,
            bucket="india-swing-research-data",
            writer=writer,
        )
        self.assertEqual(published.generation, 23)
        self.assertEqual(len(writer.calls), 1)
        self.assertEqual(writer.calls[0]["content_bytes"], payload)

    def test_cross_map_inconsistency_fails_closed(self) -> None:
        listing = self.value.latest_by_listing_key[0]
        identity = self.value.latest_by_identity[0]
        with self.assertRaises(NseArchiveResearchIdentityCheckpointError):
            NseArchiveResearchIdentityCheckpoint(
                dataset_id=self.dataset.dataset_id,
                checkpoint_session=self.value.checkpoint_session,
                checkpoint_session_snapshot_id=(
                    self.value.checkpoint_session_snapshot_id
                ),
                latest_by_listing_key=(listing,),
                latest_by_identity=(
                    NseArchiveResearchIdentityState(
                        research_identity_id=identity.research_identity_id,
                        listing_key=identity.listing_key,
                        source_isin=identity.source_isin,
                        symbol=identity.symbol,
                        record_id=_fake_sha256("different-record"),
                        market_session=identity.market_session,
                    ),
                ),
            )


class IdentityCheckpointResumeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.first, self.second, self.third = self.dataset.accepted_sessions[:3]

    def test_checkpoint_builder_consumes_only_through_requested_session(self) -> None:
        first_record = _record(
            self.first,
            symbol="AAA",
            validated_isin=_ISIN_A,
        )
        second_record = _record(
            self.second,
            symbol="AAA",
            validated_isin=_ISIN_B,
        )
        seam = _FixedSessionsIterator(
            (
                _session(self.first, (first_record,)),
                _session(self.second, (second_record,)),
            )
        )
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            seam,
        ):
            checkpoint = build_nse_archive_research_identity_checkpoint(
                self.dataset,
                object(),
                checkpoint_session=self.first,
            )
        self.assertEqual(seam.calls, 1)
        self.assertEqual(checkpoint.checkpoint_session, self.first)
        self.assertEqual(checkpoint.latest_by_listing_key[0].source_isin, _ISIN_A)

    def test_resume_replays_only_suffix_and_preserves_transitions(self) -> None:
        first_record = _record(
            self.first,
            symbol="AAA",
            validated_isin=_ISIN_A,
        )
        checkpoint = NseArchiveResearchIdentityCheckpoint(
            dataset_id=self.dataset.dataset_id,
            checkpoint_session=self.first,
            checkpoint_session_snapshot_id=self.dataset.session_snapshot_ids[0],
            latest_by_listing_key=(
                NseArchiveResearchIdentityListingState(
                    listing_key=first_record.listing_key,
                    research_identity_id=research_identity_id_for_isin(_ISIN_A),
                    source_isin=_ISIN_A,
                    symbol=first_record.symbol,
                    record_id=first_record.record_id,
                    market_session=self.first,
                ),
            ),
            latest_by_identity=(
                NseArchiveResearchIdentityState(
                    research_identity_id=research_identity_id_for_isin(_ISIN_A),
                    listing_key=first_record.listing_key,
                    source_isin=_ISIN_A,
                    symbol=first_record.symbol,
                    record_id=first_record.record_id,
                    market_session=self.first,
                ),
            ),
        )
        second_session = _session(
            self.second,
            (
                _record(
                    self.second,
                    symbol="AAA",
                    validated_isin=_ISIN_B,
                ),
            ),
        )
        third_session = _session(
            self.third,
            (
                _record(
                    self.third,
                    symbol="BBB",
                    validated_isin=_ISIN_B,
                ),
            ),
        )
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            return_value=iter((
                _session(self.first, (first_record,)),
                second_session,
                third_session,
            )),
        ):
            full = tuple(
                checkpoint_runtime.price_stream_module._iter_price_stream_sessions(
                    identity_module._iter_paired_sessions(
                        self.dataset,
                        object(),
                        yield_from_session=self.second,
                    ),
                    freshly_verified=False,
                )
            )
        calls = []

        def suffix(dataset, reader, *, after_session):
            calls.append((dataset.dataset_id, reader, after_session))
            return iter((second_session, third_session))

        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions_after",
            side_effect=suffix,
        ), patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            side_effect=AssertionError("historical prefix was reopened"),
        ):
            result = tuple(
                checkpoint_runtime.iter_nse_archive_research_price_stream_sessions_from_checkpoint(
                    self.dataset,
                    object(),
                    start_session=self.second,
                    checkpoint=checkpoint,
                )
            )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][2], self.first)
        self.assertEqual(
            tuple(value.price_stream_session_id for value in result),
            tuple(value.price_stream_session_id for value in full),
        )
        self.assertEqual(
            result[0].transitions[0].kind,
            NseArchiveResearchIdentityTransitionKind.LISTING_KEY_REBOUND,
        )
        self.assertEqual(
            result[1].transitions[0].kind,
            NseArchiveResearchIdentityTransitionKind.IDENTITY_SYMBOL_CHANGED,
        )

    def test_checkpoint_at_or_after_window_start_fails_without_fallback(self) -> None:
        checkpoint = _checkpoint(self.dataset, session=self.second)
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions_after",
        ) as suffix:
            with self.assertRaises(
                checkpoint_runtime.NseArchiveResearchIdentityCheckpointRuntimeError
            ):
                checkpoint_runtime.iter_nse_archive_research_price_stream_sessions_from_checkpoint(
                    self.dataset,
                    object(),
                    start_session=self.second,
                    checkpoint=checkpoint,
                )
        suffix.assert_not_called()


class ArchiveRangeSuffixStreamingTests(unittest.TestCase):
    def test_complete_index_is_verified_but_old_session_blobs_are_not_opened(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = date(2024, 4, 1)
            second = date(2024, 4, 2)
            third = date(2024, 4, 3)
            _stage_sessions(root, first, third)
            store = LocalMarketSnapshotStore(root / "canonical")
            _range, index = _import_range(root, store, first, third)

            class RecordingReader:
                def __init__(self) -> None:
                    self.index_calls = []
                    self.session_calls = []

                def get(self, dataset, snapshot_id):
                    self.index_calls.append((dataset, snapshot_id))
                    return store.get(dataset, snapshot_id)

                def get_hash_verified_from_date_partition(
                    self,
                    dataset,
                    partition_date,
                    snapshot_id,
                ):
                    self.session_calls.append(
                        (dataset, partition_date, snapshot_id)
                    )
                    return store.get_hash_verified_from_date_partition(
                        dataset,
                        partition_date,
                        snapshot_id,
                    )

            reader = RecordingReader()
            streamed = stream_verified_nse_historical_archive_range(
                reader,
                index_snapshot_id=index.manifest.snapshot_id,
                start_after_session=second,
            )
            sessions = tuple(streamed.sessions)

        self.assertEqual(streamed.session_start_index, 2)
        self.assertEqual(len(streamed.session_snapshot_ids), 3)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(len(reader.index_calls), 1)
        self.assertEqual(
            [value[2] for value in reader.session_calls],
            [streamed.session_snapshot_ids[2]],
        )


if __name__ == "__main__":
    unittest.main()
