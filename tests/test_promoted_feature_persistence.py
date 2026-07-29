from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from india_swing.features.codec import (
    PromotedFeatureCodecError,
    decode_cross_section_record,
    decode_technical_feature_record,
    encode_cross_section_panel,
    encode_technical_feature_panel,
)
from india_swing.features.historical_replay import (
    PromotedHistoricalReplayError,
    PromotedHistoricalReplayInput,
    PromotedHistoricalReplayService,
    PromotedHistoricalReplayStatus,
)
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionConfig,
    PromotedCrossSectionService,
)
from india_swing.features.store import (
    ExactPromotedFeatureInputPanelResolver,
    LocalPromotedCrossSectionStore,
    LocalPromotedTechnicalFeatureStore,
    PromotedFeatureStoreError,
)
from tests.test_promoted_identity_session_universe import D1
from tests.test_promoted_technical_features import (
    _feature_panel,
    _small_config,
)


class _Resolver:
    def __init__(self, values) -> None:
        self.values = {value.panel_id: value for value in values}
        self.calls: list[str] = []

    def get(self, panel_id: str):
        self.calls.append(panel_id)
        return self.values[panel_id]


def _artifacts(root: Path):
    source, technical_config, technical = _feature_panel(root)
    cross_config = PromotedCrossSectionConfig(
        minimum_computed_instruments=1
    )
    cross = PromotedCrossSectionService().materialize(
        source_panel=technical,
        config=cross_config,
        cutoff=technical.cutoff,
    )
    return (
        source,
        technical_config,
        technical,
        cross_config,
        cross,
    )


class PromotedFeatureCodecTests(unittest.TestCase):
    def test_technical_manifest_round_trips_replay_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, config, technical, _, _ = _artifacts(Path(tmp))
            payload = encode_technical_feature_panel(technical)
            record = decode_technical_feature_record(payload)
        self.assertTrue(payload.endswith(b"\n"))
        self.assertEqual(record.source_panel_id, source.panel_id)
        self.assertEqual(record.config, config)
        self.assertEqual(record.cutoff, technical.cutoff)
        self.assertEqual(record.panel_id, technical.panel_id)

    def test_cross_section_manifest_round_trips_replay_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, technical, config, cross = _artifacts(Path(tmp))
            payload = encode_cross_section_panel(cross)
            record = decode_cross_section_record(payload)
        self.assertEqual(record.source_panel_id, technical.panel_id)
        self.assertEqual(record.config, config)
        self.assertEqual(record.cutoff, cross.cutoff)
        self.assertEqual(record.panel_id, cross.panel_id)

    def test_decoder_rejects_duplicate_keys(self) -> None:
        payload = (
            b'{"codec_schema_version":"x",'
            b'"codec_schema_version":"y"}'
        )
        with self.assertRaisesRegex(
            PromotedFeatureCodecError,
            "duplicate keys",
        ):
            decode_technical_feature_record(payload)

    def test_decoder_rejects_float_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, technical, _, _ = _artifacts(Path(tmp))
            payload = encode_technical_feature_panel(technical)
            raw = json.loads(payload)
            raw["config"]["minimum_history_sessions"] = 2.5
            tampered = json.dumps(
                raw,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        with self.assertRaises(PromotedFeatureCodecError):
            decode_technical_feature_record(tampered)

    def test_decoder_rejects_authority_flag_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, technical, _, _ = _artifacts(Path(tmp))
            raw = json.loads(encode_technical_feature_panel(technical))
            raw["panel"]["actionable"] = True
            tampered = (
                json.dumps(
                    raw,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode()
        with self.assertRaises(PromotedFeatureCodecError):
            decode_technical_feature_record(tampered)


class PromotedFeatureStoreTests(unittest.TestCase):
    def test_exact_source_resolver_has_no_discovery_or_latest_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, _, _, _ = _artifacts(Path(tmp))
            resolver = ExactPromotedFeatureInputPanelResolver((source,))
            restored = resolver.get(source.panel_id)
        self.assertEqual(restored.panel_id, source.panel_id)
        public = {
            value
            for value in dir(ExactPromotedFeatureInputPanelResolver)
            if not value.startswith("_")
        }
        self.assertEqual(public, {"get"})

    def test_create_once_stores_replay_both_panels(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, _, technical, _, cross = _artifacts(root / "evidence")
            source_resolver = _Resolver((source,))
            technical_store = LocalPromotedTechnicalFeatureStore(
                root / "store",
                source_resolver,
            )
            stored_technical = technical_store.put(technical)
            cross_store = LocalPromotedCrossSectionStore(
                root / "store",
                technical_store,
            )
            stored_cross = cross_store.put(cross)
            restored_technical = technical_store.get(technical.panel_id)
            restored_cross = cross_store.get(cross.panel_id)

        self.assertEqual(stored_technical.panel_id, technical.panel_id)
        self.assertEqual(restored_technical, technical)
        self.assertEqual(stored_cross.panel_id, cross.panel_id)
        self.assertEqual(restored_cross, cross)
        self.assertIn(source.panel_id, source_resolver.calls)

    def test_repeated_put_is_idempotent_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, _, technical, _, cross = _artifacts(root / "evidence")
            technical_store = LocalPromotedTechnicalFeatureStore(
                root / "store",
                _Resolver((source,)),
            )
            cross_store = LocalPromotedCrossSectionStore(
                root / "store",
                technical_store,
            )
            first_technical = technical_store.put(technical)
            first_cross = cross_store.put(cross)
            second_technical = technical_store.put(technical)
            second_cross = cross_store.put(cross)
            technical_files = tuple(
                technical_store.panels_root.glob("*.json")
            )
            cross_files = tuple(cross_store.panels_root.glob("*.json"))

        self.assertEqual(first_technical, second_technical)
        self.assertEqual(first_cross, second_cross)
        self.assertEqual(len(technical_files), 1)
        self.assertEqual(len(cross_files), 1)

    def test_tampered_manifest_fails_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, _, technical, _, _ = _artifacts(root / "evidence")
            store = LocalPromotedTechnicalFeatureStore(
                root / "store",
                _Resolver((source,)),
            )
            store.put(technical)
            path = store.path_for(technical.panel_id)
            raw = json.loads(path.read_bytes())
            raw["panel"]["computed_history_count"] = 999
            path.write_bytes(
                (
                    json.dumps(
                        raw,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode()
            )
            with self.assertRaises(PromotedFeatureStoreError):
                store.get(technical.panel_id)

    def test_wrong_resolved_source_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, _, technical, _, _ = _artifacts(root / "evidence")
            writer = LocalPromotedTechnicalFeatureStore(
                root / "store",
                _Resolver((source,)),
            )
            writer.put(technical)
            wrong = LocalPromotedTechnicalFeatureStore(
                root / "store",
                _Resolver(()),
            )
            with self.assertRaises(PromotedFeatureStoreError):
                wrong.get(technical.panel_id)

    def test_invalid_panel_id_cannot_select_a_path(self) -> None:
        store = LocalPromotedTechnicalFeatureStore(
            Path("unused"),
            _Resolver(()),
        )
        for value in ("../latest", "latest", "A" * 64, "0" * 63):
            with self.assertRaises(PromotedFeatureStoreError):
                store.path_for(value)

    def test_stores_expose_no_list_or_latest_selection(self) -> None:
        for store_type in (
            LocalPromotedTechnicalFeatureStore,
            LocalPromotedCrossSectionStore,
        ):
            public = {
                value
                for value in dir(store_type)
                if not value.startswith("_")
            }
            self.assertEqual(public, {"get", "panels_root", "path_for", "put"})


class PromotedHistoricalReplayTests(unittest.TestCase):
    def _run(self, root: Path, *, cross_minimum: int = 1):
        source, _, _, _, _ = _artifacts(root / "evidence")
        technical_config = _small_config()
        cross_config = PromotedCrossSectionConfig(
            minimum_computed_instruments=cross_minimum
        )
        replay_input = PromotedHistoricalReplayInput(
            market_session=(
                source.adjustment_panel.signal_session
            ),
            source_panel=source,
            technical_config=technical_config,
            cross_section_config=cross_config,
            cutoff=source.cutoff,
        )
        technical_store = LocalPromotedTechnicalFeatureStore(
            root / "store",
            _Resolver((source,)),
        )
        cross_store = LocalPromotedCrossSectionStore(
            root / "store",
            technical_store,
        )
        run = PromotedHistoricalReplayService().run(
            inputs=(replay_input,),
            technical_store=technical_store,
            cross_section_store=cross_store,
        )
        return replay_input, technical_store, cross_store, run

    def test_replays_exact_session_and_persists_output_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            replay_input, technical_store, cross_store, run = self._run(
                Path(tmp)
            )
            run.verify_content_identity()
            result = run.results[0]
            technical = technical_store.get(result.technical_panel_id)
            cross = cross_store.get(result.cross_section_panel_id)

        self.assertEqual(result.market_session, replay_input.market_session)
        self.assertEqual(technical.panel_id, result.technical_panel_id)
        self.assertEqual(cross.panel_id, result.cross_section_panel_id)
        self.assertIs(
            result.status,
            (
                PromotedHistoricalReplayStatus
                .SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE
            ),
        )
        self.assertEqual(run.replayed_session_count, 1)
        self.assertEqual(run.blocked_session_count, 0)
        self.assertEqual(run.source_universe_incomplete_session_count, 1)

    def test_second_run_resumes_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            replay_input, technical_store, cross_store, first = self._run(root)
            second = PromotedHistoricalReplayService().run(
                inputs=(replay_input,),
                technical_store=technical_store,
                cross_section_store=cross_store,
            )
            technical_files = tuple(
                technical_store.panels_root.glob("*.json")
            )
            cross_files = tuple(cross_store.panels_root.glob("*.json"))

        self.assertEqual(first.run_id, second.run_id)
        self.assertEqual(first.results, second.results)
        self.assertEqual(len(technical_files), 1)
        self.assertEqual(len(cross_files), 1)

    def test_cross_section_minimum_is_recorded_as_session_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, run = self._run(Path(tmp), cross_minimum=20)
        self.assertIs(
            run.results[0].status,
            PromotedHistoricalReplayStatus.SESSION_REPLAYED_WITH_BLOCKERS,
        )
        self.assertEqual(run.blocked_session_count, 1)
        self.assertGreater(
            run.results[0].cross_section_blocked_history_count,
            0,
        )

    def test_replay_input_rejects_wrong_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, _, _, _ = _artifacts(Path(tmp))
            with self.assertRaises(PromotedHistoricalReplayError):
                PromotedHistoricalReplayInput(
                    market_session=D1,
                    source_panel=source,
                    technical_config=_small_config(),
                    cross_section_config=PromotedCrossSectionConfig(
                        minimum_computed_instruments=1
                    ),
                    cutoff=source.cutoff,
                )

    def test_replay_rejects_duplicate_session_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            replay_input, technical_store, cross_store, _ = self._run(
                Path(tmp)
            )
            with self.assertRaises(PromotedHistoricalReplayError):
                PromotedHistoricalReplayService().run(
                    inputs=(replay_input, replay_input),
                    technical_store=technical_store,
                    cross_section_store=cross_store,
                )

    def test_run_remains_collection_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, run = self._run(Path(tmp))
        self.assertFalse(run.actionable)
        self.assertFalse(run.training_eligible)
        self.assertFalse(run.ranking_eligible)
        self.assertFalse(run.alert_eligible)
        self.assertFalse(run.execution_eligible)

    def test_replay_service_exposes_no_latest_or_discovery_method(self) -> None:
        public = {
            value
            for value in dir(PromotedHistoricalReplayService)
            if not value.startswith("_")
        }
        self.assertEqual(public, {"run"})


if __name__ == "__main__":
    unittest.main()
