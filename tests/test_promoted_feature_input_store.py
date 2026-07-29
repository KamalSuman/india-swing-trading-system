from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from india_swing.features.input_codec import (
    PromotedFeatureInputCodecError,
    decode_promoted_feature_input_record,
    encode_promoted_feature_input_panel,
)
from india_swing.features.store import (
    LocalPromotedCrossSectionStore,
    LocalPromotedFeatureInputStore,
    LocalPromotedTechnicalFeatureStore,
    PromotedFeatureStoreError,
)
from tests.test_promoted_feature_persistence import _artifacts


class _Resolver:
    def __init__(self, values, identity_name: str) -> None:
        self.values = {
            getattr(value, identity_name): value for value in values
        }
        self.calls: list[str] = []

    def get(self, value_id: str):
        self.calls.append(value_id)
        return self.values[value_id]


_SHARED_TEMP = None
_SHARED_ARTIFACTS = None


def _fixture():
    global _SHARED_TEMP, _SHARED_ARTIFACTS
    if _SHARED_ARTIFACTS is None:
        _SHARED_TEMP = tempfile.TemporaryDirectory()
        _SHARED_ARTIFACTS = _artifacts(
            Path(_SHARED_TEMP.name) / "evidence"
        )
    return _SHARED_ARTIFACTS


def _store(root: Path):
    source, _, technical, _, cross = _fixture()
    adjustment_resolver = _Resolver(
        (source.adjustment_panel,),
        "bridge_id",
    )
    tick_resolver = _Resolver(
        (source.tick_panel,),
        "panel_id",
    )
    input_store = LocalPromotedFeatureInputStore(
        root,
        adjustment_resolver,
        tick_resolver,
    )
    return (
        source,
        technical,
        cross,
        adjustment_resolver,
        tick_resolver,
        input_store,
    )


def tearDownModule() -> None:
    global _SHARED_TEMP, _SHARED_ARTIFACTS
    if _SHARED_TEMP is not None:
        _SHARED_TEMP.cleanup()
    _SHARED_TEMP = None
    _SHARED_ARTIFACTS = None


class PromotedFeatureInputCodecTests(unittest.TestCase):
    def test_manifest_round_trips_only_exact_source_references(
        self,
    ) -> None:
        source, _, _, _, _, _ = _store(Path("unused"))
        payload = encode_promoted_feature_input_panel(source)
        record = decode_promoted_feature_input_record(payload)
        raw = json.loads(payload)
        self.assertEqual(
            record.adjustment_bridge_id,
            source.adjustment_panel.bridge_id,
        )
        self.assertEqual(
            record.tick_panel_id,
            source.tick_panel.panel_id,
        )
        self.assertEqual(record.cutoff, source.cutoff)
        self.assertEqual(record.panel_id, source.panel_id)
        self.assertNotIn("adjustment_panel", raw)
        self.assertNotIn("tick_panel", raw)
        self.assertTrue(payload.endswith(b"\n"))

    def test_decoder_rejects_duplicate_keys(self) -> None:
        payload = (
            b'{"codec_schema_version":"x",'
            b'"codec_schema_version":"y"}\n'
        )
        with self.assertRaises(PromotedFeatureInputCodecError):
            decode_promoted_feature_input_record(payload)

    def test_decoder_rejects_float_tokens(self) -> None:
        source, _, _, _, _, _ = _store(Path("unused"))
        raw = json.loads(encode_promoted_feature_input_panel(source))
        raw["panel"]["unassigned_entry_count"] = 1.5
        with self.assertRaises(PromotedFeatureInputCodecError):
            decode_promoted_feature_input_record(
                json.dumps(raw).encode()
            )

    def test_decoder_rejects_authority_upgrade(self) -> None:
        source, _, _, _, _, _ = _store(Path("unused"))
        raw = json.loads(encode_promoted_feature_input_panel(source))
        raw["panel"]["actionable"] = True
        payload = (
            json.dumps(
                raw,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode()
        with self.assertRaises(PromotedFeatureInputCodecError):
            decode_promoted_feature_input_record(payload)


class PromotedFeatureInputStoreTests(unittest.TestCase):
    def test_create_once_store_replays_both_exact_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                source,
                _,
                _,
                adjustment_resolver,
                tick_resolver,
                store,
            ) = _store(Path(tmp))
            first = store.put(source)
            second = store.put(source)
            restored = store.get(source.panel_id)
        self.assertEqual(first, source)
        self.assertEqual(second, source)
        self.assertEqual(restored, source)
        self.assertIn(
            source.adjustment_panel.bridge_id,
            adjustment_resolver.calls,
        )
        self.assertIn(source.tick_panel.panel_id, tick_resolver.calls)

    def test_complete_feature_store_chain_replays_after_publication(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, technical, cross, _, _, input_store = _store(root)
            input_store.put(source)
            technical_store = LocalPromotedTechnicalFeatureStore(
                root,
                input_store,
            )
            technical_store.put(technical)
            cross_store = LocalPromotedCrossSectionStore(
                root,
                technical_store,
            )
            cross_store.put(cross)
            restored = cross_store.get(cross.panel_id)
        self.assertEqual(restored, cross)

    def test_missing_adjustment_source_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, _, _, _, tick_resolver, writer = _store(root)
            writer.put(source)
            reader = LocalPromotedFeatureInputStore(
                root,
                _Resolver((), "bridge_id"),
                tick_resolver,
            )
            with self.assertRaises(PromotedFeatureStoreError):
                reader.get(source.panel_id)

    def test_put_does_not_publish_before_source_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, _, _, _, tick_resolver, _ = _store(root)
            writer = LocalPromotedFeatureInputStore(
                root,
                _Resolver((), "bridge_id"),
                tick_resolver,
            )
            with self.assertRaises(PromotedFeatureStoreError):
                writer.put(source)
            self.assertFalse(writer.path_for(source.panel_id).exists())

    def test_tampered_projection_fails_independent_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, _, _, _, _, store = _store(root)
            store.put(source)
            path = store.path_for(source.panel_id)
            raw = json.loads(path.read_bytes())
            raw["panel"]["result_ids"] = ["f" * 64]
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
                store.get(source.panel_id)

    def test_invalid_id_cannot_select_a_path(self) -> None:
        _, _, _, _, _, store = _store(Path("unused"))
        for value in ("latest", "../latest", "A" * 64, "0" * 63):
            with self.assertRaises(PromotedFeatureStoreError):
                store.path_for(value)

    def test_store_exposes_no_discovery_or_latest_operation(self) -> None:
        public = {
            value
            for value in dir(LocalPromotedFeatureInputStore)
            if not value.startswith("_")
        }
        self.assertEqual(
            public,
            {"get", "panels_root", "path_for", "put"},
        )


if __name__ == "__main__":
    unittest.main()
