from __future__ import annotations

import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from decimal import Decimal
from unittest.mock import patch

from india_swing.daily_pipeline.config import DAILY_PIPELINE_ROOT_ENV
from india_swing.daily_pipeline.derived_evidence import (
    daily_run_chain,
    materialize_daily_derived_evidence,
)
from india_swing.daily_pipeline.derived_evidence_store import (
    LocalDailyDerivedEvidenceStore,
)
from india_swing.historical_prices.config import (
    DAILY_REPORTS_ROOT_ENV,
    HISTORICAL_PRICES_ROOT_ENV,
)
from india_swing.liquidity import LocalLiquiditySnapshotStore
from india_swing.liquidity.config import LIQUIDITY_ROOT_ENV
from india_swing.shadow_scanner import (
    CollectionShadowScannerConfig,
    LocalCollectionShadowScanStore,
    ShadowScanError,
    ShadowScanStoreError,
    ShadowScanStatus,
    scan_collection_artifacts,
)
from india_swing.shadow_scanner.cli import main as scanner_main
from india_swing.shadow_scanner.config import SHADOW_SCAN_ROOT_ENV
from india_swing.tick_sizes import LocalTickSizeSnapshotStore
from india_swing.tick_sizes.config import TICK_SIZE_ROOT_ENV
from india_swing.universe import LocalCollectionUniverseSnapshotStore
from india_swing.universe.config import COLLECTION_UNIVERSE_ROOT_ENV
from india_swing.reference_data.config import REFERENCE_DATA_ROOT_ENV
from tests import test_daily_pipeline as daily_pipeline_tests


class CollectionShadowScannerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = daily_pipeline_tests.DailyPipelineTests(
            "test_bootstrap_run_persists_complete_collection_only_lineage"
        )
        self.fixture.setUp()
        self.run = self.fixture._run()
        self.tick_store = LocalTickSizeSnapshotStore(
            self.fixture.root / "ticks",
            self.fixture.reference_root,
        )
        self.liquidity_store = LocalLiquiditySnapshotStore(
            self.fixture.root / "liquidity",
            self.fixture.history_root,
            self.fixture.daily_root,
        )
        self.universe_store = LocalCollectionUniverseSnapshotStore(
            self.fixture.root / "universe",
            self.fixture.reference_root,
        )
        self.history = (
            self.fixture.historical_store.get(
                self.run.historical_price_artifact_id
            ).artifact,
        )

    def tearDown(self) -> None:
        self.fixture.tearDown()

    def inputs(self, minimum_history_sessions: int):
        derived = materialize_daily_derived_evidence(
            runs=daily_run_chain(self.run, run_store=self.fixture.run_store),
            reference_store=self.fixture.reference_store,
            historical_store=self.fixture.historical_store,
            tick_store=self.tick_store,
            liquidity_store=self.liquidity_store,
            universe_store=self.universe_store,
            minimum_history_sessions=minimum_history_sessions,
        )
        return (
            derived,
            self.universe_store.get(derived.universe_snapshot_id),
            self.liquidity_store.get(derived.liquidity_snapshot_id),
            self.tick_store.get(derived.tick_size_snapshot_id),
        )

    def test_default_policy_returns_typed_no_candidate_for_short_history(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(120)

        result = scan_collection_artifacts(
            derived=derived,
            history=self.history,
            universe=universe,
            liquidity=liquidity,
            ticks=ticks,
        )

        self.assertIs(result.status, ShadowScanStatus.NO_CANDIDATE)
        self.assertEqual(result.candidates, ())
        self.assertFalse(result.actionable)
        self.assertEqual(result.mode, "RESEARCH_ONLY")
        self.assertIn("INSUFFICIENT_HISTORY", result.blockers)
        self.assertIn(
            ("INSUFFICIENT_GLOBAL_HISTORY", len(universe.in_scope_observations)),
            result.exclusion_counts,
        )
        result.verify_content_identity()

    def test_one_session_test_policy_ranks_observation_without_trade_levels(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(1)
        config = CollectionShadowScannerConfig(
            minimum_history_sessions=1,
            momentum_lookback_sessions=1,
            minimum_median_traded_value=Decimal("100000"),
            minimum_delivery_percent=Decimal("20"),
        )

        result = scan_collection_artifacts(
            derived=derived,
            history=self.history,
            universe=universe,
            liquidity=liquidity,
            ticks=ticks,
            config=config,
        )

        self.assertIs(result.status, ShadowScanStatus.RANKED)
        self.assertEqual(len(result.candidates), 1)
        candidate = result.candidates[0]
        source_bar = next(
            value
            for value in self.history[0].bars
            if value.symbol == candidate.symbol and value.series == candidate.series
        )
        self.assertEqual(candidate.symbol, "INFY")
        self.assertEqual(candidate.series, "EQ")
        self.assertEqual(candidate.current_close, Decimal("1610.00"))
        self.assertEqual(
            candidate.lookback_return_pct,
            ((source_bar.close / source_bar.previous_close) - Decimal("1"))
            * Decimal("100"),
        )
        self.assertEqual(candidate.positive_session_fraction, Decimal("1"))
        self.assertEqual(candidate.tick_size_rupees, Decimal("0.05"))
        self.assertFalse(hasattr(candidate, "quantity"))
        self.assertFalse(hasattr(candidate, "target"))
        self.assertFalse(hasattr(candidate, "stop"))
        self.assertIn("RESEARCH_ONLY_DO_NOT_EXECUTE", candidate.warnings)
        self.assertIn("COLLECTION_ONLY_NON_ACTIONABLE", result.blockers)
        result.verify_content_identity()

    def test_low_liquidity_is_an_explicit_exclusion_not_a_weak_candidate(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(1)
        config = CollectionShadowScannerConfig(
            minimum_history_sessions=1,
            momentum_lookback_sessions=1,
            minimum_median_traded_value=Decimal("1000000"),
        )

        result = scan_collection_artifacts(
            derived=derived,
            history=self.history,
            universe=universe,
            liquidity=liquidity,
            ticks=ticks,
            config=config,
        )

        self.assertIs(result.status, ShadowScanStatus.NO_CANDIDATE)
        self.assertIn(("LIQUIDITY_BELOW_MINIMUM", 1), result.exclusion_counts)

    def test_snapshot_id_mismatch_fails_before_scoring(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(1)
        mismatched = replace(derived, universe_snapshot_id="f" * 64)
        config = CollectionShadowScannerConfig(
            minimum_history_sessions=1,
            momentum_lookback_sessions=1,
        )

        with self.assertRaisesRegex(ShadowScanError, "differ"):
            scan_collection_artifacts(
                derived=mismatched,
                history=self.history,
                universe=universe,
                liquidity=liquidity,
                ticks=ticks,
                config=config,
            )

    def test_history_order_and_exact_artifact_binding_are_mandatory(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(1)
        config = CollectionShadowScannerConfig(
            minimum_history_sessions=1,
            momentum_lookback_sessions=1,
        )

        with self.assertRaisesRegex(ShadowScanError, "unique"):
            scan_collection_artifacts(
                derived=derived,
                history=(self.history[0], self.history[0]),
                universe=universe,
                liquidity=liquidity,
                ticks=ticks,
                config=config,
            )

    def test_post_construction_bar_mutation_fails_identity_before_scoring(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(1)
        object.__setattr__(self.history[0].bars[0], "close", Decimal("9999"))
        config = CollectionShadowScannerConfig(
            minimum_history_sessions=1,
            momentum_lookback_sessions=1,
        )

        with self.assertRaisesRegex(ShadowScanError, "identity"):
            scan_collection_artifacts(
                derived=derived,
                history=self.history,
                universe=universe,
                liquidity=liquidity,
                ticks=ticks,
                config=config,
            )

    def test_scanner_rejects_non_exact_configuration_instead_of_defaulting(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(1)

        with self.assertRaisesRegex(ShadowScanError, "configuration"):
            scan_collection_artifacts(
                derived=derived,
                history=self.history,
                universe=universe,
                liquidity=liquidity,
                ticks=ticks,
                config=False,
            )

    def test_result_mutation_is_detected(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(120)
        result = scan_collection_artifacts(
            derived=derived,
            history=self.history,
            universe=universe,
            liquidity=liquidity,
            ticks=ticks,
        )
        object.__setattr__(result, "actionable", True)
        object.__setattr__(result, "result_id", result._calculated_id())

        with self.assertRaisesRegex(ShadowScanError, "identity"):
            result.verify_content_identity()

    def test_recomputed_config_id_cannot_hide_invalid_policy_mutation(self) -> None:
        config = CollectionShadowScannerConfig()
        object.__setattr__(config, "momentum_lookback_sessions", 121)
        object.__setattr__(config, "config_id", config._calculated_id())

        with self.assertRaisesRegex(ShadowScanError, "identity"):
            config.verify_content_identity()

    def test_scan_store_round_trip_idempotency_and_tamper_detection(self) -> None:
        derived, universe, liquidity, ticks = self.inputs(120)
        result = scan_collection_artifacts(
            derived=derived,
            history=self.history,
            universe=universe,
            liquidity=liquidity,
            ticks=ticks,
        )
        store = LocalCollectionShadowScanStore(
            self.fixture.root / "shadow-scans"
        )

        first = store.put(result)
        original = store.path_for(result.result_id).read_bytes()
        second = store.put(result)

        self.assertEqual(first, result)
        self.assertEqual(second, result)
        self.assertEqual(store.path_for(result.result_id).read_bytes(), original)
        path = store.path_for(result.result_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["result"]["actionable"] = True
        path.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(ShadowScanStoreError, "read"):
            store.get(result.result_id)

    def test_cli_loads_only_the_exact_derived_evidence_id(self) -> None:
        derived, _, _, _ = self.inputs(1)
        LocalDailyDerivedEvidenceStore(self.fixture.pipeline_root).publish(derived)
        stdout = io.StringIO()
        roots = {
            DAILY_PIPELINE_ROOT_ENV: str(self.fixture.pipeline_root),
            HISTORICAL_PRICES_ROOT_ENV: str(self.fixture.history_root),
            DAILY_REPORTS_ROOT_ENV: str(self.fixture.daily_root),
            LIQUIDITY_ROOT_ENV: str(self.fixture.root / "liquidity"),
            COLLECTION_UNIVERSE_ROOT_ENV: str(self.fixture.root / "universe"),
            TICK_SIZE_ROOT_ENV: str(self.fixture.root / "ticks"),
            REFERENCE_DATA_ROOT_ENV: str(self.fixture.reference_root),
            SHADOW_SCAN_ROOT_ENV: str(self.fixture.root / "shadow-scans"),
        }

        with patch.dict(os.environ, roots, clear=True), redirect_stdout(stdout):
            exit_code = scanner_main(
                [
                    "--derived-evidence-id",
                    derived.evidence_id,
                    "--momentum-lookback-sessions",
                    "1",
                    "--minimum-median-traded-value",
                    "100000",
                    "--publish",
                ]
            )
        payload = json.loads(stdout.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["derived_evidence_id"], derived.evidence_id)
        self.assertEqual(payload["status"], "RANKED")
        self.assertEqual(payload["candidate_count"], 1)
        self.assertFalse(payload["actionable"])
        self.assertTrue(payload["published_path"].endswith(".json"))
        self.assertTrue(
            LocalCollectionShadowScanStore(
                self.fixture.root / "shadow-scans"
            ).path_for(payload["result_id"]).exists()
        )

    def test_cli_failure_is_sanitized(self) -> None:
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            exit_code = scanner_main(
                ["--derived-evidence-id", "access_token=must-not-leak"]
            )

        self.assertEqual(exit_code, 2)
        self.assertNotIn("must-not-leak", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
