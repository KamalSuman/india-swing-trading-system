from __future__ import annotations

import json
import unittest
from datetime import date, datetime, timedelta, timezone

from india_swing.evaluation.nse_archive_research_dataset_gcs import (
    PinnedNseArchiveResearchDatasetRequest,
)
from india_swing.forward_paper.hydrated_cloud_control import (
    ForwardPaperHydratedCloudLaunch,
    ForwardPaperHydratedCloudLaunchError,
    decode_forward_paper_hydrated_cloud_launch,
    encode_forward_paper_hydrated_cloud_launch,
)
from india_swing.promoted_operational_hydrated_cloud_control import (
    PromotedOperationalHydratedCloudLaunch,
)
from india_swing.promoted_operational_input_gcs import (
    PromotedOperationalInputRestoreRequest,
)


def _launch() -> ForwardPaperHydratedCloudLaunch:
    signal = date(2026, 8, 12)
    assembly_id = "a" * 64
    snapshot_id = "c" * 64
    input_restore = PromotedOperationalInputRestoreRequest(
        bucket="india-swing-state",
        manifest_object_name=(
            f"promoted-operational-input/v1/{signal.isoformat()}/{assembly_id}/"
            f"manifests/{snapshot_id}.json"
        ),
        generation=17,
        expected_sha256="d" * 64,
        expected_snapshot_id=snapshot_id,
        expected_assembly_spec_id=assembly_id,
        target_session=signal,
    )
    promoted = PromotedOperationalHydratedCloudLaunch(
        expected_assembly_spec_id=assembly_id,
        expected_operational_run_spec_id="b" * 64,
        target_session=signal,
        state_bucket="india-swing-state",
        input_restore=input_restore,
    )
    sessions = tuple(signal - timedelta(days=value) for value in range(59, -1, -1))
    return ForwardPaperHydratedCloudLaunch(
        promoted_input_launch=promoted,
        dataset_request=PinnedNseArchiveResearchDatasetRequest(
            bucket="india-swing-data",
            dataset_id="e" * 64,
            generation=1786469190290325,
            expected_sha256="f" * 64,
        ),
        decision_cutoff=datetime(2026, 8, 12, 12, tzinfo=timezone.utc),
        expected_market_sessions=sessions,
        corporate_action_snapshot_id="1" * 64,
        tick_panel_id="2" * 64,
        output_bucket="india-swing-state",
    )


class ForwardPaperHydratedCloudControlTests(unittest.TestCase):
    def test_canonical_round_trip_preserves_identity(self) -> None:
        launch = _launch()
        payload = encode_forward_paper_hydrated_cloud_launch(launch)
        restored = decode_forward_paper_hydrated_cloud_launch(payload)
        self.assertEqual(restored.launch_id, launch.launch_id)
        self.assertEqual(restored.dataset_request, launch.dataset_request)
        self.assertEqual(restored.expected_market_sessions, launch.expected_market_sessions)
        self.assertEqual(encode_forward_paper_hydrated_cloud_launch(restored), payload)

    def test_duplicate_or_tampered_json_fails_closed(self) -> None:
        payload = encode_forward_paper_hydrated_cloud_launch(_launch())
        body = json.loads(payload)
        body["launch"]["launch_id"] = "0" * 64
        tampered = (
            json.dumps(body, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode()
        for value in (tampered, b'{"codec_schema_version":"x","codec_schema_version":"x"}'):
            with self.subTest(value=value), self.assertRaises(
                ForwardPaperHydratedCloudLaunchError
            ):
                decode_forward_paper_hydrated_cloud_launch(value)

    def test_signal_session_must_equal_last_history_session(self) -> None:
        launch = _launch()
        with self.assertRaises(ForwardPaperHydratedCloudLaunchError):
            ForwardPaperHydratedCloudLaunch(
                promoted_input_launch=launch.promoted_input_launch,
                dataset_request=launch.dataset_request,
                decision_cutoff=launch.decision_cutoff,
                expected_market_sessions=launch.expected_market_sessions[:-1]
                + (date(2026, 8, 13),),
                corporate_action_snapshot_id=launch.corporate_action_snapshot_id,
                tick_panel_id=launch.tick_panel_id,
                output_bucket=launch.output_bucket,
            )


if __name__ == "__main__":
    unittest.main()
