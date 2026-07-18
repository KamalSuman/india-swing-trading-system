from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from india_swing.daily_pipeline.acquisition import (
    AcquisitionError,
    GCSNSEAcquisitionAdapter,
)


class AcquisitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = GCSNSEAcquisitionAdapter(bucket_name="mock-bucket")

    @patch.object(GCSNSEAcquisitionAdapter, "_fetch_from_nse")
    @patch.object(GCSNSEAcquisitionAdapter, "_route_to_gcs")
    def test_download_security_master_uses_strict_date(
        self, mock_route, mock_fetch
    ) -> None:
        mock_fetch.return_value = b"mock-master-content"
        target = date(2026, 7, 15)
        
        acquired = self.adapter.download_security_master(target)
        
        # Verify it formulated the exact date-bound filename without lookahead/latest assumptions
        mock_fetch.assert_called_once_with("https://nse-mock/CMTR_15072026.csv.gz", target)
        mock_route.assert_called_once_with("CMTR_15072026.csv.gz", b"mock-master-content")
        
        self.assertEqual(acquired.target_date, target)
        self.assertEqual(acquired.filename, "CMTR_15072026.csv.gz")
        self.assertEqual(acquired.content_bytes, b"mock-master-content")
        self.assertEqual(
            acquired.sha256_hash, 
            "146b3d590d5e4fd6c91bfc4db5353efdf591c9f905a71f2821eb9cab9f4a1fb7"
        )

    @patch.object(GCSNSEAcquisitionAdapter, "_fetch_from_nse")
    @patch.object(GCSNSEAcquisitionAdapter, "_route_to_gcs")
    def test_download_daily_bundle_uses_strict_date(
        self, mock_route, mock_fetch
    ) -> None:
        mock_fetch.return_value = b"mock-bundle-content"
        target = date(2026, 7, 15)
        
        acquired = self.adapter.download_daily_bundle(target)
        
        mock_fetch.assert_called_once_with("https://nse-mock/CM_bhav_15072026.zip", target)
        mock_route.assert_called_once_with("CM_bhav_15072026.zip", b"mock-bundle-content")
        
        self.assertEqual(acquired.target_date, target)
        self.assertEqual(acquired.filename, "CM_bhav_15072026.zip")
        self.assertEqual(acquired.content_bytes, b"mock-bundle-content")
        self.assertEqual(
            acquired.sha256_hash, 
            "1da279d1e1574ee18b1ea5ce41fd205bad1f24a47d4f54d8559aaefd3bfa7fa2"
        )


if __name__ == "__main__":
    unittest.main()
