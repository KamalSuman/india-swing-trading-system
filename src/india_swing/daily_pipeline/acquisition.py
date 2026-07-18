from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from typing import Protocol

try:
    from google.cloud import storage
except ImportError:
    storage = None


class AcquisitionError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class AcquiredFile:
    target_date: date
    filename: str
    content_bytes: bytes
    sha256_hash: str


class NSEAcquisitionClient(Protocol):
    def download_security_master(self, target_date: date) -> AcquiredFile:
        ...

    def download_daily_bundle(self, target_date: date) -> AcquiredFile:
        ...


class GCSNSEAcquisitionAdapter:
    """
    Acquires strict date-bound reports from the NSE and immediately
    routes them into a Google Cloud Storage landing bucket.
    Prevents 'latest-wins' behavior by enforcing explicitly typed date parameters.
    """

    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name
        self._client = storage.Client() if storage is not None else None

    def _fetch_from_nse(self, url: str, target_date: date) -> bytes:
        """
        Mock network fetch that prevents 'latest-wins' by requiring date formulation.
        In a real implementation, this would use `requests` or `httpx` to GET the URL.
        """
        raise NotImplementedError("Network fetching is abstracted for blueprint review.")

    def _route_to_gcs(self, filename: str, content_bytes: bytes) -> None:
        """
        Routes the bytes to the GCS bucket. 
        """
        if self._client is None:
            return

        bucket = self._client.bucket(self.bucket_name)
        blob = bucket.blob(f"landing/{filename}")
        blob.upload_from_string(content_bytes)

    def download_security_master(self, target_date: date) -> AcquiredFile:
        # Strict Date Formatting.
        date_str = target_date.strftime("%d%m%Y")
        filename = f"CMTR_{date_str}.csv.gz"
        
        raw_bytes = self._fetch_from_nse(f"https://nse-mock/{filename}", target_date)
        file_hash = hashlib.sha256(raw_bytes).hexdigest()
        
        self._route_to_gcs(filename, raw_bytes)
        
        return AcquiredFile(
            target_date=target_date,
            filename=filename,
            content_bytes=raw_bytes,
            sha256_hash=file_hash,
        )

    def download_daily_bundle(self, target_date: date) -> AcquiredFile:
        # Strict Date Formatting.
        date_str = target_date.strftime("%d%m%Y")
        filename = f"CM_bhav_{date_str}.zip"
        
        raw_bytes = self._fetch_from_nse(f"https://nse-mock/{filename}", target_date)
        file_hash = hashlib.sha256(raw_bytes).hexdigest()
        
        self._route_to_gcs(filename, raw_bytes)

        return AcquiredFile(
            target_date=target_date,
            filename=filename,
            content_bytes=raw_bytes,
            sha256_hash=file_hash,
        )
