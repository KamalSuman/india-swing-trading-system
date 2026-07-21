from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field, fields

from india_swing.daily_pipeline.state_publication import (
    PublishedStateObject,
    StateObjectWriter,
)
from india_swing.identity import content_id

from .models import SwingOperationalRunRecord
from .store import encode_operational_run_record


_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
GCS_PUBLICATION_SCHEMA_VERSION = "swing-operational-gcs-publication/v1"
_MAXIMUM_RECORD_BYTES = 512 * 1024


class SwingOperationalGCSError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SwingOperationalGCSPublication:
    record: SwingOperationalRunRecord
    bucket: str
    published_object: PublishedStateObject
    schema_version: str = GCS_PUBLICATION_SCHEMA_VERSION
    publication_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.record) is not SwingOperationalRunRecord:
            raise SwingOperationalGCSError("record must be exact")
        self.record.verify_content_identity()
        if type(self.bucket) is not str or _BUCKET.fullmatch(self.bucket) is None:
            raise SwingOperationalGCSError("bucket is invalid")
        if type(self.published_object) is not PublishedStateObject:
            raise SwingOperationalGCSError("published object must be exact")
        expected_name = operational_record_object_name(self.record)
        if self.published_object.object_name != expected_name:
            raise SwingOperationalGCSError("published object path differs")
        payload = encode_operational_run_record(self.record)
        if (
            self.published_object.byte_count != len(payload)
            or self.published_object.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise SwingOperationalGCSError("published object content differs")
        if self.schema_version != GCS_PUBLICATION_SCHEMA_VERSION:
            raise SwingOperationalGCSError("unsupported GCS publication schema")
        object.__setattr__(self, "publication_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "publication_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        fresh = SwingOperationalGCSPublication(
            record=self.record,
            bucket=self.bucket,
            published_object=self.published_object,
            schema_version=self.schema_version,
        )
        if self.publication_id != fresh.publication_id:
            raise SwingOperationalGCSError("GCS publication content identity failed")


def operational_record_object_name(record: SwingOperationalRunRecord) -> str:
    if type(record) is not SwingOperationalRunRecord:
        raise SwingOperationalGCSError("record must be exact")
    record.verify_content_identity()
    return (
        f"operational/{record.target_session.isoformat()}/"
        f"{record.spec_id}.json"
    )


def publish_operational_record_to_gcs(
    *,
    record: SwingOperationalRunRecord,
    bucket: str,
    writer: StateObjectWriter,
) -> SwingOperationalGCSPublication:
    if type(record) is not SwingOperationalRunRecord:
        raise SwingOperationalGCSError("record must be exact")
    record.verify_content_identity()
    if type(bucket) is not str or _BUCKET.fullmatch(bucket) is None:
        raise SwingOperationalGCSError("bucket is invalid")
    payload = encode_operational_run_record(record)
    try:
        published = writer.create_or_verify(
            bucket=bucket,
            object_name=operational_record_object_name(record),
            content_bytes=payload,
            content_type="application/json",
            maximum_bytes=_MAXIMUM_RECORD_BYTES,
        )
    except Exception:
        raise SwingOperationalGCSError("operational GCS publication failed") from None
    try:
        return SwingOperationalGCSPublication(
            record=record,
            bucket=bucket,
            published_object=published,
        )
    except Exception:
        raise SwingOperationalGCSError("operational GCS publication verification failed") from None
