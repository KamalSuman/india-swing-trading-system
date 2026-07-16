from __future__ import annotations

import json

from .models import NSE_DAILY_BUNDLE_CODEC_VERSION, ParsedNseDailyBundle


def encode_daily_bundle(parsed: ParsedNseDailyBundle) -> bytes:
    if type(parsed) is not ParsedNseDailyBundle:
        raise TypeError("daily-bundle normalized payload requires an exact parsed bundle")
    value = {
        "codec_version": NSE_DAILY_BUNDLE_CODEC_VERSION,
        "original_filename": parsed.original_filename,
        "raw_sha256": parsed.raw_sha256,
        "byte_count": parsed.byte_count,
        "entries": [
            {
                "name": entry.name,
                "byte_count": entry.byte_count,
                "compressed_byte_count": entry.compressed_byte_count,
                "compression_method": entry.compression_method,
                "crc32": entry.crc32,
                "sha256": entry.sha256,
                "disposition": entry.disposition.value,
                "family": entry.family.value if entry.family is not None else None,
            }
            for entry in parsed.entries
        ],
        "reports": [
            {
                "source_entry_name": report.source_entry_name,
                "content_name": report.content_name,
                "family": report.family.value,
                "disposition": report.disposition.value,
                "claimed_report_date": (
                    report.claimed_report_date.isoformat()
                    if report.claimed_report_date is not None
                    else None
                ),
                "confirmed_row_dates": [
                    value.isoformat() for value in report.confirmed_row_dates
                ],
                "date_status": report.date_status.value,
                "date_role": report.date_role.value,
                "source_entry_sha256": report.source_entry_sha256,
                "content_sha256": report.content_sha256,
                "source_entry_byte_count": report.source_entry_byte_count,
                "content_byte_count": report.content_byte_count,
                "header": list(report.header),
                "header_sha256": report.header_sha256,
                "row_count": report.row_count,
                "ordered_row_digest": report.ordered_row_digest,
                "rows": [list(row) for row in report.rows],
            }
            for report in parsed.reports
        ],
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
