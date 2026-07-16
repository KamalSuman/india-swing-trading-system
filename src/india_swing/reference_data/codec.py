from __future__ import annotations

import json

from .models import (
    REFERENCE_NORMALIZED_CODEC_VERSION,
    ParsedNseCmSecurityMaster,
)


def encode_security_master(master: ParsedNseCmSecurityMaster) -> bytes:
    if type(master) is not ParsedNseCmSecurityMaster:
        raise TypeError("normalized payload must be an exact parsed security master")
    payload = {
        "codec_version": REFERENCE_NORMALIZED_CODEC_VERSION,
        "source_schema_version": master.source_schema_version,
        "original_filename": master.original_filename,
        "claimed_report_date": master.claimed_report_date.isoformat(),
        "header": list(master.header),
        "header_sha256": master.header_sha256,
        "raw_sha256": master.raw_sha256,
        "uncompressed_sha256": master.uncompressed_sha256,
        "compressed_byte_count": master.compressed_byte_count,
        "uncompressed_byte_count": master.uncompressed_byte_count,
        "record_count": len(master.records),
        "ordered_row_digest": master.ordered_row_digest,
        "disposition_counts": {
            "retained_unverified_equity": master.retained_unverified_equity_count,
            "excluded_non_equity": master.excluded_non_equity_count,
            "excluded_test_security": master.excluded_test_security_count,
            "excluded_alternative_venue": master.excluded_alternative_venue_count,
        },
        "records": [
            {
                "source_row_number": record.source_row_number,
                "source_record_id": record.source_record_id,
                "normalized_row_sha256": record.normalized_row_sha256,
                "financial_instrument_id": record.financial_instrument_id,
                "ticker_symbol": record.ticker_symbol,
                "security_series": record.security_series,
                "instrument_name": record.instrument_name,
                "raw_source_identifier": record.raw_source_identifier,
                "validated_isin": record.validated_isin,
                "board_lot_quantity": record.board_lot_quantity,
                "security_type_flag": record.security_type_flag,
                "bid_interval_paise": record.bid_interval_paise,
                "call_auction_indicator": record.call_auction_indicator,
                "permitted_to_trade": record.permitted_to_trade,
                "market_eligibility": [
                    {"status": value.status, "eligible": value.eligible}
                    for value in record.market_eligibility
                ],
                "listing_timestamp": record.listing_timestamp,
                "removal_timestamp": record.removal_timestamp,
                "readmission_timestamp": record.readmission_timestamp,
                "delete_flag": record.delete_flag,
                "disposition": record.disposition.value,
                "raw_fields": list(record.raw_fields),
            }
            for record in master.records
        ],
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
