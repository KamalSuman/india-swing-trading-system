from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime

from india_swing.daily_reports.codec import encode_daily_bundle
from india_swing.daily_reports.artifact_store import (
    _artifact_identity as _daily_artifact_identity,
    _manifest_identity as _daily_manifest_identity,
    verify_stored_daily_bundle_provenance,
)
from india_swing.daily_reports.models import (
    BundleEntryDisposition,
    DailyReportFamily,
    DailyReportIntegrityError,
    ParsedDailyReport,
    ReportDateRole,
    StoredDailyBundleArtifact,
)
from india_swing.reference.calendar import CalendarIntegrityError, CalendarSnapshot
from india_swing.identity import content_id
from india_swing.calendar_evidence.policy import final_report_not_before
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.codec import encode_security_master
from india_swing.reference_data.artifact_store import (
    _artifact_identity as _reference_artifact_identity,
    _manifest_identity as _reference_manifest_identity,
    verify_stored_reference_provenance,
)
from india_swing.reference_data.models import (
    ReferenceArtifactIntegrityError,
    SourceRowDisposition,
    StoredReferenceArtifact,
)

from .models import (
    BandChangeObservation,
    BandObservation,
    CollectionReconciliationSnapshot,
    EffectiveSessionResolution,
    EvidenceRowRef,
    OrphanReportKey,
    ReconciledListingEvidence,
    ReconciliationDisposition,
    ReconciliationIntegrityError,
    ReconciliationScope,
    Reg1Observation,
    ReportBinding,
    SeriesChangeObservation,
)


_REG1_LONG_ASM = (
    "Long_Term_Additional_Surveillance_Measure (Long Term ASM)"
)
_REG1_SHORT_ASM = (
    "Short_Term_Additional_Surveillance_Measure (Short Term ASM)"
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _row_sha256(row: tuple[str, ...]) -> str:
    return _sha256(
        json.dumps(
            row,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _validate_security_master_artifact(artifact: StoredReferenceArtifact) -> None:
    if type(artifact) is not StoredReferenceArtifact:
        raise TypeError("security master must be an exact stored reference artifact")
    manifest = artifact.manifest
    parsed = artifact.parsed
    if manifest.readiness is not ReferenceReadiness.COLLECTION_ONLY or manifest.actionable:
        raise ReconciliationIntegrityError(
            "security-master input must remain collection-only"
        )
    if _sha256(artifact.raw_bytes) != manifest.raw_sha256:
        raise ReconciliationIntegrityError("security-master raw bytes fail their manifest")
    if parsed.raw_sha256 != manifest.raw_sha256:
        raise ReconciliationIntegrityError("security-master parsed hash disagrees with manifest")
    expected_normalized = encode_security_master(parsed)
    if artifact.normalized_bytes != expected_normalized:
        raise ReconciliationIntegrityError(
            "security-master normalized bytes fail deterministic re-encoding"
        )
    if _sha256(expected_normalized) != manifest.normalized_sha256:
        raise ReconciliationIntegrityError(
            "security-master normalized hash disagrees with manifest"
        )
    if manifest.artifact_id != artifact.path.name:
        raise ReconciliationIntegrityError(
            "security-master artifact path and manifest identity disagree"
        )
    if content_id(_reference_artifact_identity(manifest), length=64) != manifest.artifact_id:
        raise ReconciliationIntegrityError("security-master artifact ID is invalid")
    if content_id(_reference_manifest_identity(manifest), length=64) != manifest.manifest_id:
        raise ReconciliationIntegrityError("security-master manifest ID is invalid")
    try:
        verify_stored_reference_provenance(artifact)
    except ReferenceArtifactIntegrityError as exc:
        raise ReconciliationIntegrityError(
            "security master does not match sealed provenance"
        ) from exc


def _validate_daily_bundle_artifact(artifact: StoredDailyBundleArtifact) -> None:
    if type(artifact) is not StoredDailyBundleArtifact:
        raise TypeError("daily bundles must be exact stored daily-bundle artifacts")
    manifest = artifact.manifest
    parsed = artifact.parsed
    if manifest.readiness is not ReferenceReadiness.COLLECTION_ONLY or manifest.actionable:
        raise ReconciliationIntegrityError("daily-bundle input must remain collection-only")
    if _sha256(artifact.raw_bytes) != manifest.raw_sha256:
        raise ReconciliationIntegrityError("daily-bundle raw bytes fail their manifest")
    if parsed.raw_sha256 != manifest.raw_sha256:
        raise ReconciliationIntegrityError("daily-bundle parsed hash disagrees with manifest")
    expected_normalized = encode_daily_bundle(parsed)
    if artifact.normalized_bytes != expected_normalized:
        raise ReconciliationIntegrityError(
            "daily-bundle normalized bytes fail deterministic re-encoding"
        )
    if _sha256(expected_normalized) != manifest.normalized_sha256:
        raise ReconciliationIntegrityError(
            "daily-bundle normalized hash disagrees with manifest"
        )
    if manifest.artifact_id != artifact.path.name:
        raise ReconciliationIntegrityError(
            "daily-bundle artifact path and manifest identity disagree"
        )
    if content_id(_daily_artifact_identity(manifest), length=64) != manifest.artifact_id:
        raise ReconciliationIntegrityError("daily-bundle artifact ID is invalid")
    if content_id(_daily_manifest_identity(manifest), length=64) != manifest.manifest_id:
        raise ReconciliationIntegrityError("daily-bundle manifest ID is invalid")
    try:
        verify_stored_daily_bundle_provenance(artifact)
    except DailyReportIntegrityError as exc:
        raise ReconciliationIntegrityError(
            "daily bundle does not match sealed provenance"
        ) from exc


@dataclass(frozen=True, slots=True)
class _ReportSource:
    artifact: StoredDailyBundleArtifact
    report: ParsedDailyReport


def _report_group_key(source: _ReportSource) -> tuple[DailyReportFamily, date | None]:
    return (source.report.family, source.report.claimed_report_date)


def _canonical_reports(
    artifacts: tuple[StoredDailyBundleArtifact, ...],
) -> tuple[_ReportSource, ...]:
    grouped: dict[tuple[DailyReportFamily, date | None], list[_ReportSource]] = defaultdict(list)
    for artifact in artifacts:
        for report in artifact.parsed.reports:
            if report.disposition is BundleEntryDisposition.SELECTED_VALIDATED:
                grouped[_report_group_key(_ReportSource(artifact, report))].append(
                    _ReportSource(artifact, report)
                )

    selected: list[_ReportSource] = []
    for (family, claimed_date), candidates in grouped.items():
        content_hashes = {candidate.report.content_sha256 for candidate in candidates}
        if claimed_date is not None and len(content_hashes) != 1:
            date_label = claimed_date.isoformat() if claimed_date else "mutable"
            raise ReconciliationIntegrityError(
                f"conflicting {family.value} observations for {date_label}"
            )
        if claimed_date is None:
            # A mutable file is a sequence of positive observations, not a
            # replacement snapshot. Later absence must never erase a prior
            # event. Keep one earliest-known binding per distinct content.
            for content_hash in sorted(content_hashes):
                same_content = [
                    candidate
                    for candidate in candidates
                    if candidate.report.content_sha256 == content_hash
                ]
                same_content.sort(
                    key=lambda candidate: (
                        candidate.artifact.manifest.validated_at,
                        candidate.artifact.manifest.artifact_id,
                        candidate.report.source_entry_name,
                    )
                )
                selected.append(same_content[0])
        else:
            candidates.sort(
                key=lambda candidate: (
                    candidate.artifact.manifest.validated_at,
                    candidate.artifact.manifest.artifact_id,
                    candidate.report.source_entry_name,
                )
            )
            selected.append(candidates[0])
    return tuple(
        sorted(
            selected,
            key=lambda source: (
                source.report.family.value,
                source.report.claimed_report_date.isoformat()
                if source.report.claimed_report_date
                else "",
                source.report.source_entry_name,
                source.artifact.manifest.artifact_id,
            ),
        )
    )


def _binding_for(
    source: _ReportSource,
    calendar: CalendarSnapshot | None,
    blockers: set[str],
) -> ReportBinding:
    report = source.report
    effective_session: date | None
    resolution: EffectiveSessionResolution
    if report.date_role is ReportDateRole.TRADE_DATE:
        effective_session = report.claimed_report_date
        resolution = EffectiveSessionResolution.TRADE_DATE_CONFIRMED
    elif report.date_role is ReportDateRole.CLAIMED_EFFECTIVE_DATE:
        effective_session = report.claimed_report_date
        resolution = EffectiveSessionResolution.CLAIMED_EFFECTIVE_DATE_UNVERIFIED
    elif report.date_role is ReportDateRole.PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE:
        if calendar is None:
            effective_session = None
            resolution = EffectiveSessionResolution.UNRESOLVED_NO_CALENDAR
            blockers.add("PUBLICATION_EFFECTIVE_STATE_UNRESOLVED")
        else:
            assert report.claimed_report_date is not None
            try:
                calendar.require_session(report.claimed_report_date)
                effective_session = calendar.next_session(report.claimed_report_date).day
                resolution = (
                    EffectiveSessionResolution.CALENDAR_RESOLVED_FROM_PUBLICATION_CLAIM
                )
            except CalendarIntegrityError:
                effective_session = None
                resolution = EffectiveSessionResolution.UNRESOLVED_CALENDAR_COVERAGE
                blockers.add("PUBLICATION_EFFECTIVE_CALENDAR_COVERAGE_MISSING")
    elif report.date_role is ReportDateRole.INTERNAL_EFFECTIVE_DATES:
        effective_session = None
        resolution = EffectiveSessionResolution.INTERNAL_EFFECTIVE_DATES
    else:
        raise ReconciliationIntegrityError(
            f"selected report has unsupported date role {report.date_role.value}"
        )

    return ReportBinding(
        artifact_id=source.artifact.manifest.artifact_id,
        manifest_id=source.artifact.manifest.manifest_id,
        bundle_raw_sha256=source.artifact.manifest.raw_sha256,
        bundle_normalized_sha256=source.artifact.manifest.normalized_sha256,
        family=report.family,
        source_entry_name=report.source_entry_name,
        source_entry_sha256=report.source_entry_sha256,
        content_sha256=report.content_sha256,
        ordered_row_digest=report.ordered_row_digest,
        claimed_report_date=report.claimed_report_date,
        confirmed_row_dates=report.confirmed_row_dates,
        date_role=report.date_role,
        effective_session=effective_session,
        effective_session_resolution=resolution,
        first_seen_at=source.artifact.manifest.first_seen_at,
        validated_at=source.artifact.manifest.validated_at,
        row_count=report.row_count,
    )


def _field_map(report: ParsedDailyReport, row: tuple[str, ...]) -> dict[str, str]:
    # The legacy full Bhavcopy deliberately carries leading spaces in its
    # official header. Reconciliation uses normalized field names only for
    # lookup; original rows and hashes remain untouched.
    return {
        name.strip(): value.strip()
        for name, value in zip(report.header, row, strict=True)
    }


def _listing_keys(
    report: ParsedDailyReport,
    values: dict[str, str],
) -> tuple[tuple[str, str], ...]:
    if report.family is DailyReportFamily.UDIFF_BHAVCOPY:
        return ((values["TckrSymb"], values["SctySrs"]),)
    if report.family is DailyReportFamily.FULL_BHAVCOPY_DELIVERY:
        return ((values["SYMBOL"], values["SERIES"]),)
    if report.family is DailyReportFamily.SURVEILLANCE_REG1:
        return ((values["Symbol"], values["Series"]),)
    if report.family in {
        DailyReportFamily.COMPLETE_PRICE_BANDS,
        DailyReportFamily.SME_PRICE_BANDS,
        DailyReportFamily.PRICE_BAND_CHANGES,
    }:
        return ((values["Symbol"], values["Series"]),)
    if report.family is DailyReportFamily.SERIES_CHANGES:
        return (
            (values["Symbol"], values["From Series"]),
            (values["Symbol"], values["To Series"]),
        )
    raise ReconciliationIntegrityError(
        f"selected report family {report.family.value} has no listing-key policy"
    )


def _single_observation(
    values: list[object],
    *,
    description: str,
) -> object | None:
    if len(values) > 1:
        raise ReconciliationIntegrityError(f"ambiguous {description}")
    return values[0] if values else None


def reconcile_collection_only(
    *,
    security_master: StoredReferenceArtifact,
    daily_bundles: tuple[StoredDailyBundleArtifact, ...],
    market_session: date,
    cutoff: datetime,
    calendar: CalendarSnapshot | None = None,
) -> CollectionReconciliationSnapshot:
    """Reconcile archived NSE observations without promoting them to a universe.

    All supplied artifacts are explicit inputs. The function never searches for
    a newer file, infers weekdays/holidays, or treats a filename date as a
    knowledge timestamp. Publication-next-session reports remain candidate
    observations unless the caller supplies an exact calendar artifact.
    """

    if type(market_session) is not date:
        raise TypeError("market_session must be a date")
    _require_aware(cutoff, "cutoff")
    target_final_report_boundary = final_report_not_before(market_session)
    if cutoff < target_final_report_boundary:
        raise ReconciliationIntegrityError(
            "target session has not reached the conservative final-report boundary"
        )
    if type(daily_bundles) is not tuple or not daily_bundles:
        raise ValueError("at least one exact daily-bundle artifact is required")
    if any(type(bundle) is not StoredDailyBundleArtifact for bundle in daily_bundles):
        raise TypeError("daily_bundles must contain exact stored artifacts")
    if len({bundle.manifest.artifact_id for bundle in daily_bundles}) != len(daily_bundles):
        raise ReconciliationIntegrityError("duplicate daily-bundle artifact input")

    _validate_security_master_artifact(security_master)
    for bundle in daily_bundles:
        _validate_daily_bundle_artifact(bundle)
    if security_master.manifest.validated_at > cutoff:
        raise ReconciliationIntegrityError(
            "security master was not validated by the requested cutoff"
        )
    future_bundles = tuple(
        bundle.manifest.artifact_id
        for bundle in daily_bundles
        if bundle.manifest.validated_at > cutoff
    )
    if future_bundles:
        raise ReconciliationIntegrityError(
            "daily-bundle input was not validated by the requested cutoff"
        )

    blockers = {
        "COLLECTION_ONLY_INPUTS",
        "UNVERIFIED_MANUAL_ACQUISITION",
        "UNVERIFIED_SECURITY_MASTER_REPORT_DATE",
    }
    calendar_snapshot_id: str | None = None
    if calendar is None:
        blockers.add("EXPLICIT_CALENDAR_NOT_SUPPLIED")
    else:
        if type(calendar) is not CalendarSnapshot:
            raise TypeError("calendar must be an exact CalendarSnapshot")
        try:
            calendar.verify_content_identity()
            calendar.require_session(market_session)
        except CalendarIntegrityError as exc:
            raise ReconciliationIntegrityError(
                "calendar does not validly cover the target session"
            ) from exc
        if (calendar.exchange, calendar.segment) != ("NSE", "CM"):
            raise ReconciliationIntegrityError("calendar scope is not NSE cash market")
        if calendar.cutoff > cutoff:
            raise ReconciliationIntegrityError("calendar vintage follows the requested cutoff")
        calendar_snapshot_id = calendar.snapshot_id
        if calendar.readiness is not ReferenceReadiness.POINT_IN_TIME_VERIFIED:
            blockers.add("CALENDAR_NOT_POINT_IN_TIME_VERIFIED")

    if security_master.parsed.claimed_report_date != market_session:
        raise ReconciliationIntegrityError(
            "security-master claimed date must equal the target session for a same-vintage join"
        )

    canonical_sources = _canonical_reports(daily_bundles)
    source_bindings: list[tuple[_ReportSource, ReportBinding]] = []
    for source in canonical_sources:
        if (
            source.report.family
            in {
                DailyReportFamily.UDIFF_BHAVCOPY,
                DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
            }
        ):
            assert source.report.claimed_report_date is not None
            report_boundary = final_report_not_before(
                source.report.claimed_report_date
            )
            if (
                source.artifact.manifest.validated_at < report_boundary
                or cutoff < report_boundary
            ):
                raise ReconciliationIntegrityError(
                    "final reports were available before their conservative event boundary"
                )
        source_bindings.append((source, _binding_for(source, calendar, blockers)))

    retained_records = tuple(
        record
        for record in security_master.parsed.records
        if record.disposition is SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
    )
    if not retained_records:
        raise ReconciliationIntegrityError("security master has no retained equity rows")
    master_by_key = {
        (record.ticker_symbol, record.security_series): record
        for record in retained_records
    }
    if len(master_by_key) != len(retained_records):
        raise ReconciliationIntegrityError("retained master listing keys are not unique")
    all_master_records = security_master.parsed.records
    all_master_by_key = {
        (record.ticker_symbol, record.security_series): record
        for record in all_master_records
    }
    if len(all_master_by_key) != len(all_master_records):
        raise ReconciliationIntegrityError("security-master listing keys are not unique")
    master_key_by_instrument_id = {
        str(record.financial_instrument_id): (
            record.ticker_symbol,
            record.security_series,
        )
        for record in all_master_records
    }
    if len(master_key_by_instrument_id) != len(all_master_records):
        raise ReconciliationIntegrityError(
            "security-master financial instrument IDs are not unique"
        )
    master_keys_by_source_identifier: dict[
        str, set[tuple[str, str]]
    ] = defaultdict(set)
    for record in all_master_records:
        master_keys_by_source_identifier[record.raw_source_identifier].add(
            (record.ticker_symbol, record.security_series)
        )

    reg1_by_key: dict[tuple[str, str], list[Reg1Observation]] = defaultdict(list)
    complete_bands_by_key: dict[tuple[str, str], list[BandObservation]] = defaultdict(list)
    sme_bands_by_key: dict[tuple[str, str], list[BandObservation]] = defaultdict(list)
    udiff_target: dict[tuple[str, str], EvidenceRowRef] = {}
    full_target: dict[tuple[str, str], EvidenceRowRef] = {}
    band_changes_target: dict[
        tuple[str, str], list[BandChangeObservation]
    ] = defaultdict(list)
    series_change_candidates: dict[
        tuple[str, date], list[SeriesChangeObservation]
    ] = defaultdict(list)
    orphan_keys: list[OrphanReportKey] = []

    for source, binding in source_bindings:
        report = source.report
        for row_number, row in enumerate(report.rows, start=2):
            values = _field_map(report, row)
            keys = _listing_keys(report, values)
            row_ref = EvidenceRowRef(
                binding_id=binding.binding_id,
                family=report.family,
                source_row_number=row_number,
                row_sha256=_row_sha256(row),
                listing_keys=tuple(sorted(set(keys))),
            )
            for symbol, series in keys:
                if (symbol, series) not in master_by_key:
                    orphan_keys.append(
                        OrphanReportKey(
                            family=report.family,
                            claimed_date=report.claimed_report_date,
                            symbol=symbol,
                            series=series,
                            row_ref=row_ref,
                        )
                    )

            primary_key = keys[0]
            if report.family is DailyReportFamily.SURVEILLANCE_REG1:
                assert report.claimed_report_date is not None
                indicator_codes = tuple(
                    sorted(
                        (name, values[name])
                        for name in values
                        if name not in {
                            "ScripCode",
                            "Symbol",
                            "Nse Exclusive",
                            "Status",
                            "Series",
                        }
                        and not name.startswith("Filler")
                    )
                )
                reg1_by_key[primary_key].append(
                    Reg1Observation(
                        row_ref=row_ref,
                        publication_date_claim=report.claimed_report_date,
                        effective_session=binding.effective_session,
                        status=values["Status"],
                        nse_exclusive=values["Nse Exclusive"],
                        gsm_code=values["GSM"],
                        long_term_asm_code=values[_REG1_LONG_ASM],
                        short_term_asm_code=values[_REG1_SHORT_ASM],
                        esm_code=values["ESM"],
                        indicator_codes=indicator_codes,
                    )
                )
            elif report.family in {
                DailyReportFamily.COMPLETE_PRICE_BANDS,
                DailyReportFamily.SME_PRICE_BANDS,
            }:
                assert report.claimed_report_date is not None
                observation = BandObservation(
                    row_ref=row_ref,
                    claimed_date=report.claimed_report_date,
                    effective_session=binding.effective_session,
                    band=values["Band"],
                )
                target = (
                    complete_bands_by_key
                    if report.family is DailyReportFamily.COMPLETE_PRICE_BANDS
                    else sme_bands_by_key
                )
                target[primary_key].append(observation)
            elif (
                report.family is DailyReportFamily.UDIFF_BHAVCOPY
                and report.claimed_report_date == market_session
            ):
                if primary_key in udiff_target:
                    raise ReconciliationIntegrityError("duplicate target-session UDiFF key")
                instrument_master_key = master_key_by_instrument_id.get(
                    values["FinInstrmId"]
                )
                if (
                    instrument_master_key is not None
                    and instrument_master_key != primary_key
                ):
                    raise ReconciliationIntegrityError(
                        "target UDiFF instrument ID maps to a different master listing key"
                    )
                source_identifier_keys = master_keys_by_source_identifier.get(
                    values["ISIN"],
                    set(),
                )
                if (
                    len(source_identifier_keys) == 1
                    and primary_key not in source_identifier_keys
                ):
                    raise ReconciliationIntegrityError(
                        "target UDiFF ISIN maps to a different unique master listing key"
                    )
                master_record = all_master_by_key.get(primary_key)
                if master_record is not None and (
                    str(master_record.financial_instrument_id)
                    != values["FinInstrmId"]
                    or master_record.raw_source_identifier != values["ISIN"]
                    or master_record.instrument_name != values["FinInstrmNm"]
                    or str(master_record.board_lot_quantity)
                    != values["NewBrdLotQty"]
                ):
                    raise ReconciliationIntegrityError(
                        "same-vintage security master and UDiFF identity or board-lot fields contradict"
                    )
                udiff_target[primary_key] = row_ref
            elif (
                report.family is DailyReportFamily.FULL_BHAVCOPY_DELIVERY
                and report.claimed_report_date == market_session
            ):
                if primary_key in full_target:
                    raise ReconciliationIntegrityError(
                        "duplicate target-session full-Bhavcopy key"
                    )
                full_target[primary_key] = row_ref
            elif (
                report.family is DailyReportFamily.PRICE_BAND_CHANGES
                and report.claimed_report_date == market_session
            ):
                band_changes_target[primary_key].append(
                    BandChangeObservation(
                        row_ref=row_ref,
                        claimed_effective_date=report.claimed_report_date,
                        from_band=values["From"],
                        to_band=values["To"],
                    )
                )
            elif report.family is DailyReportFamily.SERIES_CHANGES:
                effective_date = datetime.strptime(
                    values["Change Date"], "%d-%b-%Y"
                ).date()
                if effective_date == market_session:
                    observation = SeriesChangeObservation(
                        row_ref=row_ref,
                        symbol=values["Symbol"],
                        from_series=values["From Series"],
                        to_series=values["To Series"],
                        effective_date=effective_date,
                    )
                    series_change_candidates[
                        (observation.symbol, observation.effective_date)
                    ].append(observation)

    binding_by_id = {
        binding.binding_id: binding
        for _, binding in source_bindings
    }
    deduplicated_orphans: dict[
        tuple[DailyReportFamily, date | None, str, str, str],
        OrphanReportKey,
    ] = {}
    for orphan in orphan_keys:
        semantic_key = (
            orphan.family,
            orphan.claimed_date,
            orphan.symbol,
            orphan.series,
            orphan.row_ref.row_sha256,
        )
        current = deduplicated_orphans.get(semantic_key)
        if current is None or (
            binding_by_id[orphan.row_ref.binding_id].validated_at,
            orphan.row_ref.binding_id,
            orphan.row_ref.source_row_number,
        ) < (
            binding_by_id[current.row_ref.binding_id].validated_at,
            current.row_ref.binding_id,
            current.row_ref.source_row_number,
        ):
            deduplicated_orphans[semantic_key] = orphan
    orphan_keys = list(deduplicated_orphans.values())

    series_changes_target: dict[
        tuple[str, str], list[SeriesChangeObservation]
    ] = defaultdict(list)
    for claim_key, observations in series_change_candidates.items():
        transitions = {
            (value.from_series, value.to_series)
            for value in observations
        }
        if len(transitions) != 1:
            raise ReconciliationIntegrityError(
                "mutable series-change vintages contain contradictory transitions "
                f"for {claim_key[0]} on {claim_key[1].isoformat()}"
            )
        earliest = min(
            observations,
            key=lambda value: (
                binding_by_id[value.row_ref.binding_id].validated_at,
                value.row_ref.binding_id,
                value.row_ref.source_row_number,
                value.row_ref.row_sha256,
            ),
        )
        for key in (
            (earliest.symbol, earliest.from_series),
            (earliest.symbol, earliest.to_series),
        ):
            series_changes_target[key].append(earliest)

    if not udiff_target:
        blockers.add("TARGET_SESSION_UDIFF_MISSING")
    if not full_target:
        blockers.add("TARGET_SESSION_FULL_DELIVERY_MISSING")

    effective_reg1_count = sum(
        observation.effective_session == market_session
        for observations in reg1_by_key.values()
        for observation in observations
    )
    effective_complete_band_count = sum(
        observation.effective_session == market_session
        for observations in complete_bands_by_key.values()
        for observation in observations
    )
    if effective_reg1_count == 0:
        blockers.add("EFFECTIVE_REG1_STATE_MISSING")
    if effective_complete_band_count == 0:
        blockers.add("EFFECTIVE_COMPLETE_BAND_STATE_MISSING")

    entries: list[ReconciledListingEvidence] = []
    for record in retained_records:
        key = (record.ticker_symbol, record.security_series)
        reg1_observations = tuple(
            sorted(
                reg1_by_key.get(key, ()),
                key=lambda value: (
                    value.publication_date_claim,
                    value.row_ref.binding_id,
                    value.row_ref.source_row_number,
                ),
            )
        )
        complete_observations = tuple(
            sorted(
                complete_bands_by_key.get(key, ()),
                key=lambda value: (
                    value.claimed_date,
                    value.row_ref.binding_id,
                    value.row_ref.source_row_number,
                ),
            )
        )
        effective_reg1 = _single_observation(
            [
                value
                for value in reg1_observations
                if value.effective_session == market_session
            ],
            description=f"effective REG1 state for {key[0]}:{key[1]}",
        )
        effective_complete = _single_observation(
            [
                value
                for value in complete_observations
                if value.effective_session == market_session
            ],
            description=f"effective complete-band state for {key[0]}:{key[1]}",
        )
        target_sme = _single_observation(
            [
                value
                for value in sme_bands_by_key.get(key, ())
                if value.effective_session == market_session
            ],
            description=f"target SME band for {key[0]}:{key[1]}",
        )
        assert effective_reg1 is None or type(effective_reg1) is Reg1Observation
        assert effective_complete is None or type(effective_complete) is BandObservation
        assert target_sme is None or type(target_sme) is BandObservation
        if (
            target_sme is not None
            and effective_complete is not None
            and target_sme.band != effective_complete.band
        ):
            raise ReconciliationIntegrityError(
                f"SME and complete price bands contradict for {key[0]}:{key[1]}"
            )
        target_band_changes = tuple(
            sorted(
                band_changes_target.get(key, ()),
                key=lambda value: (
                    value.claimed_effective_date,
                    value.row_ref.binding_id,
                    value.row_ref.source_row_number,
                    value.row_ref.row_sha256,
                ),
            )
        )
        if effective_complete is not None and any(
            value.to_band != effective_complete.band
            for value in target_band_changes
        ):
            raise ReconciliationIntegrityError(
                f"target band change contradicts effective complete band for {key[0]}:{key[1]}"
            )

        reasons = {"UNVERIFIED_SOURCE_PROVENANCE"}
        if record.security_series == "EQ":
            scope = ReconciliationScope.MAIN_EQ
            if effective_reg1 is None:
                reasons.add("EFFECTIVE_REG1_ROW_MISSING")
            if effective_complete is None:
                reasons.add("EFFECTIVE_COMPLETE_BAND_ROW_MISSING")
            disposition = (
                ReconciliationDisposition.UNVERIFIED_MAIN_SCOPE
                if effective_reg1 is not None and effective_complete is not None
                else ReconciliationDisposition.UNRESOLVED
            )
        elif record.security_series == "SM":
            scope = ReconciliationScope.SME_SM
            if effective_reg1 is None:
                reasons.add("EFFECTIVE_REG1_ROW_MISSING")
            if effective_complete is None:
                reasons.add("EFFECTIVE_COMPLETE_BAND_ROW_MISSING")
            if target_sme is None:
                reasons.add("TARGET_SME_BAND_ROW_MISSING")
            disposition = (
                ReconciliationDisposition.UNVERIFIED_SME_WATCH_SCOPE
                if (
                    effective_reg1 is not None
                    and effective_complete is not None
                    and target_sme is not None
                )
                else ReconciliationDisposition.UNRESOLVED
            )
        else:
            scope = ReconciliationScope.UNSUPPORTED_SERIES
            disposition = ReconciliationDisposition.EXCLUDED_UNSUPPORTED_SERIES
            reasons.add("SERIES_OUTSIDE_PINNED_SCOPE")
        if effective_reg1 is not None and effective_reg1.status != "A":
            reasons.add("REG1_STATUS_NOT_ACTIVE")
        if record.delete_flag == "Y":
            reasons.add("MASTER_DELETE_FLAG_SET")

        entries.append(
            ReconciledListingEvidence(
                source_record_id=record.source_record_id,
                master_row_sha256=record.normalized_row_sha256,
                symbol=record.ticker_symbol,
                series=record.security_series,
                financial_instrument_id=record.financial_instrument_id,
                validated_isin=record.validated_isin,
                scope=scope,
                disposition=disposition,
                reason_codes=tuple(sorted(reasons)),
                reg1_observations=reg1_observations,
                effective_reg1=effective_reg1,
                complete_band_observations=complete_observations,
                effective_complete_band=effective_complete,
                target_sme_band=target_sme,
                udiff_trade_row=udiff_target.get(key),
                full_delivery_row=full_target.get(key),
                target_band_changes=target_band_changes,
                relevant_series_changes=tuple(
                    sorted(
                        series_changes_target.get(key, ()),
                        key=lambda value: (
                            value.effective_date,
                            value.row_ref.binding_id,
                            value.row_ref.source_row_number,
                        ),
                    )
                ),
            )
        )

    entries.sort(key=lambda entry: entry.source_record_id)
    bindings = tuple(
        sorted(
            (binding for _, binding in source_bindings),
            key=lambda binding: (
                binding.family.value,
                binding.claimed_report_date.isoformat()
                if binding.claimed_report_date
                else "",
                binding.source_entry_name,
                binding.binding_id,
            ),
        )
    )
    orphan_keys.sort(
        key=lambda orphan: (
            orphan.family.value,
            orphan.claimed_date.isoformat() if orphan.claimed_date else "",
            orphan.symbol,
            orphan.series,
            orphan.row_ref.row_sha256,
        )
    )
    return CollectionReconciliationSnapshot(
        exchange="NSE",
        segment="CM",
        market_session=market_session,
        cutoff=cutoff,
        calendar_snapshot_id=calendar_snapshot_id,
        security_master_manifest=security_master.manifest,
        daily_bundle_manifests=tuple(
            sorted(
                (bundle.manifest for bundle in daily_bundles),
                key=lambda value: (value.artifact_id, value.manifest_id),
            )
        ),
        report_bindings=bindings,
        retained_source_row_ids=tuple(entry.source_record_id for entry in entries),
        entries=tuple(entries),
        orphan_report_keys=tuple(orphan_keys),
        global_reason_codes=tuple(sorted(blockers)),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        actionable=False,
    )
