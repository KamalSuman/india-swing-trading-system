from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Protocol

from india_swing.market_data.kite import (
    KiteAuthenticationError,
    KiteAvailabilityError,
    KiteDataIntegrityError,
    KiteDependencyError,
    KiteMarketDataError,
    KitePermissionError,
    KiteRateLimitError,
    KiteRequestError,
    MarketSessionNotFinalError,
)
from india_swing.market_data.models import (
    DailyCandleBatch,
    FullQuoteBatch,
    InstrumentBatch,
    NseSessionFinality,
)
from india_swing.market_data.provider import (
    HistoricalEmptyProviderResponseError,
    HistoricalProviderRequestRejectedError,
)

from .canonical_response import (
    EXCHANGE_NSE,
    EndpointFamily,
    ObservationRequestIdentity,
    QualityPilotObservation,
    ResponseClassification,
)
from .capture_runner import (
    QualityPilotCaptureSpec,
    QualityPilotCollectionResult,
)


class QualityPilotKiteCollectorError(ValueError):
    """A Kite quality-pilot bridge invariant failed without exposing upstream data."""


def _fail(message: str) -> None:
    raise QualityPilotKiteCollectorError(message)


class KiteQualityPilotReadAdapter(Protocol):
    """Exact injected read surface required by the quality-only pilot.

    ``maximum_attempts`` must be one. The quality pilot records one immutable
    outcome for one scheduled request and therefore cannot use the production
    adapter's normal transparent retry policy.
    """

    @property
    def provider(self) -> str: ...

    @property
    def provider_version(self) -> str: ...

    @property
    def maximum_attempts(self) -> int: ...

    def fetch_instruments(self, exchange: str = "NSE") -> InstrumentBatch: ...

    def fetch_full_quotes(self, listing_keys: tuple[str, ...]) -> FullQuoteBatch: ...

    def fetch_daily_candle(
        self,
        instrument_token: int,
        session: date,
        *,
        session_finality: NseSessionFinality,
    ) -> DailyCandleBatch: ...


_EXPECTED_OPERATION = {
    EndpointFamily.CATALOG: "instruments",
    EndpointFamily.FULL_QUOTE: "quote",
    EndpointFamily.DAILY_OHLCV: "historical_data",
}

_CATALOG_GAP_INTEGRITY_TYPES = frozenset(
    {"EmptyInstrumentDump", "EmptyEligibleInstrumentDump"}
)
_QUOTE_GAP_INTEGRITY_TYPES = frozenset({"EmptyQuoteResponse"})
_QUOTE_PARTIAL_INTEGRITY_TYPES = frozenset({"IncompleteQuoteCoverage"})


def _reconstruct_spec(value: object) -> QualityPilotCaptureSpec:
    if type(value) is not QualityPilotCaptureSpec:
        _fail("capture spec type is invalid")
    failed = False
    reconstructed: QualityPilotCaptureSpec | None = None
    try:
        reconstructed = QualityPilotCaptureSpec(
            campaign=value.campaign,
            window=value.window,
            provider=value.provider,
            provider_version=value.provider_version,
            requested_keys=value.requested_keys,
            provider_instrument_token=value.provider_instrument_token,
            chunk_index=value.chunk_index,
            chunk_count=value.chunk_count,
            protocol_sha256=value.protocol_sha256,
        )
    except Exception:
        failed = True
    if (
        failed
        or reconstructed is None
        or value.capture_spec_id != reconstructed.capture_spec_id
    ):
        _fail("capture spec failed independent verification")
    return reconstructed


def _read_clock(clock: Callable[[], datetime]) -> datetime:
    failed = False
    value: object = None
    aware = False
    try:
        value = clock()
        aware = (
            type(value) is datetime
            and value.tzinfo is not None
            and value.utcoffset() is not None
        )
    except Exception:
        failed = True
    if failed or not aware:
        _fail("collector clock did not return an aware exact datetime")
    return value  # type: ignore[return-value]


def _inside_interval(value: object, start: datetime, end: datetime) -> bool:
    valid = False
    try:
        valid = (
            type(value) is datetime
            and value.tzinfo is not None
            and value.utcoffset() is not None
            and start <= value <= end
        )
    except Exception:
        valid = False
    return valid


def _request_interval_is_valid(
    spec: QualityPilotCaptureSpec, start: datetime, end: datetime
) -> bool:
    valid = False
    try:
        valid = (
            start <= end
            and spec.window.opens_at <= start
            and end <= spec.window.closes_at
        )
    except Exception:
        valid = False
    return valid


def _success_payload_timestamp_is_valid(
    payload: object, start: datetime, end: datetime
) -> bool:
    if type(payload) is InstrumentBatch:
        return _inside_interval(payload.observed_at, start, end)
    if type(payload) is FullQuoteBatch:
        return _inside_interval(payload.requested_at, start, end) and _inside_interval(
            payload.observed_at, start, end
        )
    if type(payload) is DailyCandleBatch:
        return _inside_interval(payload.observed_at, start, end)
    return False


def _success_payload_has_expected_shape(
    spec: QualityPilotCaptureSpec, payload: object
) -> bool:
    expected_type = {
        EndpointFamily.CATALOG: InstrumentBatch,
        EndpointFamily.FULL_QUOTE: FullQuoteBatch,
        EndpointFamily.DAILY_OHLCV: DailyCandleBatch,
    }[spec.window.endpoint_family]
    if type(payload) is not expected_type:
        return False
    if spec.window.endpoint_family is not EndpointFamily.DAILY_OHLCV:
        return True
    valid_token = False
    try:
        valid_token = (
            type(payload.instrument_token) is int
            and payload.instrument_token == spec.provider_instrument_token
        )
    except Exception:
        valid_token = False
    return valid_token


def _success_payload_is_canonical(
    spec: QualityPilotCaptureSpec,
    payload: object,
    start: datetime,
    end: datetime,
) -> bool:
    if not _success_payload_has_expected_shape(spec, payload):
        return False

    failed = False
    try:
        request = ObservationRequestIdentity(
            provider=spec.provider,
            provider_version=spec.provider_version,
            endpoint_family=spec.window.endpoint_family,
            exchange=EXCHANGE_NSE,
            window_id=spec.window.window_id,
            requested_session=spec.window.market_session,
            requested_keys=spec.requested_keys,
            requested_range_start=(
                spec.window.market_session
                if spec.window.endpoint_family is EndpointFamily.DAILY_OHLCV
                else None
            ),
            requested_range_end=(
                spec.window.market_session
                if spec.window.endpoint_family is EndpointFamily.DAILY_OHLCV
                else None
            ),
            request_started_at=start,
            request_ended_at=end,
            chunk_index=spec.chunk_index,
            chunk_count=spec.chunk_count,
            response_classification=ResponseClassification.SUCCESS,
            protocol_sha256=spec.protocol_sha256,
        )
        QualityPilotObservation(
            window=spec.window,
            request=request,
            payload=payload,
            corrects_observation_id=None,
        )
    except Exception:
        failed = True
    return not failed


def _historical_error_classification(
    error: Exception,
    spec: QualityPilotCaptureSpec,
    start: datetime,
    end: datetime,
) -> ResponseClassification:
    if spec.window.endpoint_family is not EndpointFamily.DAILY_OHLCV:
        return ResponseClassification.CANONICALIZATION_FAILURE
    valid = False
    observed_at: object = None
    try:
        valid = (
            error.provider == spec.provider
            and error.provider_version == spec.provider_version
            and error.provider_instrument_id == str(spec.provider_instrument_token)
            and error.session == spec.window.market_session
        )
        observed_at = error.observed_at
    except Exception:
        valid = False
    if not valid:
        return ResponseClassification.CANONICALIZATION_FAILURE
    if not _inside_interval(observed_at, start, end):
        return ResponseClassification.TIMESTAMP_VIOLATION
    if type(error) is HistoricalEmptyProviderResponseError:
        return ResponseClassification.PROVIDER_GAP
    return ResponseClassification.REQUEST_REJECTED


def _kite_error_classification(
    error: Exception, spec: QualityPilotCaptureSpec
) -> ResponseClassification | None:
    family = spec.window.endpoint_family
    if type(error) is MarketSessionNotFinalError:
        return (
            ResponseClassification.TIMESTAMP_VIOLATION
            if family is EndpointFamily.DAILY_OHLCV
            else ResponseClassification.CANONICALIZATION_FAILURE
        )
    if type(error) is KiteDependencyError:
        return None
    if not isinstance(error, KiteMarketDataError):
        return None
    if type(error) not in {
        KiteAuthenticationError,
        KitePermissionError,
        KiteRateLimitError,
        KiteRequestError,
        KiteAvailabilityError,
        KiteDataIntegrityError,
    }:
        return None

    valid_operation = False
    upstream_type: object = None
    try:
        valid_operation = error.operation == _EXPECTED_OPERATION[family]
        upstream_type = error.upstream_type
    except Exception:
        valid_operation = False
    if not valid_operation or type(upstream_type) is not str:
        return ResponseClassification.CANONICALIZATION_FAILURE
    if type(error) is KiteRateLimitError:
        return ResponseClassification.RATE_LIMITED
    if type(error) in {KiteAuthenticationError, KitePermissionError, KiteRequestError}:
        return ResponseClassification.REQUEST_REJECTED
    if type(error) is KiteAvailabilityError:
        return (
            ResponseClassification.CATALOG_GAP
            if family is EndpointFamily.CATALOG
            else ResponseClassification.PROVIDER_GAP
        )
    if family is EndpointFamily.CATALOG and upstream_type in _CATALOG_GAP_INTEGRITY_TYPES:
        return ResponseClassification.CATALOG_GAP
    if family is EndpointFamily.FULL_QUOTE:
        if upstream_type in _QUOTE_PARTIAL_INTEGRITY_TYPES:
            return ResponseClassification.PARTIAL_KEY_COVERAGE
        if upstream_type in _QUOTE_GAP_INTEGRITY_TYPES:
            return ResponseClassification.PROVIDER_GAP
    return ResponseClassification.CANONICALIZATION_FAILURE


class KiteQualityPilotCollector:
    """Convert one pinned capture spec into one canonical collection result.

    The bridge owns no credentials, SDK construction, storage, scheduler,
    sleep, retry, trading, notification, or clock fallback. The caller must
    inject a read adapter configured for exactly one attempt and an aware
    clock. Known terminal provider outcomes become immutable classifications;
    capability defects and unknown exceptions fail closed.
    """

    def __init__(
        self,
        adapter: KiteQualityPilotReadAdapter,
        *,
        clock: Callable[[], datetime],
    ) -> None:
        if adapter is None:
            _fail("Kite adapter is required")
        if not callable(clock):
            _fail("collector clock is required")
        self._adapter = adapter
        self._clock = clock

    def collect(self, spec: QualityPilotCaptureSpec) -> QualityPilotCollectionResult:
        verified_spec = _reconstruct_spec(spec)
        metadata_failed = False
        provider: object = None
        provider_version: object = None
        maximum_attempts: object = None
        try:
            provider = self._adapter.provider
            provider_version = self._adapter.provider_version
            maximum_attempts = self._adapter.maximum_attempts
        except Exception:
            metadata_failed = True
        if metadata_failed:
            _fail("Kite adapter metadata could not be verified")
        identity_valid = False
        try:
            identity_valid = (
                type(provider) is str
                and provider == verified_spec.provider
                and type(provider_version) is str
                and provider_version == verified_spec.provider_version
            )
        except Exception:
            identity_valid = False
        if not identity_valid:
            _fail("Kite adapter identity disagrees with the capture spec")
        if type(maximum_attempts) is not int or maximum_attempts != 1:
            _fail("Kite adapter must use exactly one request attempt")

        started_at = _read_clock(self._clock)
        if not _request_interval_is_valid(verified_spec, started_at, started_at):
            _fail("capture did not start inside its scheduled window")

        payload: object = None
        caught_error: Exception | None = None
        try:
            family = verified_spec.window.endpoint_family
            if family is EndpointFamily.CATALOG:
                payload = self._adapter.fetch_instruments(EXCHANGE_NSE)
            elif family is EndpointFamily.FULL_QUOTE:
                payload = self._adapter.fetch_full_quotes(verified_spec.requested_keys)
            else:
                payload = self._adapter.fetch_daily_candle(
                    verified_spec.provider_instrument_token,  # type: ignore[arg-type]
                    verified_spec.window.market_session,
                    session_finality=NseSessionFinality.regular_collection_guard(
                        verified_spec.window.market_session
                    ),
                )
        except Exception as error:
            caught_error = error

        ended_at = _read_clock(self._clock)
        if not _request_interval_is_valid(verified_spec, started_at, ended_at):
            _fail("capture did not complete inside its scheduled window")

        classification: ResponseClassification
        result_payload: InstrumentBatch | FullQuoteBatch | DailyCandleBatch | None
        if caught_error is None:
            if not _success_payload_has_expected_shape(verified_spec, payload):
                classification = ResponseClassification.CANONICALIZATION_FAILURE
                result_payload = None
            elif not _success_payload_timestamp_is_valid(payload, started_at, ended_at):
                classification = ResponseClassification.TIMESTAMP_VIOLATION
                result_payload = None
            elif not _success_payload_is_canonical(
                verified_spec, payload, started_at, ended_at
            ):
                classification = ResponseClassification.CANONICALIZATION_FAILURE
                result_payload = None
            else:
                classification = ResponseClassification.SUCCESS
                result_payload = payload  # type: ignore[assignment]
        elif type(caught_error) in {
            HistoricalEmptyProviderResponseError,
            HistoricalProviderRequestRejectedError,
        }:
            classification = _historical_error_classification(
                caught_error, verified_spec, started_at, ended_at
            )
            result_payload = None
        else:
            mapped = _kite_error_classification(caught_error, verified_spec)
            if mapped is None:
                _fail("Kite adapter failed without a classifiable provider outcome")
            classification = mapped
            result_payload = None

        construction_failed = False
        result: QualityPilotCollectionResult | None = None
        try:
            result = QualityPilotCollectionResult(
                request_started_at=started_at,
                request_ended_at=ended_at,
                response_classification=classification,
                payload=result_payload,
            )
        except Exception:
            construction_failed = True
        if construction_failed or result is None:
            _fail("quality-pilot collection result could not be constructed")
        return result
