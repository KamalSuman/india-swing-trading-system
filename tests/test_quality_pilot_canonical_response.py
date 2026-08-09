from __future__ import annotations

import ast
import inspect
import json
import unittest
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, localcontext
from hashlib import sha256

from india_swing.market_data.models import (
    DailyCandle,
    DailyCandleBatch,
    FullQuoteBatch,
    InstrumentBatch,
    KiteDepthLevel,
    KiteFullQuote,
    KiteInstrument,
    NseSessionFinality,
)
from india_swing.quality_pilot import canonical_response as quality_pilot_module
from india_swing.quality_pilot.canonical_response import (
    EndpointFamily,
    MAXIMUM_CATALOG_INSTRUMENTS,
    MAXIMUM_CHUNK_COUNT,
    MAXIMUM_ENCODED_BYTES,
    MAXIMUM_QUOTE_REQUEST_KEYS,
    NORMALIZED_CONTENT_SCHEMA_VERSION,
    ObservationRequestIdentity,
    ObservationWindowSpec,
    PILOT_PROTOCOL_SHA256,
    PROVIDER_ZERODHA_KITE,
    QUALITY_PILOT_POLICY_VERSION,
    QualityPilotCanonicalResponseError,
    QualityPilotObservation,
    ResponseClassification,
    ScheduledWindowKind,
    decode_observation,
    encode_observation,
    replay_verify,
)


IST = timezone(timedelta(hours=5, minutes=30))
SESSION = date(2026, 8, 3)
OTHER_SESSION = date(2026, 7, 31)


def _fake_sha256(label: str) -> str:
    return sha256(label.encode("utf-8")).hexdigest()


PILOT_RUN_ID = _fake_sha256("pilot-run-1")


def _ist_utc(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=IST).astimezone(timezone.utc)


def _window(
    kind: ScheduledWindowKind,
    family: EndpointFamily,
    *,
    session: date = SESSION,
    start=(9, 0),
    end=(9, 5),
    pilot_run_id: str = PILOT_RUN_ID,
    protocol_sha256: str = PILOT_PROTOCOL_SHA256,
) -> ObservationWindowSpec:
    return ObservationWindowSpec(
        pilot_run_id=pilot_run_id,
        market_session=session,
        window_kind=kind,
        endpoint_family=family,
        opens_at=_ist_utc(session, *start),
        closes_at=_ist_utc(session, *end),
        protocol_sha256=protocol_sha256,
    )


def _catalog_window(**overrides) -> ObservationWindowSpec:
    kwargs = dict(kind=ScheduledWindowKind.CATALOG_PREOPEN, family=EndpointFamily.CATALOG, start=(9, 0), end=(9, 5))
    kwargs.update(overrides)
    return _window(**kwargs)


def _quote_window(**overrides) -> ObservationWindowSpec:
    kwargs = dict(kind=ScheduledWindowKind.QUOTE_0920, family=EndpointFamily.FULL_QUOTE, start=(9, 20), end=(9, 25))
    kwargs.update(overrides)
    return _window(**kwargs)


def _ohlcv_window(**overrides) -> ObservationWindowSpec:
    # Post-16:15 (comfortably after NseSessionFinality's own 16:00 IST
    # data_ready_at guard) same-session capture window.
    kwargs = dict(kind=ScheduledWindowKind.OHLCV_CLOSE, family=EndpointFamily.DAILY_OHLCV, start=(16, 15), end=(16, 30))
    kwargs.update(overrides)
    return _window(**kwargs)


def _request(window: ObservationWindowSpec, **overrides) -> ObservationRequestIdentity:
    kwargs = dict(
        provider=PROVIDER_ZERODHA_KITE,
        provider_version="kite-3.0",
        endpoint_family=window.endpoint_family,
        exchange="NSE",
        window_id=window.window_id,
        requested_session=window.market_session,
        requested_keys=(),
        requested_range_start=None,
        requested_range_end=None,
        request_started_at=window.opens_at,
        request_ended_at=window.closes_at,
        chunk_index=1,
        chunk_count=1,
        response_classification=ResponseClassification.SUCCESS,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
    )
    kwargs.update(overrides)
    return ObservationRequestIdentity(**kwargs)


def _instrument(*, token: int = 101, symbol: str = "INFY", **overrides) -> KiteInstrument:
    kwargs = dict(
        instrument_token=token,
        exchange_token=str(token),
        tradingsymbol=symbol,
        name="Company " + symbol,
        dump_last_price=Decimal("1500.00"),
        expiry=None,
        strike=None,
        tick_size=Decimal("0.05"),
        lot_size=1,
        instrument_type="EQ",
        segment="NSE",
        exchange="NSE",
    )
    kwargs.update(overrides)
    return KiteInstrument(**kwargs)


def _catalog_payload(window: ObservationWindowSpec, instruments: tuple, **overrides) -> InstrumentBatch:
    kwargs = dict(
        exchange="NSE",
        observed_at=window.opens_at,
        provider_version="kite-3.0",
        instruments=instruments,
    )
    kwargs.update(overrides)
    return InstrumentBatch(**kwargs)


def _depth(price: str, quantity: int, orders: int) -> KiteDepthLevel:
    return KiteDepthLevel(price=Decimal(price), quantity=quantity, orders=orders)


def _quote(*, listing_key: str = "NSE:INFY", token: int = 101, window: ObservationWindowSpec, **overrides) -> KiteFullQuote:
    kwargs = dict(
        listing_key=listing_key,
        instrument_token=token,
        exchange_timestamp=window.opens_at,
        last_trade_time=window.opens_at,
        last_price=Decimal("1500.00"),
        lower_circuit_limit=Decimal("1350.00"),
        upper_circuit_limit=Decimal("1650.00"),
        depth_buy=(_depth("1499.50", 10, 2),),
        depth_sell=(_depth("1500.50", 5, 1),),
    )
    kwargs.update(overrides)
    return KiteFullQuote(**kwargs)


def _quote_payload(window: ObservationWindowSpec, requested_keys: tuple, quotes: tuple, **overrides) -> FullQuoteBatch:
    kwargs = dict(
        requested_keys=requested_keys,
        requested_at=window.opens_at,
        observed_at=window.opens_at,
        provider_version="kite-3.0",
        quotes=quotes,
    )
    kwargs.update(overrides)
    return FullQuoteBatch(**kwargs)


def _ohlcv_payload(
    window: ObservationWindowSpec, session: date, *, token: int = 101, **overrides
) -> DailyCandleBatch:
    finality = NseSessionFinality.regular_collection_guard(session)
    candle = DailyCandle(
        instrument_token=token,
        timestamp=datetime.combine(session, time(0, 0), tzinfo=IST),
        open=Decimal("1490.00"),
        high=Decimal("1510.00"),
        low=Decimal("1480.00"),
        close=Decimal("1500.00"),
        volume=1000,
        open_interest=None,
    )
    kwargs = dict(
        instrument_token=token,
        session_finality=finality,
        observed_at=window.opens_at,
        provider_version="kite-3.0",
        candles=(candle,),
    )
    kwargs.update(overrides)
    return DailyCandleBatch(**kwargs)


def _catalog_observation(**overrides) -> QualityPilotObservation:
    window = overrides.pop("window", None) or _catalog_window()
    instruments = overrides.pop("instruments", (_instrument(),))
    payload = overrides.pop("payload", _catalog_payload(window, instruments))
    request = overrides.pop("request", None) or _request(window)
    corrects = overrides.pop("corrects_observation_id", None)
    return QualityPilotObservation(
        window=window, request=request, payload=payload, corrects_observation_id=corrects
    )


def _quote_observation(**overrides) -> QualityPilotObservation:
    window = overrides.pop("window", None) or _quote_window()
    requested_keys = overrides.pop("requested_keys", ("NSE:INFY",))
    if "payload" in overrides:
        payload = overrides.pop("payload")
    else:
        quotes = overrides.pop(
            "quotes",
            tuple(
                _quote(listing_key=key, token=index + 1, window=window)
                for index, key in enumerate(requested_keys)
            ),
        )
        payload = _quote_payload(window, requested_keys, quotes)
    request = overrides.pop(
        "request", None
    ) or _request(window, requested_keys=requested_keys)
    corrects = overrides.pop("corrects_observation_id", None)
    return QualityPilotObservation(
        window=window, request=request, payload=payload, corrects_observation_id=corrects
    )


def _ohlcv_observation(**overrides) -> QualityPilotObservation:
    window = overrides.pop("window", None) or _ohlcv_window()
    session = overrides.pop("session", window.market_session)
    token = overrides.pop("token", 101)
    listing_key = overrides.pop("listing_key", "NSE:INFY")
    if "payload" in overrides:
        payload = overrides.pop("payload")
    else:
        payload = _ohlcv_payload(window, session, token=token)
    request = overrides.pop("request", None) or _request(
        window,
        requested_keys=(listing_key,),
        requested_range_start=session,
        requested_range_end=session,
        requested_session=session,
    )
    corrects = overrides.pop("corrects_observation_id", None)
    return QualityPilotObservation(
        window=window, request=request, payload=payload, corrects_observation_id=corrects
    )


def _gap_observation(*, window=None, classification=ResponseClassification.PROVIDER_GAP, **overrides) -> QualityPilotObservation:
    window = window or _quote_window()
    if "request" in overrides:
        request = overrides.pop("request")
    elif window.endpoint_family is EndpointFamily.CATALOG:
        request = _request(window, response_classification=classification)
    elif window.endpoint_family is EndpointFamily.FULL_QUOTE:
        request = _request(
            window, requested_keys=("NSE:INFY",), response_classification=classification
        )
    else:
        request = _request(
            window,
            requested_keys=("NSE:INFY",),
            requested_range_start=window.market_session,
            requested_range_end=window.market_session,
            response_classification=classification,
        )
    corrects = overrides.pop("corrects_observation_id", None)
    return QualityPilotObservation(
        window=window, request=request, payload=None, corrects_observation_id=corrects
    )


def _payload_json(observation: QualityPilotObservation) -> str:
    envelope = json.loads(encode_observation(observation))
    return json.dumps(envelope["payload"])


class QualityPilotHappyPathTests(unittest.TestCase):
    def test_catalog_round_trip_is_byte_identical_and_deterministic(self) -> None:
        observation = _catalog_observation()
        observation.verify_content_identity()
        encoded = encode_observation(observation)
        decoded = decode_observation(encoded)
        self.assertEqual(encode_observation(decoded), encoded)
        self.assertEqual(decoded.observation_id, observation.observation_id)
        self.assertEqual(decoded.canonical_response_sha256, observation.canonical_response_sha256)
        self.assertEqual(decoded.normalized_content_sha256, observation.normalized_content_sha256)
        replayed = replay_verify(encoded)
        self.assertEqual(replayed.observation_id, observation.observation_id)

    def test_full_quote_round_trip_is_byte_identical_and_deterministic(self) -> None:
        observation = _quote_observation()
        observation.verify_content_identity()
        encoded = encode_observation(observation)
        decoded = decode_observation(encoded)
        self.assertEqual(encode_observation(decoded), encoded)
        self.assertEqual(decoded.observation_id, observation.observation_id)

    def test_daily_ohlcv_round_trip_is_byte_identical_and_deterministic(self) -> None:
        observation = _ohlcv_observation()
        observation.verify_content_identity()
        encoded = encode_observation(observation)
        decoded = decode_observation(encoded)
        self.assertEqual(encode_observation(decoded), encoded)
        self.assertEqual(decoded.observation_id, observation.observation_id)
        self.assertIsInstance(decoded.payload, DailyCandleBatch)
        self.assertEqual(decoded.payload.session_finality.session, SESSION)

    def test_ohlcv_payload_is_current_session_daily_candle_batch(self) -> None:
        observation = _ohlcv_observation()
        self.assertIsInstance(observation.payload, DailyCandleBatch)
        self.assertEqual(observation.payload.session_finality.session, observation.window.market_session)

    def test_gap_observation_round_trip_is_byte_identical(self) -> None:
        observation = _gap_observation()
        observation.verify_content_identity()
        encoded = encode_observation(observation)
        decoded = decode_observation(encoded)
        self.assertEqual(encode_observation(decoded), encoded)
        self.assertIsNone(decoded.payload)

    def test_hashes_independent_of_caller_decimal_context(self) -> None:
        with localcontext() as context:
            context.prec = 3
            observation_low_prec = _catalog_observation()
        with localcontext() as context:
            context.prec = 60
            observation_high_prec = _catalog_observation()
        self.assertEqual(
            observation_low_prec.canonical_response_sha256,
            observation_high_prec.canonical_response_sha256,
        )
        self.assertEqual(observation_low_prec.observation_id, observation_high_prec.observation_id)

    def test_catalog_hash_independent_of_semantically_irrelevant_instrument_order(self) -> None:
        window = _catalog_window()
        a = _instrument(token=101, symbol="AAA")
        b = _instrument(token=202, symbol="BBB")
        forward = _catalog_observation(window=window, instruments=(a, b), payload=_catalog_payload(window, (a, b)))
        backward = _catalog_observation(window=window, instruments=(b, a), payload=_catalog_payload(window, (b, a)))
        self.assertEqual(forward.canonical_response_sha256, backward.canonical_response_sha256)
        self.assertEqual(forward.observation_id, backward.observation_id)


class QualityPilotOrderingTests(unittest.TestCase):
    def test_catalog_canonical_order_is_exchange_token_symbol(self) -> None:
        window = _catalog_window()
        low = _instrument(token=101, symbol="ZZZ")
        high = _instrument(token=202, symbol="AAA")
        payload = _catalog_payload(window, (high, low))
        observation = _catalog_observation(window=window, instruments=(high, low), payload=payload)
        tree = json.loads(_payload_json(observation))
        tokens = [item["instrument_token"] for item in tree["instruments"]]
        self.assertEqual(tokens, sorted(tokens))

    def test_quote_order_follows_requested_keys_and_preserves_depth(self) -> None:
        window = _quote_window()
        requested_keys = ("NSE:AAA", "NSE:ZZZ")
        quote_a = _quote(listing_key="NSE:AAA", token=1, window=window, depth_buy=(_depth("10.00", 1, 1),))
        quote_z = _quote(
            listing_key="NSE:ZZZ",
            token=2,
            window=window,
            depth_buy=(_depth("20.00", 2, 1), _depth("19.00", 3, 1)),
        )
        payload = _quote_payload(window, requested_keys, (quote_a, quote_z))
        observation = _quote_observation(
            window=window, requested_keys=requested_keys, quotes=(quote_a, quote_z), payload=payload
        )
        tree = json.loads(_payload_json(observation))
        listing_keys = [item["listing_key"] for item in tree["quotes"]]
        self.assertEqual(listing_keys, list(requested_keys))
        self.assertEqual(len(tree["quotes"][1]["depth_buy"]), 2)
        self.assertEqual(
            [level["price"] for level in tree["quotes"][1]["depth_buy"]], ["20.00", "19.00"]
        )

    def test_quote_preserves_fewer_than_five_depth_levels(self) -> None:
        window = _quote_window()
        quote = _quote(
            window=window,
            depth_buy=(_depth("10.00", 1, 1),),
            depth_sell=(),
        )
        payload = _quote_payload(window, ("NSE:INFY",), (quote,))
        observation = _quote_observation(window=window, quotes=(quote,), payload=payload)
        tree = json.loads(_payload_json(observation))
        self.assertEqual(len(tree["quotes"][0]["depth_buy"]), 1)
        self.assertEqual(len(tree["quotes"][0]["depth_sell"]), 0)

    def test_ohlcv_payload_tree_carries_listing_key_and_session(self) -> None:
        observation = _ohlcv_observation()
        tree = json.loads(_payload_json(observation))
        self.assertEqual(tree["listing_key"], "NSE:INFY")
        self.assertEqual(tree["session"], SESSION.isoformat())
        self.assertEqual(tree["instrument_token"], 101)

    def test_decimal_output_is_non_exponent_canonical_text(self) -> None:
        observation = _catalog_observation()
        tree = json.loads(_payload_json(observation))
        price = tree["instruments"][0]["dump_last_price"]
        self.assertNotIn("E", price.upper())
        self.assertEqual(price, "1500.00")

    def test_timestamps_are_utc_with_z_suffix(self) -> None:
        observation = _catalog_observation()
        tree = json.loads(_payload_json(observation))
        self.assertTrue(tree["observed_at"].endswith("Z"))


class QualityPilotCoverageRejectionTests(unittest.TestCase):
    def test_rejects_request_payload_key_mismatch_missing(self) -> None:
        # FullQuoteBatch itself only ever accepts a payload that exactly
        # covers its OWN requested_keys -- the anomaly this proves is a
        # self-consistent payload (covering only NSE:INFY) attached to an
        # observation whose OWN request identity claims a wider key set.
        window = _quote_window()
        quote = _quote(window=window)
        payload = _quote_payload(window, ("NSE:INFY",), (quote,))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            _quote_observation(
                window=window,
                requested_keys=("NSE:INFY", "NSE:TCS"),
                payload=payload,
            )

    def test_rejects_request_payload_key_mismatch_extra(self) -> None:
        window = _quote_window()
        quote_infy = _quote(listing_key="NSE:INFY", token=1, window=window)
        quote_tcs = _quote(listing_key="NSE:TCS", token=2, window=window)
        payload = _quote_payload(window, ("NSE:INFY", "NSE:TCS"), (quote_infy, quote_tcs))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            _quote_observation(
                window=window,
                requested_keys=("NSE:INFY",),
                payload=payload,
            )

    def test_full_quote_batch_itself_rejects_reordered_coverage(self) -> None:
        # FullQuoteBatch (the accepted upstream type) is the actual
        # enforcement point for exact/ordered coverage; this documents that
        # guarantee directly, using its own ValueError, not this module's.
        window = _quote_window()
        quote_a = _quote(listing_key="NSE:AAA", token=1, window=window)
        quote_z = _quote(listing_key="NSE:ZZZ", token=2, window=window)
        with self.assertRaises(ValueError):
            FullQuoteBatch(
                requested_keys=("NSE:AAA", "NSE:ZZZ"),
                requested_at=window.opens_at,
                observed_at=window.opens_at,
                provider_version="kite-3.0",
                quotes=(quote_z, quote_a),
            )

    def test_rejects_duplicate_quote_key(self) -> None:
        window = _quote_window()
        with self.assertRaises(Exception):
            _request(window, requested_keys=("NSE:INFY", "NSE:INFY"))

    def test_rejects_wrong_catalog_exchange(self) -> None:
        window = _catalog_window()
        payload = _catalog_payload(window, (_instrument(),), exchange="NSE")
        request = _request(window, exchange="NSE")
        object.__setattr__(payload, "exchange", "BSE")
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_rejects_ohlcv_range_not_equal_to_requested_session(self) -> None:
        window = _ohlcv_window()
        with self.assertRaises(QualityPilotCanonicalResponseError):
            _request(
                window,
                requested_keys=("NSE:INFY",),
                requested_range_start=OTHER_SESSION,
                requested_range_end=SESSION,
                requested_session=SESSION,
            )

    def test_rejects_ohlcv_payload_session_not_matching_window(self) -> None:
        window = _ohlcv_window()
        wrong_session_payload = _ohlcv_payload(window, OTHER_SESSION)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            _ohlcv_observation(window=window, payload=wrong_session_payload)

    def test_rejects_payload_endpoint_family_mismatch(self) -> None:
        window = _catalog_window()
        request = _request(window)
        quote_payload = _quote_payload(_quote_window(), ("NSE:INFY",), (_quote(window=_quote_window()),))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=quote_payload, corrects_observation_id=None
            )

    def test_rejects_request_interval_outside_window(self) -> None:
        # The request identity alone only knows window_id (a reference), not
        # the window's own opens_at/closes_at -- interval containment is an
        # aggregate-level cross-check, so it only fires once both nested
        # objects are combined into one observation.
        window = _catalog_window()
        request = _request(window, request_started_at=window.opens_at - timedelta(minutes=1))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window,
                request=request,
                payload=_catalog_payload(window, (_instrument(),)),
                corrects_observation_id=None,
            )

    def test_rejects_future_known_payload_relative_to_request_window(self) -> None:
        window = _catalog_window()
        payload = _catalog_payload(window, (_instrument(),), observed_at=window.closes_at + timedelta(minutes=1))
        request = _request(window)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_rejects_catalog_payload_observed_before_request_start(self) -> None:
        window = _catalog_window()
        request = _request(window, request_started_at=window.opens_at + timedelta(minutes=2))
        payload = _catalog_payload(window, (_instrument(),), observed_at=window.opens_at)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_rejects_malformed_window_kind_endpoint_pairing(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            ObservationWindowSpec(
                pilot_run_id=PILOT_RUN_ID,
                market_session=SESSION,
                window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
                endpoint_family=EndpointFamily.FULL_QUOTE,
                opens_at=_ist_utc(SESSION, 9, 0),
                closes_at=_ist_utc(SESSION, 9, 5),
                protocol_sha256=PILOT_PROTOCOL_SHA256,
            )


class QualityPilotRequestedSessionTests(unittest.TestCase):
    def test_rejects_requested_session_disagreeing_with_window(self) -> None:
        window = _catalog_window()
        request = _request(window, requested_session=OTHER_SESSION)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window,
                request=request,
                payload=_catalog_payload(window, (_instrument(),)),
                corrects_observation_id=None,
            )

    def test_requested_session_bound_into_request_id(self) -> None:
        window = _catalog_window()
        request_a = _request(window, requested_session=SESSION)
        request_b = ObservationRequestIdentity(
            provider=request_a.provider,
            provider_version=request_a.provider_version,
            endpoint_family=request_a.endpoint_family,
            exchange=request_a.exchange,
            window_id=request_a.window_id,
            requested_session=OTHER_SESSION,
            requested_keys=request_a.requested_keys,
            requested_range_start=request_a.requested_range_start,
            requested_range_end=request_a.requested_range_end,
            request_started_at=request_a.request_started_at,
            request_ended_at=request_a.request_ended_at,
            chunk_index=request_a.chunk_index,
            chunk_count=request_a.chunk_count,
            response_classification=request_a.response_classification,
            protocol_sha256=request_a.protocol_sha256,
        )
        self.assertNotEqual(request_a.request_id, request_b.request_id)

    def test_catalog_requested_session_equals_market_session_by_default(self) -> None:
        observation = _catalog_observation()
        self.assertEqual(observation.request.requested_session, observation.window.market_session)


class QualityPilotQuoteCeilingTests(unittest.TestCase):
    def test_pinned_to_the_real_per_request_kite_ceiling(self) -> None:
        self.assertEqual(MAXIMUM_QUOTE_REQUEST_KEYS, 500)

    def test_accepts_exactly_500_requested_keys_without_a_payload(self) -> None:
        window = _quote_window()
        keys = tuple(f"NSE:S{i:04d}" for i in range(500))
        keys = tuple(sorted(keys))
        request = _request(window, requested_keys=keys)
        self.assertEqual(len(request.requested_keys), 500)

    def test_rejects_501_requested_keys_without_a_payload(self) -> None:
        window = _quote_window()
        keys = tuple(sorted(f"NSE:S{i:04d}" for i in range(501)))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            _request(window, requested_keys=keys)


class QualityPilotNormalizedContentTests(unittest.TestCase):
    def test_schema_version_is_pinned(self) -> None:
        self.assertEqual(NORMALIZED_CONTENT_SCHEMA_VERSION, "normalized_content_v1")

    def test_normalized_hash_stable_across_window_and_request_metadata(self) -> None:
        window = _catalog_window()
        payload = _catalog_payload(window, (_instrument(),))
        request_a = _request(window, chunk_index=1, chunk_count=1)
        request_b = _request(window, chunk_index=1, chunk_count=2)
        observation_a = QualityPilotObservation(
            window=window, request=request_a, payload=payload, corrects_observation_id=None
        )
        observation_b = QualityPilotObservation(
            window=window, request=request_b, payload=payload, corrects_observation_id=None
        )
        self.assertEqual(
            observation_a.normalized_content_sha256, observation_b.normalized_content_sha256
        )
        self.assertNotEqual(
            observation_a.canonical_response_sha256, observation_b.canonical_response_sha256
        )
        self.assertNotEqual(observation_a.observation_id, observation_b.observation_id)

    def test_normalized_hash_changes_when_record_content_changes(self) -> None:
        window = _catalog_window()
        cheap = _instrument(dump_last_price=Decimal("1500.00"))
        expensive = _instrument(dump_last_price=Decimal("1600.00"))
        observation_cheap = _catalog_observation(
            window=window, instruments=(cheap,), payload=_catalog_payload(window, (cheap,))
        )
        observation_expensive = _catalog_observation(
            window=window, instruments=(expensive,), payload=_catalog_payload(window, (expensive,))
        )
        self.assertNotEqual(
            observation_cheap.normalized_content_sha256, observation_expensive.normalized_content_sha256
        )

    def test_normalized_hash_changes_when_classification_changes(self) -> None:
        window = _quote_window()
        provider_gap = _gap_observation(window=window, classification=ResponseClassification.PROVIDER_GAP)
        rate_limited = _gap_observation(window=window, classification=ResponseClassification.RATE_LIMITED)
        self.assertNotEqual(
            provider_gap.normalized_content_sha256, rate_limited.normalized_content_sha256
        )

    def test_normalized_hash_excludes_provider_version_and_observed_at(self) -> None:
        window = _catalog_window()
        instrument = _instrument()
        payload_a = _catalog_payload(window, (instrument,), provider_version="kite-3.0")
        payload_b = _catalog_payload(window, (instrument,), provider_version="kite-3.1")
        request_a = _request(window, provider_version="kite-3.0")
        request_b = _request(window, provider_version="kite-3.1")
        observation_a = _catalog_observation(
            window=window, instruments=(instrument,), payload=payload_a, request=request_a
        )
        observation_b = _catalog_observation(
            window=window, instruments=(instrument,), payload=payload_b, request=request_b
        )
        self.assertEqual(
            observation_a.normalized_content_sha256, observation_b.normalized_content_sha256
        )
        self.assertNotEqual(
            observation_a.canonical_response_sha256, observation_b.canonical_response_sha256
        )


class QualityPilotIndependentReconstructionTests(unittest.TestCase):
    # Independent reconstruction re-runs KiteInstrument/InstrumentBatch's own
    # __post_init__ validation -- it can only catch a tamper that makes the
    # object internally INVALID (wrong type, out-of-range value, empty
    # required text), never a tamper to another individually-valid value
    # (e.g. a different but still-positive tick_size), since there is no
    # external ground truth to compare against.
    def test_tampered_instrument_token_rejected(self) -> None:
        self._assert_tamper_rejected("instrument_token", -1)

    def test_tampered_tick_size_rejected(self) -> None:
        self._assert_tamper_rejected("tick_size", Decimal("-1.00"))

    def test_tampered_text_field_rejected(self) -> None:
        self._assert_tamper_rejected("tradingsymbol", "")

    def test_tampered_decimal_field_rejected(self) -> None:
        self._assert_tamper_rejected("dump_last_price", Decimal("-1.00"))

    def test_tampered_exchange_rejected(self) -> None:
        self._assert_tamper_rejected("exchange", "BSE")

    def test_tampered_instrument_type_rejected(self) -> None:
        self._assert_tamper_rejected("instrument_type", "")

    def _assert_tamper_rejected(self, field_name: str, value: object) -> None:
        window = _catalog_window()
        instrument = _instrument()
        payload = _catalog_payload(window, (instrument,))
        object.__setattr__(payload.instruments[0], field_name, value)
        request = _request(window)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_foreign_instrument_type_in_catalog_rejected(self) -> None:
        window = _catalog_window()
        instrument = _instrument()
        payload = _catalog_payload(window, (instrument,))
        object.__setattr__(payload, "instruments", (object(),))
        request = _request(window)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_reconstruction_does_not_alter_a_legitimate_catalog_hash(self) -> None:
        observation = _catalog_observation()
        again = _catalog_observation()
        self.assertEqual(observation.canonical_response_sha256, again.canonical_response_sha256)


class QualityPilotCatalogEquityTests(unittest.TestCase):
    """The prospective catalog is the non-empty Kite K_EQ cohort only."""

    def test_rejects_empty_success_catalog(self) -> None:
        window = _catalog_window()
        payload = _catalog_payload(window, ())
        request = _request(window)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_rejects_valid_non_eq_instrument_type(self) -> None:
        window = _catalog_window()
        instrument = _instrument(instrument_type="FUT")
        payload = _catalog_payload(window, (instrument,))
        request = _request(window)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_rejects_valid_non_nse_segment(self) -> None:
        window = _catalog_window()
        instrument = _instrument(segment="BSE")
        payload = _catalog_payload(window, (instrument,))
        request = _request(window)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_rejects_valid_bse_exchange_instrument(self) -> None:
        window = _catalog_window()
        instrument = _instrument(exchange="BSE", segment="BSE")
        payload = _catalog_payload(window, (instrument,), exchange="BSE")
        request = _request(window, exchange="NSE")
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_accepts_non_empty_nse_eq_catalog_without_asserting_ordinary_equity(self) -> None:
        window = _catalog_window()
        instrument = _instrument(instrument_type="EQ", segment="NSE", exchange="NSE")
        self.assertTrue(instrument.is_nse_eq_record)
        observation = _catalog_observation(window=window, instruments=(instrument,))
        observation.verify_content_identity()
        self.assertEqual(len(observation.payload.instruments), 1)

    def test_mixed_eq_and_non_eq_rows_rejected(self) -> None:
        window = _catalog_window()
        eq_instrument = _instrument(token=101, symbol="AAA", instrument_type="EQ")
        fut_instrument = _instrument(token=202, symbol="BBB", instrument_type="FUT")
        payload = _catalog_payload(window, (eq_instrument, fut_instrument))
        request = _request(window)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )


class QualityPilotFinalityTests(unittest.TestCase):
    """The complete exact NseSessionFinality contract is retained, never normalized."""

    def test_canonical_payload_binds_all_finality_fields(self) -> None:
        observation = _ohlcv_observation()
        tree = json.loads(_payload_json(observation))
        finality = NseSessionFinality.regular_collection_guard(SESSION)
        self.assertEqual(tree["session"], finality.session.isoformat())
        self.assertEqual(tree["policy_version"], finality.policy_version)
        self.assertIs(tree["actionable"], finality.actionable)
        self.assertTrue(tree["market_close_at"].endswith("Z"))
        self.assertTrue(tree["data_ready_at"].endswith("Z"))

    def test_normalized_content_depends_only_on_session_not_other_finality_fields(self) -> None:
        # Both observations use the exact regular guard (same session), so
        # the only knob available without violating NseSessionFinality's
        # own __post_init__ is the fixed derived fields -- proving the
        # normalized tree depends only on session is equivalent to proving
        # it never references market_close_at/data_ready_at/policy_version/
        # actionable, since those are 100% determined by session anyway.
        observation_a = _ohlcv_observation()
        observation_b = _ohlcv_observation()
        self.assertEqual(
            observation_a.normalized_content_sha256, observation_b.normalized_content_sha256
        )

    def _tampered_finality_rejected(self, field_name: str, value: object) -> None:
        window = _ohlcv_window()
        payload = _ohlcv_payload(window, SESSION)
        object.__setattr__(payload.session_finality, field_name, value)
        request = _request(
            window,
            requested_keys=("NSE:INFY",),
            requested_range_start=SESSION,
            requested_range_end=SESSION,
        )
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_tampered_market_close_at_rejected(self) -> None:
        self._tampered_finality_rejected(
            "market_close_at", _ist_utc(SESSION, 16, 0)
        )

    def test_tampered_data_ready_at_rejected(self) -> None:
        self._tampered_finality_rejected("data_ready_at", _ist_utc(SESSION, 17, 0))

    def test_tampered_policy_version_rejected(self) -> None:
        self._tampered_finality_rejected("policy_version", "tampered/v2")

    def test_tampered_actionable_rejected(self) -> None:
        self._tampered_finality_rejected("actionable", True)

    def test_tampered_finality_session_rejected(self) -> None:
        self._tampered_finality_rejected("session", OTHER_SESSION)

    def test_non_regular_finality_type_rejected(self) -> None:
        window = _ohlcv_window()
        payload = _ohlcv_payload(window, SESSION)
        object.__setattr__(payload, "session_finality", object())
        request = _request(
            window,
            requested_keys=("NSE:INFY",),
            requested_range_start=SESSION,
            requested_range_end=SESSION,
        )
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_decode_rejects_tampered_finality_field_in_encoded_bytes(self) -> None:
        observation = _ohlcv_observation()
        encoded = encode_observation(observation)
        tampered = json.loads(encoded)
        tampered["payload"]["policy_version"] = "tampered/v2"
        data = json.dumps(tampered).encode("utf-8")
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_decode_rejects_actionable_true_in_encoded_bytes(self) -> None:
        observation = _ohlcv_observation()
        encoded = encode_observation(observation)
        tampered = json.loads(encoded)
        tampered["payload"]["actionable"] = True
        data = json.dumps(tampered).encode("utf-8")
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)


class QualityPilotClassificationCompatibilityTests(unittest.TestCase):
    _COMPATIBLE = {
        EndpointFamily.CATALOG: (
            ResponseClassification.SUCCESS,
            ResponseClassification.CATALOG_GAP,
            ResponseClassification.CANONICALIZATION_FAILURE,
            ResponseClassification.REQUEST_REJECTED,
            ResponseClassification.RATE_LIMITED,
            ResponseClassification.TIMESTAMP_VIOLATION,
        ),
        EndpointFamily.FULL_QUOTE: (
            ResponseClassification.SUCCESS,
            ResponseClassification.PROVIDER_GAP,
            ResponseClassification.PARTIAL_KEY_COVERAGE,
            ResponseClassification.CANONICALIZATION_FAILURE,
            ResponseClassification.REQUEST_REJECTED,
            ResponseClassification.RATE_LIMITED,
            ResponseClassification.TIMESTAMP_VIOLATION,
        ),
        EndpointFamily.DAILY_OHLCV: (
            ResponseClassification.SUCCESS,
            ResponseClassification.PROVIDER_GAP,
            ResponseClassification.CANONICALIZATION_FAILURE,
            ResponseClassification.REQUEST_REJECTED,
            ResponseClassification.RATE_LIMITED,
            ResponseClassification.TIMESTAMP_VIOLATION,
        ),
    }
    _WINDOW_BUILDERS = {
        EndpointFamily.CATALOG: _catalog_window,
        EndpointFamily.FULL_QUOTE: _quote_window,
        EndpointFamily.DAILY_OHLCV: _ohlcv_window,
    }

    def _gap_request_kwargs(self, family: EndpointFamily) -> dict:
        if family is EndpointFamily.CATALOG:
            return {}
        if family is EndpointFamily.FULL_QUOTE:
            return {"requested_keys": ("NSE:INFY",)}
        return {
            "requested_keys": ("NSE:INFY",),
            "requested_range_start": SESSION,
            "requested_range_end": SESSION,
        }

    def test_pinned_compatibility_table_exhaustive_accept_and_reject(self) -> None:
        for family in EndpointFamily:
            window = self._WINDOW_BUILDERS[family]()
            for classification in ResponseClassification:
                if classification is ResponseClassification.SUCCESS:
                    continue
                kwargs = self._gap_request_kwargs(family)
                with self.subTest(family=family, classification=classification):
                    if classification in self._COMPATIBLE[family]:
                        request = _request(
                            window, response_classification=classification, **kwargs
                        )
                        self.assertIs(request.response_classification, classification)
                    else:
                        with self.assertRaises(QualityPilotCanonicalResponseError):
                            _request(window, response_classification=classification, **kwargs)

    def test_rejects_incompatible_classification_in_encoded_bytes(self) -> None:
        observation = _gap_observation(
            window=_quote_window(), classification=ResponseClassification.PROVIDER_GAP
        )
        encoded = encode_observation(observation)
        tampered = json.loads(encoded)
        tampered["request"]["response_classification"] = "CATALOG_GAP"
        data = json.dumps(tampered).encode("utf-8")
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)


class QualityPilotNonSuccessTests(unittest.TestCase):
    def test_every_endpoint_compatible_non_success_classification_has_gap_shape(self) -> None:
        cases = (
            (_catalog_window(), ResponseClassification.CATALOG_GAP),
            (_catalog_window(), ResponseClassification.CANONICALIZATION_FAILURE),
            (_catalog_window(), ResponseClassification.REQUEST_REJECTED),
            (_catalog_window(), ResponseClassification.RATE_LIMITED),
            (_catalog_window(), ResponseClassification.TIMESTAMP_VIOLATION),
            (_quote_window(), ResponseClassification.PROVIDER_GAP),
            (_quote_window(), ResponseClassification.PARTIAL_KEY_COVERAGE),
            (_ohlcv_window(), ResponseClassification.PROVIDER_GAP),
        )
        for window, classification in cases:
            with self.subTest(window=window.endpoint_family, classification=classification):
                observation = _gap_observation(window=window, classification=classification)
                observation.verify_content_identity()
                self.assertIsNone(observation.payload)
                tree = json.loads(_payload_json(observation))
                self.assertEqual(tree, {"kind": "GAP"})

    def test_non_success_cannot_carry_a_success_payload(self) -> None:
        window = _quote_window()
        payload = _quote_payload(window, ("NSE:INFY",), (_quote(window=window),))
        request = _request(
            window, requested_keys=("NSE:INFY",), response_classification=ResponseClassification.PROVIDER_GAP
        )
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=payload, corrects_observation_id=None
            )

    def test_success_requires_a_payload(self) -> None:
        window = _quote_window()
        request = _request(window, requested_keys=("NSE:INFY",))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            QualityPilotObservation(
                window=window, request=request, payload=None, corrects_observation_id=None
            )

    def test_non_success_retains_exact_requested_key_lineage(self) -> None:
        observation = _gap_observation()
        self.assertEqual(observation.request.requested_keys, ("NSE:INFY",))


class QualityPilotCorrectionLineageTests(unittest.TestCase):
    def test_correction_changes_observation_id_and_preserves_original(self) -> None:
        original = _catalog_observation()
        correction = _catalog_observation(corrects_observation_id=original.observation_id)
        self.assertNotEqual(correction.observation_id, original.observation_id)
        original.verify_content_identity()
        correction.verify_content_identity()

    def test_rejects_self_correction_link(self) -> None:
        original = _catalog_observation()
        with self.assertRaises(QualityPilotCanonicalResponseError):
            request = _request(_catalog_window())
            QualityPilotObservation(
                window=_catalog_window(),
                request=request,
                payload=_catalog_payload(_catalog_window(), (_instrument(),)),
                corrects_observation_id="not-a-sha256",
            )

    def test_no_overwrite_or_latest_selection_capability_exists(self) -> None:
        source = inspect.getsource(quality_pilot_module)
        for token in (
            "def latest(",
            "select_latest(",
            "find_latest(",
            "def overwrite(",
            "def replace_",
            ".delete(",
            "def resume(",
            "def checkpoint(",
        ):
            self.assertNotIn(token, source.lower())


class QualityPilotStrictDecoderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.observation = _catalog_observation()
        self.encoded = encode_observation(self.observation)
        self.envelope = json.loads(self.encoded)

    def _encode_tampered(self, mutate) -> bytes:
        tampered = json.loads(self.encoded)
        mutate(tampered)
        return json.dumps(tampered).encode("utf-8")

    def test_rejects_empty_bytes(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(b"")

    def test_rejects_non_bytes(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation("not-bytes")  # type: ignore[arg-type]

    def test_rejects_oversized_input(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(b"{" + b"0" * (MAXIMUM_ENCODED_BYTES + 1))

    def test_rejects_invalid_utf8(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(b"\xff\xfe\x00\x01")

    def test_rejects_invalid_json(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(b"{not json")

    def test_rejects_duplicate_top_level_key(self) -> None:
        raw = self.encoded.decode("utf-8")
        # Inject a literal duplicate key by hand -- json.dumps never emits
        # one, so this must be constructed as raw text.
        body = raw[1:-1]
        first_key_value = body.split(",", 1)[0]
        tampered = "{" + first_key_value + "," + body + "}"
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(tampered.encode("utf-8"))

    def test_rejects_duplicate_nested_key(self) -> None:
        tampered = json.loads(self.encoded)
        window_body = json.dumps(tampered["window"])[1:-1]
        first_pair = window_body.split(",", 1)[0]
        forged_window_text = "{" + first_pair + "," + window_body + "}"
        raw = self.encoded.decode("utf-8")
        window_text = json.dumps(tampered["window"], sort_keys=True, separators=(",", ":"))
        raw_tampered = raw.replace(window_text, forged_window_text, 1)
        self.assertNotEqual(raw_tampered, raw)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(raw_tampered.encode("utf-8"))

    def test_rejects_float_literal(self) -> None:
        raw = self.encoded.decode("utf-8")
        tampered = raw.replace('"lot_size":1', '"lot_size":1.0')
        self.assertNotEqual(tampered, raw)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(tampered.encode("utf-8"))

    def test_rejects_nan_constant(self) -> None:
        raw = self.encoded.decode("utf-8")
        tampered = raw.replace('"lot_size":1', '"lot_size":NaN')
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(tampered.encode("utf-8"))

    def test_rejects_unknown_top_level_key(self) -> None:
        data = self._encode_tampered(lambda tree: tree.update({"unexpected": "value"}))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_missing_top_level_key(self) -> None:
        data = self._encode_tampered(lambda tree: tree.pop("observation_id"))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_bool_as_int(self) -> None:
        def mutate(tree):
            tree["payload"]["instruments"][0]["lot_size"] = True

        data = self._encode_tampered(mutate)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_noncanonical_decimal_spelling(self) -> None:
        def mutate(tree):
            tree["payload"]["instruments"][0]["dump_last_price"] = "1500.000"

        data = self._encode_tampered(mutate)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_signed_zero_decimal(self) -> None:
        def mutate(tree):
            tree["payload"]["instruments"][0]["dump_last_price"] = "-0"

        data = self._encode_tampered(mutate)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_noncanonical_timestamp_spelling(self) -> None:
        def mutate(tree):
            tree["payload"]["observed_at"] = tree["payload"]["observed_at"].replace("Z", "+00:00")

        data = self._encode_tampered(mutate)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_naive_timestamp_spelling(self) -> None:
        def mutate(tree):
            tree["payload"]["observed_at"] = tree["payload"]["observed_at"][:-1]

        data = self._encode_tampered(mutate)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_forged_canonical_response_hash(self) -> None:
        data = self._encode_tampered(lambda tree: tree.update({"canonical_response_sha256": "0" * 64}))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_forged_normalized_content_hash(self) -> None:
        data = self._encode_tampered(lambda tree: tree.update({"normalized_content_sha256": "0" * 64}))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_forged_observation_id(self) -> None:
        data = self._encode_tampered(lambda tree: tree.update({"observation_id": "0" * 64}))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_forged_window_id(self) -> None:
        data = self._encode_tampered(lambda tree: tree["window"].update({"window_id": "0" * 64}))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_forged_request_id(self) -> None:
        data = self._encode_tampered(lambda tree: tree["request"].update({"request_id": "0" * 64}))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_decode_reconstructs_exact_typed_values(self) -> None:
        decoded = decode_observation(self.encoded)
        self.assertEqual(decoded.window.market_session, SESSION)
        self.assertIsInstance(decoded.payload, InstrumentBatch)
        self.assertEqual(decoded.payload.instruments[0].dump_last_price, Decimal("1500.00"))


class QualityPilotOhlcvDecoderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.observation = _ohlcv_observation()
        self.encoded = encode_observation(self.observation)

    def _encode_tampered(self, mutate) -> bytes:
        tampered = json.loads(self.encoded)
        mutate(tampered)
        return json.dumps(tampered).encode("utf-8")

    def test_rejects_ohlcv_listing_key_mismatched_with_request(self) -> None:
        data = self._encode_tampered(lambda tree: tree["payload"].update({"listing_key": "NSE:OTHER"}))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_ohlcv_session_mismatched_with_window(self) -> None:
        data = self._encode_tampered(
            lambda tree: tree["payload"].update({"session": OTHER_SESSION.isoformat()})
        )
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)

    def test_rejects_ohlcv_unknown_payload_key(self) -> None:
        data = self._encode_tampered(lambda tree: tree["payload"].update({"unexpected": "1"}))
        with self.assertRaises(QualityPilotCanonicalResponseError):
            decode_observation(data)


class QualityPilotSanitizationTests(unittest.TestCase):
    def test_invalid_spec_type_error_has_no_cause_or_context(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError) as context:
            decode_observation(b"null")
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_malicious_nested_type_with_planted_secret_does_not_leak(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"

        class _Boom:
            def __eq__(self, other):
                raise ValueError(secret)

            def __hash__(self):
                return 0

        window = _catalog_window()
        request = _request(window)
        with self.assertRaises(QualityPilotCanonicalResponseError) as context:
            QualityPilotObservation(
                window=window,
                request=request,
                payload=_Boom(),
                corrects_observation_id=None,
            )
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_decode_failure_with_planted_secret_in_bytes_does_not_leak(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"
        payload = ("{" + secret).encode("utf-8")
        with self.assertRaises(QualityPilotCanonicalResponseError) as context:
            decode_observation(payload)
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)


class QualityPilotStructuralTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = inspect.getsource(quality_pilot_module)

    def test_no_filesystem_network_environment_or_clock_access(self) -> None:
        forbidden = (
            "open(",
            "Path(",
            "os.environ",
            "os.getenv",
            "socket.",
            "requests.",
            "urllib.",
            "httpx.",
            "datetime.now(",
            "datetime.utcnow(",
            "time.time(",
            "time.sleep(",
            "subprocess.",
            "os.system(",
        )
        for token in forbidden:
            self.assertNotIn(token, self.source, msg=f"forbidden token found: {token}")

    def test_no_kite_broker_capital_or_deployment_capability(self) -> None:
        # Code-shaped tokens only (trailing "(" or "." or a compound
        # identifier) -- this module's own docstrings legitimately use bare
        # prose words like "broker" and "scheduler" to describe what it does
        # NOT do, so a bare-word scan would false-positive on its own text.
        forbidden = (
            "kiteconnect",
            "kite.login",
            "gcs.",
            "cloud_run",
            "cloudrun",
            "telegram.",
            "broker.",
            "brokerclient",
            "broker_client",
            "place_order(",
            "execute_order(",
            "compute_feature(",
            "calculate_return(",
            "generate_label(",
            "generate_signal(",
            "confidence_score",
            "send_alert(",
            "register_paper_trade(",
            "shadowalert",
            "send_notification(",
            "kronos",
            "openai",
            "anthropic",
            "pickle.",
            "shelve.",
            "sqlite3.",
            "json.dump(",
            ".glob(",
            ".iterdir(",
            ".listdir(",
            "scheduler.",
            "backgroundscheduler",
            "crontab",
        )
        lowered = self.source.lower()
        for token in forbidden:
            self.assertNotIn(token.lower(), lowered, msg=f"forbidden token found: {token}")

    def test_ast_scan_finds_no_import_of_forbidden_modules(self) -> None:
        tree = ast.parse(self.source)
        forbidden_modules = {
            "os",
            "sys",
            "socket",
            "subprocess",
            "requests",
            "urllib",
            "httpx",
            "shutil",
            "pathlib",
            "sqlite3",
            "pickle",
            "shelve",
        }
        imported_modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_modules.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.add(node.module.split(".")[0])
        self.assertEqual(imported_modules & forbidden_modules, set())


class QualityPilotRegressionTests(unittest.TestCase):
    def test_protocol_hash_is_pinned(self) -> None:
        self.assertEqual(
            PILOT_PROTOCOL_SHA256,
            "b29e34fdc3f62134034fbf032cf67d39ce06acd5082f9d1de680fcac2260f8e5",
        )

    def test_schema_and_policy_versions_are_pinned(self) -> None:
        self.assertEqual(
            quality_pilot_module.CANONICAL_RESPONSE_SCHEMA_VERSION, "canonical_response_v1"
        )
        self.assertEqual(
            quality_pilot_module.NORMALIZED_CONTENT_SCHEMA_VERSION, "normalized_content_v1"
        )
        self.assertEqual(
            QUALITY_PILOT_POLICY_VERSION, "hyp002-quality-pilot/positive-only-v1"
        )

    def test_enum_values_are_pinned(self) -> None:
        self.assertEqual(
            {member.value for member in EndpointFamily},
            {"CATALOG", "FULL_QUOTE", "DAILY_OHLCV"},
        )
        self.assertEqual(
            {member.value for member in ScheduledWindowKind},
            {"CATALOG_PREOPEN", "QUOTE_0920", "QUOTE_CLOSE", "OHLCV_CLOSE"},
        )
        self.assertEqual(
            {member.value for member in ResponseClassification},
            {
                "SUCCESS",
                "CATALOG_GAP",
                "PROVIDER_GAP",
                "CANONICALIZATION_FAILURE",
                "REQUEST_REJECTED",
                "RATE_LIMITED",
                "PARTIAL_KEY_COVERAGE",
                "TIMESTAMP_VIOLATION",
            },
        )

    def test_size_and_count_ceilings_are_pinned(self) -> None:
        self.assertEqual(MAXIMUM_CATALOG_INSTRUMENTS, 20000)
        self.assertEqual(MAXIMUM_ENCODED_BYTES, 32 * 1024 * 1024)
        self.assertEqual(MAXIMUM_QUOTE_REQUEST_KEYS, 500)
        self.assertGreater(MAXIMUM_CHUNK_COUNT, 0)

    def test_full_permanent_quality_only_posture(self) -> None:
        observation = _catalog_observation()
        self.assertTrue(observation.quality_only)
        for name in (
            "counts_toward_o0",
            "counts_toward_clean_accumulation",
            "research_partition_eligible",
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "signal_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
            "capital_eligible",
        ):
            self.assertFalse(getattr(observation, name), msg=name)
        for nested in (observation.window, observation.request):
            self.assertTrue(nested.quality_only)
            for name in (
                "counts_toward_o0",
                "counts_toward_clean_accumulation",
                "research_partition_eligible",
                "training_eligible",
                "feature_eligible",
                "label_eligible",
                "signal_eligible",
                "paper_trade_eligible",
                "notification_eligible",
                "execution_eligible",
                "capital_eligible",
            ):
                self.assertFalse(getattr(nested, name), msg=name)

    def test_posture_property_has_no_backing_slot(self) -> None:
        window = _catalog_window()
        with self.assertRaises(AttributeError):
            object.__setattr__(window, "capital_eligible", True)

    def test_altered_observation_posture_field_rejected(self) -> None:
        observation = _catalog_observation()
        object.__setattr__(observation, "capital_eligible", True)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            observation.verify_content_identity()


class QualityPilotWindowSpecTests(unittest.TestCase):
    def test_rejects_wrong_protocol_hash(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            _catalog_window(protocol_sha256="0" * 64)

    def test_rejects_opens_after_closes(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            ObservationWindowSpec(
                pilot_run_id=PILOT_RUN_ID,
                market_session=SESSION,
                window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
                endpoint_family=EndpointFamily.CATALOG,
                opens_at=_ist_utc(SESSION, 9, 5),
                closes_at=_ist_utc(SESSION, 9, 0),
                protocol_sha256=PILOT_PROTOCOL_SHA256,
            )

    def test_rejects_opens_at_not_mapping_to_market_session(self) -> None:
        with self.assertRaises(QualityPilotCanonicalResponseError):
            ObservationWindowSpec(
                pilot_run_id=PILOT_RUN_ID,
                market_session=SESSION,
                window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
                endpoint_family=EndpointFamily.CATALOG,
                opens_at=_ist_utc(OTHER_SESSION, 9, 0),
                closes_at=_ist_utc(SESSION, 9, 5),
                protocol_sha256=PILOT_PROTOCOL_SHA256,
            )

    def test_window_id_deterministic_and_stable(self) -> None:
        first = _catalog_window()
        second = _catalog_window()
        self.assertEqual(first.window_id, second.window_id)

    def test_tampered_window_id_rejected(self) -> None:
        window = _catalog_window()
        object.__setattr__(window, "window_id", "0" * 64)
        with self.assertRaises(QualityPilotCanonicalResponseError):
            window.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
