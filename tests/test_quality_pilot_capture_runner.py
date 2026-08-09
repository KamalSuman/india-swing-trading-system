from __future__ import annotations

import ast
import inspect
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal, localcontext
from hashlib import sha256

from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.quality_pilot import capture_runner as runner_module
from india_swing.quality_pilot.canonical_response import (
    MAXIMUM_CHUNK_COUNT,
    MAXIMUM_QUOTE_REQUEST_KEYS,
    MAXIMUM_TEXT_FIELD_LENGTH,
    PILOT_PROTOCOL_SHA256,
    PROVIDER_ZERODHA_KITE,
    EndpointFamily,
    ObservationWindowSpec,
    ResponseClassification,
    ScheduledWindowKind,
)
from india_swing.quality_pilot.capture_runner import (
    CONFIRMED_SESSION_COUNT,
    QUALITY_PILOT_CAMPAIGN_SCHEMA_VERSION,
    QUALITY_PILOT_CAPTURE_RUN_RESULT_SCHEMA_VERSION,
    QUALITY_PILOT_CAPTURE_SPEC_SCHEMA_VERSION,
    QualityPilotCampaignSpec,
    QualityPilotCaptureRunner,
    QualityPilotCaptureRunnerError,
    QualityPilotCaptureRunResult,
    QualityPilotCaptureSpec,
    QualityPilotCollectionResult,
)
from tests.test_quality_pilot_canonical_response import (
    PILOT_RUN_ID,
    SESSION,
    _catalog_payload,
    _catalog_window,
    _instrument,
    _ohlcv_payload,
    _ohlcv_window,
    _quote,
    _quote_payload,
    _quote_window,
)
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter


BUCKET = "test-quality-pilot-bucket"


def _hash(label: str) -> str:
    return sha256(label.encode("utf-8")).hexdigest()


def _sessions() -> tuple[date, ...]:
    return tuple(SESSION - timedelta(days=19 - index) for index in range(20))


def _decision_ids(sessions: tuple[date, ...] | None = None) -> tuple[str, ...]:
    values = sessions or _sessions()
    return tuple(_hash(f"calendar-decision:{value.isoformat()}") for value in values)


def _campaign(**overrides) -> QualityPilotCampaignSpec:
    sessions = overrides.pop("confirmed_sessions", _sessions())
    calendar_decision_ids = overrides.pop(
        "calendar_decision_ids", _decision_ids() if sessions != _sessions() else _decision_ids(sessions)
    )
    kwargs = dict(
        pilot_run_id=PILOT_RUN_ID,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
        confirmed_sessions=sessions,
        calendar_decision_ids=calendar_decision_ids,
    )
    kwargs.update(overrides)
    return QualityPilotCampaignSpec(**kwargs)


def _spec(window: ObservationWindowSpec, **overrides) -> QualityPilotCaptureSpec:
    family = window.endpoint_family
    if family is EndpointFamily.CATALOG:
        requested_keys: tuple[str, ...] = ()
        token = None
    elif family is EndpointFamily.FULL_QUOTE:
        requested_keys = ("NSE:INFY",)
        token = None
    else:
        requested_keys = ("NSE:INFY",)
        token = 101
    kwargs = dict(
        campaign=_campaign(),
        window=window,
        provider=PROVIDER_ZERODHA_KITE,
        provider_version="kite-3.0",
        requested_keys=requested_keys,
        provider_instrument_token=token,
        chunk_index=1,
        chunk_count=1,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
    )
    kwargs.update(overrides)
    return QualityPilotCaptureSpec(**kwargs)


def _success_result(spec: QualityPilotCaptureSpec) -> QualityPilotCollectionResult:
    window = spec.window
    if window.endpoint_family is EndpointFamily.CATALOG:
        payload = _catalog_payload(window, (_instrument(),))
    elif window.endpoint_family is EndpointFamily.FULL_QUOTE:
        quotes = tuple(
            _quote(listing_key=key, token=index + 101, window=window)
            for index, key in enumerate(spec.requested_keys)
        )
        payload = _quote_payload(window, spec.requested_keys, quotes)
    else:
        payload = _ohlcv_payload(
            window,
            window.market_session,
            token=spec.provider_instrument_token,
        )
    return QualityPilotCollectionResult(
        request_started_at=window.opens_at,
        request_ended_at=window.closes_at,
        response_classification=ResponseClassification.SUCCESS,
        payload=payload,
    )


class FakeCollector:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[QualityPilotCaptureSpec] = []
        self.error: Exception | None = None

    def collect(self, spec: QualityPilotCaptureSpec) -> QualityPilotCollectionResult:
        self.calls.append(spec)
        if self.error is not None:
            raise self.error
        return self.result  # type: ignore[return-value]


def _run(
    spec: QualityPilotCaptureSpec,
    result: QualityPilotCollectionResult | None = None,
    *,
    writer: FakeStateObjectWriter | None = None,
):
    collector = FakeCollector(result or _success_result(spec))
    writer = writer or FakeStateObjectWriter()
    run = QualityPilotCaptureRunner().run(spec, collector, BUCKET, writer)
    return run, collector, writer


class CampaignTests(unittest.TestCase):
    def test_exact_twenty_session_campaign_is_deterministic(self) -> None:
        first = _campaign()
        with localcontext() as context:
            context.prec = 5
            second = _campaign()
        self.assertEqual(first.campaign_id, second.campaign_id)
        self.assertEqual(len(first.confirmed_sessions), CONFIRMED_SESSION_COUNT)
        first.verify_content_identity()

    def test_rejects_wrong_session_counts(self) -> None:
        for count in (19, 21):
            sessions = tuple(SESSION - timedelta(days=count - 1 - i) for i in range(count))
            with self.subTest(count=count), self.assertRaises(QualityPilotCaptureRunnerError):
                _campaign(confirmed_sessions=sessions, calendar_decision_ids=_decision_ids(sessions))

    def test_rejects_duplicate_reordered_and_foreign_session_values(self) -> None:
        sessions = _sessions()
        invalid = (
            sessions[:-1] + (sessions[-2],),
            tuple(reversed(sessions)),
            sessions[:-1] + ("2026-08-03",),
        )
        for value in invalid:
            with self.subTest(value=value[-1]), self.assertRaises(QualityPilotCaptureRunnerError):
                _campaign(confirmed_sessions=value, calendar_decision_ids=_decision_ids())

    def test_rejects_invalid_calendar_decision_ids(self) -> None:
        ids = _decision_ids()
        invalid = (ids[:-1], ids[:-1] + (ids[0],), ids[:-1] + (ids[-1].upper(),))
        for value in invalid:
            with self.subTest(length=len(value)), self.assertRaises(QualityPilotCaptureRunnerError):
                _campaign(calendar_decision_ids=value)

    def test_rejects_wrong_protocol_and_run_id(self) -> None:
        with self.assertRaises(QualityPilotCaptureRunnerError):
            _campaign(protocol_sha256="0" * 64)
        with self.assertRaises(QualityPilotCaptureRunnerError):
            _campaign(pilot_run_id="not-a-hash")

    def test_tampered_campaign_id_is_rejected(self) -> None:
        campaign = _campaign()
        object.__setattr__(campaign, "campaign_id", "0" * 64)
        with self.assertRaises(QualityPilotCaptureRunnerError):
            campaign.verify_content_identity()


class CaptureSpecTests(unittest.TestCase):
    def test_endpoint_specs_are_deterministic(self) -> None:
        for window in (_catalog_window(), _quote_window(), _ohlcv_window()):
            with self.subTest(window=window.window_kind):
                self.assertEqual(_spec(window).capture_spec_id, _spec(window).capture_spec_id)

    def test_daily_ohlcv_one_key_can_bind_full_session_chunk_route(self) -> None:
        spec = _spec(_ohlcv_window(), chunk_index=7, chunk_count=250)
        self.assertEqual((spec.chunk_index, spec.chunk_count), (7, 250))

    def test_provider_version_uses_canonical_text_ceiling_before_collection(self) -> None:
        self.assertEqual(MAXIMUM_TEXT_FIELD_LENGTH, 128)
        _spec(_catalog_window(), provider_version="x" * MAXIMUM_TEXT_FIELD_LENGTH)
        with self.assertRaises(QualityPilotCaptureRunnerError):
            _spec(_catalog_window(), provider_version="x" * (MAXIMUM_TEXT_FIELD_LENGTH + 1))

    def test_rejects_session_outside_campaign(self) -> None:
        outside = SESSION + timedelta(days=1)
        window = _catalog_window(session=outside)
        with self.assertRaises(QualityPilotCaptureRunnerError):
            _spec(window)

    def test_rejects_catalog_keys_token_or_non_single_chunk(self) -> None:
        window = _catalog_window()
        for overrides in (
            {"requested_keys": ("NSE:INFY",)},
            {"provider_instrument_token": 101},
            {"chunk_index": 1, "chunk_count": 2},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(QualityPilotCaptureRunnerError):
                _spec(window, **overrides)

    def test_rejects_quote_token_empty_reordered_duplicate_and_501_keys(self) -> None:
        window = _quote_window()
        keys_501 = tuple(f"NSE:S{index:04d}" for index in range(501))
        for overrides in (
            {"provider_instrument_token": 1},
            {"requested_keys": ()},
            {"requested_keys": ("NSE:TCS", "NSE:INFY")},
            {"requested_keys": ("NSE:INFY", "NSE:INFY")},
            {"requested_keys": keys_501},
        ):
            with self.subTest(kind=tuple(overrides)), self.assertRaises(QualityPilotCaptureRunnerError):
                _spec(window, **overrides)

    def test_accepts_500_quote_keys_and_rejects_bad_chunk_route(self) -> None:
        keys = tuple(f"NSE:S{index:04d}" for index in range(500))
        spec = _spec(_quote_window(), requested_keys=keys, chunk_index=2, chunk_count=3)
        self.assertEqual(len(spec.requested_keys), MAXIMUM_QUOTE_REQUEST_KEYS)
        for route in ((0, 1), (2, 1), (1, MAXIMUM_CHUNK_COUNT + 1)):
            with self.subTest(route=route), self.assertRaises(QualityPilotCaptureRunnerError):
                _spec(_quote_window(), chunk_index=route[0], chunk_count=route[1])

    def test_rejects_invalid_daily_key_and_token(self) -> None:
        for overrides in (
            {"requested_keys": ()},
            {"requested_keys": ("NSE:INFY", "NSE:TCS")},
            {"provider_instrument_token": None},
            {"provider_instrument_token": True},
            {"provider_instrument_token": 0},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(QualityPilotCaptureRunnerError):
                _spec(_ohlcv_window(), **overrides)

    def test_tampered_spec_id_is_rejected_before_collector(self) -> None:
        spec = _spec(_catalog_window())
        object.__setattr__(spec, "capture_spec_id", "0" * 64)
        collector = FakeCollector(object())
        with self.assertRaises(QualityPilotCaptureRunnerError):
            QualityPilotCaptureRunner().run(spec, collector, BUCKET, FakeStateObjectWriter())
        self.assertEqual(collector.calls, [])


class CollectionResultTests(unittest.TestCase):
    def test_rejects_naive_or_reversed_timestamps(self) -> None:
        window = _catalog_window()
        for start, end in (
            (datetime(2026, 8, 3, 9, 0), window.closes_at),
            (window.closes_at, window.opens_at),
        ):
            with self.subTest(start=start), self.assertRaises(QualityPilotCaptureRunnerError):
                QualityPilotCollectionResult(start, end, ResponseClassification.CATALOG_GAP, None)

    def test_rejects_success_without_exact_payload_and_gap_with_payload(self) -> None:
        window = _catalog_window()
        payload = _catalog_payload(window, (_instrument(),))
        with self.assertRaises(QualityPilotCaptureRunnerError):
            QualityPilotCollectionResult(
                window.opens_at, window.closes_at, ResponseClassification.SUCCESS, None
            )
        with self.assertRaises(QualityPilotCaptureRunnerError):
            QualityPilotCollectionResult(
                window.opens_at, window.closes_at, ResponseClassification.CATALOG_GAP, payload
            )

    def test_malicious_timezone_is_sanitized(self) -> None:
        secret = "SECRET-TIMEZONE-PATH/C:/private/token"

        class HostileTimezone(tzinfo):
            def utcoffset(self, dt):
                raise RuntimeError(secret)

            def dst(self, dt):
                return timedelta(0)

        value = datetime(2026, 8, 3, 9, 0, tzinfo=HostileTimezone())
        with self.assertRaises(QualityPilotCaptureRunnerError) as context:
            QualityPilotCollectionResult(
                value, value, ResponseClassification.CATALOG_GAP, None
            )
        self.assertNotIn(secret, str(context.exception))
        self.assertIsNone(context.exception.__cause__)
        self.assertIsNone(context.exception.__context__)


class RunnerHappyPathTests(unittest.TestCase):
    def test_catalog_quote_and_ohlcv_publish_exactly_once(self) -> None:
        for window in (_catalog_window(), _quote_window(), _ohlcv_window()):
            with self.subTest(window=window.window_kind):
                spec = _spec(window)
                run, collector, writer = _run(spec)
                self.assertEqual(len(collector.calls), 1)
                self.assertEqual(len(writer.calls), 1)
                self.assertEqual(run.campaign_id, spec.campaign.campaign_id)
                self.assertEqual(run.capture_spec_id, spec.capture_spec_id)
                self.assertEqual(run.requested_bucket, BUCKET)
                self.assertEqual(run.observation.request.requested_keys, spec.requested_keys)
                self.assertEqual(run.published.observation_id, run.observation.observation_id)
                run.verify_content_identity()

    def test_ohlcv_preserves_range_token_and_multi_chunk_route(self) -> None:
        spec = _spec(_ohlcv_window(), chunk_index=7, chunk_count=250)
        run, _, _ = _run(spec)
        self.assertEqual(run.observation.request.requested_range_start, SESSION)
        self.assertEqual(run.observation.request.requested_range_end, SESSION)
        self.assertEqual((run.published.chunk_index, run.published.chunk_count), (7, 250))
        self.assertEqual(run.observation.payload.instrument_token, 101)

    def test_endpoint_compatible_gaps_are_published_with_lineage(self) -> None:
        cases = (
            (_catalog_window(), ResponseClassification.CATALOG_GAP),
            (_quote_window(), ResponseClassification.PARTIAL_KEY_COVERAGE),
            (_ohlcv_window(), ResponseClassification.PROVIDER_GAP),
            (_quote_window(), ResponseClassification.RATE_LIMITED),
        )
        for window, classification in cases:
            with self.subTest(classification=classification):
                spec = _spec(window)
                result = QualityPilotCollectionResult(
                    window.opens_at, window.closes_at, classification, None
                )
                run, collector, writer = _run(spec, result)
                self.assertEqual(run.observation.request.response_classification, classification)
                self.assertIsNone(run.observation.payload)
                self.assertEqual(len(collector.calls), 1)
                self.assertEqual(len(writer.calls), 1)


class RunnerRejectionTests(unittest.TestCase):
    def test_collector_exception_is_sanitized_and_never_writes(self) -> None:
        secret = "SECRET-COLLECTOR/C:/credentials.json"
        spec = _spec(_catalog_window())
        collector = FakeCollector(object())
        collector.error = RuntimeError(secret)
        writer = FakeStateObjectWriter()
        with self.assertRaises(QualityPilotCaptureRunnerError) as context:
            QualityPilotCaptureRunner().run(spec, collector, BUCKET, writer)
        self.assertNotIn(secret, str(context.exception))
        self.assertIsNone(context.exception.__cause__)
        self.assertIsNone(context.exception.__context__)
        self.assertEqual(len(collector.calls), 1)
        self.assertEqual(writer.calls, [])

    def test_foreign_and_subclassed_results_are_rejected(self) -> None:
        spec = _spec(_catalog_window())
        valid = _success_result(spec)

        class Subclass(QualityPilotCollectionResult):
            pass

        for result in (object(), Subclass(
            valid.request_started_at,
            valid.request_ended_at,
            valid.response_classification,
            valid.payload,
        )):
            writer = FakeStateObjectWriter()
            with self.subTest(result=type(result)), self.assertRaises(QualityPilotCaptureRunnerError):
                QualityPilotCaptureRunner().run(spec, FakeCollector(result), BUCKET, writer)
            self.assertEqual(writer.calls, [])

    def test_outside_window_and_incompatible_classification_do_not_write(self) -> None:
        spec = _spec(_catalog_window())
        cases = (
            QualityPilotCollectionResult(
                spec.window.opens_at - timedelta(seconds=1),
                spec.window.closes_at,
                ResponseClassification.CATALOG_GAP,
                None,
            ),
            QualityPilotCollectionResult(
                spec.window.opens_at,
                spec.window.closes_at,
                ResponseClassification.PROVIDER_GAP,
                None,
            ),
        )
        for result in cases:
            writer = FakeStateObjectWriter()
            with self.assertRaises(QualityPilotCaptureRunnerError):
                QualityPilotCaptureRunner().run(spec, FakeCollector(result), BUCKET, writer)
            self.assertEqual(writer.calls, [])

    def test_wrong_endpoint_payload_and_partial_quote_do_not_write(self) -> None:
        catalog = _spec(_catalog_window())
        quote_payload = _quote_payload(
            _quote_window(), ("NSE:INFY",), (_quote(listing_key="NSE:INFY", token=101, window=_quote_window()),)
        )
        wrong = QualityPilotCollectionResult(
            catalog.window.opens_at,
            catalog.window.closes_at,
            ResponseClassification.SUCCESS,
            quote_payload,
        )
        writer = FakeStateObjectWriter()
        with self.assertRaises(QualityPilotCaptureRunnerError):
            QualityPilotCaptureRunner().run(catalog, FakeCollector(wrong), BUCKET, writer)
        self.assertEqual(writer.calls, [])

        quote = _spec(_quote_window(), requested_keys=("NSE:INFY", "NSE:TCS"))
        partial_payload = _quote_payload(
            quote.window,
            ("NSE:INFY",),
            (_quote(listing_key="NSE:INFY", token=101, window=quote.window),),
        )
        partial = QualityPilotCollectionResult(
            quote.window.opens_at,
            quote.window.closes_at,
            ResponseClassification.SUCCESS,
            partial_payload,
        )
        writer = FakeStateObjectWriter()
        with self.assertRaises(QualityPilotCaptureRunnerError):
            QualityPilotCaptureRunner().run(quote, FakeCollector(partial), BUCKET, writer)
        self.assertEqual(writer.calls, [])

    def test_malicious_writer_result_is_sanitized(self) -> None:
        spec = _spec(_catalog_window())
        writer = FakeStateObjectWriter()
        writer.malicious_result = PublishedStateObject(
            object_name="wrong/path.json",
            generation=1,
            byte_count=1,
            sha256="0" * 64,
        )
        with self.assertRaises(QualityPilotCaptureRunnerError) as context:
            QualityPilotCaptureRunner().run(spec, FakeCollector(_success_result(spec)), BUCKET, writer)
        self.assertIsNone(context.exception.__cause__)
        self.assertIsNone(context.exception.__context__)
        self.assertEqual(len(writer.calls), 1)


class RunResultIntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = _spec(_catalog_window())
        self.run, _, _ = _run(self.spec)

    def _kwargs(self, **overrides) -> dict:
        values = dict(
            campaign=self.run.campaign,
            capture_spec=self.run.capture_spec,
            campaign_id=self.run.campaign_id,
            capture_spec_id=self.run.capture_spec_id,
            requested_bucket=self.run.requested_bucket,
            calendar_decision_id=self.run.calendar_decision_id,
            observation=self.run.observation,
            published=self.run.published,
        )
        values.update(overrides)
        return values

    def test_rejects_campaign_spec_calendar_and_bucket_mismatches(self) -> None:
        cases = (
            {"campaign_id": "0" * 64},
            {"capture_spec_id": "0" * 64},
            {"calendar_decision_id": "0" * 64},
            {"requested_bucket": "different-bucket"},
        )
        for values in cases:
            with self.subTest(values=values), self.assertRaises(QualityPilotCaptureRunnerError):
                QualityPilotCaptureRunResult(**self._kwargs(**values))

    def test_tampered_nested_ids_and_run_result_id_are_rejected(self) -> None:
        campaign = _campaign()
        object.__setattr__(campaign, "campaign_id", "0" * 64)
        with self.assertRaises(QualityPilotCaptureRunnerError):
            QualityPilotCaptureRunResult(**self._kwargs(campaign=campaign))

        object.__setattr__(self.run, "run_result_id", "0" * 64)
        with self.assertRaises(QualityPilotCaptureRunnerError):
            self.run.verify_content_identity()

    def test_rejects_valid_ohlcv_observation_for_a_different_provider_token(self) -> None:
        original_spec = _spec(_ohlcv_window(), provider_instrument_token=101)
        original_run, _, _ = _run(original_spec)
        different_spec = _spec(_ohlcv_window(), provider_instrument_token=202)
        decision_id = different_spec.campaign.calendar_decision_ids[
            different_spec.campaign.confirmed_sessions.index(SESSION)
        ]
        with self.assertRaises(QualityPilotCaptureRunnerError):
            QualityPilotCaptureRunResult(
                campaign=different_spec.campaign,
                capture_spec=different_spec,
                campaign_id=different_spec.campaign.campaign_id,
                capture_spec_id=different_spec.capture_spec_id,
                requested_bucket=BUCKET,
                calendar_decision_id=decision_id,
                observation=original_run.observation,
                published=original_run.published,
            )


class PostureAndCapabilityTests(unittest.TestCase):
    def test_all_public_values_have_fixed_quality_only_posture(self) -> None:
        spec = _spec(_catalog_window())
        result = _success_result(spec)
        run, _, _ = _run(spec, result)
        for value in (spec.campaign, spec, result, run):
            self.assertTrue(value.quality_only)
            for name in runner_module._POSTURE_NAMES:
                self.assertEqual(getattr(value, name), name == "quality_only")

    def test_versions_counts_protocol_and_ceilings_are_pinned(self) -> None:
        self.assertEqual(QUALITY_PILOT_CAMPAIGN_SCHEMA_VERSION, "quality_pilot_campaign_v1")
        self.assertEqual(QUALITY_PILOT_CAPTURE_SPEC_SCHEMA_VERSION, "quality_pilot_capture_spec_v1")
        self.assertEqual(
            QUALITY_PILOT_CAPTURE_RUN_RESULT_SCHEMA_VERSION,
            "quality_pilot_capture_run_result_v1",
        )
        self.assertEqual(CONFIRMED_SESSION_COUNT, 20)
        self.assertEqual(runner_module.PILOT_PROTOCOL_SHA256, PILOT_PROTOCOL_SHA256)
        self.assertEqual(runner_module.MAXIMUM_CHUNK_COUNT, MAXIMUM_CHUNK_COUNT)
        self.assertEqual(runner_module.MAXIMUM_QUOTE_REQUEST_KEYS, MAXIMUM_QUOTE_REQUEST_KEYS)
        self.assertEqual(runner_module.MAXIMUM_TEXT_FIELD_LENGTH, MAXIMUM_TEXT_FIELD_LENGTH)

    def test_ast_has_no_forbidden_capability(self) -> None:
        source = inspect.getsource(runner_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "pathlib", "socket", "subprocess", "requests", "urllib", "httpx",
            "google", "kiteconnect", "time", "sqlite3", "pickle", "shelve",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden_modules, set())
        lowered = source.lower()
        for token in (
            "datetime.now(", "utcnow(", "sleep(", "retry", ".list(", "list_blobs(",
            "select_latest(", "find_latest(", ".delete(", ".overwrite(",
            "fetch_instruments(", "fetch_full_quotes(",
            "fetch_daily_candle(", "place_order(", "run_paper_trade(", "generate_signal(",
        ):
            self.assertNotIn(token, lowered, msg=token)


if __name__ == "__main__":
    unittest.main()
