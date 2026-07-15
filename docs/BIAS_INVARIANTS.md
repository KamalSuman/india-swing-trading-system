# Bias and Research-Integrity Invariants

Status: **normative**. These rules apply to research backtests, shadow runs, pilot alerts, and production alerts. `MUST`, `MUST NOT`, and `SHOULD` have their usual requirements meaning. A run that violates a `MUST` is invalid and cannot be used for model selection or performance claims.

## Core contracts

Every decision is evaluated inside an immutable `DecisionContext` containing:

- `decision_time_utc` and `decision_time_ist`;
- `market_session` and `calendar_version`;
- `data_cutoff_time`;
- `data_snapshot_id` and content hash;
- `universe_snapshot_id`;
- `trial_id`, `model_bundle_id`, and source commit;
- execution-policy and cost-schedule versions.

Every external record MUST carry `event_time`, `knowledge_time`, `source`, and `content_hash`. `knowledge_time` is the earliest independently supportable time that the exact record was available to the strategy. For revised data, each vintage is a separate record with its own `knowledge_time`.

On missing timestamps, ambiguous provenance, stale data, unregistered configuration, or failed validation, the system MUST fail closed: emit no candidate or `NO TRADE`, record the reason, and never silently substitute current data.

## 1. No-lookahead invariants

### NL-01: Point-in-time eligibility

For a decision with cutoff `C`, every price bar, filing, fundamental, news item, macro observation, social post, corporate action, benchmark value, and model input MUST satisfy `knowledge_time <= C`. Financial-period end dates and article event dates are not substitutes for publication/availability timestamps.

Tests:

- `tests/test_no_lookahead.py::test_all_inputs_have_knowledge_time_at_or_before_cutoff`
- `tests/test_no_lookahead.py::test_period_end_does_not_replace_filing_publication_time`
- `tests/test_no_lookahead.py::test_undated_historical_news_is_rejected`
- `tests/test_no_lookahead.py::test_revised_macro_value_uses_cutoff_vintage`

### NL-02: Bar finality

An EOD bar is unavailable until the exchange session has closed and the configured ingestion/validation delay has elapsed. A strategy using session `D`'s close MUST NOT execute at that close; its earliest executable order time is in the next eligible session.

Tests:

- `tests/test_no_lookahead.py::test_eod_bar_is_unavailable_before_finalization_time`
- `tests/test_no_lookahead.py::test_close_based_signal_cannot_fill_at_same_close`

### NL-03: Feature-window bounds

Feature computation MUST receive a cutoff-bounded data view. Every rolling window, cross-sectional rank, benchmark value, normalization statistic, imputation value, and learned transform MUST be fitted or computed solely from records known by the cutoff. A feature implementation MUST NOT be able to query an unbounded global frame.

Tests:

- `tests/test_no_lookahead.py::test_feature_window_never_reads_rows_after_cutoff`
- `tests/test_no_lookahead.py::test_cross_section_contains_only_cutoff_eligible_rows`
- `tests/test_no_lookahead.py::test_scaler_is_fit_on_training_partition_only`
- `tests/test_no_lookahead.py::test_forward_fill_never_backfills_from_future_value`

### NL-04: Point-in-time corporate-action adjustment

Historical prices MUST NOT be adjusted using splits, dividends, symbol mappings, or other corporate actions that were not known at the decision cutoff. Research MAY use point-in-time adjustment factors or raw prices plus contemporaneously known actions; it MUST NOT use a present-day fully adjusted series without proving point-in-time equivalence.

Tests:

- `tests/test_no_lookahead.py::test_future_split_does_not_change_preannouncement_features`
- `tests/test_no_lookahead.py::test_adjustment_factor_is_versioned_by_knowledge_time`

### NL-05: Label and outcome isolation

Forward returns, stops, targets, trade outcomes, and post-trade reflections are labels. They MUST be inaccessible to feature generation and decision agents until the entire label horizon has elapsed in the simulated clock. A ten-session label for a decision on `D` cannot mature before the close of the tenth eligible session after `D`.

Tests:

- `tests/test_no_lookahead.py::test_pending_trade_outcome_is_not_resolved_before_horizon_matures`
- `tests/test_no_lookahead.py::test_reflection_memory_excludes_future_trade_outcomes`
- `tests/test_no_lookahead.py::test_simulated_clock_blocks_wall_clock_future_fetches`

### NL-06: Purged temporal splits

Training, validation, and test partitions MUST be chronological. A training example is excluded when its label interval overlaps a later partition. The split MUST apply a purge/embargo at least as long as the maximum forward-label horizon; for the initial swing model this is ten trading sessions.

Tests:

- `tests/test_no_lookahead.py::test_training_labels_do_not_overlap_validation_window`
- `tests/test_no_lookahead.py::test_ten_session_embargo_is_enforced`
- `tests/test_no_lookahead.py::test_random_cross_validation_is_rejected_for_time_series_trial`

## 2. Survivorship and universe invariants

### SV-01: Point-in-time universe

The eligible universe for session `D` MUST be reconstructed from a dated security master and dated eligibility inputs. Present-day listings, index constituents, tickers, or liquidity ranks MUST NOT be projected backward. Listed, delisted, suspended, renamed, merged, and failed securities remain in historical snapshots when they were eligible at the time.

Tests:

- `tests/test_survivorship.py::test_historical_universe_does_not_use_current_listing_set`
- `tests/test_survivorship.py::test_delisted_security_remains_in_prior_universe_snapshot`
- `tests/test_survivorship.py::test_future_index_membership_is_not_backfilled`
- `tests/test_survivorship.py::test_liquidity_filter_uses_only_trailing_cutoff_data`

### SV-02: Stable instrument identity

All joins MUST use a stable instrument identifier with validity-dated ticker and exchange mappings. Ticker reuse or renaming MUST NOT merge two economic instruments or split one instrument incorrectly.

Tests:

- `tests/test_survivorship.py::test_symbol_change_preserves_stable_instrument_identity`
- `tests/test_survivorship.py::test_reused_ticker_does_not_join_distinct_instruments`

### SV-03: Untradeable outcomes remain observable

Suspensions, missing quotes, lower circuits, delistings, and unsuccessful exits MUST NOT cause observations or losing trades to disappear. The registered execution policy MUST define conservative marking and eventual liquidation behavior; exclusions after signal generation are prohibited unless recorded as execution failures.

Tests:

- `tests/test_survivorship.py::test_suspended_position_is_not_dropped_from_equity_curve`
- `tests/test_survivorship.py::test_lower_circuit_prevents_assumed_stop_fill`
- `tests/test_survivorship.py::test_delisting_applies_registered_conservative_exit_rule`
- `tests/test_survivorship.py::test_post_signal_missing_quote_is_not_reclassified_as_no_trade`

## 3. Execution-timing invariants

### EX-01: Explicit order lifecycle

Every simulated or live order MUST record `signal_time`, `submit_time`, `first_eligible_fill_time`, `expiry_time`, order type, quantity, and price constraints. `submit_time >= signal_time`, and no fill may precede `first_eligible_fill_time`.

Tests:

- `tests/test_execution_timing.py::test_order_cannot_fill_before_submission`
- `tests/test_execution_timing.py::test_after_close_signal_first_fills_next_eligible_session`
- `tests/test_execution_timing.py::test_expired_signal_cannot_fill`

### EX-02: Deterministic next-session fills

For a next-session buy limit `L` using daily bars:

- if `open <= L`, the pre-slippage fill is the open;
- if `open > L` and `low <= L`, the pre-slippage fill is `L`;
- otherwise there is no fill.

Slippage can only worsen the executable price, subject to the order constraint. A protective sell stop that is gapped through fills no better than the next executable open. Volume and circuit constraints may further delay or reduce a fill.

Tests:

- `tests/test_execution_timing.py::test_buy_limit_gapped_below_fills_at_open`
- `tests/test_execution_timing.py::test_buy_limit_touched_intraday_fills_at_limit`
- `tests/test_execution_timing.py::test_untouched_buy_limit_does_not_fill`
- `tests/test_execution_timing.py::test_sell_stop_gap_fills_no_better_than_open`
- `tests/test_execution_timing.py::test_slippage_never_improves_fill_price`

### EX-03: Intrabar ambiguity is pessimistic

When a daily bar touches both stop and target and their ordering is unknown, the engine MUST use registered higher-frequency evidence or the adverse ordering. It MUST NOT choose the profitable ordering.

Tests:

- `tests/test_execution_timing.py::test_same_bar_stop_and_target_uses_adverse_ordering`
- `tests/test_execution_timing.py::test_intraday_evidence_must_predate_resolution_decision`

### EX-04: Portfolio-state causality

Sizing MUST use cash, holdings, and open risk as known immediately before order submission. Long-only runs MUST reject negative inventory, leverage, averaging down when prohibited, and reuse of unsettled or otherwise unavailable proceeds under the registered settlement policy.

Tests:

- `tests/test_execution_timing.py::test_position_size_uses_preorder_portfolio_state`
- `tests/test_execution_timing.py::test_long_only_engine_rejects_negative_inventory`
- `tests/test_execution_timing.py::test_order_rejected_when_cash_or_open_risk_limit_is_exceeded`
- `tests/test_execution_timing.py::test_settlement_policy_is_date_versioned`

### EX-05: Calendar correctness

All horizons, embargoes, expiries, and holding periods MUST use a versioned NSE trading calendar rather than calendar-day arithmetic. Early closes, holidays, and unscheduled closures MUST be represented explicitly.

Tests:

- `tests/test_execution_timing.py::test_holding_horizon_counts_trading_sessions_not_calendar_days`
- `tests/test_execution_timing.py::test_holiday_does_not_create_phantom_fill_bar`
- `tests/test_execution_timing.py::test_calendar_version_is_recorded_in_run_manifest`

## 4. Cost invariants

### CO-01: Complete, dated Indian-equity cost schedule

Every fill MUST use a versioned schedule effective on the trade date. The schedule MUST model all applicable brokerage, STT, exchange transaction charges, regulatory turnover fees, GST, stamp duty, depository/DP charges, and registered slippage/market-impact assumptions, including correct buy/sell applicability and rounding.

Tests:

- `tests/test_costs.py::test_cost_schedule_selected_by_trade_date`
- `tests/test_costs.py::test_buy_and_sell_legs_apply_correct_charge_components`
- `tests/test_costs.py::test_dp_charge_applies_once_per_eligible_sell_instruction`
- `tests/test_costs.py::test_tax_bases_and_rounding_match_registered_schedule`

### CO-02: Net-first performance accounting

Cash and equity curves MUST deduct costs at the fill where they arise. The canonical P&L and all selection metrics are net of costs. Gross results may be shown only when clearly labeled alongside net results. Costs MUST use executed quantity, never desired quantity, and must not be double counted.

Tests:

- `tests/test_costs.py::test_net_pnl_equals_gross_pnl_less_itemized_costs`
- `tests/test_costs.py::test_costs_use_filled_not_requested_quantity`
- `tests/test_costs.py::test_costs_are_not_double_counted_on_round_trip`
- `tests/test_costs.py::test_model_selection_defaults_to_net_metrics`

### CO-03: No free execution assumption

Zero slippage or zero statutory costs are forbidden for reportable trials except a unit test explicitly labeled `synthetic`. Every confirmatory trial MUST include the base cost schedule and a pre-registered stressed-slippage scenario. Models and baselines MUST receive identical execution and cost treatment.

Tests:

- `tests/test_costs.py::test_reportable_trial_rejects_zero_cost_schedule`
- `tests/test_costs.py::test_confirmatory_trial_requires_stressed_slippage_scenario`
- `tests/test_costs.py::test_strategy_and_baseline_share_execution_cost_policy`

## 5. Trial-registry invariants

### TR-01: Register before evaluation

Every research run MUST receive a `trial_id` before evaluation begins. Its immutable registration records:

- exploratory or confirmatory status and hypothesis;
- strategy family and parent trial;
- universe, dates, splits, label horizon, and benchmark;
- primary and secondary metrics;
- execution/cost assumptions;
- model bundle, data snapshot, code, dependency, and configuration hashes;
- exclusions, risk limits, and pass/fail thresholds;
- multiple-testing policy and seed/repetition protocol.

Tests:

- `tests/test_trial_registry.py::test_evaluation_cannot_start_without_registered_trial`
- `tests/test_trial_registry.py::test_trial_registration_contains_required_hashes_and_thresholds`
- `tests/test_trial_registry.py::test_confirmatory_trial_declares_primary_metric_before_run`

### TR-02: Append-only history

Registrations and outcomes MUST be append-only. Failed, negative, aborted, and invalidated trials remain queryable. A changed parameter, prompt, dataset, cost rule, or code version creates a new `trial_id` linked by `parent_trial_id`; existing outcomes are never overwritten.

Tests:

- `tests/test_trial_registry.py::test_completed_trial_cannot_be_mutated_or_deleted`
- `tests/test_trial_registry.py::test_changed_configuration_requires_new_trial_id`
- `tests/test_trial_registry.py::test_failed_and_negative_trials_remain_visible`

### TR-03: Holdout access control

A confirmatory holdout MUST be sealed before model selection. Any access to its features, labels, or results is logged. After first unsealing, changes informed by the result require a new untouched holdout or the resulting run is relabeled exploratory.

Tests:

- `tests/test_trial_registry.py::test_holdout_labels_are_inaccessible_before_unseal`
- `tests/test_trial_registry.py::test_holdout_access_is_audited`
- `tests/test_trial_registry.py::test_post_unseal_retuning_cannot_remain_confirmatory`

### TR-04: Multiple trials are part of the result

All variants sharing a research question MUST share a `strategy_family_id`. Promotion decisions MUST account for the registered number of attempted variants through a predeclared correction or a genuinely untouched final holdout. Cherry-picking one run while hiding siblings is prohibited.

Tests:

- `tests/test_trial_registry.py::test_strategy_family_includes_all_attempted_variants`
- `tests/test_trial_registry.py::test_promotion_requires_registered_multiple_testing_policy_or_untouched_holdout`

## 6. Frozen-model invariants

### FM-01: Content-addressed model bundle

Every decision MUST reference an immutable `model_bundle_id` derived from the complete decision stack, including:

- Kronos code, weights, tokenizer, inference parameters, and artifact digests;
- feature schema, transforms, calibration, ensemble weights, thresholds, and risk rules;
- TradingAgents code, prompts, tool schemas, selected agents, debate depth, and memory policy;
- LLM provider, model identifier/API revision when available, sampling parameters, and cached raw responses;
- dependency lock, source commit, and runtime/container digest.

Tests:

- `tests/test_frozen_model.py::test_model_bundle_id_changes_when_any_decision_component_changes`
- `tests/test_frozen_model.py::test_production_decision_references_registered_model_bundle`
- `tests/test_frozen_model.py::test_kronos_weights_and_tokenizer_digests_are_pinned`
- `tests/test_frozen_model.py::test_prompt_or_tool_schema_change_requires_new_bundle`

### FM-02: Freeze during a registered pilot or holdout

The bundle, universe rules, cost model, execution model, and acceptance threshold MUST remain fixed for the full registered evaluation. A bug fix may invalidate the trial and start a successor; it MUST NOT rewrite prior decisions or backfill them under the old identity.

Tests:

- `tests/test_frozen_model.py::test_bundle_cannot_change_mid_trial`
- `tests/test_frozen_model.py::test_bugfix_creates_successor_trial_without_rewriting_history`
- `tests/test_frozen_model.py::test_historical_alert_is_not_backfilled_after_model_change`

### FM-03: Honest handling of nondeterministic LLMs

An external LLM is not considered reproducibly frozen merely because its model name and temperature are fixed. Historical evaluation MUST cache the exact request and raw response, and confirmatory trials MUST pre-register repetition/aggregation rules. Replaying a recorded trial uses the cached response; a fresh provider call is a new run.

Tests:

- `tests/test_frozen_model.py::test_llm_replay_uses_cached_request_response_pair`
- `tests/test_frozen_model.py::test_fresh_llm_call_receives_new_run_id`
- `tests/test_frozen_model.py::test_confirmatory_llm_trial_has_preregistered_repetition_rule`

### FM-04: No hidden online mutation

Production and evaluation inference MUST NOT refit scalers, update weights, alter prompts, learn from outcomes, or modify memory unless the registered bundle explicitly defines a causal online-learning protocol. Any permitted update produces a new version and may consume only matured outcomes.

Tests:

- `tests/test_frozen_model.py::test_inference_does_not_mutate_model_or_transform_state`
- `tests/test_frozen_model.py::test_online_update_consumes_only_matured_outcomes`
- `tests/test_frozen_model.py::test_online_update_emits_new_bundle_version`

## 7. Audit and replay invariants

### AU-01: Immutable decision manifest

Every scan and alert MUST write an append-only manifest containing the full `DecisionContext`, candidate universe, exclusion reasons, input hashes, feature/forecast hashes, raw component outputs, ensemble result, risk-gate result, final action including `NO TRADE`, and notification status. Manual fills and exits are later linked as separate events and never overwrite the original alert.

Tests:

- `tests/test_audit.py::test_scan_manifest_contains_complete_decision_lineage`
- `tests/test_audit.py::test_no_trade_decision_is_audited`
- `tests/test_audit.py::test_manual_fill_is_linked_without_mutating_alert`
- `tests/test_audit.py::test_candidate_exclusions_include_machine_readable_reason`

### AU-02: Tamper evidence and ordering

Audit events MUST have immutable IDs, UTC and IST timestamps, monotonic sequence numbers, actor/service identity, and a previous-event hash or equivalent immutable-storage guarantee. Duplicate or out-of-order writes MUST be rejected or explicitly reconciled.

Tests:

- `tests/test_audit.py::test_event_hash_chain_detects_mutation`
- `tests/test_audit.py::test_event_sequence_rejects_duplicate_or_out_of_order_write`
- `tests/test_audit.py::test_audit_event_records_utc_ist_and_actor_identity`

### AU-03: Deterministic replay boundary

Given a manifest and retained artifacts, all deterministic stages from input validation through final risk gating MUST replay bit-for-bit. Nondeterministic or external stages replay from their recorded raw outputs. A replay mismatch invalidates the affected run until explained.

Tests:

- `tests/test_audit.py::test_manifest_replay_reproduces_final_decision`
- `tests/test_audit.py::test_replay_performs_no_unregistered_network_access`
- `tests/test_audit.py::test_replay_mismatch_marks_run_invalid`

### AU-04: No silent fallback

Vendor failures, stale bars, missing corporate actions, LLM errors, parsing failures, and model timeouts MUST appear as typed audit events. A fallback is allowed only when it was pre-registered, uses cutoff-eligible data, and records both the failure and chosen path.

Tests:

- `tests/test_audit.py::test_vendor_failure_cannot_silently_change_data_source`
- `tests/test_audit.py::test_stale_input_forces_no_trade_and_typed_reason`
- `tests/test_audit.py::test_registered_fallback_preserves_cutoff_and_lineage`

### AU-05: Secrets and retention

Audit artifacts MUST NOT contain broker tokens, API keys, session cookies, or unredacted credentials. Access is least-privilege and logged. The retention policy MUST preserve trial registrations, manifests, inputs required for replay, alerts, and outcomes for the declared audit period.

Tests:

- `tests/test_audit.py::test_audit_payload_redacts_known_secret_patterns`
- `tests/test_audit.py::test_audit_read_access_is_logged`
- `tests/test_audit.py::test_retention_policy_preserves_replay_artifacts`

## CI release gate

A research or production release MUST fail CI if any invariant test fails. Waivers are not environment variables or skipped tests: they require a dated registry event naming the invariant, scope, owner, reason, expiry, and the fact that affected results are non-confirmatory. No waiver may permit future information, deletion of failed trials, or mutation of historical alerts.

Tests:

- `tests/test_bias_release_gate.py::test_invariant_suite_is_mandatory_in_ci`
- `tests/test_bias_release_gate.py::test_waiver_is_scoped_dated_and_audited`
- `tests/test_bias_release_gate.py::test_nonwaivable_integrity_rules_reject_waiver`
