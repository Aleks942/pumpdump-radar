# ALEKS TRADING — MASTER STATUS

Updated: 2026-11-26 06:53 CET (Poland). Preliminary audit, not a completed strategy validation.

## Scope
PumpDump Radar only; other bots remain paused. SWING 1H / 4H / 1D, entry refinement 15m / 5m; WATCH → BREAKOUT → RETEST → ENTRY → RESULT. No real automated trading without separate approval. Research SHORT after pump: target 15% from confirmed entry, no lookahead. Preserve historical data; minimize costs.

## Verified infrastructure and runtime
- Railway charming-friendship, pumpdump-radar: deployment SUCCESS 2026-11-24 18:21 UTC; one EU replica, /data persistent volume.
- Runtime logs 2026-11-26: OKX HTTP 200, 542 instruments; rotation 500 + 42.
- Aggregated OI saved/requested: OKX 118/118, Bybit 116/116, Binance 61/61 (not full universe coverage).
- Spot CVD and delta observed from Binance; funding available from OKX for sampled instruments.
- Liquidations OBSERVED_PARTIAL; OKX unavailable for sampled ETHFI/BABY; zero observed events is not proof of no liquidations.
- Telegram API confirmed ETHFI 69326 (blocked, WAIT_RETEST), BABY 69327 (RETEST); no confirmed ENTRY in sampled logs.
- SWING compression_found=6, compression_duplicate=6; no new compression alert in sampled cycles.
- MARKET_MEMORY_PENDING=0 in sampled logs; historical completeness not yet verified.
- POST_PUMP local flow research-only; MON, OPG buyers still control; BIGTIME buy volume expansion; insufficient spot traded notional blocks SHORT approval for BIGTIME in sampled log.
- Other Railway services mostly scheduled 0 0 29 2 *; one Whale Scanner deployment FAILED. Actual runtime/billing of paused services and retained volumes not yet verified.

## Code inventory
Root includes swing_market_scanner, swing_alert_state, swing_result_tracker, swing_backtest, swing_replay_backtest, swing_trade_backtest; post_pump_short_research, post_pump_short_paper, post_pump_short_strategy, post_pump_short_evidence, pump_exit_research, pump_edge_report, pump_retrace_stats; multiple generations of chief trader and liquidation engines; numerous test files. Presence is not proof of active execution or passing tests.

## Open tasks (priority order)
1. Map active imports/configuration and deployed commit; audit signal state transitions and data quality, especially liquidation coverage, futures CVD, volumes and BTC context.
2. Retrieve persisted result/statistics and test outcomes; separate alerts, entries, blocked signals and outcomes; verify MARKET MEMORY history.
3. Audit year-long pump research and datasets; chronological out-of-sample test, fees/funding/slippage, MFE/MAE, 15% target from entry, failed setups, survivorship bias.
4. Validate SWING structural stop, 2R/3R, checkpoints 1/4/12/24/72h; report counts, win rate, profit factor, expectancy, drawdown, net R, LONG/SHORT and regimes separately.
5. Check actual Railway expenses and retained storage; avoid deletion without dependency/data checks.
6. Remove duplicate/dead logic only after dependency audit and tests.

## Statistics / tests / costs
Not verified in this audit. No invented metrics. No trading logic or Railway configuration changed.

## Next concrete task
Read project_map.py and main.py, compare actual deployed commit, identify live strategy paths and result store, then audit historical/statistical evidence.
