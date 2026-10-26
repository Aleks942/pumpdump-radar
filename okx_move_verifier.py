"""Strict sequential verification for OKX-detected PumpDump moves.

Uses the existing OKX swap trade WebSocket, aggregated OI collector and the
targeted cross-exchange Spot fallback. It mirrors the Bybit verifier's
NOW/1m/3m/5m discipline and sends Telegram only on a confirmed 5m result.

No order placement. Missing data fail closed.
"""
import math
import threading
import time

from aggregated_oi import get_aggregated_oi, watch_symbol
from bybit_move_verifier import classify, build_message
from liquidation_engine import (
    fetch_okx_liquidations,
    get_liquidation_summary,
    watch_bybit_symbol,
)
from market_data_v3 import get_candles, get_funding, get_ticker
from pattern_detector import detect_pattern
from move_reversal_maturity import (
    FOLLOWUP_INTERVAL_SEC,
    FOLLOWUP_TTL_SEC,
    build_cancel_message as build_reversal_cancel_message,
    build_confirmed_message as build_reversal_confirmed_message,
    build_pre_message as build_reversal_pre_message,
    evaluate as evaluate_reversal,
    reversal_from_extreme_pct,
    update_extreme as update_reversal_extreme,
)
from spot_fallback_stream import (
    get_flow as get_fallback_spot_flow,
    unwatch_symbol as unwatch_fallback_spot,
    watch_symbol as watch_fallback_spot,
)
from trade_flow_collector_v3 import get_futures_windows


CHECKPOINTS = ((2.0, "NOW"), (60.0, "1m"), (180.0, "3m"), (300.0, "5m"))
SYMBOL_COOLDOWN_SEC = 10 * 60
FLOW_STALE_SEC = 45.0
MIN_TRADES = 15
_lock = threading.Lock()
_last_started = {}


def _finite(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _okx_oi_from_aggregate(agg_5m):
    source = ((agg_5m or {}).get("sources") or {}).get("OKX") or {}
    value = _finite(source.get("change_pct"))
    ready = source.get("quality") == "READY" and value is not None
    return {
        "quality": "READY" if ready else source.get("quality", "UNAVAILABLE"),
        "ready": ready,
        "change_pct": value,
        "period_sec": source.get("period_sec"),
        "age_sec": source.get("age_sec"),
        "oi": source.get("oi_coin"),
        "source": "OKX_AGGREGATED_OI",
    }


def _okx_futures_flow(symbol):
    row = ((get_futures_windows(symbol) or {}).get("5m") or {})
    imbalance = _finite(row.get("imbalance_pct"))
    trade_count = int(row.get("trade_count") or 0)
    total = _finite(row.get("total_quote")) or 0.0
    stale = _finite(row.get("last_trade_age_sec"))
    coverage = _finite(row.get("coverage_seconds")) or 0.0
    ready = (
        row.get("window_ready") is True
        and row.get("quality") == "READY"
        and imbalance is not None
        and trade_count >= MIN_TRADES
        and total > 0
        and stale is not None
        and 0 <= stale <= FLOW_STALE_SEC
        and coverage >= 240.0
    )
    return {
        "quality": "READY" if ready else row.get("quality", "UNAVAILABLE"),
        "ready": ready,
        "trade_count": trade_count,
        "coverage_sec": coverage,
        "last_trade_age_sec": stale,
        "buy_quote": row.get("buy_quote"),
        "sell_quote": row.get("sell_quote"),
        "delta_quote": row.get("delta_quote"),
        "imbalance_pct": imbalance,
        "source": "OKX_SWAP_WS",
    }


def _closed_trend(symbol, bar):
    try:
        rows = get_candles(symbol, bar=bar, limit=8) or []
        closes = [
            _finite(row.get("close"))
            for row in rows
            if isinstance(row, dict) and row.get("confirmed") is True
        ]
        closes = [value for value in closes if value is not None and value > 0]
        if len(closes) < 3:
            return {"quality": "WARMING", "ready": False, "trend": "UNKNOWN"}
        closes = closes[-3:]
        if closes[0] < closes[1] < closes[2]:
            trend = "UP"
        elif closes[0] > closes[1] > closes[2]:
            trend = "DOWN"
        else:
            trend = "MIXED"
        return {
            "quality": "READY",
            "ready": True,
            "trend": trend,
            "closes": closes,
        }
    except Exception as exc:
        return {
            "quality": "UNAVAILABLE",
            "ready": False,
            "trend": "UNKNOWN",
            "error": type(exc).__name__,
        }


def _latest_price(symbol):
    try:
        ticker = get_ticker(symbol) or {}
        value = _finite(ticker.get("price"))
        return value if value is not None and value > 0 else None
    except Exception:
        return None


def _funding(symbol):
    try:
        row = get_funding(symbol)
        value = _finite((row or {}).get("rate_pct"))
        if value is None:
            raise ValueError("OKX_FUNDING_MISSING")
        return {"quality": "READY", "ready": True, "rate_pct": value}
    except Exception as exc:
        return {
            "quality": "UNAVAILABLE",
            "ready": False,
            "error": type(exc).__name__,
        }


def collect(signal, now=None):
    now = time.time() if now is None else float(now)
    symbol = str(signal.get("symbol") or "").upper()
    kind = str(signal.get("type") or "").upper()
    change = _finite(signal.get("change"))
    trigger_price = _finite(signal.get("price"))
    if (
        not symbol.endswith("USDT")
        or kind not in ("PUMP", "DUMP")
        or change is None
        or trigger_price is None
        or trigger_price <= 0
    ):
        return {"status": "INVALID"}

    watch_symbol(symbol)
    watch_bybit_symbol(symbol)

    aggregated = get_aggregated_oi(symbol)
    agg_5m = (aggregated.get("windows") or {}).get("5m") or {}
    oi = _okx_oi_from_aggregate(agg_5m)
    futures = _okx_futures_flow(symbol)
    spot = get_fallback_spot_flow(
        symbol,
        now=now,
        since=_finite(signal.get("_verification_started_at")),
    )
    funding = _funding(symbol)
    trend_5m = _closed_trend(symbol, "5m")
    trend_60m = _closed_trend(symbol, "1H")
    current_price = _latest_price(symbol)

    try:
        fetch_okx_liquidations(symbol)
    except Exception:
        pass
    liquidations = get_liquidation_summary(symbol)

    spot_value = spot.get("imbalance_pct") if spot.get("ready") else None
    if oi.get("ready") and futures.get("ready"):
        pattern = detect_pattern(
            price_change=change,
            oi_change=oi.get("change_pct"),
            futures_cvd=futures.get("imbalance_pct") or 0.0,
            spot_cvd=spot_value,
            delta=futures.get("delta_quote") or 0.0,
            long_liquidations=liquidations.get("long_liq", 0.0),
            short_liquidations=liquidations.get("short_liq", 0.0),
        )
    else:
        pattern = {
            "pattern": "NONE",
            "direction": "NONE",
            "reason": "OKX OI/Futures 5m data not fully ready",
        }

    expected = "UP" if kind == "PUMP" else "DOWN"
    post_trigger_pct = (
        (current_price / trigger_price - 1.0) * 100.0
        if current_price is not None else None
    )

    result = {
        "status": "OK",
        "symbol": symbol,
        "type": kind,
        "change": change,
        "trigger_price": trigger_price,
        "current_price": current_price,
        "post_trigger_pct": post_trigger_pct,
        "window": signal.get("window"),
        "exchange": "OKX",
        "oi": oi,
        "futures": futures,
        "spot": spot,
        "funding": funding,
        "trend_5m": trend_5m,
        "trend_60m": trend_60m,
        "liquidations": liquidations,
        "aggregated_oi": aggregated,
        "agg_5m": agg_5m,
        "pattern": pattern,
        "trend_aligned": trend_5m.get("trend") == expected,
        "trade_approved": False,
        "research_only": True,
    }
    result["verdict"] = classify(result)
    return result


def schedule(signal, sender, now=None):
    if str(signal.get("exchange") or "").upper() != "OKX":
        return False
    symbol = str(signal.get("symbol") or "").upper()
    kind = str(signal.get("type") or "").upper()
    if not symbol.endswith("USDT") or kind not in ("PUMP", "DUMP"):
        return False

    at = time.time() if now is None else float(now)
    key = (symbol, kind)
    with _lock:
        if at - _last_started.get(key, -float("inf")) < SYMBOL_COOLDOWN_SEC:
            return False
        _last_started[key] = at

    watch_symbol(symbol)
    watch_bybit_symbol(symbol)
    watch_fallback_spot(symbol, started_at=at)
    verification_signal = dict(signal)
    verification_signal["_verification_started_at"] = at

    def worker():
        extreme = _finite(signal.get("price"))
        pre_active = False
        pre_notified = False
        last_result = None
        last_maturity = None
        weak_followups = 0

        def inspect_reversal(result, checkpoint):
            nonlocal extreme, pre_active, pre_notified, last_result, last_maturity
            previous_extreme = extreme
            extreme, reclaimed = update_reversal_extreme(
                kind,
                result.get("current_price"),
                extreme,
                pre_active=pre_active,
            )
            result["reversal_extreme_price"] = extreme
            result["reversal_from_extreme_pct"] = reversal_from_extreme_pct(
                kind,
                result.get("current_price"),
                extreme,
            )
            maturity = evaluate_reversal(result)
            last_result = result
            last_maturity = maturity

            if pre_active and reclaimed:
                if pre_notified:
                    sender(
                        build_reversal_cancel_message(
                            result, maturity, "NEW_EXTREME"
                        ),
                        symbol=symbol,
                    )
                print(
                    "[OKX_REVERSAL_CANCELLED]",
                    symbol,
                    "checkpoint=", checkpoint,
                    "reason=NEW_EXTREME",
                    "old_extreme=", previous_extreme,
                    "new_extreme=", extreme,
                    flush=True,
                )
                return "STOP", maturity

            if maturity.get("code") == "CONFIRMED":
                sent = sender(
                    build_reversal_confirmed_message(
                        result, maturity, checkpoint
                    ),
                    symbol=symbol,
                )
                print(
                    "[OKX_REVERSAL_CONFIRMED]",
                    symbol,
                    "checkpoint=", checkpoint,
                    "sent=", sent is True,
                    "strong_score=", maturity.get("strong_score"),
                    "reversal_pct=", maturity.get("reversal_pct"),
                    flush=True,
                )
                if sent is not False:
                    return "CONFIRMED", maturity
                pre_active = True
                return "CONTINUE", maturity

            if maturity.get("code") == "PRE":
                pre_active = True
                if not pre_notified:
                    sent = sender(
                        build_reversal_pre_message(
                            result, maturity, checkpoint
                        ),
                        symbol=symbol,
                    )
                    if sent is not False:
                        pre_notified = True
                print(
                    "[OKX_REVERSAL_PRE]",
                    symbol,
                    "checkpoint=", checkpoint,
                    "early_score=", maturity.get("early_score"),
                    "strong_score=", maturity.get("strong_score"),
                    "missing=", ",".join(maturity.get("missing") or []),
                    flush=True,
                )
            elif pre_active and maturity.get("code") == "BROKEN":
                if pre_notified:
                    sender(
                        build_reversal_cancel_message(
                            result, maturity, "BROKEN"
                        ),
                        symbol=symbol,
                    )
                print(
                    "[OKX_REVERSAL_CANCELLED]",
                    symbol,
                    "checkpoint=", checkpoint,
                    "reason=BROKEN",
                    flush=True,
                )
                return "STOP", maturity

            return "CONTINUE", maturity

        try:
            for target_sec, checkpoint in CHECKPOINTS:
                delay = max(0.0, at + target_sec - time.time())
                if delay:
                    time.sleep(delay)
                result = collect(verification_signal, now=time.time())
                if result.get("status") != "OK":
                    print("[OKX_VERIFY_INVALID]", symbol, checkpoint, flush=True)
                    return

                reversal_action, maturity = inspect_reversal(result, checkpoint)
                if reversal_action in ("STOP", "CONFIRMED"):
                    return

                verdict_code = (result.get("verdict") or {}).get("code")
                should_send = (
                    checkpoint == "5m"
                    and verdict_code == "CONFIRMED"
                    and not pre_active
                )
                sent = False
                if should_send:
                    sent = sender(
                        build_message(result, checkpoint=checkpoint),
                        symbol=symbol,
                    )
                print(
                    "[OKX_VERIFY_SENT]" if should_send else "[OKX_VERIFY_INTERNAL]",
                    symbol,
                    "checkpoint=", checkpoint,
                    "sent=", sent is True,
                    "verdict=", verdict_code,
                    "reversal=", maturity.get("code"),
                    "reversal_early=", maturity.get("early_score"),
                    "reversal_strong=", maturity.get("strong_score"),
                    "pattern=", (result.get("pattern") or {}).get("pattern"),
                    "oi_ready=", (result.get("oi") or {}).get("ready"),
                    "futures_ready=", (result.get("futures") or {}).get("ready"),
                    "futures_coverage_sec=", round(
                        float((result.get("futures") or {}).get("coverage_sec") or 0), 1
                    ),
                    "spot_ready=", (result.get("spot") or {}).get("ready"),
                    "spot_source=", (result.get("spot") or {}).get("source"),
                    "spot_coverage_sec=", round(
                        float((result.get("spot") or {}).get("coverage_sec") or 0), 1
                    ),
                    "agg_ready=", (result.get("agg_5m") or {}).get("ready"),
                    "agg_confidence=", (result.get("agg_5m") or {}).get("confidence"),
                    "agg_mode=", (result.get("agg_5m") or {}).get("consensus_mode"),
                    flush=True,
                )

            if not (
                pre_active
                or (
                    last_maturity
                    and last_maturity.get("continue_watch") is True
                )
            ):
                print(
                    "[OKX_REVERSAL_STOP]",
                    symbol,
                    "reason=NO_MATURING_EVIDENCE_AT_5M",
                    flush=True,
                )
                return

            next_target = 300.0 + FOLLOWUP_INTERVAL_SEC
            while next_target <= FOLLOWUP_TTL_SEC:
                delay = max(0.0, at + next_target - time.time())
                if delay:
                    time.sleep(delay)
                checkpoint = f"{int(next_target // 60)}m"
                result = collect(verification_signal, now=time.time())
                if result.get("status") != "OK":
                    return
                reversal_action, maturity = inspect_reversal(result, checkpoint)
                if reversal_action in ("STOP", "CONFIRMED"):
                    return

                if maturity.get("continue_watch") is True:
                    weak_followups = 0
                else:
                    weak_followups += 1
                    if pre_active and weak_followups >= 2:
                        if pre_notified:
                            sender(
                                build_reversal_cancel_message(
                                    result, maturity, "EVIDENCE_FADED"
                                ),
                                symbol=symbol,
                            )
                        print(
                            "[OKX_REVERSAL_CANCELLED]",
                            symbol,
                            "checkpoint=", checkpoint,
                            "reason=EVIDENCE_FADED",
                            flush=True,
                        )
                        return
                    if not pre_active and weak_followups >= 1:
                        return
                next_target += FOLLOWUP_INTERVAL_SEC

            if pre_active and last_result is not None and pre_notified:
                sender(
                    build_reversal_cancel_message(
                        last_result, last_maturity or {}, "TTL_EXPIRED"
                    ),
                    symbol=symbol,
                )
                print(
                    "[OKX_REVERSAL_CANCELLED]",
                    symbol,
                    "reason=TTL_EXPIRED",
                    flush=True,
                )
        except Exception as exc:
            print(
                "[OKX_VERIFY_ERROR]",
                symbol,
                type(exc).__name__,
                str(exc)[:120],
                flush=True,
            )
        finally:
            unwatch_fallback_spot(symbol)

    try:
        threading.Thread(
            target=worker,
            name="okx-verify-" + symbol,
            daemon=True,
        ).start()
        print(
            "[OKX_VERIFY_SCHEDULED]",
            symbol,
            kind,
            "checkpoints=NOW,1m,3m,5m internal; telegram=confirmed_5m_only; reversal=PRE+event_followup_to_15m",
            flush=True,
        )
        return True
    except Exception:
        unwatch_fallback_spot(symbol)
        with _lock:
            _last_started.pop(key, None)
        return False
