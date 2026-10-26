"""Sequential Bybit mover verification for PumpDump Radar.

A delivered Bybit price WATCH starts one research-only verification sequence:
NOW -> +1m -> +3m -> +5m.  Live publicTrade streams accumulate real taker
flow across the whole interval; REST data are only an early fallback.

This module never approves or places trades. Missing/partial data stay
UNKNOWN/PARTIAL and never become directional evidence by themselves.
"""
import math
import threading
import time
from html import escape

import requests

from aggregated_oi import get_aggregated_oi, watch_symbol
from bybit_taker_stream import (
    get_flow as get_live_flow,
    unwatch_symbol as unwatch_flow_symbol,
    watch_symbol as watch_flow_symbol,
)
from liquidation_engine import (
    fetch_okx_liquidations,
    get_liquidation_summary,
    watch_bybit_symbol,
)
from pattern_detector import detect_pattern
from move_reversal_maturity import (
    FOLLOWUP_INTERVAL_SEC,
    FOLLOWUP_TTL_SEC,
    build_cancel_message as build_reversal_cancel_message,
    build_confirmed_message as build_reversal_confirmed_message,
    build_early_message as build_reversal_early_message,
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


BYBIT_BASE = "https://api.bybit.com"
CHECKPOINTS = ((2.0, "NOW"), (60.0, "1m"), (180.0, "3m"), (300.0, "5m"))
SYMBOL_COOLDOWN_SEC = 10 * 60
REVERSAL_BREAK_PCT = 2.0
FLOW_CONFIRM_PCT = 10.0
SPOT_CONFIRM_PCT = 5.0
_lock = threading.Lock()
_last_started = {}


def _finite(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _bybit_json(path, params, request_get=None):
    request_get = request_get or requests.get
    response = request_get(BYBIT_BASE + path, params=params, timeout=6)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("retCode") != 0:
        raise ValueError("BYBIT_API_REJECTED")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("BYBIT_BAD_SCHEMA")
    return payload, result


def _latest_price(symbol, request_get=None):
    try:
        _, result = _bybit_json(
            "/v5/market/tickers",
            {"category": "linear", "symbol": symbol},
            request_get,
        )
        rows = result.get("list")
        row = rows[0] if isinstance(rows, list) and rows else None
        if not isinstance(row, dict) or row.get("symbol") != symbol:
            raise ValueError("BYBIT_TICKER_MISSING")
        value = _finite(row.get("lastPrice"))
        if value is None or value <= 0:
            raise ValueError("BYBIT_PRICE_MISSING")
        return value
    except Exception:
        return None


def _oi_5m(symbol, request_get=None, now=None):
    now = time.time() if now is None else float(now)
    try:
        _, result = _bybit_json(
            "/v5/market/open-interest",
            {
                "category": "linear",
                "symbol": symbol,
                "intervalTime": "5min",
                "limit": 4,
            },
            request_get,
        )
        rows = result.get("list")
        if not isinstance(rows, list):
            raise ValueError("BYBIT_BAD_OI_LIST")
        points = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = _finite(row.get("timestamp"))
            value = _finite(row.get("openInterest"))
            if ts is None or value is None or value <= 0:
                continue
            if ts > 100_000_000_000:
                ts /= 1000.0
            if -5 <= now - ts <= 15 * 60:
                points.append((ts, value))
        points.sort()
        if len(points) < 2:
            return {"quality": "WARMING", "ready": False}
        end_ts, current = points[-1]
        start_ts, previous = min(
            points[:-1],
            key=lambda item: abs((end_ts - item[0]) - 300.0),
        )
        period = end_ts - start_ts
        if not 240 <= period <= 420:
            return {
                "quality": "WINDOW_MISMATCH",
                "ready": False,
                "period_sec": period,
            }
        return {
            "quality": "READY",
            "ready": True,
            "change_pct": (current / previous - 1.0) * 100.0,
            "period_sec": period,
            "age_sec": max(0.0, now - end_ts),
            "oi": current,
        }
    except Exception as exc:
        return {
            "quality": "UNAVAILABLE",
            "ready": False,
            "error": type(exc).__name__,
        }


def _rest_trade_flow(symbol, category, request_get=None, now=None):
    """Early fallback only; never invents 5m coverage from 1000 recent trades."""
    now = time.time() if now is None else float(now)
    try:
        _, result = _bybit_json(
            "/v5/market/recent-trade",
            {"category": category, "symbol": symbol, "limit": 1000},
            request_get,
        )
        raw_rows = result.get("list")
        if not isinstance(raw_rows, list):
            raise ValueError("BYBIT_BAD_TRADES_LIST")
        rows = []
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            ts = _finite(row.get("time"))
            price = _finite(row.get("price"))
            size = _finite(row.get("size"))
            side = str(row.get("side") or "").upper()
            if ts is None or price is None or size is None:
                continue
            if ts > 100_000_000_000:
                ts /= 1000.0
            if side not in ("BUY", "SELL") or price <= 0 or size <= 0:
                continue
            if 0 <= now - ts <= 300:
                rows.append((ts, side, price * size))
        if not rows:
            return {
                "quality": "UNAVAILABLE",
                "ready": False,
                "trade_count": 0,
                "source": "BYBIT_RECENT_TRADES_REST",
            }
        newest = max(row[0] for row in rows)
        oldest = min(row[0] for row in rows)
        coverage = max(0.0, min(300.0, newest - oldest))
        stale = max(0.0, now - newest)
        buy = sum(row[2] for row in rows if row[1] == "BUY")
        sell = sum(row[2] for row in rows if row[1] == "SELL")
        total = buy + sell
        delta = buy - sell
        imbalance = 100.0 * delta / total if total > 0 else 0.0
        ready = coverage >= 240.0 and stale <= 30.0 and len(rows) >= 15
        return {
            "quality": "READY" if ready else "PARTIAL",
            "ready": ready,
            "trade_count": len(rows),
            "coverage_sec": coverage,
            "last_trade_age_sec": stale,
            "buy_quote": buy,
            "sell_quote": sell,
            "delta_quote": delta,
            "imbalance_pct": imbalance,
            "source": "BYBIT_RECENT_TRADES_REST",
        }
    except Exception as exc:
        return {
            "quality": "UNAVAILABLE",
            "ready": False,
            "trade_count": 0,
            "error": type(exc).__name__,
            "source": "BYBIT_RECENT_TRADES_REST",
        }


def _flow(symbol, category, request_get=None, now=None, since=None):
    """Futures use Bybit; Spot can fail over Bybit -> OKX -> Binance."""
    live = get_live_flow(symbol, category=category, now=now, since=since)

    if category == "spot":
        fallback = get_fallback_spot_flow(symbol, now=now, since=since)

        # Prefer a real Bybit Spot stream whenever that market exists.
        if live.get("ready") or (
            live.get("trade_count", 0) > 0
            and live.get("quality") == "PARTIAL"
        ):
            return live

        # If Bybit has no Spot pair (common for mover contracts), use a
        # real accumulated OKX/Binance Spot stream instead.
        if fallback.get("ready") or (
            fallback.get("trade_count", 0) > 0
            and fallback.get("quality") == "PARTIAL"
        ):
            return fallback

        # Preserve the most informative availability state. Missing data
        # remain UNKNOWN and never become confirmation.
        if fallback.get("quality") not in (
            "NOT_WATCHED", "CHECKING", "UNAVAILABLE"
        ):
            return fallback
        if live.get("quality") not in ("NOT_WATCHED", "WARMING"):
            return live
        return fallback if fallback else live

    if (
        live.get("trade_count", 0) > 0
        or live.get("quality") in ("READY", "PARTIAL")
    ):
        return live
    if since is not None:
        # Sequential checks must represent trades observed after this trigger,
        # not a pre-trigger REST snapshot.
        return live
    return _rest_trade_flow(
        symbol,
        category,
        request_get=request_get,
        now=now,
    )


def _funding(symbol, request_get=None):
    try:
        _, result = _bybit_json(
            "/v5/market/tickers",
            {"category": "linear", "symbol": symbol},
            request_get,
        )
        rows = result.get("list")
        row = rows[0] if isinstance(rows, list) and rows else None
        if not isinstance(row, dict) or row.get("symbol") != symbol:
            raise ValueError("BYBIT_TICKER_MISSING")
        rate = _finite(row.get("fundingRate"))
        if rate is None:
            raise ValueError("BYBIT_FUNDING_MISSING")
        return {"quality": "READY", "ready": True, "rate_pct": rate * 100.0}
    except Exception as exc:
        return {
            "quality": "UNAVAILABLE",
            "ready": False,
            "error": type(exc).__name__,
        }


def _closed_trend(symbol, interval, request_get=None, now=None):
    now = time.time() if now is None else float(now)
    minutes = int(interval)
    try:
        _, result = _bybit_json(
            "/v5/market/kline",
            {
                "category": "linear",
                "symbol": symbol,
                "interval": str(interval),
                "limit": 6,
            },
            request_get,
        )
        rows = result.get("list")
        if not isinstance(rows, list):
            raise ValueError("BYBIT_BAD_KLINE_LIST")
        candles = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 5:
                continue
            start = _finite(row[0])
            close = _finite(row[4])
            if start is None or close is None or close <= 0:
                continue
            if start > 100_000_000_000:
                start /= 1000.0
            if start + minutes * 60 <= now:
                candles.append((start, close))
        candles.sort()
        if len(candles) < 3:
            return {
                "quality": "WARMING",
                "ready": False,
                "trend": "UNKNOWN",
            }
        closes = [row[1] for row in candles[-3:]]
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


def collect(signal, request_get=None, now=None):
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

    since = _finite(signal.get("_verification_started_at"))
    oi = _oi_5m(symbol, request_get=request_get, now=now)
    futures = _flow(
        symbol, "linear", request_get=request_get, now=now, since=since
    )
    spot = _flow(
        symbol, "spot", request_get=request_get, now=now, since=since
    )
    funding = _funding(symbol, request_get=request_get)
    trend_5m = _closed_trend(symbol, 5, request_get=request_get, now=now)
    trend_60m = _closed_trend(symbol, 60, request_get=request_get, now=now)
    current_price = _latest_price(symbol, request_get=request_get)

    try:
        fetch_okx_liquidations(symbol)
    except Exception:
        pass
    liquidations = get_liquidation_summary(symbol)

    aggregated = get_aggregated_oi(symbol)
    agg_5m = (aggregated.get("windows") or {}).get("5m") or {}

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
            "reason": "OI/Futures 5m data not fully ready",
        }

    expected = "UP" if kind == "PUMP" else "DOWN"
    trend_aligned = trend_5m.get("trend") == expected
    anti_late_ok = abs(change) < 5.5
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
        "exchange": "Bybit",
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
        "trend_aligned": trend_aligned,
        "anti_late_ok": anti_late_ok,
        "trade_approved": False,
        "research_only": True,
    }
    result["verdict"] = classify(result)
    return result


def classify(result):
    """Research verdict only: CONFIRMED / WATCH / BROKEN."""
    kind = result.get("type")
    expected = "UP" if kind == "PUMP" else "DOWN"
    opposite = "DOWN" if expected == "UP" else "UP"
    post = _finite(result.get("post_trigger_pct"))
    futures = result.get("futures") or {}
    spot = result.get("spot") or {}
    trend5 = (result.get("trend_5m") or {}).get("trend")
    trend60 = (result.get("trend_60m") or {}).get("trend")
    pattern = result.get("pattern") or {}
    pattern_dir = pattern.get("direction")
    pattern_name = pattern.get("pattern")
    agg = result.get("agg_5m") or {}
    agg_ready = agg.get("ready") is True
    agg_change = _finite(agg.get("change_pct"))
    fimb = _finite(futures.get("imbalance_pct"))
    simb = _finite(spot.get("imbalance_pct"))

    against_price = (
        post is not None
        and ((expected == "DOWN" and post >= REVERSAL_BREAK_PCT)
             or (expected == "UP" and post <= -REVERSAL_BREAK_PCT))
    )
    futures_opposite = (
        futures.get("ready")
        and fimb is not None
        and ((expected == "DOWN" and fimb >= FLOW_CONFIRM_PCT)
             or (expected == "UP" and fimb <= -FLOW_CONFIRM_PCT))
    )
    if (
        against_price
        or pattern_dir == opposite
        or (trend5 == opposite and futures_opposite)
    ):
        return {
            "code": "BROKEN",
            "icon": "🔴",
            "label": "ИДЕЯ СЛОМАНА / РАЗВОРОТ",
        }

    futures_aligned = (
        futures.get("ready")
        and fimb is not None
        and ((expected == "DOWN" and fimb <= -FLOW_CONFIRM_PCT)
             or (expected == "UP" and fimb >= FLOW_CONFIRM_PCT))
    )
    spot_aligned = (
        spot.get("ready")
        and simb is not None
        and ((expected == "DOWN" and simb <= -SPOT_CONFIRM_PCT)
             or (expected == "UP" and simb >= SPOT_CONFIRM_PCT))
    )
    strong_pattern = pattern_dir == expected
    h1_blocks = trend60 == opposite

    requires_oi_expansion = pattern_name in (
        "NEW_LONG_BUILDUP",
        "NEW_SHORT_BUILDUP",
    )
    requires_oi_contraction = pattern_name in (
        "SHORT_SQUEEZE",
        "LONG_LIQUIDATION",
    )
    agg_pattern_aligned = (
        agg_ready
        and agg_change is not None
        and (
            (requires_oi_expansion and agg_change > 0)
            or (requires_oi_contraction and agg_change < 0)
            or not strong_pattern
        )
    )

    if (
        futures_aligned
        and trend5 == expected
        and (strong_pattern or spot_aligned)
        and agg_pattern_aligned
        and not h1_blocks
    ):
        label = (
            "ПРОДАВЦЫ ПОДТВЕРЖДАЮТСЯ"
            if expected == "DOWN"
            else "ПОКУПАТЕЛИ ПОДТВЕРЖДАЮТСЯ"
        )
        return {"code": "CONFIRMED", "icon": "🟢", "label": label}

    return {
        "code": "WATCH",
        "icon": "🟡",
        "label": "НАБЛЮДАЕМ — ПОЛНОГО ПОДТВЕРЖДЕНИЯ НЕТ",
    }


def _num(value, signed=False, suffix=""):
    value = _finite(value)
    if value is None:
        return "нет данных"
    return (f"{value:+.2f}" if signed else f"{value:.2f}") + suffix


def _flow_line(data):
    quality = data.get("quality", "UNAVAILABLE")
    if quality in (
        "UNAVAILABLE", "UNSUPPORTED", "CHECKING", "DISCONNECTED", "DATA_ERROR"
    ):
        return {
            "UNSUPPORTED": "Spot-пары нет на Bybit/OKX/Binance",
            "CHECKING": "ищем Spot: Bybit → OKX → Binance",
            "DISCONNECTED": "Spot-поток переподключается",
            "DATA_ERROR": "Spot-проверка временно недоступна",
        }.get(quality, "нет данных")
    return (
        f"{_num(data.get('imbalance_pct'), signed=True, suffix='%')} "
        f"({quality}; {int(data.get('trade_count') or 0)} сделок; "
        f"охват {int(data.get('coverage_sec') or 0)}с; "
        f"{escape(str(data.get('source') or 'source unknown'))})"
    )


def trend_text(value):
    return {
        "UP": "⬆️ UP",
        "DOWN": "⬇️ DOWN",
        "MIXED": "↔️ MIXED",
        "UNKNOWN": "нет данных",
    }.get(value, "нет данных")


def _signal_traffic(value, expected):
    value = _finite(value)
    if value is None:
        return "🟡", "нет данных"
    aligned = value > 0 if expected == "UP" else value < 0
    return ("🟢", "подтверждает") if aligned else ("🔴", "против сигнала")


def build_message(result, checkpoint="NOW"):
    """Compact Telegram card. Raw diagnostics remain in Railway logs."""
    symbol = escape(result["symbol"])
    kind = result["type"]
    expected = "UP" if kind == "PUMP" else "DOWN"
    side = "LONG" if expected == "UP" else "SHORT"
    icon = "🟢" if side == "LONG" else "🔴"

    oi = result.get("oi") or {}
    futures = result.get("futures") or {}
    spot = result.get("spot") or {}
    agg = result.get("agg_5m") or {}
    pattern = result.get("pattern") or {}

    pattern_name = pattern.get("pattern") or "NONE"
    pattern_text = {
        "NEW_LONG_BUILDUP": "Новые лонги заходят в рынок",
        "NEW_SHORT_BUILDUP": "Новые шорты заходят в рынок",
        "SHORT_SQUEEZE": "Шорты закрываются и толкают цену вверх",
        "LONG_LIQUIDATION": "Лонги закрываются и усиливают падение",
        "NONE": "Движение подтверждено потоком, без отдельного паттерна",
    }.get(pattern_name, "Рынок подтвердил направление")

    confidence = agg.get("confidence")
    confidence_text = {
        "HIGH": "🟢 ВЫСОКАЯ",
        "MEDIUM": "🟡 СРЕДНЯЯ",
    }.get(confidence, "🟡 СРЕДНЯЯ")

    agg_mode = agg.get("consensus_mode")
    if agg_mode == "3/3":
        agg_source = "3/3 биржи"
    elif agg_mode == "2/2_SUPPORTED":
        agg_source = "2/2 доступных биржи"
    else:
        agg_source = "консенсус бирж"

    f_icon, f_note = _signal_traffic(futures.get("imbalance_pct"), expected)
    s_icon, s_note = _signal_traffic(spot.get("imbalance_pct"), expected)

    requires_expansion = pattern_name in (
        "NEW_LONG_BUILDUP",
        "NEW_SHORT_BUILDUP",
    )
    requires_contraction = pattern_name in (
        "SHORT_SQUEEZE",
        "LONG_LIQUIDATION",
    )

    def oi_icon(value):
        value = _finite(value)
        if value is None:
            return "🟡"
        if requires_expansion:
            return "🟢" if value > 0 else "🔴"
        if requires_contraction:
            return "🟢" if value < 0 else "🔴"
        return "🟡"

    if side == "LONG":
        now_text = "Покупатели контролируют движение."
        cancel_text = "Отмена идеи: 5м поток и OI разворачиваются вниз."
    else:
        now_text = "Продавцы контролируют движение."
        cancel_text = "Отмена идеи: 5м поток и OI разворачиваются вверх."

    return "\n".join((
        f"{icon} <b>{side} — {symbol}</b>",
        f"✅ <b>СИГНАЛ ПОДТВЕРЖДЁН</b> · сила: {confidence_text}",
        f"💰 Цена: <b>{_num(result.get('current_price'))}</b>",
        f"⚡ Импульс: {_num(result.get('change'), signed=True, suffix='%')}",
        "",
        f"📌 {escape(pattern_text)}",
        f"{oi_icon(oi.get('change_pct'))} OI 5м: {_num(oi.get('change_pct'), signed=True, suffix='%')}",
        f"{oi_icon(agg.get('change_pct'))} OI рынка: {_num(agg.get('change_pct'), signed=True, suffix='%')} · {agg_source}",
        f"{f_icon} Фьючерсы: {_num(futures.get('imbalance_pct'), signed=True, suffix='%')} · {f_note}",
        f"{s_icon} Спот: {_num(spot.get('imbalance_pct'), signed=True, suffix='%')} · {s_note}",
        "",
        f"🎯 <b>Сейчас:</b> {escape(now_text)}",
        "⚠️ Не догонять свечу — лучше дождаться отката/ретеста.",
        f"❌ {escape(cancel_text)}",
    ))


def schedule(signal, sender, now=None):
    if str(signal.get("exchange") or "").upper() != "BYBIT":
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
    watch_flow_symbol(symbol, started_at=at)
    watch_fallback_spot(symbol, started_at=at)
    verification_signal = dict(signal)
    verification_signal["_verification_started_at"] = at

    def worker():
        extreme = _finite(signal.get("price"))
        pre_active = False
        pre_notified = False
        early_notified = False
        last_result = None
        last_maturity = None
        weak_followups = 0

        def inspect_reversal(result, checkpoint):
            nonlocal extreme, pre_active, pre_notified, early_notified, last_result, last_maturity
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
                    "[BYBIT_REVERSAL_CANCELLED]",
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
                    "[BYBIT_REVERSAL_CONFIRMED]",
                    symbol,
                    "checkpoint=", checkpoint,
                    "sent=", sent is True,
                    "strong_score=", maturity.get("strong_score"),
                    "reversal_pct=", maturity.get("reversal_pct"),
                    flush=True,
                )
                # False is an explicit delivery failure: keep the bounded
                # observer alive so the next event can retry.  None is
                # ambiguous and must not be blindly duplicated.
                if sent is not False:
                    return "CONFIRMED", maturity
                pre_active = True
                return "CONTINUE", maturity

            if maturity.get("code") == "EARLY":
                if not early_notified:
                    sent = sender(
                        build_reversal_early_message(
                            result, maturity, checkpoint
                        ),
                        symbol=symbol,
                    )
                    if sent is not False:
                        early_notified = True
                print(
                    "[BYBIT_REVERSAL_EARLY]",
                    symbol,
                    "checkpoint=", checkpoint,
                    "early_score=", maturity.get("early_score"),
                    "strong_score=", maturity.get("strong_score"),
                    flush=True,
                )

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
                    "[BYBIT_REVERSAL_PRE]",
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
                    "[BYBIT_REVERSAL_CANCELLED]",
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
                    print("[BYBIT_VERIFY_INVALID]", symbol, checkpoint, flush=True)
                    return

                reversal_action, maturity = inspect_reversal(result, checkpoint)
                if reversal_action in ("STOP", "CONFIRMED"):
                    return

                verdict_code = result["verdict"].get("code")
                should_send = (
                    checkpoint == "5m"
                    and verdict_code == "CONFIRMED"
                    and not pre_active
                    and maturity.get("continue_watch") is not True
                )
                sent = False
                if should_send:
                    sent = sender(
                        build_message(result, checkpoint=checkpoint),
                        symbol=symbol,
                    )
                print(
                    "[BYBIT_VERIFY_SENT]" if should_send else "[BYBIT_VERIFY_INTERNAL]",
                    symbol,
                    "checkpoint=", checkpoint,
                    "sent=", sent is True,
                    "verdict=", verdict_code,
                    "reversal=", maturity.get("code"),
                    "reversal_early=", maturity.get("early_score"),
                    "reversal_strong=", maturity.get("strong_score"),
                    "pattern=", result["pattern"].get("pattern"),
                    "oi_ready=", result["oi"].get("ready"),
                    "futures_ready=", result["futures"].get("ready"),
                    "futures_coverage_sec=", round(
                        float(result["futures"].get("coverage_sec") or 0), 1
                    ),
                    "spot_ready=", result["spot"].get("ready"),
                    "spot_source=", result["spot"].get("source"),
                    "spot_coverage_sec=", round(
                        float(result["spot"].get("coverage_sec") or 0), 1
                    ),
                    "agg_ready=", result["agg_5m"].get("ready"),
                    "agg_confidence=", result["agg_5m"].get("confidence"),
                    "agg_mode=", result["agg_5m"].get("consensus_mode"),
                    flush=True,
                )

            # Five minutes is the main decision point, not a mandatory 15m wait.
            # Continue only when at least a real early reversal story exists.
            if not (
                pre_active
                or (
                    last_maturity
                    and last_maturity.get("continue_watch") is True
                )
            ):
                print(
                    "[BYBIT_REVERSAL_STOP]",
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
                            "[BYBIT_REVERSAL_CANCELLED]",
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
                    "[BYBIT_REVERSAL_CANCELLED]",
                    symbol,
                    "reason=TTL_EXPIRED",
                    flush=True,
                )
        except Exception as exc:
            print(
                "[BYBIT_VERIFY_ERROR]",
                symbol,
                type(exc).__name__,
                str(exc)[:120],
                flush=True,
            )
        finally:
            unwatch_flow_symbol(symbol)
            unwatch_fallback_spot(symbol)

    try:
        threading.Thread(
            target=worker,
            name="bybit-verify-" + symbol,
            daemon=True,
        ).start()
        print(
            "[BYBIT_VERIFY_SCHEDULED]",
            symbol,
            kind,
            "checkpoints=NOW,1m,3m,5m internal; telegram=confirmed_5m_only; reversal=PRE+event_followup_to_15m",
            flush=True,
        )
        return True
    except Exception:
        unwatch_flow_symbol(symbol)
        unwatch_fallback_spot(symbol)
        with _lock:
            _last_started.pop(key, None)
        return False
