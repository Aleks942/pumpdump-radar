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
    pattern_dir = (result.get("pattern") or {}).get("direction")
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

    if (
        futures_aligned
        and trend5 == expected
        and (strong_pattern or spot_aligned)
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


def build_message(result, checkpoint="NOW"):
    symbol = escape(result["symbol"])
    kind = result["type"]
    oi = result["oi"]
    futures = result["futures"]
    spot = result["spot"]
    funding = result["funding"]
    liq = result["liquidations"]
    agg = result["agg_5m"]
    pattern = result["pattern"]
    verdict = result["verdict"]

    title = "ПАМП" if kind == "PUMP" else "ДАМП"
    if checkpoint == "NOW":
        heading = f"🔬 АВТОПРОВЕРКА {title} — {symbol}"
    elif checkpoint == "5m":
        heading = f"⏱ ФИНАЛ 5М — {title} — {symbol}"
    else:
        heading = f"⏱ ПРОВЕРКА +{checkpoint} — {title} — {symbol}"

    agg_text = (
        _num(agg.get("change_pct"), signed=True, suffix="%") + " (3/3)"
        if agg.get("ready")
        else f"нет общего 3/3; готово источников {int(agg.get('ready_sources') or 0)}/3"
    )

    lines = [
        heading,
        f"{verdict['icon']} {verdict['label']}",
        f"Исходный импульс: {_num(result.get('change'), signed=True, suffix='%')}",
        f"После триггера: {_num(result.get('post_trigger_pct'), signed=True, suffix='%')}",
        "",
        f"OI Bybit ≈5м: {_num(oi.get('change_pct'), signed=True, suffix='%')} · {oi.get('quality')}",
        f"Aggregated OI ≈5м: {agg_text}",
        f"Futures CVD/дельта: {_flow_line(futures)}",
        f"Spot CVD: {_flow_line(spot)}",
        f"Funding Bybit: {_num(funding.get('rate_pct'), signed=True, suffix='%')}",
        f"Тренд закрытых 5м: {trend_text(result['trend_5m'].get('trend'))}",
        f"Тренд закрытых H1: {trend_text(result['trend_60m'].get('trend'))}",
        (
            "Ликвидации 5м: long "
            + _num(liq.get("long_liq"))
            + " / short "
            + _num(liq.get("short_liq"))
            + " USDT · "
            + escape(str(liq.get("quality") or "UNAVAILABLE"))
        ),
    ]
    if pattern.get("pattern") != "NONE":
        lines.append("Паттерн: " + escape(str(pattern.get("pattern"))))
    if not result.get("anti_late_ok"):
        lines.append("⚠️ Anti-late: движение уже растянуто; не догонять.")
    if result["trend_5m"].get("trend") not in (
        "DOWN" if kind == "DUMP" else "UP",
        "UNKNOWN",
    ):
        lines.append("⚠️ Закрытый 5м тренд не подтверждает исходное направление.")
    lines.extend((
        "",
        "Research-only: это не команда на сделку.",
        "READY требует реального накопленного окна; PARTIAL не считается подтверждением.",
    ))
    return "\n".join(lines)


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
        try:
            for target_sec, checkpoint in CHECKPOINTS:
                delay = max(0.0, at + target_sec - time.time())
                if delay:
                    time.sleep(delay)
                result = collect(verification_signal, now=time.time())
                if result.get("status") != "OK":
                    print("[BYBIT_VERIFY_INVALID]", symbol, checkpoint, flush=True)
                    return
                verdict_code = result["verdict"].get("code")
                should_send = (
                    checkpoint == "5m"
                    and verdict_code == "CONFIRMED"
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
            "checkpoints=NOW,1m,3m,5m internal; telegram=confirmed_5m_only",
            flush=True,
        )
        return True
    except Exception:
        unwatch_flow_symbol(symbol)
        unwatch_fallback_spot(symbol)
        with _lock:
            _last_started.pop(key, None)
        return False
