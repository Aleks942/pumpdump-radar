"""Verify Bybit price-threshold observations with exchange data.

This module is deliberately separate from the strict trade gate.  A Bybit
price-only WATCH starts this verifier after Telegram delivery.  The verifier
checks official Bybit 5m OI, futures/spot taker flow, funding, closed-candle
trend, available liquidation feeds and the existing aggregated-OI cache, then
sends one follow-up research message.

Missing/partial data stay UNKNOWN/PARTIAL and never become bearish/bullish
evidence by themselves.
"""
import math
import threading
import time
from html import escape

import requests

from aggregated_oi import get_aggregated_oi, watch_symbol
from liquidation_engine import (
    fetch_okx_liquidations,
    get_liquidation_summary,
    watch_bybit_symbol,
)
from pattern_detector import detect_pattern


BYBIT_BASE = "https://api.bybit.com"
VERIFY_DELAY_SEC = 2.0
SYMBOL_COOLDOWN_SEC = 10 * 60
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
        baseline = min(
            points[:-1],
            key=lambda item: abs((end_ts - item[0]) - 300.0),
        )
        start_ts, previous = baseline
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


def _trade_flow(symbol, category, request_get=None, now=None):
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
            return {"quality": "UNAVAILABLE", "ready": False, "trade_count": 0}
        newest = max(row[0] for row in rows)
        oldest = min(row[0] for row in rows)
        coverage = max(0.0, min(300.0, now - oldest))
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
        }
    except Exception as exc:
        return {
            "quality": "UNAVAILABLE",
            "ready": False,
            "trade_count": 0,
            "error": type(exc).__name__,
        }


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
        return {"quality": "UNAVAILABLE", "ready": False, "error": type(exc).__name__}


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
            # Use closed candles only.
            if start + minutes * 60 <= now:
                candles.append((start, close))
        candles.sort()
        if len(candles) < 3:
            return {"quality": "WARMING", "ready": False, "trend": "UNKNOWN"}
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
    if (
        not symbol.endswith("USDT")
        or kind not in ("PUMP", "DUMP")
        or change is None
    ):
        return {"status": "INVALID"}

    # Start future live coverage immediately for every Bybit mover.
    watch_symbol(symbol)
    watch_bybit_symbol(symbol)

    oi = _oi_5m(symbol, request_get=request_get, now=now)
    futures = _trade_flow(
        symbol, "linear", request_get=request_get, now=now
    )
    spot = _trade_flow(
        symbol, "spot", request_get=request_get, now=now
    )
    funding = _funding(symbol, request_get=request_get)
    trend_5m = _closed_trend(
        symbol, 5, request_get=request_get, now=now
    )
    trend_60m = _closed_trend(
        symbol, 60, request_get=request_get, now=now
    )

    # OKX historical liquidation REST may be unavailable for Bybit-only
    # symbols; that is recorded as unavailable, never as zero evidence.
    try:
        fetch_okx_liquidations(symbol)
    except Exception:
        pass
    liquidations = get_liquidation_summary(symbol)

    aggregated = get_aggregated_oi(symbol)
    agg_5m = (aggregated.get("windows") or {}).get("5m") or {}

    spot_value = (
        spot.get("imbalance_pct")
        if spot.get("ready")
        else None
    )
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

    return {
        "status": "OK",
        "symbol": symbol,
        "type": kind,
        "change": change,
        "price": signal.get("price"),
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


def _num(value, signed=False, suffix=""):
    value = _finite(value)
    if value is None:
        return "нет данных"
    return (f"{value:+.2f}" if signed else f"{value:.2f}") + suffix


def _flow_line(data):
    quality = data.get("quality", "UNAVAILABLE")
    if quality == "UNAVAILABLE":
        return "нет данных"
    return (
        f"{_num(data.get('imbalance_pct'), signed=True, suffix='%')} "
        f"({quality}; {int(data.get('trade_count') or 0)} сделок; "
        f"охват {int(data.get('coverage_sec') or 0)}с)"
    )


def build_message(result):
    symbol = escape(result["symbol"])
    kind = result["type"]
    oi = result["oi"]
    futures = result["futures"]
    spot = result["spot"]
    funding = result["funding"]
    liq = result["liquidations"]
    agg = result["agg_5m"]
    pattern = result["pattern"]

    title = "ПАМП" if kind == "PUMP" else "ДАМП"
    if pattern.get("pattern") != "NONE":
        conclusion = (
            "🧩 Найден паттерн: " + escape(str(pattern.get("pattern")))
            + ". Это ещё не разрешение на вход."
        )
    elif not oi.get("ready") or not futures.get("ready"):
        conclusion = (
            "🟡 Полного подтверждения пока нет: часть 5-минутных данных "
            "не имеет достаточного покрытия."
        )
    else:
        conclusion = "⚪ Полного подтверждённого паттерна сейчас нет."

    agg_text = (
        _num(agg.get("change_pct"), signed=True, suffix="%") + " (3/3)"
        if agg.get("ready")
        else f"нет общего 3/3; готово источников {int(agg.get('ready_sources') or 0)}/3"
    )

    lines = [
        f"🔬 АВТОПРОВЕРКА {title} — {symbol}",
        "Источник движения: Bybit SWAP",
        f"Движение цены: {_num(result.get('change'), signed=True, suffix='%')}",
        "",
        f"OI Bybit ≈5м: {_num(oi.get('change_pct'), signed=True, suffix='%')} · {oi.get('quality')}",
        f"Aggregated OI ≈5м: {agg_text}",
        f"Futures CVD/дельта Bybit ≈5м: {_flow_line(futures)}",
        f"Spot CVD Bybit ≈5м: {_flow_line(spot)}",
        f"Funding Bybit: {_num(funding.get('rate_pct'), signed=True, suffix='%')}",
        f"Тренд закрытых 5м свечей: {trend_text(result['trend_5m'].get('trend'))}",
        f"Тренд закрытых H1 свечей: {trend_text(result['trend_60m'].get('trend'))}",
        (
            "Ликвидации 5м: long "
            + _num(liq.get("long_liq"))
            + " / short "
            + _num(liq.get("short_liq"))
            + " USDT · "
            + escape(str(liq.get("quality") or "UNAVAILABLE"))
        ),
        "",
        conclusion,
    ]
    if not result.get("anti_late_ok"):
        lines.append("⚠️ Движение уже растянуто по anti-late фильтру.")
    if not result.get("trend_aligned"):
        lines.append("⚠️ Закрытый 5м тренд пока не подтверждает направление импульса.")
    lines.extend((
        "",
        "Это проверка качества движения, а не команда на сделку.",
        "Недоступные данные отмечаются как «нет данных» и не считаются подтверждением.",
    ))
    return "\n".join(lines)


def trend_text(value):
    return {
        "UP": "⬆️ UP",
        "DOWN": "⬇️ DOWN",
        "MIXED": "↔️ MIXED",
        "UNKNOWN": "нет данных",
    }.get(value, "нет данных")


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

    # Start live OI/liquidation observation before the delayed snapshot.
    watch_symbol(symbol)
    watch_bybit_symbol(symbol)

    def worker():
        try:
            time.sleep(VERIFY_DELAY_SEC)
            result = collect(dict(signal), now=time.time())
            if result.get("status") != "OK":
                print("[BYBIT_VERIFY_INVALID]", symbol, flush=True)
                return
            sent = sender(build_message(result), symbol=symbol)
            print(
                "[BYBIT_VERIFY_SENT]",
                symbol,
                "sent=", sent is True,
                "pattern=", result["pattern"].get("pattern"),
                "oi_ready=", result["oi"].get("ready"),
                "futures_ready=", result["futures"].get("ready"),
                "spot_ready=", result["spot"].get("ready"),
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

    try:
        threading.Thread(
            target=worker,
            name="bybit-verify-" + symbol,
            daemon=True,
        ).start()
        print("[BYBIT_VERIFY_SCHEDULED]", symbol, kind, flush=True)
        return True
    except Exception:
        with _lock:
            _last_started.pop(key, None)
        return False
