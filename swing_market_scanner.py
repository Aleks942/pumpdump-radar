"""Low-cost market-wide 1H prefilter for the medium-term SWING scanner.

Purpose:
- scan the whole OKX USDT-SWAP universe in small batches;
- use CLOSED 1H candles only;
- rank directional candidates without using 5m/15m data;
- cache results so the main loop does not hammer exchange REST limits.

This is a prefilter only. Final direction is decided by swing_strength_engine
using 1H/4H/1D + OI.
"""
import math
import os
import threading
import time

import requests

OKX_CANDLES = "https://www.okx.com/api/v5/market/candles"
CACHE_TTL_SEC = int(os.getenv("SWING_PREFILTER_TTL_SEC", 20 * 60))
CACHE_MAX_AGE_SEC = int(os.getenv("SWING_PREFILTER_MAX_AGE_SEC", 2 * 3600))
DEFAULT_REFRESH_PER_SCAN = int(os.getenv("SWING_PREFILTER_REFRESH_PER_SCAN", 24))
DEFAULT_TOP_N = int(os.getenv("SWING_PREFILTER_TOP_N", 12))
DEFAULT_MIN_ABS_SCORE = float(os.getenv("SWING_PREFILTER_MIN_ABS_SCORE", 22))

_lock = threading.RLock()
_cache = {}
_cursor = 0


def _num(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _ema(values, n):
    if len(values) < n:
        return None
    out = sum(values[:n]) / n
    alpha = 2.0 / (n + 1)
    for value in values[n:]:
        out += alpha * (value - out)
    return out


def _symbol_from_ticker(ticker):
    raw = ticker.get("instId") if isinstance(ticker, dict) else None
    if not isinstance(raw, str) or not raw.endswith("-USDT-SWAP"):
        return None
    return raw.replace("-USDT-SWAP", "USDT")


def _closed_1h(inst_id, request_get=None):
    request_get = request_get or requests.get
    response = request_get(
        OKX_CANDLES,
        params={"instId": inst_id, "bar": "1H", "limit": "36"},
        timeout=8,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("code") != "0":
        raise RuntimeError("OKX_API")

    rows = []
    for row in payload.get("data") or []:
        if not isinstance(row, list) or len(row) < 6:
            continue
        # OKX row[8] is confirm when present: 1 = closed, 0 = in progress.
        if len(row) > 8 and str(row[8]) != "1":
            continue
        ts = _num(row[0])
        o = _num(row[1])
        h = _num(row[2])
        l = _num(row[3])
        c = _num(row[4])
        v = _num(row[5])
        if ts is None or any(x is None or x <= 0 for x in (o, h, l, c)):
            continue
        if v is None or v < 0:
            continue
        if not l <= min(o, c) <= max(o, c) <= h:
            continue
        rows.append({"ts": ts, "o": o, "h": h, "l": l, "c": c, "v": v})

    rows.sort(key=lambda x: x["ts"])
    return rows


def _score_hourly(rows):
    if len(rows) < 25:
        return None
    closes = [x["c"] for x in rows]
    ema20 = _ema(closes, 20)
    ema20_prev = _ema(closes[:-3], 20)
    if ema20 is None or ema20_prev is None or ema20_prev <= 0:
        return None

    last = closes[-1]
    slope = 100.0 * (ema20 / ema20_prev - 1.0)
    recent = rows[-6:]
    prior = rows[-12:-6]
    recent_hi = max(x["h"] for x in recent)
    recent_lo = min(x["l"] for x in recent)
    prior_hi = max(x["h"] for x in prior)
    prior_lo = min(x["l"] for x in prior)
    hh_hl = recent_hi > prior_hi and recent_lo > prior_lo
    lh_ll = recent_hi < prior_hi and recent_lo < prior_lo

    change3 = 100.0 * (closes[-1] / closes[-4] - 1.0)
    recent_vol = sum(x["v"] for x in rows[-3:]) / 3.0
    prior_vol = sum(x["v"] for x in rows[-12:-3]) / 9.0
    volume_ratio = recent_vol / prior_vol if prior_vol > 0 else 1.0

    signed = 0.0
    if last > ema20:
        signed += 16.0
    elif last < ema20:
        signed -= 16.0

    if slope > 0.04:
        signed += 18.0
    elif slope < -0.04:
        signed -= 18.0

    if hh_hl:
        signed += 26.0
    if lh_ll:
        signed -= 26.0

    # Momentum is capped so one large candle cannot dominate structure.
    signed += max(-18.0, min(18.0, change3 * 4.0))

    if volume_ratio >= 1.35:
        if signed > 0:
            signed += 8.0
        elif signed < 0:
            signed -= 8.0

    signed = max(-100.0, min(100.0, signed))
    if signed >= 0:
        direction = "LONG"
    else:
        direction = "SHORT"

    return {
        "signed_score": signed,
        "direction": direction,
        "change3_pct": change3,
        "ema20_slope_pct": slope,
        "hh_hl": hh_hl,
        "lh_ll": lh_ll,
        "volume_ratio": volume_ratio,
        "last": last,
    }


def _refresh_symbol(ticker, now, request_get=None):
    symbol = _symbol_from_ticker(ticker)
    if symbol is None:
        return None
    raw = ticker["instId"]
    rows = _closed_1h(raw, request_get=request_get)
    scored = _score_hourly(rows)
    if scored is None:
        return None
    last_price = _num(ticker.get("last"))
    result = {
        "symbol": symbol,
        "instId": raw,
        "price": last_price if last_price and last_price > 0 else scored["last"],
        "updated_at": now,
        **scored,
    }
    with _lock:
        _cache[symbol] = result
    return result


def scan(tickers, max_refresh=None, top_n=None, now=None, request_get=None):
    """Refresh a bounded batch and return strongest cached 1H candidates."""
    global _cursor
    now = time.time() if now is None else float(now)
    max_refresh = DEFAULT_REFRESH_PER_SCAN if max_refresh is None else int(max_refresh)
    top_n = DEFAULT_TOP_N if top_n is None else int(top_n)
    if max_refresh <= 0 or top_n <= 0:
        return []

    universe = [t for t in (tickers or []) if _symbol_from_ticker(t)]
    if not universe:
        return []

    refreshed = 0
    inspected = 0
    total = len(universe)
    start = _cursor % total

    while inspected < total and refreshed < max_refresh:
        ticker = universe[(start + inspected) % total]
        inspected += 1
        symbol = _symbol_from_ticker(ticker)
        with _lock:
            cached = _cache.get(symbol)
        if cached and 0 <= now - cached.get("updated_at", 0) < CACHE_TTL_SEC:
            continue
        try:
            _refresh_symbol(ticker, now, request_get=request_get)
        except Exception:
            # Prefilter failure must never stop the production scanner.
            pass
        refreshed += 1

    _cursor = (start + inspected) % total

    active = set(_symbol_from_ticker(t) for t in universe)
    with _lock:
        rows = [
            dict(v) for k, v in _cache.items()
            if k in active
            and 0 <= now - v.get("updated_at", 0) <= CACHE_MAX_AGE_SEC
            and abs(float(v.get("signed_score", 0))) >= DEFAULT_MIN_ABS_SCORE
        ]

    rows.sort(key=lambda x: abs(x.get("signed_score", 0)), reverse=True)
    return rows[:top_n]


def clear_cache():
    global _cursor
    with _lock:
        _cache.clear()
    _cursor = 0
