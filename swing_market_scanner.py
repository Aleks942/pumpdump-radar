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
COMPRESSION_HOURS = int(os.getenv("SWING_COMPRESSION_HOURS", 6))
COMPRESSION_PRIOR_HOURS = int(os.getenv("SWING_COMPRESSION_PRIOR_HOURS", 12))
COMPRESSION_MIN_IMPULSE_PCT = float(os.getenv("SWING_COMPRESSION_MIN_IMPULSE_PCT", 3.0))
COMPRESSION_MAX_WIDTH_PCT = float(os.getenv("SWING_COMPRESSION_MAX_WIDTH_PCT", 5.0))
COMPRESSION_MAX_WIDTH_RATIO = float(os.getenv("SWING_COMPRESSION_MAX_WIDTH_RATIO", 0.68))
COMPRESSION_MAX_CANDLE_RATIO = float(os.getenv("SWING_COMPRESSION_MAX_CANDLE_RATIO", 0.72))
COMPRESSION_TOP_N = int(os.getenv("SWING_COMPRESSION_TOP_N", 6))

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


def _range_width_pct(rows):
    if not rows:
        return None, None, None
    high = max(x["h"] for x in rows)
    low = min(x["l"] for x in rows)
    mid = (high + low) / 2.0
    if mid <= 0:
        return None, None, None
    return 100.0 * (high - low) / mid, low, high


def _avg_candle_range_pct(rows):
    values = []
    for row in rows:
        close = row.get("c")
        if close and close > 0:
            values.append(100.0 * (row["h"] - row["l"]) / close)
    return sum(values) / len(values) if values else None


def _compression_snapshot(rows):
    current_n = max(3, COMPRESSION_HOURS)
    prior_n = max(6, COMPRESSION_PRIOR_HOURS)
    if len(rows) < current_n + prior_n:
        return {"active": False}

    current = rows[-current_n:]
    prior = rows[-(current_n + prior_n):-current_n]
    if len(current) < current_n or len(prior) < prior_n:
        return {"active": False}

    current_width, range_low, range_high = _range_width_pct(current)
    prior_width, _, _ = _range_width_pct(prior)
    current_candle = _avg_candle_range_pct(current)
    prior_candle = _avg_candle_range_pct(prior)
    if any(v is None or v <= 0 for v in (
        current_width, prior_width, current_candle, prior_candle
    )):
        return {"active": False}

    prior_start = prior[0]["c"]
    prior_end = prior[-1]["c"]
    if prior_start <= 0:
        return {"active": False}
    impulse_pct = 100.0 * (prior_end / prior_start - 1.0)
    width_ratio = current_width / prior_width
    candle_ratio = current_candle / prior_candle

    active = (
        abs(impulse_pct) >= COMPRESSION_MIN_IMPULSE_PCT
        and current_width <= COMPRESSION_MAX_WIDTH_PCT
        and width_ratio <= COMPRESSION_MAX_WIDTH_RATIO
        and candle_ratio <= COMPRESSION_MAX_CANDLE_RATIO
    )
    strength = max(
        0.0,
        min(100.0, 100.0 * (1.0 - 0.5 * (width_ratio + candle_ratio))),
    )
    context = "POST_DROP" if impulse_pct < 0 else "POST_RISE"
    return {
        "active": bool(active),
        "context": context,
        "direction_hint": None,
        "prior_move_pct": impulse_pct,
        "range_low": range_low,
        "range_high": range_high,
        "range_width_pct": current_width,
        "prior_range_width_pct": prior_width,
        "width_ratio": width_ratio,
        "candle_ratio": candle_ratio,
        "strength": strength,
        "hours": current_n,
    }


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

    compression = _compression_snapshot(rows)

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
        "compression": compression,
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


def compression_watches(tickers=None, top_n=None, now=None):
    """Return active direction-neutral 1H compression watches from the scan cache.

    This function performs no network requests. It only reads symbols already
    refreshed by scan(), so early-warning coverage does not add REST load.
    """
    now = time.time() if now is None else float(now)
    top_n = COMPRESSION_TOP_N if top_n is None else int(top_n)
    if top_n <= 0:
        return []

    active = None
    if tickers is not None:
        active = set(
            symbol for symbol in (_symbol_from_ticker(t) for t in (tickers or []))
            if symbol
        )

    with _lock:
        rows = []
        for symbol, cached in _cache.items():
            if active is not None and symbol not in active:
                continue
            if not (0 <= now - cached.get("updated_at", 0) <= CACHE_MAX_AGE_SEC):
                continue
            compression = cached.get("compression") or {}
            if compression.get("active") is not True:
                continue
            rows.append({
                "symbol": symbol,
                "instId": cached.get("instId"),
                "price": cached.get("price"),
                "updated_at": cached.get("updated_at"),
                **compression,
            })

    rows.sort(
        key=lambda x: (
            float(x.get("strength", 0)),
            abs(float(x.get("prior_move_pct", 0))),
        ),
        reverse=True,
    )
    return rows[:top_n]


def clear_cache():
    global _cursor
    with _lock:
        _cache.clear()
    _cursor = 0
