"""Independent Bybit V5 linear price watcher for informational alerts only.
Does not feed OKX trading signals or reuse the OKX price history.
"""
import math
import time
from collections import deque

_HISTORY = {}
MAX_SECONDS = 720
MAX_SYMBOLS = 1200


def sample(tickers, now=None):
    now = time.time() if now is None else float(now)
    if not math.isfinite(now):
        return []
    out = []
    for ticker in tickers or ():
        if not isinstance(ticker, dict):
            continue
        symbol = ticker.get("symbol")
        if (not isinstance(symbol, str) or not symbol.endswith("USDT")
                or len(symbol) <= 4 or not symbol[:-4].isalnum()):
            continue
        try:
            price = float(ticker.get("lastPrice"))
        except (ValueError, TypeError, OverflowError):
            continue
        if not math.isfinite(price) or price <= 0:
            continue
        history = _HISTORY.setdefault(symbol, deque(maxlen=32))
        while history and now-history[0][0] > MAX_SECONDS:
            history.popleft()
        if not history or now-history[-1][0] >= 25:
            history.append((now, price))
        older = [x for x in history if 240 <= now-x[0] <= 420]
        reference = min(older, key=lambda x: abs(now-x[0]-300)) if older else None
        out.append({"symbol": symbol, "price": price,
                    "change5": 100*(price/reference[1]-1) if reference else None,
                    "sample_age_sec": now-reference[0] if reference else None,
                    "exchange": "Bybit"})
    if len(_HISTORY) > MAX_SYMBOLS:
        for symbol in list(_HISTORY):
            if not _HISTORY[symbol] or now-_HISTORY[symbol][-1][0] > MAX_SECONDS:
                _HISTORY.pop(symbol, None)
        while len(_HISTORY) > MAX_SYMBOLS:
            _HISTORY.pop(next(iter(_HISTORY)))
    return out


def fetch(request_get=None):
    if request_get is None:
        import requests
        request_get = requests.get
    try:
        response = request_get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "linear"}, timeout=8)
        if response.status_code != 200:
            print("[BYBIT_WATCH_FETCH] HTTP", response.status_code, flush=True)
            return None
        data = response.json()
        if not isinstance(data, dict) or data.get("retCode") != 0:
            print("[BYBIT_WATCH_FETCH] API_ERROR", flush=True)
            return None
        result = data.get("result")
        rows = result.get("list") if isinstance(result, dict) else None
        if not isinstance(rows, list):
            return None
        return [r for r in rows if isinstance(r, dict)]
    except Exception as exc:
        print("[BYBIT_WATCH_FETCH] ERROR", type(exc).__name__, flush=True)
        return None
