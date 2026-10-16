"""Chronological 10/20/30-minute price alerts, independent of trade gating.

Only public ticker prices already fetched by the running scanner are sampled.
Missing or stale history never produces a fabricated movement. OKX and Bybit
maintain separate price histories; the alert event state is cross-exchange.
"""
import math
import time
from collections import deque

PERIODS = {"10m": 600, "20m": 1200, "30m": 1800}
# A 2-3-minute scanner cycle cannot produce an exact-to-the-second quote.
AGE_BOUNDS = {"10m": (480, 750), "20m": (1050, 1350), "30m": (1650, 1950)}
KEEP_SECONDS = 2100
MAX_RECORDS = 70
MAX_INSTRUMENTS = 2600
MIN_SAMPLE_SEPARATION = 25
_history = {}
_notified = set()


def _positive_price(raw):
    try:
        value = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def sample_prices(tickers, exchange, now=None):
    """Return price snapshots for the three longer windows, if actually sampled."""
    if exchange not in ("OKX", "Bybit"):
        raise ValueError("Unsupported exchange")
    now = time.time() if now is None else float(now)
    if not math.isfinite(now):
        return []
    output = []
    for ticker in tickers or ():
        if not isinstance(ticker, dict):
            continue
        if exchange == "OKX":
            instrument = ticker.get("instId")
            if (not isinstance(instrument, str) or
                    not instrument.endswith("-USDT-SWAP")):
                continue
            symbol = instrument[:-10] + "USDT"
            price = _positive_price(ticker.get("last"))
        else:
            symbol = ticker.get("symbol")
            price = _positive_price(ticker.get("lastPrice"))
        if (not isinstance(symbol, str) or len(symbol) < 5
                or not symbol.endswith("USDT") or not symbol[:-4].isalnum()
                or price is None):
            continue

        key = (exchange, symbol)
        hist = _history.setdefault(key, deque(maxlen=MAX_RECORDS))
        if hist and now < hist[-1][0]:
            continue  # never compare future observations to older ones
        while hist and now - hist[0][0] > KEEP_SECONDS:
            hist.popleft()
        if not hist or now - hist[-1][0] >= MIN_SAMPLE_SEPARATION:
            hist.append((now, price))
        windows = {}
        for period, target_age in PERIODS.items():
            lo, hi = AGE_BOUNDS[period]
            eligible = [(when, old_price) for when, old_price in hist
                        if lo <= now - when <= hi]
            if eligible:
                at, previous = min(eligible,
                                   key=lambda item: abs(now-item[0]-target_age))
                windows[period] = {
                    "change": (price / previous - 1) * 100,
                    "age_sec": now - at,
                }
        output.append({
            "symbol": symbol, "price": price,
            "exchange": exchange, "windows": windows,
        })

    if len(_history) > MAX_INSTRUMENTS:
        for key in list(_history):
            if not _history[key] or now - _history[key][-1][0] > KEEP_SECONDS:
                _history.pop(key, None)
        while len(_history) > MAX_INSTRUMENTS:
            _history.pop(next(iter(_history)), None)
    return output


def select_events(rows, time_windows):
    """New threshold crossings across both exchanges (not repeated plateaus).

    The resulting event is marked as sent only after Telegram confirms it.
    When ALL observed valid prices fall below a threshold, its event rearms.
    """
    observed = set()
    qualifying = {}
    for row in rows or ():
        if not isinstance(row, dict) or row.get("exchange") not in ("OKX", "Bybit"):
            continue
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol.endswith("USDT"):
            continue
        price = _positive_price(row.get("price"))
        if price is None:
            continue
        for period in PERIODS:
            sample = (row.get("windows") or {}).get(period)
            if not isinstance(sample, dict):
                continue
            try:
                move = float(sample["change"])
                age = float(sample["age_sec"])
                cfg = time_windows[period]
                up, down = float(cfg["pump"]), -float(cfg["dump"])
            except (KeyError, ValueError, TypeError, OverflowError):
                continue
            lo, hi = AGE_BOUNDS[period]
            if (not all(math.isfinite(v) for v in (move, age, up, down))
                    or not lo <= age <= hi or up <= 0 or down <= 0):
                continue
            observed.add((symbol, period))
            direction = "UP" if move >= up else ("DOWN" if move <= -down else None)
            if direction is None:
                continue
            key = (symbol, period, direction)
            event = {
                "symbol": symbol,
                "type": "PUMP" if direction == "UP" else "DUMP",
                "decision": {"direction": direction},
                "price": price, "change": move,
                "window": period, "sample_age_sec": age,
                "exchange": row["exchange"],
            }
            old = qualifying.get(key)
            if old is None or abs(move) > abs(old["change"]):
                qualifying[key] = event

    active = set(qualifying)
    for key in list(_notified):
        if (key[0], key[1]) in observed and key not in active:
            _notified.discard(key)
    return sorted(
        (event for key, event in qualifying.items() if key not in _notified),
        key=lambda x: abs(x["change"]), reverse=True,
    )


def mark_sent(event):
    """Commit a crossing only after sendMessage returns an accepted response."""
    direction = (event.get("decision") or {}).get("direction")
    key = (event.get("symbol"), event.get("window"), direction)
    if key[1] in PERIODS and direction in ("UP", "DOWN"):
        _notified.add(key)
