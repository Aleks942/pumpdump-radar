"""Zero-extra-API early pump/dump discovery. Research only, never trades.

Reads the existing OKX swap ticker batch. A 24h high/low *range* is NOT an
ordered or tradable move; 5m changes use two chronologically sampled quotes.
"""
import math
import time
from collections import deque

from ticker_metrics import approx_quote_volume_usdt, positive_float, spread_bps

HISTORY_SECONDS = 12 * 60
SAMPLE_STEP_SECONDS = 25
MAX_HISTORY_PER_SYMBOL = 32
MAX_SYMBOLS = 1200
REPORT_EVERY_SECONDS = 300
MIN_QUOTE_USDT = 10_000_000
MAX_SPREAD_BPS = 40.0
_history = {}
_last_report = None


def inspect(tickers, now=None):
    """Inspect all existing tickers with bounded RAM and no extra API calls."""
    now = time.time() if now is None else float(now)
    results = []
    for ticker in tickers or ():
        if not isinstance(ticker, dict):
            continue
        inst = str(ticker.get("instId") or "")
        if not inst.endswith("-USDT-SWAP"):
            continue
        price = positive_float(ticker.get("last"))
        opening = positive_float(ticker.get("open24h"))
        high = positive_float(ticker.get("high24h"))
        low = positive_float(ticker.get("low24h"))
        quote_volume = approx_quote_volume_usdt(ticker)
        if not all((price, opening, high, low, quote_volume)) or low > high:
            continue
        symbol = inst.replace("-USDT-SWAP", "USDT")
        records = _history.setdefault(symbol, deque(maxlen=MAX_HISTORY_PER_SYMBOL))
        while records and now - records[0][0] > HISTORY_SECONDS:
            records.popleft()
        if not records or now - records[-1][0] >= SAMPLE_STEP_SECONDS:
            records.append((now, price))
        older = [
            (at, px) for at, px in records
            if 240 <= now - at <= 420
        ]
        reference = min(older, key=lambda r: abs((now-r[0])-300)) if older else None
        change5 = 100*(price/reference[1]-1) if reference else None
        spread = spread_bps(ticker)
        quote_liquid = quote_volume >= MIN_QUOTE_USDT
        spread_safe = spread is not None and spread <= MAX_SPREAD_BPS
        base_volume = positive_float(ticker.get("volCcy24h"))
        results.append({
            "symbol": symbol,
            "price": price,
            "range24": 100*(high/low-1),
            "change24": 100*(price/opening-1),
            "change5": change5,
            "sample_age_sec": now-reference[0] if reference else None,
            "quote_usdt_approx": quote_volume,
            "spread_bps": spread,
            "eligible_research": quote_liquid and spread_safe,
            "old_volume_filter_missed": quote_liquid and bool(base_volume) and base_volume < MIN_QUOTE_USDT,
            "old_price_filter_missed": quote_liquid and spread_safe and price < 0.01,
        })

    if len(_history) > MAX_SYMBOLS:
        live_symbols = {row["symbol"] for row in results}
        for symbol in list(_history):
            if symbol not in live_symbols:
                _history.pop(symbol, None)
            if len(_history) <= MAX_SYMBOLS:
                break
        while len(_history) > MAX_SYMBOLS:
            _history.pop(next(iter(_history)))
    return results


def observe(tickers, now=None):
    """Log the largest ranges and early accelerations, never send Telegram."""
    global _last_report
    now = time.time() if now is None else float(now)
    records = inspect(tickers, now)
    if _last_report is not None and now-_last_report < REPORT_EVERY_SECONDS:
        return records
    _last_report = now
    big = [r for r in records if r["range24"] >= 7]
    viable = [r for r in big if r["eligible_research"]]
    early = [r for r in records if (
        r["eligible_research"]
        and r["change5"] is not None
        and 1.0 <= abs(r["change5"]) <= 5.5
    )]
    print(
        f"[MOVER_RESEARCH] scanned={len(records)} "
        f"range_7pct={len(big)} liquid_spread_ok={len(viable)} "
        f"old_base_volume_filter_missed={sum(r['old_volume_filter_missed'] for r in records)} "
        f"old_price_filter_missed={sum(r['old_price_filter_missed'] for r in records)} "
        f"early_5m={len(early)} no_trade_alerts=true",
        flush=True,
    )
    for row in sorted(viable, key=lambda r: r["range24"], reverse=True)[:5]:
        print(
            f"[MOVER_24H] {row['symbol']} range={row['range24']:.2f}% "
            f"net={row['change24']:+.2f}% "
            f"approx_usdt={row['quote_usdt_approx']:.0f} "
            f"spread_bps={row['spread_bps']:.1f} "
            f"old_filter_missed={row['old_volume_filter_missed']}",
            flush=True,
        )
    for row in sorted(early, key=lambda r: abs(r["change5"]), reverse=True)[:5]:
        print(
            f"[MOVER_EARLY] {row['symbol']} "
            f"direction={'LONG_WATCH' if row['change5'] > 0 else 'SHORT_WATCH'} "
            f"move5={row['change5']:+.2f}% "
            f"approx_usdt={row['quote_usdt_approx']:.0f} "
            f"spread_bps={row['spread_bps']:.1f} "
            "research_only=true",
            flush=True,
        )
    return records



def inspect_price_quotes(tickers, now=None):
    """Independently sample all finite positive OKX swap prices for alerts.

    Unlike research inspect(), requires no 24h volume/open/high/low, spread
    or liquidity metadata. Shares the same bounded chronological quote
    history, so adding notices never makes extra market HTTP requests.
    """
    now = time.time() if now is None else float(now)
    if not math.isfinite(now):
        return []
    out = []
    for ticker in tickers or ():
        if not isinstance(ticker, dict):
            continue
        inst = ticker.get("instId")
        if not isinstance(inst, str) or not inst.endswith("-USDT-SWAP"):
            continue
        price = positive_float(ticker.get("last"))
        if price is None:
            continue
        symbol = inst[:-10] + "USDT"
        records = _history.setdefault(symbol, deque(maxlen=MAX_HISTORY_PER_SYMBOL))
        while records and now - records[0][0] > HISTORY_SECONDS:
            records.popleft()
        if not records or now - records[-1][0] >= SAMPLE_STEP_SECONDS:
            records.append((now, price))
        older = [(at, px) for at, px in records if 240 <= now - at <= 420]
        reference = min(older, key=lambda r: abs(now - r[0] - 300)) if older else None
        change5 = 100 * (price / reference[1] - 1) if reference else None
        out.append({
            "symbol": symbol,
            "price": price,
            "change5": change5,
            "sample_age_sec": now - reference[0] if reference else None,
        })

    if len(_history) > MAX_SYMBOLS:
        live = {row["symbol"] for row in out}
        for old in list(_history):
            if old not in live:
                _history.pop(old, None)
            if len(_history) <= MAX_SYMBOLS:
                break
        while len(_history) > MAX_SYMBOLS:
            _history.pop(next(iter(_history)))
    return out

def select_threshold_moves(rows, pump_threshold, dump_threshold):
    """Price-only Telegram candidates from chronologically sampled OKX quotes.

    Neither liquidity, spread, OI, CVD nor pattern recognition is a gate here.
    Existing trade entry rules are not affected. A missing roughly-5m history
    cannot be converted into a fabricated price change.
    """
    try:
        pump = float(pump_threshold)
        dump = float(dump_threshold)
    except (ValueError, TypeError, OverflowError):
        return []
    if not (math.isfinite(pump) and pump > 0 and
            math.isfinite(dump) and dump > 0):
        return []

    candidates = []
    for row in rows or ():
        if not isinstance(row, dict):
            continue
        symbol = row.get("symbol")
        if (not isinstance(symbol, str) or not symbol.endswith("USDT")
                or len(symbol) <= 4):
            continue
        try:
            move = float(row.get("change5"))
            price = float(row.get("price"))
            age = float(row.get("sample_age_sec"))
        except (TypeError, ValueError, OverflowError):
            continue
        if (not all(math.isfinite(v) for v in (move, price, age))
                or price <= 0 or not 240 <= age <= 420):
            continue
        if move >= pump:
            kind, direction = "PUMP", "UP"
        elif move <= -dump:
            kind, direction = "DUMP", "DOWN"
        else:
            continue
        candidates.append({
            "symbol": symbol, "type": kind,
            "decision": {"direction": direction},
            "price": price, "change": move, "window": "~5m",
            "sample_age_sec": age,
        })
    # Limited Telegram budget goes to the largest observed movements first.
    return sorted(candidates, key=lambda r: abs(r["change"]), reverse=True)


# Lightweight continuation tracking for informational notices only. This
# doesn't modify research cohorts or the strict trading-signal path.
_CONTINUATION_MIN_SECONDS = 60
_CONTINUATION_MAX_SECONDS = 210
_CONTINUATION_STEP_PCT = 0.05
_CONTINUATION_TOTAL_PCT = 0.20
_CONTINUATION_MAX_RETRACE_PCT = 0.35
_CONTINUATION_MAX_EXTENSION_PCT = 1.50
_continuation_pending = {}


def select_continuing_moves(rows, pump_threshold, dump_threshold, now=None):
    """Emit ONLY a sustained follow-through after a configurable 5m move.

    Require two fresh consecutive directional advances over >=60 seconds,
    at least 0.2% extra movement since the threshold was first observed,
    no 0.35% reversal and no >1.5% late chase. All values are sampled
    quotes, NOT execution prices or evidence of future profitability.
    """
    now = time.time() if now is None else float(now)
    if not math.isfinite(now):
        return []
    try:
        pump = float(pump_threshold)
        dump = float(dump_threshold)
    except (TypeError, ValueError, OverflowError):
        return []
    if not (math.isfinite(pump) and pump > 0 and
            math.isfinite(dump) and dump > 0):
        return []

    # Select the first crossing with the existing, user-controlled thresholds.
    threshold_now = {r["symbol"]: r for r in
                     select_threshold_moves(rows, pump, dump)}
    # Expire absent/old observations; never build permanent state.
    for symbol, state in list(_continuation_pending.items()):
        if now - state["first_ts"] > 600:
            del _continuation_pending[symbol]

    ready = []
    for row in rows or ():
        if not isinstance(row, dict):
            continue
        symbol = row.get("symbol")
        if (not isinstance(symbol, str) or not symbol.endswith("USDT")
                or len(symbol) <= 4):
            continue
        try:
            price = float(row.get("price"))
            move = float(row.get("change5"))
            age = float(row.get("sample_age_sec"))
        except (TypeError, ValueError, OverflowError):
            continue
        if (not all(math.isfinite(v) for v in (price, move, age))
                or price <= 0 or not 240 <= age <= 420):
            continue

        initial = threshold_now.get(symbol)
        old = _continuation_pending.get(symbol)
        if old is not None and old["status"] != "PENDING":
            # Require the previous 5m threshold condition to clear
            # before the same symbol can be armed again.
            if initial is None:
                del _continuation_pending[symbol]
                old = None
            elif initial["type"] != old["type"]:
                old = None
            else:
                continue

        if old is None:
            if initial is None:
                continue
            _continuation_pending[symbol] = {
                "type": initial["type"],
                "first_ts": now,
                "first_price": price,
                "trigger_change": initial["change"],
                "last_ts": now,
                "last_price": price,
                "steps": 0,
                "best_follow": 0.0,
                "status": "PENDING",
            }
            continue

        if initial is not None and initial["type"] != old["type"]:
            # Never confirm a bullish start with bearish follow-through.
            _continuation_pending[symbol] = {
                "type": initial["type"],
                "first_ts": now, "first_price": price,
                "trigger_change": initial["change"],
                "last_ts": now, "last_price": price,
                "steps": 0, "best_follow": 0.0, "status": "PENDING",
            }
            continue

        elapsed = now - old["first_ts"]
        if elapsed > _CONTINUATION_MAX_SECONDS:
            old["status"] = "EXPIRED"
            continue
        if now - old["last_ts"] < SAMPLE_STEP_SECONDS:
            continue

        sign = 1 if old["type"] == "PUMP" else -1
        follow = sign * (price / old["first_price"] - 1) * 100
        step = sign * (price / old["last_price"] - 1) * 100
        old["last_ts"], old["last_price"] = now, price
        if (follow < -_CONTINUATION_MAX_RETRACE_PCT or
                old["best_follow"] - follow > _CONTINUATION_MAX_RETRACE_PCT or
                follow > _CONTINUATION_MAX_EXTENSION_PCT):
            old["status"] = "FAILED"
            continue
        old["best_follow"] = max(old["best_follow"], follow)
        old["steps"] = old["steps"] + 1 if step >= _CONTINUATION_STEP_PCT else 0

        if (elapsed >= _CONTINUATION_MIN_SECONDS and
                follow >= _CONTINUATION_TOTAL_PCT and old["steps"] >= 2):
            old["status"] = "CONFIRMED"
            ready.append({
                "symbol": symbol, "type": old["type"],
                "decision": {"direction": "UP" if sign == 1 else "DOWN"},
                "price": price,
                "change": move,
                "trigger_change": old["trigger_change"],
                "followthrough_pct": follow,
                "confirmation_seconds": elapsed,
                "window": "~5m", "sample_age_sec": age,
            })

    if len(_continuation_pending) > MAX_SYMBOLS:
        for symbol, _ in sorted(
                _continuation_pending.items(),
                key=lambda item: item[1]["first_ts"])[:len(_continuation_pending)-MAX_SYMBOLS]:
            _continuation_pending.pop(symbol, None)
    return sorted(ready, key=lambda r: r["followthrough_pct"], reverse=True)
