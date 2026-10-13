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
