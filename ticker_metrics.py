"""Finite, conservative OKX USDT swap ticker metrics. No network calls."""
import math


def positive_float(value):
    try:
        result = float(value)
        return result if math.isfinite(result) and result > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def approx_quote_volume_usdt(ticker):
    """Approximate 24h USDT notional from OKX swap BASE volume * last price.

    OKX volCcy24h is in base currency for derivatives, not USDT.
    Using the latest price gives an estimate, not actual historic turnover.
    """
    if not isinstance(ticker, dict):
        return None
    inst_id = str(ticker.get("instId") or "")
    if not inst_id.endswith("-USDT-SWAP"):
        return None
    volume_base = positive_float(ticker.get("volCcy24h"))
    price = positive_float(ticker.get("last"))
    if volume_base is None or price is None:
        return None
    notional = volume_base * price
    return notional if math.isfinite(notional) else None


def spread_bps(ticker):
    """Full bid/ask spread in basis points, or None for bad quotes."""
    if not isinstance(ticker, dict):
        return None
    bid = positive_float(ticker.get("bidPx"))
    ask = positive_float(ticker.get("askPx"))
    if bid is None or ask is None or ask < bid:
        return None
    value = 20_000 * (ask - bid) / (ask + bid)
    return value if math.isfinite(value) else None
