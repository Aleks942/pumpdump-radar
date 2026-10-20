"""Conservative, research-only early weakening of an observed +30% pump.

Uses only the ticker observations and CLOSED 5m candles already fetched
by PumpDump Radar. No additional API calls, alerts or trading permissions.
A 24h high alone never proves a new downward move.
"""
import math


def _positive(value):
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def is_early_weakening(previous_price, price, peak, previous_structure, structure):
    """True only on a new closed 5m bar AND a lower observed ticker price.

    The earlier WATCH->WEAKENING 2.5% gate is not relaxed: this is an
    informational/research phase, never a SHORT READY confirmation.
    """
    if not isinstance(previous_structure, dict) or not isinstance(structure, dict):
        return False
    if structure.get("ready") is not True or structure.get("bearish5") is not True:
        return False
    before = previous_structure.get("last5")
    after = structure.get("last5")
    if not isinstance(before, dict) or not isinstance(after, dict):
        return False
    old_price, current_price, high = (_positive(x) for x in (
        previous_price, price, peak))
    old_ts, new_ts = _positive(before.get("ts")), _positive(after.get("ts"))
    old_close, new_close = _positive(before.get("c")), _positive(after.get("c"))
    if any(x is None for x in (
            old_price, current_price, high, old_ts, new_ts, old_close, new_close)):
        return False
    # Chronology matters: a 24h high may have occurred many hours earlier.
    if not 0 < new_ts-old_ts <= 900:
        return False
    retrace = 100 * (1 - current_price / high)
    return (1.0 <= retrace < 2.5
            and current_price <= old_price * 0.995
            and new_close < old_close)
