"""Read-only pattern outcome report from persisted closed 1-minute OKX candles.

Measures first touch of fixed ±1% levels over 5/10/20/30 *full* minutes
after the entry minute, or marks to the final close if neither level is hit.
Not actual fills, orders, or verified trading profitability.
"""
import math
import os
import time
from collections import Counter, defaultdict
from contextlib import closing

from market_memory import get_connection

PATTERNS = (
    "NEW_LONG_BUILDUP",
    "NEW_SHORT_BUILDUP",
    "SHORT_SQUEEZE",
    "LONG_LIQUIDATION",
)
HORIZONS = (5, 10, 20, 30)
MAX_CONTEXTS = 400
LOOKBACK_SEC = 30 * 86400
MIN_SAMPLE = 30


def _cost():
    value = 0.0
    for key, default in (
        ("SHADOW_FEE_BPS_SIDE", 6.0),
        ("SHADOW_SLIPPAGE_BPS_SIDE", 5.0),
    ):
        try:
            cost = float(os.getenv(key, str(default)))
            if not math.isfinite(cost) or cost < 0 or cost > 100:
                cost = default
        except (TypeError, ValueError):
            cost = default
        value += cost
    return 2 * value / 100.0


def first_touch(entry, direction, candles, minutes, cost_pct=0.22):
    """Pure chronological evaluator. Candles are (minute_ms, hi, lo, close)."""
    if not (entry > 0 and math.isfinite(entry)) or direction not in ("LONG", "SHORT"):
        return ("INVALID", None)
    if len(candles) < minutes:
        return ("INCOMPLETE", None)
    sign = 1 if direction == "LONG" else -1
    tp = entry * (1.01 if sign == 1 else 0.99)
    sl = entry * (0.99 if sign == 1 else 1.01)
    for _ts, hi, lo, close in candles[:minutes]:
        if not all(math.isfinite(v) and v > 0 for v in (hi, lo, close)):
            return ("INVALID", None)
        if not lo <= close <= hi:
            return ("INVALID", None)
        hit_tp = hi >= tp if sign == 1 else lo <= tp
        hit_sl = lo <= sl if sign == 1 else hi >= sl
        if hit_tp and hit_sl:
            return ("SAME_MINUTE", None)
        if hit_tp:
            return ("TP_FIRST", round(1.0 - cost_pct, 6))
        if hit_sl:
            return ("SL_FIRST", round(-1.0 - cost_pct, 6))
    close = candles[minutes - 1][3]
    return ("TIME_EXIT", round(sign * (close / entry - 1) * 100.0 - cost_pct, 6))


def _parse_groups(rows):
    grouped = defaultdict(list)
    meta = {}
    for cid, pattern, direction, entry, first_ms, ts, hi, lo, close in rows:
        meta[cid] = (str(pattern), str(direction), float(entry), int(first_ms))
        grouped[cid].append((int(ts), float(hi), float(lo), float(close)))
    return grouped, meta


def _full_history(candles, first_ms):
    return len(candles) == 30 and all(
        c[0] == first_ms + i * 60000
        for i, c in enumerate(candles)
    )


def print_candle_edge_report():
    """One bounded read-only SQLite query per hour; no trading dependencies."""
    try:
        with closing(get_connection()) as db:
            present = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='entry_candles_1m'"
            ).fetchone()
            if not present:
                print("[PUMP_EDGE] status=NO_CANDLE_TABLE", flush=True)
                return
            rows = db.execute(
                """
                WITH recent AS (
                    SELECT j.context_id, j.first_ms
                    FROM candle_path_jobs j
                    JOIN entry_contexts e ON e.context_id=j.context_id
                    WHERE j.status='READY' AND e.entry_ts >= ?
                    ORDER BY e.entry_ts DESC LIMIT ?
                )
                SELECT e.context_id, e.pattern, e.direction, e.entry_price,
                       recent.first_ms, b.open_ms, b.h, b.l, b.c
                FROM recent
                JOIN entry_contexts e ON e.context_id=recent.context_id
                JOIN entry_candles_1m b ON b.context_id=recent.context_id
                ORDER BY e.entry_ts DESC, e.context_id, b.open_ms
                """,
                (time.time() - LOOKBACK_SEC, MAX_CONTEXTS),
            ).fetchall()

        grouped, meta = _parse_groups(rows)
        data = defaultdict(lambda: Counter())
        sums = defaultdict(float)
        unusable = 0
        cost = _cost()
        for cid, candles in grouped.items():
            pattern, side, entry, first_ms = meta[cid]
            if pattern not in PATTERNS:
                continue
            if not _full_history(candles, first_ms):
                unusable += 1
                continue
            for minutes in HORIZONS:
                verdict, net = first_touch(entry, side, candles, minutes, cost)
                for kind in ("ALL", pattern):
                    key = (minutes, kind)
                    data[key][verdict] += 1
                    if net is not None:
                        data[key]["priced"] += 1
                        data[key]["positive"] += net > 0
                        sums[key] += net

        print(
            f"[PUMP_EDGE_READY] contexts={len(grouped)} "
            f"unusable_history={unusable} "
            f"cost_pct={cost:.3f} "
            f"sample_limit={MAX_CONTEXTS} "
            f"mode=READ_ONLY_FULL_1M_FIRST_TOUCH_NO_ORDERS",
            flush=True,
        )

        for minutes in HORIZONS:
            for pattern in ("ALL",) + PATTERNS:
                key = (minutes, pattern)
                c = data[key]
                priced = c["priced"]
                avg = sums[key] / priced if priced else None
                sample = (c["TP_FIRST"] + c["SL_FIRST"]
                          + c["TIME_EXIT"] + c["SAME_MINUTE"])
                status = (
                    "NO_SAMPLE" if priced == 0
                    else "EXPLORATORY_LOW_N" if priced < MIN_SAMPLE
                    else "RESEARCH_ONLY_NO_OOS_CONFIRMATION"
                )
                print(
                    f"[PUMP_EDGE] horizon={minutes}m pattern={pattern} "
                    f"n={sample} priced={priced} "
                    f"tp={c['TP_FIRST']} sl={c['SL_FIRST']} "
                    f"time={c['TIME_EXIT']} ambiguous={c['SAME_MINUTE']} "
                    f"net_positive={c['positive']} "
                    f"mean_net={f'{avg:+.4f}%' if avg is not None else 'NA'} "
                    f"status={status}",
                    flush=True,
                )
    except Exception as exc:
        print(
            f"[PUMP_EDGE_ERROR] {type(exc).__name__}: {str(exc)[:160]}",
            flush=True,
        )
