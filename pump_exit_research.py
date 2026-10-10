"""PumpDump: prespecified alternative-exit research, not executable orders.

Use ONLY candles captured after signals. Fixed candidate list avoids claiming
an optimized best-fit TP/SL. Evaluate older and later chronological cohorts,
with 30-minute embargo at boundary to reduce time-window leakage. Models
first intrabar TP or SL; an unknowable simultaneous touch is SL (conservative).
"""
import math
import time
from collections import defaultdict
from contextlib import closing

from market_memory import get_connection
from pump_edge_report import PATTERNS, _cost, _full_history

# TP%, SL%, max hold minutes. Baseline included as an explicit comparator.
# Deliberately small fixed menu; altering candidates requires a new experiment.
EXPERIMENT_ID = "PUMPDUMP_EXIT_V1_20261010"
EXITS = (
    ("BASE_1_1_10", 1.0, 1.0, 10),
    ("TP15_SL1_10", 1.5, 1.0, 10),
    ("TP15_SL1_20", 1.5, 1.0, 20),
    ("TP2_SL1_20", 2.0, 1.0, 20),
    ("TP2_SL15_30", 2.0, 1.5, 30),
    ("TP1_SL075_10", 1.0, 0.75, 10),
)
CONTEXT_LIMIT = 800
LOOKBACK_SECONDS = 30 * 86400
EMBARGO_SECONDS = 30 * 60
MIN_TRAIN = 60
MIN_LATER = 40
MIN_DAYS_LATER = 5


def evaluate_exit(entry, direction, candles, tp_pct, sl_pct, minutes, cost_pct):
    """Return (verdict, net percent). Missing or invalid bars are excluded."""
    try:
        entry = float(entry)
        if not math.isfinite(entry) or entry <= 0:
            return "INVALID_ENTRY", None
        if direction not in ("LONG", "SHORT"):
            return "INVALID_SIDE", None
        if len(candles) < minutes:
            return "INCOMPLETE", None
        if not all(
            math.isfinite(float(v)) and float(v) > 0
            for v in (tp_pct, sl_pct)
        ):
            return "INVALID_LEVELS", None
        sign = 1 if direction == "LONG" else -1
        target = entry * (1 + sign * tp_pct / 100)
        stop = entry * (1 - sign * sl_pct / 100)
        if stop <= 0 or target <= 0:
            return "INVALID_LEVELS", None

        for _ts, hi, lo, close in candles[:minutes]:
            if not (
                all(math.isfinite(v) and v > 0 for v in (hi, lo, close))
                and lo <= close <= hi
            ):
                return "INVALID_BAR", None
            target_hit = hi >= target if sign == 1 else lo <= target
            stop_hit = lo <= stop if sign == 1 else hi >= stop
            if target_hit and stop_hit:
                return "BOTH_ASSUME_STOP", round(-sl_pct-cost_pct, 6)
            if stop_hit:
                return "SL_FIRST", round(-sl_pct-cost_pct, 6)
            if target_hit:
                return "TP_FIRST", round(tp_pct-cost_pct, 6)

        close = candles[minutes-1][3]
        net = sign * (close / entry - 1) * 100 - cost_pct
        return "TIME_EXIT", round(net, 6)
    except (ValueError, TypeError, OverflowError, ZeroDivisionError):
        return "INVALID_DATA", None


def _load_cohorts(now):
    with closing(get_connection()) as db:
        rows = db.execute(
            """
            WITH selected AS (
                SELECT e.context_id, e.entry_ts, e.symbol, e.pattern,
                       e.direction, e.entry_price, j.first_ms
                FROM entry_contexts e
                JOIN candle_path_jobs j ON j.context_id=e.context_id
                WHERE j.status='READY' AND e.entry_ts >= ?
                ORDER BY e.entry_ts DESC, e.context_id
                LIMIT ?
            )
            SELECT s.context_id, s.entry_ts, s.symbol, s.pattern,
                   s.direction, s.entry_price, s.first_ms,
                   b.open_ms, b.o, b.h, b.l, b.c
            FROM selected s
            JOIN entry_candles_1m b ON b.context_id=s.context_id
            ORDER BY s.entry_ts, s.context_id, b.open_ms
            """,
            (now-LOOKBACK_SECONDS, CONTEXT_LIMIT)
        ).fetchall()

    entries = []
    current_id = None
    candles = []
    meta = None
    for cid, entry_ts, symbol, pattern, side, entry, first_ms, open_ms, o, hi, lo, close in rows:
        if cid != current_id:
            if meta is not None:
                entries.append((*meta, candles))
            current_id = cid
            meta = (float(entry_ts), str(symbol), str(pattern), str(side),
                    float(entry), int(first_ms), float(o))
            candles = []
        candles.append((int(open_ms), float(hi), float(lo), float(close)))
    if meta is not None:
        entries.append((*meta, candles))
    return entries


def _cohort_split(entries):
    """Chronological 70/30 with ±30-minute embargo at the boundary."""
    if len(entries) < 2:
        return [], []
    entries = sorted(entries, key=lambda z: (z[0], z[1]))
    cutoff = entries[int(len(entries)*0.70)][0]
    older = [x for x in entries if x[0] < cutoff-EMBARGO_SECONDS]
    later = [x for x in entries if x[0] >= cutoff+EMBARGO_SECONDS]
    return older, later


def _stats(entries, tp, sl, minutes, cost, entry_mode="ALERT_PRICE"):
    values = []
    verdicts = defaultdict(int)
    dates = set()
    symbols = set()
    for ts, symbol, pattern, side, entry, first_ms, first_open, candles in entries:
        if not _full_history(candles, first_ms):
            verdicts["INCOMPLETE"] += 1
            continue
        if entry_mode == "NEXT_FULL_1M_OPEN":
            if (not math.isfinite(first_open) or first_open <= 0
                    or not candles[0][2] <= first_open <= candles[0][1]):
                verdicts["INVALID_OPEN"] += 1
                continue
            model_entry = first_open
        else:
            model_entry = entry
        result, value = evaluate_exit(
            model_entry, side, candles, tp, sl, minutes, cost,
        )
        verdicts[result] += 1
        if value is None:
            continue
        values.append(value)
        dates.add(int(ts//86400))
        symbols.add(symbol)
    n = len(values)
    return {
        "n": n,
        "mean": sum(values)/n if n else None,
        "positive": sum(v > 0 for v in values),
        "days": len(dates),
        "symbols": len(symbols),
        "ambiguous": verdicts["BOTH_ASSUME_STOP"],
        "timeouts": verdicts["TIME_EXIT"],
    }


def print_exit_research():
    """Read-only on-disk data; no new exchange calls or altered bot behavior."""
    try:
        entries = _load_cohorts(time.time())
        entries = [x for x in entries if x[2] in PATTERNS]
        older, later = _cohort_split(entries)
        cost = _cost()
        print(
            f"[PUMP_EXIT_READY] experiment={EXPERIMENT_ID} total={len(entries)} "
            f"older={len(older)} later={len(later)} embargo_min=30 "
            f"cost_pct={cost:.3f} "
            f"split=CHRONOLOGICAL_70_30 "
            f"status=RESEARCH_ONLY_NO_LIVE_FILTER_CHANGE",
            flush=True,
        )
        for pattern in ("ALL",)+PATTERNS:
            train = older if pattern == "ALL" else [x for x in older if x[2] == pattern]
            verify = later if pattern == "ALL" else [x for x in later if x[2] == pattern]
            for name, tp, sl, minutes in EXITS:
                a = _stats(train, tp, sl, minutes, cost)
                b = _stats(verify, tp, sl, minutes, cost)
                delayed_train = _stats(
                    train, tp, sl, minutes, cost, entry_mode="NEXT_FULL_1M_OPEN"
                )
                delayed_later = _stats(
                    verify, tp, sl, minutes, cost, entry_mode="NEXT_FULL_1M_OPEN"
                )
                # A positive exploratory split is NOT OOS verification:
                # multiple candidate testing and shared market shocks remain.
                status = (
                    "INSUFFICIENT" if a["n"] < MIN_TRAIN
                    or b["n"] < MIN_LATER
                    or b["days"] < MIN_DAYS_LATER
                    else "NEGATIVE" if a["mean"] <= 0 or b["mean"] <= 0
                    else "CANDIDATE_FOR_FUTURE_PROSPECTIVE_TEST"
                )
                old_mean = (f'{a["mean"]:+.4f}%' if a["mean"] is not None else "NA")
                later_mean = (f'{b["mean"]:+.4f}%' if b["mean"] is not None else "NA")
                delayed_train_text = (
                    f"{delayed_train['mean']:+.4f}%"
                    if delayed_train['mean'] is not None else "NA"
                )
                delayed_later_text = (
                    f"{delayed_later['mean']:+.4f}%"
                    if delayed_later['mean'] is not None else "NA"
                )
                print(
                    f"[PUMP_EXIT_TEST] pattern={pattern} exit={name} "
                    f"train_n={a['n']} train_mean={old_mean} "
                    f"later_n={b['n']} later_mean={later_mean} "
                    f"later_days={b['days']} later_symbols={b['symbols']} "
                    f"later_both_as_sl={b['ambiguous']} "
                    f"delayed_train_n={delayed_train['n']} "
                    f"delayed_train_mean={delayed_train_text} "
                    f"delayed_later_n={delayed_later['n']} "
                    f"delayed_later_mean={delayed_later_text} "
                    f"status={status}",
                    flush=True,
                )
    except Exception as exc:
        print(
            f"[PUMP_EXIT_ERROR] {type(exc).__name__}: {str(exc)[:160]}",
            flush=True,
        )
