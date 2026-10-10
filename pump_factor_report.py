"""PumpDump: read-only historical selection-factor study.

All factors are computed from snapshots saved at signal time. Never uses
post-signal prices as predictive features. Evaluates two fixed exit policies,
on earlier/later cohorts and at both alert and next-1m-open hypothetical fills.
Not a trading signal or evidence of executable net P&L.
"""
import json
import math
import time
from collections import defaultdict
from contextlib import closing

from market_memory import get_connection
from pump_edge_report import PATTERNS, _cost, _full_history
from pump_exit_research import _cohort_split, evaluate_exit

EXPERIMENT_ID = "PUMPDUMP_ENTRY_FACTORS_V1_20261010"
SAMPLE_LIMIT = 800
LOOKBACK_SEC = 30 * 86400
FILL_MODES = ("ALERT_PRICE", "NEXT_FULL_1M_OPEN")
# Deliberately restricted exits; do not optimize exits and filters together.
EXIT_POLICIES = (
    ("TP1_SL1_10M", 1.0, 1.0, 10),
    ("TP2_SL1_20M", 2.0, 1.0, 20),
)
FACTORS = (
    "SPOT_CVD_GE20",
    "FUTURES_IMBALANCE_GE20",
    "BOTH_CVD_GE20",
    "LOCAL_OI_ABS_GE05",
    "AGGREGATED_OI_ABS_GE05",
    "MOVE_15_TO_30",
    "LIQUIDATION_DOMINANCE_3X",
    "LOW_RETRACE_10PCT",
)
MIN_TRAIN_N = 40
MIN_LATER_N = 30
MIN_LATER_DAYS = 5


def _num(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError, OverflowError):
        return None


def extract_features(snapshot, pattern, direction):
    """Return factor True/False/None (None = not observed, never impute)."""
    if not isinstance(snapshot, dict) or direction not in ("LONG", "SHORT"):
        return {name: None for name in FACTORS}
    direction_sign = 1 if direction == "LONG" else -1
    futures = snapshot.get("futures_flow") or {}
    spot = snapshot.get("spot_cvd") or {}
    oi = snapshot.get("aggregated_oi") or {}
    liq = snapshot.get("liquidations") or {}
    anti_late = snapshot.get("anti_late") or {}
    if not all(isinstance(x, dict) for x in (futures, spot, oi, liq, anti_late)):
        return {name: None for name in FACTORS}
    oi5 = ((oi.get("windows") or {}).get("5m") or {})
    if not isinstance(oi5, dict):
        oi5 = {}
    change = _num(snapshot.get("change"))
    loc_oi = _num(snapshot.get("oi_change"))
    agg_oi = _num(oi5.get("change_pct"))
    fut = _num(futures.get("imbalance_pct"))
    spot_value = _num(spot.get("cvd_percent"))
    long_liq = _num(liq.get("long_liq"))
    short_liq = _num(liq.get("short_liq"))
    retrace = _num(anti_late.get("retrace_share"))
    # Values may be present but stream/spot quality must also be valid.
    if futures.get("window_ready") is not True:
        fut = None
    if spot.get("available") is not True:
        spot_value = None
    if oi5.get("ready") is not True:
        agg_oi = None
    if anti_late.get("allow") is not True:
        retrace = None

    fut_aligned = fut * direction_sign if fut is not None else None
    spot_aligned = spot_value * direction_sign if spot_value is not None else None
    spot_good = spot_aligned >= 20 if spot_aligned is not None else None
    fut_good = fut_aligned >= 20 if fut_aligned is not None else None

    dominant = None
    if liq.get("available") is True and long_liq is not None and short_liq is not None:
        if pattern == "SHORT_SQUEEZE":
            dominant = short_liq > 0 and short_liq >= 3 * max(long_liq, 1.0)
        elif pattern == "LONG_LIQUIDATION":
            dominant = long_liq > 0 and long_liq >= 3 * max(short_liq, 1.0)

    return {
        "SPOT_CVD_GE20": spot_good,
        "FUTURES_IMBALANCE_GE20": fut_good,
        "BOTH_CVD_GE20": (
            bool(spot_good and fut_good)
            if spot_good is not None and fut_good is not None else None
        ),
        "LOCAL_OI_ABS_GE05": abs(loc_oi) >= 0.5 if loc_oi is not None else None,
        "AGGREGATED_OI_ABS_GE05": (
            abs(agg_oi) >= 0.5 if agg_oi is not None else None
        ),
        "MOVE_15_TO_30": (1.5 <= abs(change) < 3.0) if change is not None else None,
        "LIQUIDATION_DOMINANCE_3X": dominant,
        "LOW_RETRACE_10PCT": retrace <= 10 if retrace is not None else None,
    }


def _load_cases():
    with closing(get_connection()) as db:
        rows = db.execute(
            """
            WITH chosen AS (
                SELECT e.context_id, e.entry_ts, e.symbol, e.pattern,
                       e.direction, e.entry_price, e.snapshot_json, j.first_ms
                FROM entry_contexts e
                JOIN candle_path_jobs j ON j.context_id=e.context_id
                WHERE j.status='READY' AND e.entry_ts >= ?
                ORDER BY e.entry_ts DESC, e.context_id
                LIMIT ?
            )
            SELECT z.context_id, z.entry_ts, z.symbol, z.pattern,
                   z.direction, z.entry_price, z.snapshot_json, z.first_ms,
                   b.open_ms, b.o, b.h, b.l, b.c
            FROM chosen z
            JOIN entry_candles_1m b ON b.context_id=z.context_id
            ORDER BY z.entry_ts, z.context_id, b.open_ms
            """, (time.time() - LOOKBACK_SEC, SAMPLE_LIMIT)
        ).fetchall()
    cases = []
    current_id = None
    meta = None
    candles = []
    first_open = None
    invalid_snapshots = 0
    for cid, ts, symbol, pattern, side, entry, raw, first_ms, ms, o, hi, lo, close in rows:
        if cid != current_id:
            if meta is not None:
                cases.append((*meta, first_open, candles))
            current_id = cid
            try:
                snapshot = json.loads(raw or "{}")
                if not isinstance(snapshot, dict):
                    raise ValueError("not object")
            except (ValueError, TypeError):
                snapshot = {}
                invalid_snapshots += 1
            meta = (float(ts), str(symbol), str(pattern), str(side),
                    float(entry), int(first_ms), snapshot)
            candles = []
            first_open = float(o)
        candles.append((int(ms), float(hi), float(lo), float(close)))
    if meta is not None:
        cases.append((*meta, first_open, candles))
    return cases, invalid_snapshots


def _metric(cases, factor, tp, sl, minutes, cost, fill_mode):
    data = []
    missing_factor = 0
    missing_candles = 0
    priced_days = defaultdict(list)
    for ts, symbol, pattern, side, price, start_ms, snap, first_open, bars in cases:
        if not _full_history(bars, start_ms):
            missing_candles += 1
            continue
        if factor != "ALL":
            feature = extract_features(snap, pattern, side).get(factor)
            if feature is None:
                missing_factor += 1
                continue
            if not feature:
                continue
        if fill_mode == "NEXT_FULL_1M_OPEN":
            if not (
                first_open > 0 and math.isfinite(first_open)
                and bars[0][2] <= first_open <= bars[0][1]
            ):
                missing_candles += 1
                continue
            model_price = first_open
        else:
            model_price = price
        verdict, net = evaluate_exit(
            model_price, side, bars, tp, sl, minutes, cost
        )
        if net is None:
            continue
        data.append(net)
        priced_days[int(ts // 86400)].append(net)
    n = len(data)
    return {
        "n": n,
        "days": len(priced_days),
        "positive_days": sum(sum(v) > 0 for v in priced_days.values()),
        "mean": sum(data) / n if n else None,
        "missing_factor": missing_factor,
        "missing_candles": missing_candles,
    }


def _fmt(n):
    return f"{n:+.4f}%" if n is not None else "NA"


def print_factor_edge():
    """Hourly bounded read-only research; does not change signals or storage."""
    try:
        all_cases, invalid_snapshots = _load_cases()
        all_cases = [x for x in all_cases if x[2] in PATTERNS]
        older, later = _cohort_split(all_cases)
        cost = _cost()
        print(
            f"[PUMP_FACTOR_READY] id={EXPERIMENT_ID} "
            f"total={len(all_cases)} older={len(older)} later={len(later)} "
            f"bad_snapshots={invalid_snapshots} "
            f"cost_pct={cost:.3f} embargo_min=30 "
            f"candidate_factors={len(FACTORS)} "
            f"status=EXPLORATORY_MULTIPLE_TESTS_NOT_FORWARD_VALIDATED",
            flush=True,
        )
        for pattern in PATTERNS:
            train = [x for x in older if x[2] == pattern]
            verify = [x for x in later if x[2] == pattern]
            for exit_id, tp, sl, minutes in EXIT_POLICIES:
                for factor in ("ALL",) + FACTORS:
                    scores = []
                    for label, cohort in (("older", train), ("later", verify)):
                        for mode in FILL_MODES:
                            scores.append(_metric(
                                cohort, factor, tp, sl, minutes, cost, mode
                            ))
                    # [older_alert, older_next, later_alert, later_next]
                    a, b, c, d = scores
                    # Compare with same-pattern baseline, never a pooled
                    # ALL market baseline (avoids obvious pattern confounding).
                    valid = (a["n"] >= MIN_TRAIN_N
                             and b["n"] >= MIN_TRAIN_N
                             and c["n"] >= MIN_LATER_N
                             and d["n"] >= MIN_LATER_N
                             and c["days"] >= MIN_LATER_DAYS
                             and d["days"] >= MIN_LATER_DAYS)
                    status = (
                        "INSUFFICIENT" if not valid
                        else "NEGATIVE" if any(
                            x["mean"] <= 0 for x in scores
                        ) else "FORWARD_TEST_CANDIDATE_NOT_PROVEN"
                    )
                    print(
                        f"[PUMP_FACTOR] pattern={pattern} "
                        f"exit={exit_id} factor={factor} "
                        f"early_n={a['n']} early_net={_fmt(a['mean'])} "
                        f"late_n={c['n']} late_net={_fmt(c['mean'])} "
                        f"early_next_net={_fmt(b['mean'])} "
                        f"late_next_net={_fmt(d['mean'])} "
                        f"late_days={c['days']} "
                        f"positive_days={c['positive_days']} "
                        f"missing_feature={c['missing_factor']} "
                        f"status={status}",
                        flush=True,
                    )
    except Exception as error:
        print(
            f"[PUMP_FACTOR_ERROR] {type(error).__name__}: {str(error)[:180]}",
            flush=True,
        )
