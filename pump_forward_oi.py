"""Frozen, future-only OI hypotheses. Research only: no alerts/orders altered."""
import hashlib
import json
import time
from contextlib import closing

from market_memory import get_connection
from pump_factor_report import _metric

ID = "PUMP_FORWARD_OI_V1_20261010"
COST_PCT = 0.22
TP, SL, MINUTES = 2.0, 1.0, 20
LIMIT = 2500
RULES = (
    ("LONG_ALL", "NEW_LONG_BUILDUP", "ALL"),
    ("LONG_OI05", "NEW_LONG_BUILDUP", "LOCAL_OI_ABS_GE05"),
    ("SQUEEZE_ALL", "SHORT_SQUEEZE", "ALL"),
    ("SQUEEZE_OI05", "SHORT_SQUEEZE", "AGGREGATED_OI_ABS_GE05"),
)
SPEC = json.dumps(
    {"id": ID, "cost_pct": COST_PCT, "tp_pct": TP, "sl_pct": SL,
     "horizon_m": MINUTES, "rules": RULES,
     "fill_models": ("ALERT_PRICE", "NEXT_FULL_1M_OPEN")},
    sort_keys=True,
)
RULE_HASH = hashlib.sha256(SPEC.encode()).hexdigest()


def _registered(db):
    db.execute("""CREATE TABLE IF NOT EXISTS pump_forward_oi_registry (
        experiment_id TEXT PRIMARY KEY,
        start_ts REAL NOT NULL,
        rule_hash TEXT NOT NULL,
        specification TEXT NOT NULL
    )""")
    with db:
        db.execute("""INSERT OR IGNORE INTO pump_forward_oi_registry
          (experiment_id, start_ts, rule_hash, specification)
          VALUES (?, ?, ?, ?)""", (ID, time.time() + 1.0, RULE_HASH, SPEC))
    record = db.execute(
        "SELECT start_ts, rule_hash FROM pump_forward_oi_registry "
        "WHERE experiment_id=?", (ID,),
    ).fetchone()
    if record is None or record[1] != RULE_HASH:
        raise ValueError("RULE_HASH_MISMATCH_USE_NEW_EXPERIMENT_ID")
    return float(record[0])


def register_forward_oi():
    try:
        with closing(get_connection()) as db:
            start = _registered(db)
        print(
            f"[PUMP_FORWARD_READY] id={ID} start_ts={start:.3f} "
            f"hash={RULE_HASH[:12]} no_historical_signals=true", flush=True
        )
    except Exception as error:
        print(f"[PUMP_FORWARD_ERROR] {type(error).__name__}: {error}", flush=True)


def _cases(db, start):
    data = db.execute("""WITH selected AS (
      SELECT e.context_id, e.entry_ts, e.symbol, e.pattern, e.direction,
             e.entry_price, e.snapshot_json, j.first_ms
      FROM entry_contexts e
      JOIN candle_path_jobs j ON j.context_id=e.context_id
      WHERE j.status='READY' AND e.entry_ts >= ?
      ORDER BY e.entry_ts, e.context_id LIMIT ?
    )
    SELECT x.context_id, x.entry_ts, x.symbol, x.pattern, x.direction,
           x.entry_price, x.snapshot_json, x.first_ms,
           b.open_ms, b.o, b.h, b.l, b.c
    FROM selected x
    JOIN entry_candles_1m b ON b.context_id=x.context_id
    ORDER BY x.entry_ts, x.context_id, b.open_ms
    """, (start, LIMIT)).fetchall()
    result = []
    previous = None
    meta = None
    bars = []
    for cid, ts, symbol, pattern, side, price, raw, ms, candle_ms, o, h, l, c in data:
        if cid != previous:
            if meta is not None:
                result.append((*meta, bars))
            previous = cid
            try:
                snap = json.loads(raw or "{}")
                if not isinstance(snap, dict):
                    snap = {}
            except (ValueError, TypeError):
                snap = {}
            meta = (float(ts), str(symbol), str(pattern), str(side),
                    float(price), int(ms), snap, float(o))
            bars = []
        bars.append((int(candle_ms), float(h), float(l), float(c)))
    if meta is not None:
        result.append((*meta, bars))
    return result


def _format(value):
    return f"{value:+.4f}%" if value is not None else "NA"


def report_forward_oi():
    try:
        with closing(get_connection()) as db:
            start = _registered(db)
            cases = _cases(db, start)
        print(
            f"[PUMP_FORWARD] id={ID} cases={len(cases)} limit={LIMIT} "
            f"age_days={(time.time()-start)/86400:.2f} "
            f"research_only=true no_orders=true",
            flush=True,
        )
        for mode in ("ALERT_PRICE", "NEXT_FULL_1M_OPEN"):
            baselines = {}
            for name, pattern, factor in RULES:
                subset = [x for x in cases if x[2] == pattern]
                stats = _metric(subset, factor, TP, SL, MINUTES, COST_PCT, mode)
                if factor == "ALL":
                    baselines[pattern] = stats
                base = baselines[pattern]
                delta = (
                    stats["mean"] - base["mean"]
                    if stats["mean"] is not None and base["mean"] is not None
                    else None
                )
                ready = stats["n"] >= 100 and stats["days"] >= 15
                if len(cases) >= LIMIT:
                    status = "SAMPLE_LIMIT_REACHED"
                elif not ready:
                    status = "COLLECTING"
                elif stats["mean"] <= 0 or (factor != "ALL" and delta <= 0):
                    status = "NOT_POSITIVE"
                else:
                    status = "MANUAL_REVIEW_ONLY"
                print(
                    f"[PUMP_FORWARD_RULE] name={name} mode={mode} "
                    f"n={stats['n']} days={stats['days']} "
                    f"positive_days={stats['positive_days']} "
                    f"net={_format(stats['mean'])} "
                    f"vs_same_pattern={_format(delta)} "
                    f"missing_feature={stats['missing_factor']} "
                    f"status={status}",
                    flush=True,
                )
    except Exception as error:
        print(
            f"[PUMP_FORWARD_ERROR] {type(error).__name__}: {str(error)[:160]}",
            flush=True,
        )
