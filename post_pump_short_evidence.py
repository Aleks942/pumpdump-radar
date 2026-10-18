"""Point-in-time, cached evidence for existing POST-PUMP SHORT research.

No external HTTP, no new workers, no orders, no Telegram and no trade gates.
The flow metrics are bounded taker buy/sell imbalances, NOT true cumulative
Spot/Futures CVD. Sampled quote returns are NOT OHLC H1/MTF trend confirmation.
Missing/stale readings must remain NULL rather than becoming bearish votes.
"""
import json
import math
import time

QUOTE_MINUTES = (5,15,60)
QUOTE_TOLERANCE_SEC = {5:120,15:150,60:180}
MAX_SAMPLE_GAP_SEC = 240
RETENTION_SEC = 2*3600
MAX_FLOW_STALE_SEC = 60
MAX_AGG_AGE_SEC = 120

METRIC_COLUMNS = {
    "local_oi_5m_pct":"REAL",
    "agg_oi_5m_pct":"REAL",
    "futures_taker_5m_imbalance_pct":"REAL",
    "futures_taker_5m_delta_quote":"REAL",
    "spot_taker_5m_imbalance_pct":"REAL",
    "spot_taker_5m_delta_quote":"REAL",
    "sampled_return_5m_pct":"REAL",
    "sampled_return_15m_pct":"REAL",
    "sampled_return_60m_pct":"REAL",
    "evidence_captured_at":"REAL",
    "evidence_quality_json":"TEXT",
}
METRIC_NAMES = tuple(METRIC_COLUMNS)


def _finite(value):
    try:
        x=float(value)
    except (TypeError,ValueError,OverflowError):
        return None
    return x if math.isfinite(x) else None


def ensure_schema(db):
    cols={row[1] for row in db.execute(
        "PRAGMA table_info(post_pump_short_events)")}
    if not cols:
        raise ValueError("post_pump_short_events table must exist before migration")
    for key,kind in METRIC_COLUMNS.items():
        if key not in cols:
            db.execute(f"ALTER TABLE post_pump_short_events ADD COLUMN {key} {kind}")
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_quote_samples(
        symbol TEXT NOT NULL,ts REAL NOT NULL,price REAL NOT NULL,
        PRIMARY KEY(symbol,ts)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_post_pump_quote_ts
        ON post_pump_short_quote_samples(ts)""")


def take_cached_evidence(symbol, now=None, sources=None):
    """Read in-process caches only; supports deterministic source injection.

    sources: (oi.read, aggregated_oi.get_aggregated_oi,
              trade_flow.get_futures_windows, trade_flow.get_spot_windows).
    """
    now=time.time() if now is None else float(now)
    values={k:None for k in METRIC_COLUMNS}
    quality={}
    if sources is None:
        try:
            from oi_collector import read as local_oi
            from aggregated_oi import get_aggregated_oi
            from trade_flow_collector_v3 import get_futures_windows,get_spot_windows
            sources=(local_oi,get_aggregated_oi,get_futures_windows,get_spot_windows)
        except ImportError as exc:
            # Unavailable research dependency is missing evidence, NEVER
            # a reason to interrupt the live PumpDump scanner.
            values["evidence_captured_at"]=now
            values["evidence_quality_json"]=json.dumps(
                {"source_import":type(exc).__name__},sort_keys=True)
            return values
    local,aggregated,futures,spot=sources
    try:
        v=local(symbol,now) or {}
        quality["local_oi"]=str(v.get("quality","UNKNOWN"))
        if v.get("ready") is True and v.get("quality")=="READY":
            age=_finite(v.get("age_sec"))
            change=_finite(v.get("change_pct"))
            if age is not None and -5<=age<=60 and change is not None:
                values["local_oi_5m_pct"]=round(change,6)
    except Exception as exc:
        quality["local_oi"]=type(exc).__name__
    try:
        v=aggregated(symbol) or {}
        w=(v.get("windows") or {}).get("5m") or {}
        quality["aggregated_oi"]=str(w.get("quality","UNKNOWN"))
        stamp=_finite(v.get("observed_ts"))
        if (w.get("ready") is True and stamp is not None
                and -5<=now-stamp<=MAX_AGG_AGE_SEC):
            change=_finite(w.get("change_pct"))
            if change is not None:
                values["agg_oi_5m_pct"]=round(change,6)
    except Exception as exc:
        quality["aggregated_oi"]=type(exc).__name__
    for label,reader in (("futures",futures),("spot",spot)):
        try:
            w=(reader(symbol) or {}).get("5m") or {}
            quality[label]=str(w.get("quality","UNKNOWN"))
            stale=_finite(w.get("stream_stale_seconds"))
            if (w.get("window_ready") is not True
                    or w.get("stream_healthy") is not True
                    or stale is None or not 0<=stale<=MAX_FLOW_STALE_SEC):
                continue
            imb=_finite(w.get("imbalance_pct"))
            delta=_finite(w.get("delta_quote"))
            if imb is None or not -100<=imb<=100 or delta is None:
                continue
            values[f"{label}_taker_5m_imbalance_pct"]=round(imb,6)
            values[f"{label}_taker_5m_delta_quote"]=round(delta,6)
        except Exception as exc:
            quality[label]=type(exc).__name__
    values["evidence_captured_at"]=now
    values["evidence_quality_json"]=json.dumps(
        quality,sort_keys=True,ensure_ascii=True)
    return values


def sampled_returns(db,symbol,now,price):
    """Return past-only 5/15/60 minute price changes when quotes are continuous."""
    result={f"sampled_return_{m}m_pct":None for m in QUOTE_MINUTES}
    current=_finite(price)
    if current is None or current<=0:
        return result
    for minutes in QUOTE_MINUTES:
        duration=minutes*60
        target=now-duration
        tol=QUOTE_TOLERANCE_SEC[minutes]
        rows=db.execute("""SELECT ts,price FROM post_pump_short_quote_samples
            WHERE symbol=? AND ts BETWEEN ? AND ? ORDER BY ts ASC""",
            (symbol,target-tol,now-0.000001)).fetchall()
        if len(rows)<2:
            continue
        valid=[(float(t),_finite(p)) for t,p in rows]
        if any(p is None or p<=0 or t>=now for t,p in valid):
            continue
        older=[(t,p) for t,p in valid if abs(t-target)<=tol]
        if not older:
            continue
        base_t,base_price=min(older,key=lambda x:abs(x[0]-target))
        continuous=[t for t,p in valid if t>=base_t]
        if not continuous or now-continuous[-1]>MAX_SAMPLE_GAP_SEC:
            continue
        if any(b-a>MAX_SAMPLE_GAP_SEC for a,b in zip(continuous,continuous[1:])):
            continue
        value=100*(current/base_price-1)
        result[f"sampled_return_{minutes}m_pct"]=round(value,6)
    return result


def record_quote(db,symbol,now,price):
    """Store the current quote AFTER computing all past-only event evidence."""
    p=_finite(price)
    if p is not None and p>0:
        db.execute("""INSERT OR IGNORE INTO post_pump_short_quote_samples
            (symbol,ts,price) VALUES (?,?,?)""",(symbol,now,p))


def prune_quote_samples(db,now):
    db.execute("DELETE FROM post_pump_short_quote_samples WHERE ts<?",
               (now-RETENTION_SEC,))


def missing_counter(values):
    keys=METRIC_NAMES[:6]
    return sum(values.get(k) is None for k in keys)
