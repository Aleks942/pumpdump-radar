"""Causal stage-to-stage seller/buyer-control research for PumpDump Radar.

Compares point-in-time evidence from DISTINCT observed stages of ONE pump
episode and ONE rise band. Never equates a single negative flow reading with
a confirmed reversal. No entries, signals, orders, REST, or Telegram.
"""
import math
import sqlite3

PHASE_ORDER = {
    "WATCH": 0,
    "WEAKENING": 1,
    "STRUCTURE_BREAK": 2,
    "FLOW_ALIGNED_RESEARCH": 3,
}
MAX_PREVIOUS_GAP_SEC = 3*3600
FEATURES = {
    "local_oi_5m_pct":"local_oi_delta_pp",
    "agg_oi_5m_pct":"agg_oi_delta_pp",
    "futures_taker_5m_imbalance_pct":"futures_imbalance_delta_pp",
    "spot_taker_5m_imbalance_pct":"spot_imbalance_delta_pp",
}
COLUMNS = tuple(FEATURES)


def _num(x):
    try:
        number=float(x)
    except (TypeError,ValueError,OverflowError):
        return None
    return number if math.isfinite(number) else None


def setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_stage_comparisons(
        event_id INTEGER PRIMARY KEY,
        previous_event_id INTEGER, previous_phase TEXT,
        observation_gap_sec REAL,
        local_oi_delta_pp REAL, agg_oi_delta_pp REAL,
        futures_imbalance_delta_pp REAL, spot_imbalance_delta_pp REAL,
        flow_control TEXT NOT NULL,
        price_reaction TEXT NOT NULL,
        oi_context TEXT NOT NULL,
        research_only INTEGER NOT NULL DEFAULT 1
    )""")


def _previous(db,symbol,episode_at,band,phase,now):
    rank=PHASE_ORDER.get(phase)
    if rank is None or rank==0:
        return None
    prior_phases=tuple(name for name,value in PHASE_ORDER.items()
                       if value<rank)
    placeholders=",".join("?" for _ in prior_phases)
    # Latest higher-progress stage, then freshest chronological observation.
    # A stage from the SAME scan is not a historical baseline.
    clause="CASE phase "+ " ".join(
        f"WHEN '{name}' THEN {value}" for name,value in PHASE_ORDER.items()
    )+" ELSE -1 END"
    return db.execute(f"""SELECT id,phase,observed_at,
            local_oi_5m_pct,agg_oi_5m_pct,
            futures_taker_5m_imbalance_pct,
            spot_taker_5m_imbalance_pct
        FROM post_pump_short_events
        WHERE symbol=? AND episode_at=? AND rise_band=?
          AND phase IN ({placeholders})
          AND observed_at<? AND observed_at>=?
          AND evidence_captured_at IS NOT NULL
        ORDER BY {clause} DESC, observed_at DESC LIMIT 1""",
        (symbol,episode_at,band,*prior_phases,now,now-MAX_PREVIOUS_GAP_SEC)
    ).fetchone()


def classify(previous,current,stage):
    """Human-readable research strata, NOT a trade approval or prediction."""
    changes={}
    for feature,delta_key in FEATURES.items():
        a=_num(current.get(feature))
        b=_num(previous[feature]) if previous is not None else None
        changes[delta_key] = round(a-b,6) if a is not None and b is not None else None
    future=_num(current.get("futures_taker_5m_imbalance_pct"))
    spot=_num(current.get("spot_taker_5m_imbalance_pct"))
    fd=changes["futures_imbalance_delta_pp"]
    sd=changes["spot_imbalance_delta_pp"]
    if previous is None:
        control="NO_PRIOR_STAGE"
    elif any(x is None for x in (future,spot,fd,sd)):
        control="INSUFFICIENT_DATA"
    elif future<=-10 and spot<=-10 and fd<=-10 and sd<=-10:
        control="BOTH_SELL_PRESSURE_STRENGTHENING"
    elif future>=10 and spot>=10 and fd>=0 and sd>=0:
        control="BUYERS_STILL_STRONG"
    elif future<=-10 and spot<=-10:
        control="BOTH_SELL_PRESSURE_PRESENT"
    elif future>=10 and spot>=10:
        control="BOTH_BUY_PRESSURE_PRESENT"
    elif future*spot<0:
        control="SPOT_FUTURES_DIVERGENCE"
    else:
        control="MIXED_FLOW"
    r5=_num(current.get("sampled_return_5m_pct"))
    if r5 is None or future is None or spot is None:
        response="INSUFFICIENT_DATA"
    elif future<=-10 and spot<=-10:
        if r5<=-0.5:
            response="SELL_PRESSURE_AND_PRICE_DOWN"
        else:
            response="SELL_PRESSURE_WITHOUT_PRICE_DROP"
    elif future>=10 and spot>=10 and r5>=0.5:
        response="BUY_PRESSURE_AND_PRICE_UP"
    else:
        response="MIXED_PRICE_RESPONSE"
    oi_local=_num(current.get("local_oi_5m_pct"))
    oi_agg=_num(current.get("agg_oi_5m_pct"))
    if oi_local is None or oi_agg is None:
        oi="INSUFFICIENT_DATA"
    elif oi_local<0 and oi_agg<0:
        oi="OI_BOTH_CONTRACTING"
    elif oi_local>0 and oi_agg>0:
        oi="OI_BOTH_EXPANDING"
    else:
        oi="OI_MIXED_OR_FLAT"
    return changes,control,response,oi


def record(db,*,event_id,symbol,episode_at,band,phase,now,current):
    """Write exactly once, after the event's immutable evidence is stored."""
    setup(db)
    if phase not in PHASE_ORDER:
        return None
    exists=db.execute("""SELECT 1 FROM post_pump_stage_comparisons
        WHERE event_id=?""",(event_id,)).fetchone()
    if exists:
        return None
    old=_previous(db,symbol,episode_at,band,phase,now)
    changes,control,response,oi=classify(old,current,phase)
    prior_id=old["id"] if old is not None else None
    prior_phase=old["phase"] if old is not None else None
    gap=round(now-old["observed_at"],3) if old is not None else None
    db.execute("""INSERT OR IGNORE INTO post_pump_stage_comparisons(
        event_id,previous_event_id,previous_phase,observation_gap_sec,
        local_oi_delta_pp,agg_oi_delta_pp,
        futures_imbalance_delta_pp,spot_imbalance_delta_pp,
        flow_control,price_reaction,oi_context
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
    (event_id,prior_id,prior_phase,gap,
     changes["local_oi_delta_pp"],changes["agg_oi_delta_pp"],
     changes["futures_imbalance_delta_pp"],changes["spot_imbalance_delta_pp"],
     control,response,oi))
    return {"previous_phase":prior_phase,"gap_sec":gap,
            "control":control,"price_response":response,"oi":oi,
            "changes":changes}


def prune(db):
    db.execute("""DELETE FROM post_pump_stage_comparisons
        WHERE NOT EXISTS(
            SELECT 1 FROM post_pump_short_events e
            WHERE e.id=post_pump_stage_comparisons.event_id)""")


def quality(db):
    """Descriptive 30m outcomes by mutually exclusive flow/price strata.

    Observations of the same episode across bands/phases are NOT independent.
    Excludes incomplete / missing observations from means.
    """
    setup(db)
    rows=db.execute("""SELECT e.rise_band,e.phase,
            COALESCE(s.flow_control,'NO_COMPARISON') AS control,
            COALESCE(s.price_reaction,'INSUFFICIENT_DATA') AS reaction,
            COUNT(*) AS total,
            SUM(CASE WHEN o.status='DONE' THEN 1 ELSE 0 END) AS priced,
            SUM(CASE WHEN o.status='MISSING' THEN 1 ELSE 0 END) AS missing,
            COUNT(DISTINCT CASE WHEN o.status='DONE' THEN e.symbol END) AS symbols,
            COUNT(DISTINCT CASE WHEN o.status='DONE'
                THEN CAST(e.observed_at/86400 AS INTEGER) END) AS days,
            AVG(CASE WHEN o.status='DONE' THEN o.model_net_short_pct END) AS mean,
            SUM(CASE WHEN o.status='DONE'
                 AND o.model_net_short_pct>0 THEN 1 ELSE 0 END) AS positives
        FROM post_pump_short_events e
        JOIN post_pump_short_markouts o
          ON o.event_id=e.id AND o.horizon_min=30
        LEFT JOIN post_pump_stage_comparisons s ON s.event_id=e.id
        GROUP BY e.rise_band,e.phase,control,reaction
        ORDER BY e.rise_band,e.phase,control,reaction""").fetchall()
    return [
        {"band":band,"stage":phase,"flow":control,"response":reaction,
         "total":total,"priced":priced or 0,"missing":missing or 0,
         "symbols":symbols or 0,"days":days or 0,
         "mean_model_short_pct":round(mean,5) if mean is not None else None,
         "positive":positives or 0}
        for band,phase,control,reaction,total,priced,missing,symbols,days,mean,positives in rows
    ]
