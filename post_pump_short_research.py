"""Research-only POST-PUMP SHORT cohorts inside the existing PumpDump Radar.

Separate +50-99% / +100% (24h high versus 24h opening quote) at observation
time. Correlate causal pump_cycle stage transitions with later OKX ticker
markouts. No new API calls, Telegram, orders, scores, risk or live trade gates.

CAUTION: exchange 24h high is a *historical rolling high*, not proof of the
time the high occurred. Trend stages require live chronological observations
from pump_cycle_state. FLOW_ALIGNED is an observed taker-flow verdict, NOT
validated price impact, H1/H4 trend, a retest or a trade-ready signal.
"""
import math
import os
import re
import sqlite3
import time
from collections import defaultdict
from contextlib import closing

from ticker_metrics import approx_quote_volume_usdt, spread_bps
from post_pump_short_evidence import (
    ensure_schema as ensure_evidence_schema,
    take_cached_evidence, sampled_returns, record_quote,
    prune_quote_samples, METRIC_NAMES, missing_counter,
)

DB_PATH = os.getenv("PUMP_WATCH_DB", "/data/pump_watch.sqlite3")
MIN_RISE_PCT = 50.0
MIN_QUOTE_VOLUME_USDT = 10_000_000.0
MAX_SPREAD_BPS = 40.0
HORIZONS = (5, 15, 30, 60)
MAX_MARKOUT_LAG_SEC = 240
MAX_EVENTS = 5000
RETENTION_SEC = 30 * 86400
MODEL_ROUNDTRIP_COST_PCT = 0.22
_last_report_at = None

PHASES = ("WATCH", "WEAKENING", "STRUCTURE_BREAK", "FLOW_ALIGNED_RESEARCH")


def _n(value):
    try:
        x = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return x if math.isfinite(x) else None


def _ticker(t):
    if not isinstance(t, dict):
        return None
    inst = t.get("instId")
    if not isinstance(inst, str) or not re.fullmatch(r"[A-Z0-9]{2,25}-USDT-SWAP", inst):
        return None
    symbol = inst[:-10] + "USDT"
    price = _n(t.get("last"))
    opening = _n(t.get("open24h"))
    high = _n(t.get("high24h"))
    volume = approx_quote_volume_usdt(t)
    spread = spread_bps(t)
    if (any(v is None or v <= 0 for v in (price,opening,high,volume))
            or high < max(price, opening)
            or volume < MIN_QUOTE_VOLUME_USDT
            or spread is None or spread > MAX_SPREAD_BPS):
        return None
    rise = 100*(high/opening-1)
    if rise < MIN_RISE_PCT:
        return None
    retrace = 100*(1-price/high)
    return (symbol, price, rise, retrace, "100_PLUS" if rise >= 100 else "50_TO_99")


def _setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_events(
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL, episode_at REAL NOT NULL,
        observed_at REAL NOT NULL, phase TEXT NOT NULL,
        rise_band TEXT NOT NULL, reference_price REAL NOT NULL,
        rise_24h_pct REAL NOT NULL, retrace_from_24h_high_pct REAL NOT NULL,
        cycle_stage TEXT NOT NULL, structure_level REAL,
        flow_status TEXT, flow_delta REAL,
        mtf_confirmed INTEGER NOT NULL DEFAULT 0,
        UNIQUE(symbol,episode_at,phase,rise_band)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_markouts(
        event_id INTEGER NOT NULL, horizon_min INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        observed_at REAL, lag_sec REAL, price REAL,
        gross_short_pct REAL, model_net_short_pct REAL,
        PRIMARY KEY(event_id,horizon_min)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_post_pump_due
        ON post_pump_short_events(observed_at)""")
    ensure_evidence_schema(db)


def _quotes(tickers):
    """Quote map and qualifying strong-pump metadata from one OKX REST batch."""
    quotes = {}
    pumps = {}
    for t in tickers or ():
        if not isinstance(t,dict):
            continue
        inst = t.get("instId")
        price = _n(t.get("last"))
        if (isinstance(inst,str) and
                re.fullmatch(r"[A-Z0-9]{2,25}-USDT-SWAP",inst) and
                price is not None and price>0):
            quotes[inst[:-10]+"USDT"] = price
            profile = _ticker(t)
            if profile:
                pumps[profile[0]] = profile
    return quotes,pumps


def _phases(state, now):
    """Causal stages only. Never reinterpret old stages using a later quote."""
    phase = ["WATCH"]
    stage = state["stage"]
    if stage == "PUMP_WEAKENING" and state["weakening_at"] is not None:
        if 0 <= now-state["weakening_at"] <= 4*3600:
            phase.append("WEAKENING")
    if (stage in ("PUMP_WEAKENING","REVERSAL_CONFIRMED","DUMP_CONTINUATION")
            and state["break_at"] is not None
            and 0 <= now-state["break_at"] <= 180
            and state["support"] is not None
            and state["last_price"] < state["support"]*0.998):
        phase.append("STRUCTURE_BREAK")
    if (stage == "REVERSAL_CONFIRMED"
            and state["flow_status"] == "CONFIRMED"
            and state["flow_delta"] is not None
            and state["flow_delta"] <= -0.20
            and state["flow_verified_at"] is not None
            and 0 <= now-state["flow_verified_at"] <= 180
            and state["weakening_at"] is not None
            and state["flow_observed_at"] is not None
            and state["flow_observed_at"] >= state["weakening_at"]
            and state["support"] is not None
            and state["last_price"] < state["support"]*0.998):
        phase.append("FLOW_ALIGNED_RESEARCH")
    return phase


def _prune(db,now):
    expiry = now-RETENTION_SEC
    db.execute("""DELETE FROM post_pump_short_markouts WHERE event_id IN
        (SELECT id FROM post_pump_short_events WHERE observed_at < ?)""",
        (expiry,))
    db.execute("DELETE FROM post_pump_short_events WHERE observed_at < ?",
               (expiry,))
    db.execute("""DELETE FROM post_pump_short_markouts WHERE event_id IN
        (SELECT id FROM post_pump_short_events
         ORDER BY id DESC LIMIT -1 OFFSET ?)""",(MAX_EVENTS,))
    db.execute("""DELETE FROM post_pump_short_events WHERE id NOT IN (
        SELECT DISTINCT event_id FROM post_pump_short_markouts)""")


def observe(tickers, now=None, db_path=None):
    """Take immutable pump/phase observations and follow already stored cases."""
    now = time.time() if now is None else float(now)
    if not math.isfinite(now) or now <= 0:
        return (0,0)
    db_path = DB_PATH if db_path is None else db_path
    quotes,pumps = _quotes(tickers)
    inserted = priced = missed = 0
    try:
        os.makedirs(os.path.dirname(db_path) or ".",exist_ok=True)
        with closing(sqlite3.connect(db_path,timeout=8)) as db:
            db.row_factory=sqlite3.Row
            db.execute("PRAGMA busy_timeout=8000")
            _setup(db)
            has_cycle=db.execute("""SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='pump_cycle_state'""").fetchone()
            if has_cycle and pumps:
                keys=set(pumps)
                states=db.execute("""SELECT symbol,start_at,last_at,stage,
                        weakening_at,break_at,support,last_price,
                        flow_status,flow_delta,flow_verified_at,flow_observed_at
                    FROM pump_cycle_state WHERE last_at>=?""",(now-75,)).fetchall()
                for state in states:
                    symbol=state["symbol"]
                    if symbol not in keys:
                        continue
                    symbol,price,rise,retrace,band=pumps[symbol]
                    # No back-filling 24h rise or flow at event time.
                    if state["start_at"]>now or state["last_at"]>now:
                        continue
                    captured=None
                    for phase in _phases(state,now):
                        cursor=db.execute("""INSERT OR IGNORE INTO
                            post_pump_short_events(
                                symbol,episode_at,observed_at,phase,
                                rise_band,reference_price,rise_24h_pct,
                                retrace_from_24h_high_pct,cycle_stage,
                                structure_level,flow_status,flow_delta)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (symbol,state["start_at"],now,phase,band,price,
                             rise,retrace,state["stage"],state["support"],
                             state["flow_status"],state["flow_delta"]))
                        if cursor.rowcount!=1:
                            continue
                        # Snapshot cached evidence at the FIRST causal event,
                        # never retrofit future observations on old events.
                        if captured is None:
                            captured=take_cached_evidence(symbol,now)
                            captured.update(sampled_returns(db,symbol,now,price))
                        columns=", ".join(f"{name}=?" for name in METRIC_NAMES)
                        values=tuple(captured.get(name) for name in METRIC_NAMES)
                        db.execute(
                            f"UPDATE post_pump_short_events SET {columns} WHERE id=?",
                            values+(cursor.lastrowid,))
                        db.executemany("""INSERT INTO post_pump_short_markouts
                            (event_id,horizon_min) VALUES (?,?)""",
                            ((cursor.lastrowid,h) for h in HORIZONS))
                        inserted+=1
                        print(f"[POST_PUMP_SHORT_EVENT] symbol={symbol} "
                              f"band={band} phase={phase} rise24={rise:+.2f}% "
                              f"retrace24={retrace:.2f}% flow={state['flow_status']} "
                              f"missing_cached_metrics={missing_counter(captured)} "
                              f"return5={captured.get('sampled_return_5m_pct')} "
                              f"return15={captured.get('sampled_return_15m_pct')} "
                              f"return60={captured.get('sampled_return_60m_pct')} "
                              "mtf=UNVERIFIED trade_approved=false",flush=True)
            # Save a bounded quote trail for ALL observed +50% pump symbols.
            # This happens AFTER event features, preventing self-observation
            # from creating a fake historic return on the same scan.
            for symbol,price,*_rest in pumps.values():
                record_quote(db,symbol,now,price)
            prune_quote_samples(db,now)

            # Forward-only markouts use ANY valid quote from the same OKX batch
            # even if the symbol is no longer a +50% 24h pump by then.
            due=db.execute("""SELECT o.event_id,o.horizon_min,
                        a.symbol,a.observed_at,a.reference_price,a.phase,
                        a.rise_band FROM post_pump_short_markouts o
                    JOIN post_pump_short_events a ON a.id=o.event_id
                    WHERE o.status='PENDING' AND a.observed_at<=?
                    ORDER BY a.observed_at LIMIT 2500""",(now,)).fetchall()
            for row in due:
                ident,h,symbol,seen,ref,phase,band=row
                lag=now-seen-60*h
                if lag<0:
                    continue
                quote=quotes.get(symbol)
                if lag>MAX_MARKOUT_LAG_SEC:
                    db.execute("""UPDATE post_pump_short_markouts
                        SET status='MISSING' WHERE event_id=? AND horizon_min=?
                        AND status='PENDING'""",(ident,h))
                    missed+=1
                elif quote is not None:
                    gross=100*(ref-quote)/ref
                    net=gross-MODEL_ROUNDTRIP_COST_PCT
                    db.execute("""UPDATE post_pump_short_markouts SET
                        status='DONE',observed_at=?,lag_sec=?,price=?,
                        gross_short_pct=?,model_net_short_pct=?
                        WHERE event_id=? AND horizon_min=? AND status='PENDING'""",
                        (now,lag,quote,round(gross,6),round(net,6),ident,h))
                    priced+=1
            _prune(db,now)
            db.commit()
        if inserted or priced or missed:
            print(f"[POST_PUMP_SHORT_BATCH] events={inserted} "
                  f"markouts={priced} missing={missed} "
                  f"qualifying_pumps={len(pumps)} research_only=true "
                  f"no_new_http=true no_orders=true",flush=True)
        return (inserted,priced)
    except (sqlite3.Error,ValueError,TypeError,OverflowError) as exc:
        print(f"[POST_PUMP_SHORT_ERROR] kind={type(exc).__name__} "
              f"detail={str(exc)[:100]}",flush=True)
        return (0,0)


def report(now=None,db_path=None):
    """Hourly descriptive report of distinct +50%/+100% stage markouts."""
    global _last_report_at
    now=time.time() if now is None else float(now)
    if _last_report_at is not None and now-_last_report_at<3600:
        return []
    db_path=DB_PATH if db_path is None else db_path
    try:
        with closing(sqlite3.connect(db_path,timeout=5)) as db:
            _setup(db)
            rows=db.execute("""SELECT a.rise_band,a.phase,
                       o.horizon_min,COUNT(*) AS total,
                       SUM(CASE WHEN o.status='DONE' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN o.status='MISSING' THEN 1 ELSE 0 END),
                       COUNT(DISTINCT CASE WHEN o.status='DONE' THEN a.symbol END),
                       COUNT(DISTINCT CASE WHEN o.status='DONE'
                           THEN CAST(a.observed_at/86400 AS INTEGER) END),
                       AVG(CASE WHEN o.status='DONE' THEN o.model_net_short_pct END),
                       SUM(CASE WHEN o.status='DONE'
                           AND o.model_net_short_pct>0 THEN 1 ELSE 0 END)
                    FROM post_pump_short_events a JOIN post_pump_short_markouts o
                    ON a.id=o.event_id
                    GROUP BY a.rise_band,a.phase,o.horizon_min
                    ORDER BY a.rise_band,a.phase,o.horizon_min""").fetchall()
        _last_report_at=now
        result=[]
        for band,phase,h,total,done,missing,symbols,days,avg,positive in rows:
            x={"band":band,"phase":phase,"horizon_min":h,
               "total":total,"done":done or 0,"missing":missing or 0,
               "symbols":symbols or 0,"days":days or 0,
               "mean_model_short_pct":round(avg,5) if avg is not None else None,
               "positive":positive or 0}
            result.append(x)
            print(f"[POST_PUMP_SHORT_QUALITY] {x} "
                  "observational_quote_only=true not_first_touch=true "
                  "mtf_not_verified=true no_trade_approval=true",flush=True)
        if not rows:
            print("[POST_PUMP_SHORT_SUMMARY] events=0 "
                  "status=WAITING_FOR_STRONG_PUMPS research_only=true",flush=True)
        return result
    except (sqlite3.Error,ValueError,TypeError) as exc:
        print("[POST_PUMP_SHORT_REPORT_ERROR]",type(exc).__name__,flush=True)
        return []
