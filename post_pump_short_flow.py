"""Autonomous local aggression/absorption verifier for POST-PUMP SHORT.

Uses only PumpDump Radar's existing in-process trade-flow caches plus the
already-computed closed-candle structure. No HTTP, no Order Flow service,
no new websocket, no Telegram and no live order permission.

The result is deliberately narrow:
- CONFIRMED = fresh SELL taker aggression + Spot confirmation + bearish response
- CONTRADICTION = strong BUY aggression (optionally absorbed by bearish price)
- INSUFFICIENT_DATA = anything stale, warming, mixed or incomplete

This is not order-book reconstruction and must not be presented as such.
"""
import json
import math
import os
import sqlite3
import threading
import time

_lock=threading.Lock()
_recent={}
COOLDOWN_SEC=120
MAX_RESULT_AGE_SEC=240
MAX_STREAM_STALE_SEC=60
MIN_1M_TRADES=5
MIN_5M_TRADES=15
MIN_1M_NOTIONAL=500.0
MIN_5M_NOTIONAL=1000.0


def _num(value):
    try:
        x=float(value)
    except (TypeError,ValueError,OverflowError):
        return None
    return x if math.isfinite(x) else None


def setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_flow_results(
        symbol TEXT NOT NULL, episode_at REAL NOT NULL,
        observed_at REAL NOT NULL, verified_at REAL,
        status TEXT NOT NULL, reason TEXT,
        delta_ratio REAL, price_change_pct REAL,
        trade_span_sec REAL, evidence_event TEXT,
        PRIMARY KEY(symbol,episode_at,observed_at)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_post_pump_flow_lookup
        ON post_pump_short_flow_results(symbol,verified_at)""")


def latest(db,symbol,now,episode_at=None):
    setup(db)
    if episode_at is None:
        row=db.execute("""SELECT * FROM post_pump_short_flow_results
            WHERE symbol=? AND verified_at>=? AND verified_at<=?
            ORDER BY verified_at DESC LIMIT 1""",
            (symbol,now-MAX_RESULT_AGE_SEC,now)).fetchone()
    else:
        row=db.execute("""SELECT * FROM post_pump_short_flow_results
            WHERE symbol=? AND episode_at=? AND verified_at>=? AND verified_at<=?
            ORDER BY verified_at DESC LIMIT 1""",
            (symbol,episode_at,now-MAX_RESULT_AGE_SEC,now)).fetchone()
    return dict(row) if row is not None else None


def store_result(db,symbol,episode_at,observed_at,result,now):
    """Persist only finite, bounded local-verifier output."""
    if not isinstance(result,dict) or result.get("status") not in (
            "CONFIRMED","CONTRADICTION","INSUFFICIENT_DATA"):
        return False
    try:
        stamp=float(result.get("finished_at"))
        req=float(observed_at)
        at=float(now)
        episode=float(episode_at)
        price=_num(result.get("price_change_pct"))
        delta=_num(result.get("delta_ratio"))
        span=_num(result.get("trade_span_sec"))
    except (ValueError,TypeError,OverflowError):
        return False
    if (not all(math.isfinite(x) for x in (stamp,req,at,episode))
            or not req<=stamp<=at or stamp-req>5 or at-stamp>5
            or delta is not None and not -1<=delta<=1
            or span is not None and not 0<span<=300):
        return False
    setup(db)
    db.execute("""INSERT OR REPLACE INTO post_pump_short_flow_results(
        symbol,episode_at,observed_at,verified_at,status,reason,
        delta_ratio,price_change_pct,trade_span_sec,evidence_event)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (symbol,episode,req,stamp,result["status"],
         str(result.get("reason") or "")[:120],
         delta,price,span,str(result.get("evidence_event") or "NONE")[:90]))
    return True


def _window(window,min_trades,min_notional):
    if not isinstance(window,dict):
        return None
    stale=_num(window.get("stream_stale_seconds"))
    trade_age=_num(window.get("last_trade_age_sec"))
    trades=_num(window.get("trade_count"))
    total=_num(window.get("total_quote"))
    imbalance=_num(window.get("imbalance_pct"))
    if (window.get("window_ready") is not True
            or window.get("stream_healthy") is not True
            or str(window.get("quality"))!="READY"
            or stale is None or not 0<=stale<=MAX_STREAM_STALE_SEC
            or trade_age is None or not 0<=trade_age<=60
            or trades is None or trades<min_trades
            or total is None or total<min_notional
            or imbalance is None or not -100<=imbalance<=100):
        return None
    return imbalance


def _price_response(structure,price):
    structure=structure if isinstance(structure,dict) else {}
    last5=structure.get("last5") if isinstance(structure.get("last5"),dict) else {}
    o=_num(last5.get("o",last5.get("open")))
    c=_num(last5.get("c",last5.get("close")))
    response=None
    if o is not None and c is not None and o>0:
        response=100*(c/o-1)
    elif c is not None and c>0 and _num(price) is not None:
        response=100*(float(price)/c-1)
    bearish=bool(structure.get("trend_shift") and structure.get("bearish5"))
    return response,bearish


def _verified_spot_imbalance(snapshot):
    """Accept only a separately verified 5m Spot sample from the strategy."""
    if not isinstance(snapshot,dict) or snapshot.get("ready") is not True:
        return None
    reason=snapshot.get("reason")
    if reason not in ("OKX_SPOT_WS_5M","OVERLAP_VERIFIED_5M","SNAPSHOT_FALLBACK"):
        return None
    if reason=="SNAPSHOT_FALLBACK":
        age=_num(snapshot.get("last_trade_age_sec"))
        if (snapshot.get("source_reason")!="OKX_SPOT_WS_5M"
                or age is None or not 0<=age<=300):
            return None
    count=_num(snapshot.get("count"))
    value=_num(snapshot.get("imbalance_pct"))
    if count is None or count<MIN_5M_TRADES or value is None or not -100<=value<=100:
        return None
    return value


def verify_local(price,futures_windows,spot_windows,structure,now=None,
                 verified_spot=None):
    """Classify cached taker aggression and price response, fail-closed."""
    now=time.time() if now is None else float(now)
    fw=futures_windows if isinstance(futures_windows,dict) else {}
    sw=spot_windows if isinstance(spot_windows,dict) else {}
    f1=_window(fw.get("1m"),MIN_1M_TRADES,MIN_1M_NOTIONAL)
    f5=_window(fw.get("5m"),MIN_5M_TRADES,MIN_5M_NOTIONAL)
    s5=_window(sw.get("5m"),MIN_5M_TRADES,MIN_5M_NOTIONAL)
    spot_source="TRADE_FLOW_CACHE"
    if s5 is None:
        fallback=_verified_spot_imbalance(verified_spot)
        if fallback is not None:
            s5=fallback
            spot_source=str((verified_spot or {}).get("reason"))
    response,bearish=_price_response(structure,price)

    base={"finished_at":now,
          "delta_ratio":round(f1/100,6) if f1 is not None else None,
          "price_change_pct":round(response,6) if response is not None else None,
          "trade_span_sec":60.0,
          "evidence_event":"NONE",
          "research_only":True,
          "source":"PUMPDUMP_LOCAL_TAKER_FLOW",
          "spot_source":spot_source,
          "orderbook_verified":False}

    if f1 is None or f5 is None or s5 is None:
        # Identify missing components without relaxing evidence requirements.
        missing=[name for name,value in (("FUTURES_1M",f1),
                 ("FUTURES_5M",f5),("SPOT_5M",s5)) if value is None]
        return dict(base,status="INSUFFICIENT_DATA",
                    reason="LOCAL_FLOW_NOT_READY",
                    missing_windows=missing)

    # Immediate and 5m futures pressure must agree. Spot must also sell and
    # the already-confirmed price structure must respond down.
    if f1<=-15 and f5<=-10 and s5<=-10 and bearish:
        return dict(base,status="CONFIRMED",
                    reason="LOCAL_SELL_AGGRESSION_PRICE_ALIGNED",
                    evidence_event="SELL_AGGRESSION_CONFIRMED")

    buy_aggression=(f1>=15 and f5>=10)
    if buy_aggression and bearish:
        # Buyers are still crossing the spread, but price/structure no longer
        # rewards that aggression. Useful research evidence, not SHORT approval.
        return dict(base,status="CONTRADICTION",
                    reason="LOCAL_BUY_AGGRESSION_ABSORBED",
                    evidence_event="BUY_ABSORPTION_CANDIDATE")

    if buy_aggression and s5>=10 and not bearish:
        return dict(base,status="CONTRADICTION",
                    reason="LOCAL_BUYERS_STILL_CONTROL",
                    evidence_event="NONE")

    return dict(base,status="INSUFFICIENT_DATA",
                reason="LOCAL_FLOW_MIXED")


def _read_structure(path,symbol,episode_at):
    try:
        with sqlite3.connect(path,timeout=3) as db:
            db.row_factory=sqlite3.Row
            row=db.execute("""SELECT last_structure_json
                FROM post_pump_short_strategy
                WHERE symbol=? AND episode_at=?""",
                (symbol,episode_at)).fetchone()
        if not row or not row["last_structure_json"]:
            return {}
        value=json.loads(row["last_structure_json"])
        return value if isinstance(value,dict) else {}
    except (sqlite3.Error,ValueError,TypeError):
        return {}


def _read_verified_spot(path,symbol,episode_at,now):
    """Read the strategy's fresh, already-validated 5m Spot snapshot."""
    try:
        with sqlite3.connect(path,timeout=3) as db:
            db.row_factory=sqlite3.Row
            row=db.execute("""SELECT last_context_at,last_evidence_json
                FROM post_pump_short_strategy
                WHERE symbol=? AND episode_at=?""",
                (symbol,episode_at)).fetchone()
        if not row or not row["last_evidence_json"]:
            return None
        at=_num(row["last_context_at"])
        if at is None or not 0<=now-at<=MAX_RESULT_AGE_SEC:
            return None
        evidence=json.loads(row["last_evidence_json"])
        spot=evidence.get("verified_spot") if isinstance(evidence,dict) else None
        return spot if _verified_spot_imbalance(spot) is not None else None
    except (sqlite3.Error,ValueError,TypeError):
        return None


def dispatch(symbol,price,episode_at,db_path=None,now=None,readers=None):
    """Refresh local verification at most once per 120s per pump episode."""
    now=time.time() if now is None else float(now)
    path=db_path or os.getenv("PUMP_WATCH_DB","/data/pump_watch.sqlite3")
    p=_num(price)
    ep=_num(episode_at)
    if not isinstance(symbol,str) or not symbol.endswith("USDT") or p is None or p<=0 or ep is None:
        return False
    key=(symbol,ep)
    with _lock:
        if now-_recent.get(key,-1e20)<COOLDOWN_SEC:
            return False
        _recent[key]=now
        if len(_recent)>500:
            cutoff=now-24*3600
            for k,ts in list(_recent.items()):
                if ts<cutoff:
                    _recent.pop(k,None)
    if readers is None:
        try:
            from trade_flow_collector_v3 import get_futures_windows,get_spot_windows
            readers=(get_futures_windows,get_spot_windows)
        except ImportError:
            readers=(lambda _:{},lambda _:{})
    futures_reader,spot_reader=readers
    try:
        futures=futures_reader(symbol) or {}
    except Exception:
        futures={}
    try:
        spot=spot_reader(symbol) or {}
    except Exception:
        spot={}
    structure=_read_structure(path,symbol,ep)
    verified_spot=_read_verified_spot(path,symbol,ep,now)
    result=verify_local(p,futures,spot,structure,now=now,
                        verified_spot=verified_spot)
    try:
        with sqlite3.connect(path,timeout=5) as db:
            db.row_factory=sqlite3.Row
            ok=store_result(db,symbol,ep,now,result,now)
            db.commit()
    except sqlite3.Error:
        ok=False
    print(f"[POST_PUMP_LOCAL_FLOW] symbol={symbol} "
          f"status={result.get('status')} reason={result.get('reason')} "
          f"event={result.get('evidence_event')} stored={ok} "
          f"missing_windows={','.join(result.get('missing_windows') or []) or 'NONE'} "
          f"spot_source={result.get('spot_source')} "
          "no_external_service=true research_only=true",flush=True)
    return ok
