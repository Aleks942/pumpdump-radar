"""Authenticated independent Order Flow research for direct +30% post-pump cases.

Best-effort background HTTPS query to the EXISTING smart-money-order-flow
service. Tied to a fresh, immutable (symbol,episode,observed_at) key.
Never turns Flow into an order, Telegram or live trading permission.
"""
import math
import os
import re
import sqlite3
import threading
import time

_lock=threading.Lock()
_slots=threading.BoundedSemaphore(2)
_recent={}
COOLDOWN_SEC=180
MAX_RESULT_AGE_SEC=240


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
    if row is None:
        return None
    return dict(row)


def store_result(db,symbol,episode_at,observed_at,result,now):
    """Reject stale, unverifiable response and unrecognized statuses."""
    if not isinstance(result,dict) or result.get("status") not in (
            "CONFIRMED","CONTRADICTION","INSUFFICIENT_DATA"):
        return False
    try:
        stamp=float(result.get("finished_at"))
        req=float(observed_at)
        at=float(now)
        episode=float(episode_at)
        price=float(result.get("price_change_pct")) if result.get("price_change_pct") is not None else None
        delta=float(result.get("delta_ratio")) if result.get("delta_ratio") is not None else None
        span=float(result.get("trade_span_sec")) if result.get("trade_span_sec") is not None else None
    except (ValueError,TypeError,OverflowError):
        return False
    if (not all(math.isfinite(x) for x in (stamp,req,at,episode))
            or not req<=stamp<=at or stamp-req>90 or at-stamp>15
            or any(v is not None and not math.isfinite(v) for v in (price,delta,span))
            or delta is not None and not -1<=delta<=1):
        return False
    setup(db)
    db.execute("""INSERT OR IGNORE INTO post_pump_short_flow_results(
        symbol,episode_at,observed_at,verified_at,status,reason,
        delta_ratio,price_change_pct,trade_span_sec,evidence_event)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (symbol,episode,req,stamp,result["status"],
         str(result.get("reason") or "")[:90],
         delta,price,span,str(result.get("evidence_event") or "")[:90]))
    return True


def dispatch(symbol,price,episode_at,db_path=None,now=None):
    """Start at most two concurrent verifications, never block a market scan."""
    now=time.time() if now is None else float(now)
    base=os.getenv("PUMP_FLOW_URL","").strip()
    token=os.getenv("PUMP_FLOW_SHARED_TOKEN","")
    mode=os.getenv("PUMP_FLOW_MODE","OFF").upper()
    path=db_path or os.getenv("PUMP_WATCH_DB","/data/pump_watch.sqlite3")
    if (mode!="RESEARCH" or not token or not base.startswith("https://")
            or not base.endswith("/api/pump-candidate")):
        return False
    if (not isinstance(symbol,str)
            or re.fullmatch(r"[A-Z0-9]{2,25}USDT",symbol) is None
            or not isinstance(price,(int,float))
            or not math.isfinite(price) or price<=0):
        return False
    with _lock:
        if now-_recent.get((symbol,episode_at),-1e20)<COOLDOWN_SEC:
            return False
        if not _slots.acquire(blocking=False):
            return False
        _recent[(symbol,episode_at)]=now
        if len(_recent)>500:
            cutoff=now-24*3600
            for key,ts in list(_recent.items()):
                if ts<cutoff:
                    _recent.pop(key,None)
    def worker():
        try:
            import requests
            observed=time.time()
            payload={"schema":1,"source":"pumpdump-radar",
                     "symbol":symbol,"side":"SHORT","price":price,
                     "observed_at":observed,"expires_at":observed+90}
            headers={"X-Pump-Flow-Token":token}
            res=requests.post(base,json=payload,headers=headers,timeout=3)
            if res.status_code!=202:
                print("[POST_PUMP_DIRECT_FLOW] rejected",symbol,
                      res.status_code,flush=True)
                return
            url=base.rsplit("/",1)[0]+"/pump-result"
            for _ in range(12):
                time.sleep(3)
                reply=requests.get(url,params={
                    "symbol":symbol,"observed_at":observed},
                    headers=headers,timeout=3)
                if reply.status_code!=200:
                    return
                outcome=reply.json()
                if outcome.get("status")=="PENDING":
                    continue
                with sqlite3.connect(path,timeout=5) as db:
                    db.row_factory=sqlite3.Row
                    success=store_result(
                        db,symbol,episode_at,observed,outcome,time.time())
                    db.commit()
                print(f"[POST_PUMP_DIRECT_FLOW] symbol={symbol} "
                      f"status={outcome.get('status')} "
                      f"reason={outcome.get('reason')} stored={success} "
                      "research_only=true",flush=True)
                return
            print("[POST_PUMP_DIRECT_FLOW] timeout",symbol,flush=True)
        except Exception as exc:
            print("[POST_PUMP_DIRECT_FLOW_ERROR]",
                  type(exc).__name__,flush=True)
        finally:
            _slots.release()
    try:
        threading.Thread(target=worker,daemon=True,
                         name="post-pump-flow-"+symbol).start()
        return True
    except Exception:
        _slots.release()
        return False
