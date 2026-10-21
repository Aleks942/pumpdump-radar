"""Independent evidence enrichment for POST-PUMP SHORT research.

This module NEVER approves a trade. It captures causal, point-in-time context:
authenticated Order Flow/absorption evidence already stored by pump_cycle,
bounded 5m trade-CVD divergence, OI+bearish-structure capital state,
long/short liquidations, funding context, and feature-cohort outcome reports.
Missing data stays explicit and never becomes a bearish vote.
"""
import json
import math
import os
import sqlite3
import threading
import time
from collections import defaultdict

DB_PATH=os.getenv("PUMP_WATCH_DB","/data/pump_watch.sqlite3")
FLOW_TTL_SEC=240
FUNDING_TTL_SEC=300
REPORT_INTERVAL_SEC=3600
_funding_cache={}
_funding_pending=set()
_funding_lock=threading.Lock()
_last_report_at=0.0


def _num(value):
    try:
        x=float(value)
    except (TypeError,ValueError,OverflowError):
        return None
    return x if math.isfinite(x) else None


def read_order_flow(symbol,now=None,db_path=None,episode_at=None):
    """Read authenticated Flow result, tied to a single observed pump episode."""
    now=time.time() if now is None else float(now)
    path=DB_PATH if db_path is None else db_path
    if episode_at is not None:
        try:
            from post_pump_short_flow import latest
            with sqlite3.connect(path,timeout=3) as db:
                db.row_factory=sqlite3.Row
                row=latest(db,symbol,now,episode_at)
            if row is None:
                return {"ready":False,"status":"INSUFFICIENT_DATA",
                        "reason":"POST_PUMP_FLOW_NOT_VERIFIED"}
            return {"ready":row["status"] in ("CONFIRMED","CONTRADICTION"),
                    "status":row["status"],
                    "delta_ratio":row["delta_ratio"],
                    "price_change_pct":row["price_change_pct"],
                    "trade_span_sec":row["trade_span_sec"],
                    "evidence_event":row["evidence_event"],
                    "buy_absorption_candidate":
                        row["evidence_event"]=="BUY_ABSORPTION_CANDIDATE",
                    "observed_at":row["observed_at"],
                    "verified_at":row["verified_at"],
                    "age_sec":round(now-row["verified_at"],3),
                    "reason":row["reason"],
                    "source":"PUMPDUMP_LOCAL_TAKER_FLOW",
                    "orderbook_verified":False}
        except sqlite3.Error as exc:
            return {"ready":False,"status":"INSUFFICIENT_DATA",
                    "reason":"POST_PUMP_FLOW_DB_"+type(exc).__name__}
    try:
        with sqlite3.connect(path,timeout=3) as db:
            db.row_factory=sqlite3.Row
            row=db.execute("""SELECT flow_status,flow_delta,flow_verified_at,
                flow_observed_at,flow_evidence,weakening_at
                FROM pump_cycle_state WHERE symbol=?""",(symbol,)).fetchone()
        if not row or row["flow_verified_at"] is None:
            return {"ready":False,"status":"INSUFFICIENT_DATA",
                    "reason":"NO_VERIFIED_ORDER_FLOW"}
        age=now-float(row["flow_verified_at"])
        delta=_num(row["flow_delta"])
        if age<0 or age>FLOW_TTL_SEC:
            return {"ready":False,"status":"INSUFFICIENT_DATA",
                    "reason":"ORDER_FLOW_STALE","age_sec":round(age,3)}
        status=str(row["flow_status"] or "INSUFFICIENT_DATA")
        evidence=row["flow_evidence"]
        return {
            "ready":status in ("CONFIRMED","CONTRADICTION"),
            "status":status,
            "delta_ratio":delta,
            "evidence_event":evidence,
            "buy_absorption_candidate":evidence=="BUY_ABSORPTION_CANDIDATE",
            "observed_at":_num(row["flow_observed_at"]),
            "verified_at":_num(row["flow_verified_at"]),
            "age_sec":round(age,3),
            "reason":"AUTHENTICATED_PUMP_FLOW_RESULT",
        }
    except sqlite3.Error as exc:
        return {"ready":False,"status":"INSUFFICIENT_DATA",
                "reason":"ORDER_FLOW_DB_"+type(exc).__name__}


def classify_bounded_cvd(evidence,spot):
    """Classify 5m trade-level cumulative delta without calling it full-history CVD."""
    f_imb=_num(evidence.get("futures_taker_5m_imbalance_pct"))
    f_delta=_num(evidence.get("futures_taker_5m_delta_quote"))
    if spot.get("ready") is True:
        s_imb=_num(spot.get("imbalance_pct"))
        s_delta=_num(spot.get("delta_quote"))
        spot_quality=spot.get("reason","READY")
    else:
        s_imb=None
        s_delta=None
        spot_quality=spot.get("reason","SPOT_NOT_READY")
    if f_imb is None or s_imb is None or f_delta is None or s_delta is None:
        state="INSUFFICIENT_DATA"
    elif f_imb<=-10 and s_imb<=-10:
        state="SPOT_AND_FUTURES_SELL"
    elif f_imb<=-10 and s_imb>=10:
        state="FUTURES_SELL_SPOT_BUY_DIVERGENCE"
    elif f_imb<=-10:
        state="FUTURES_SELL_SPOT_NOT_CONFIRMING"
    elif f_imb>=10 and s_imb<=-10:
        state="SPOT_SELL_FUTURES_BUY_DIVERGENCE"
    elif f_imb>=10 and s_imb>=10:
        state="SPOT_AND_FUTURES_BUY"
    else:
        state="MIXED"
    return {
        "quality":"BOUNDED_5M_TRADE_CVD",
        "state":state,
        "futures_imbalance_pct":f_imb,
        "spot_imbalance_pct":s_imb,
        "futures_delta_quote":f_delta,
        "spot_delta_quote":s_delta,
        "spot_quality":spot_quality,
        "not_full_history_cvd":True,
    }


def classify_capital(evidence,structure):
    """Price/structure + OI classification; OI alone never assigns direction."""
    local=_num(evidence.get("local_oi_5m_pct"))
    agg=_num(evidence.get("agg_oi_5m_pct"))
    oi=agg if agg is not None else local
    bearish=bool(structure.get("trend_shift"))
    if oi is None:
        state="OI_UNAVAILABLE"
    elif not bearish:
        state="NO_BEARISH_STRUCTURE"
    elif oi>=0.10:
        state="NEW_SHORTS_WITH_BEARISH_STRUCTURE"
    elif oi<=-0.10:
        state="LONG_UNWIND_WITH_BEARISH_STRUCTURE"
    else:
        state="OI_NEUTRAL_WITH_BEARISH_STRUCTURE"
    return {"state":state,"oi_5m_pct":oi,
            "oi_source":"AGGREGATED" if agg is not None else
                        "LOCAL" if local is not None else "NONE",
            "requires_price_structure":True}


def classify_liquidations(liquidations):
    liq=liquidations if isinstance(liquidations,dict) else {}
    long_usdt=_num(liq.get("long_usdt"))
    short_usdt=_num(liq.get("short_usdt"))
    available=liq.get("available") is True
    complete=liq.get("complete") is True
    if not available or long_usdt is None or short_usdt is None:
        dominance="UNAVAILABLE"
        ratio=None
    else:
        ratio=(long_usdt/max(short_usdt,1e-9)) if long_usdt>0 else 0.0
        if long_usdt>=3*max(short_usdt,1.0):
            dominance="LONG_LIQUIDATIONS_DOMINANT"
        elif short_usdt>=3*max(long_usdt,1.0):
            dominance="SHORT_LIQUIDATIONS_DOMINANT"
        else:
            dominance="MIXED_LIQUIDATIONS"
    return {
        "available":available,"complete":complete,
        "quality":liq.get("quality","UNAVAILABLE"),
        "long_usdt":long_usdt,"short_usdt":short_usdt,
        "sources":liq.get("sources", {}),
        "exchanges":liq.get("exchanges"),
        "observed_at":liq.get("observed_at"),
        "event_count":liq.get("event_count"),
        "long_to_short_ratio":round(ratio,4) if ratio is not None else None,
        "dominance":dominance,
        "context_only":True,
    }


def _funding_state(value):
    if value is None:
        return "FUNDING_UNAVAILABLE"
    if value>=0.10:
        return "EXTREME_POSITIVE"
    if value>=0.03:
        return "ELEVATED_POSITIVE"
    if value<=-0.03:
        return "NEGATIVE"
    return "NORMAL"


def _store_funding(symbol,value,captured_at,metadata=None):
    value=_num(value)
    result={"available":value is not None,"state":_funding_state(value),
            "funding_pct":value,"captured_at":captured_at,
            "context_only":True}
    result.update(metadata or {})
    result["source"]="OKX"
    with _funding_lock:
        _funding_cache[symbol]=result
        _funding_pending.discard(symbol)
    return dict(result)


def _fetch_funding(inst, request_get=None):
    if request_get is None:
        import requests
        request_get=requests.get
    response=request_get("https://www.okx.com/api/v5/public/funding-rate",
                         params={"instId":inst},timeout=(3,6))
    response.raise_for_status()
    data=response.json()
    rows=data.get("data") if data.get("code")=="0" else None
    if not isinstance(rows,list) or len(rows)!=1:
        raise ValueError("FUNDING_SCHEMA")
    row=rows[0]
    rate=_num(row.get("fundingRate"))
    stamp=_num(row.get("ts"))
    now=time.time()
    if (row.get("instId")!=inst or rate is None or abs(rate)>1
            or stamp is None or not -5<=now-stamp/1000<=FUNDING_TTL_SEC):
        raise ValueError("FUNDING_STALE_OR_INVALID")
    funding_at=_num(row.get("fundingTime"))
    return rate*100, {"exchange_ts":stamp/1000,
                      "funding_at":funding_at/1000 if funding_at else None}


def funding_context(symbol,now=None,getter=None):
    """Bounded OKX funding snapshot. Descriptive only, never a short trigger.

    Live collection is asynchronous so a slow exchange endpoint cannot delay
    the market scan. Injected getters stay synchronous for deterministic tests.
    """
    now=time.time() if now is None else float(now)
    with _funding_lock:
        cached=_funding_cache.get(symbol)
        age=now-cached.get("exchange_ts",cached["captured_at"]) if cached else None
        fresh=dict(cached,age_sec=round(age,1)) if cached and 0<=age<=FUNDING_TTL_SEC else None
        if fresh and now-cached["captured_at"]<120:
            return fresh
    inst=(symbol[:-4]+"-USDT-SWAP"
          if isinstance(symbol,str) and symbol.endswith("USDT") else symbol)
    if getter is not None:
        try:
            return _store_funding(symbol,getter(inst),now)
        except Exception:
            return _store_funding(symbol,None,now)

    with _funding_lock:
        if symbol in _funding_pending:
            return fresh or {"available":False,"state":"FUNDING_WARMING",
                    "funding_pct":None,"captured_at":now,
                    "context_only":True}
        _funding_pending.add(symbol)

    def worker():
        try:
            value,metadata=_fetch_funding(inst)
        except Exception as exc:
            value=None
            metadata={"error":type(exc).__name__}
        _store_funding(symbol,value,time.time(),metadata)
        print("[POST_PUMP_FUNDING]",symbol,"available=",value is not None,
              "source=OKX",flush=True)

    try:
        threading.Thread(target=worker,daemon=True,
                         name="post-pump-funding").start()
    except Exception:
        with _funding_lock:
            _funding_pending.discard(symbol)
        return _store_funding(symbol,None,now)
    return fresh or {"available":False,"state":"FUNDING_WARMING",
            "funding_pct":None,"captured_at":now,"context_only":True}


def enrich(symbol,now,evidence,spot,structure,liquidations=None,db_path=None,
           funding_getter=None,episode_at=None):
    """Return an enriched COPY; no gate or score is changed."""
    out=dict(evidence or {})
    order_flow=read_order_flow(symbol,now=now,db_path=db_path,
                               episode_at=episode_at)
    bounded=classify_bounded_cvd(out,spot or {})
    capital=classify_capital(out,structure or {})
    liq=classify_liquidations(liquidations or out.get("liquidations") or {})
    funding=funding_context(symbol,now=now,getter=funding_getter)
    tags=[
        "OF_"+str(order_flow.get("status","INSUFFICIENT_DATA")),
        "ABSORPTION_"+("YES" if order_flow.get("buy_absorption_candidate") else "NO"),
        "CVD_"+bounded["state"],
        "CAPITAL_"+capital["state"],
        "LIQ_"+liq["dominance"],
        "FUNDING_"+funding["state"],
    ]
    out["order_flow"]=order_flow
    out["bounded_cvd_5m"]=bounded
    out["capital_state"]=capital
    out["liquidation_context"]=liq
    out["funding_context"]=funding
    out["research_feature_tags"]=tags
    out["feature_combo"]="|".join(tags)
    out["research_only"]=True
    out["trade_approved"]=False
    return out


def _cohort_key_values(evidence):
    tags=evidence.get("research_feature_tags")
    if not isinstance(tags,list):
        return []
    out=[str(x) for x in tags if isinstance(x,str)]
    combo=evidence.get("feature_combo")
    if isinstance(combo,str) and combo:
        out.append("COMBO:"+combo)
    return out


def maybe_report_cohorts(db,now=None):
    """Print immutable 30m paper markout cohorts at most hourly."""
    global _last_report_at
    now=time.time() if now is None else float(now)
    if now-_last_report_at<REPORT_INTERVAL_SEC:
        return []
    _last_report_at=now
    try:
        rows=db.execute("""SELECT a.evidence_json,o.model_net_short_pct
            FROM post_pump_short_strategy_audit a
            JOIN post_pump_short_strategy_outcomes o ON o.audit_id=a.id
            WHERE o.horizon_min=30 AND o.status='DONE'
              AND a.evidence_json IS NOT NULL AND a.at>=?""",
            (now-14*86400,)).fetchall()
    except sqlite3.Error:
        return []
    groups=defaultdict(list)
    for raw,net in rows:
        value=_num(net)
        if value is None:
            continue
        try:
            evidence=json.loads(raw)
        except (TypeError,ValueError):
            continue
        groups["ALL"].append(value)
        for key in _cohort_key_values(evidence):
            groups[key].append(value)
    report=[]
    for key,values in groups.items():
        if not values:
            continue
        item={"cohort":key,"n":len(values),
              "mean_net_short_pct":round(sum(values)/len(values),5),
              "positive":sum(v>0 for v in values)}
        report.append(item)
    report.sort(key=lambda x:(-x["n"],x["cohort"]))
    for item in report[:30]:
        print("[POST_PUMP_FEATURE_COHORT]",item,
              "descriptive_only=true no_trade_approval=true",flush=True)
    return report
