"""Point-in-time historical replay of the current SWING core lifecycle.

Uses only historically reproducible data:
- 15m/1H/4H/1D price/volume structure
- Bybit 1H open interest
- current breakout/retest/trade-plan logic

Live-only Spot CVD / futures taker flow / liquidations are intentionally excluded.
Decision uses only closed candles; entry is the NEXT 15m candle open.
"""
import argparse
import json
import math
import statistics
import time

import requests

from swing_backtest import _resample, _slice_before, _oi_change_at
from swing_strength_engine import (
    _score, _trend, _volume_state, _entry_setup, _trade_plan, _breakout_state,
)

DEFAULT_DAYS=30
DEFAULT_SYMBOLS=("BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT",
                 "ADAUSDT","LINKUSDT","AVAXUSDT","SUIUSDT","APTUSDT")


OKX_BASE="https://www.okx.com/api/v5"


def _okx_get(path,params=None,request_get=None):
    getter=request_get or requests.get
    r=getter(OKX_BASE+path,params=params,timeout=15)
    r.raise_for_status()
    data=r.json()
    if not isinstance(data,dict) or str(data.get("code"))!="0":
        raise RuntimeError("OKX_API")
    rows=data.get("data") or []
    if not isinstance(rows,list):
        raise RuntimeError("OKX_SCHEMA")
    return rows


def fetch_okx_klines(symbol,bar,start_ms,end_ms,request_get=None):
    """Historical closed OKX SWAP candles, oldest -> newest."""
    inst=symbol[:-4]+"-USDT-SWAP"
    out={}
    cursor=int(end_ms)
    loops=0
    while cursor>start_ms and loops<300:
        loops+=1
        rows=_okx_get("/market/history-candles",{
            "instId":inst,"bar":bar,"after":str(cursor),"limit":"300"
        },request_get)
        if not rows:
            break
        oldest=None
        for row in rows:
            if not isinstance(row,list) or len(row)<9 or str(row[8])!="1":
                continue
            ts=int(float(row[0]))
            if not (start_ms<=ts<=end_ms):
                continue
            o=float(row[1]); h=float(row[2]); l=float(row[3]); close=float(row[4]); v=float(row[5])
            if min(o,h,l,close)<=0 or v<0 or not l<=min(o,close)<=max(o,close)<=h:
                continue
            out[ts]={"ts":ts/1000.0,"o":o,"h":h,"l":l,"c":close,"v":v}
            oldest=ts if oldest is None else min(oldest,ts)
        if oldest is None or oldest<=start_ms:
            break
        if oldest>=cursor:
            break
        cursor=oldest
        time.sleep(0.06)
    return [out[k] for k in sorted(out)]


def fetch_okx_oi_1h(symbol,start_ms,end_ms,request_get=None):
    """OKX contract OI history: [ts, oi, oiCcy, oiUsd], oldest -> newest."""
    inst=symbol[:-4]+"-USDT-SWAP"
    out={}
    cursor=int(end_ms)
    loops=0
    while cursor>start_ms and loops<30:
        loops+=1
        rows=_okx_get("/rubik/stat/contracts/open-interest-history",{
            "instId":inst,"period":"1H","end":str(cursor),"limit":"100"
        },request_get)
        if not rows:
            break
        oldest=None
        for row in rows:
            if not isinstance(row,list) or len(row)<4:
                continue
            ts=int(float(row[0]))
            oi_ccy=_num(row[2])
            if oi_ccy is None or oi_ccy<=0:
                continue
            if start_ms<=ts<=end_ms:
                out[ts]=oi_ccy
                oldest=ts if oldest is None else min(oldest,ts)
        if oldest is None or oldest<=start_ms:
            break
        if oldest>=cursor:
            break
        cursor=oldest
        time.sleep(0.22)
    return [(k/1000.0,out[k]) for k in sorted(out)]


def _num(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError,OverflowError):
        return None


def _closed_raw_before(rows,ts,sec,limit=None):
    """Raw exchange bars are usable only after their full interval closes."""
    data=[x for x in rows if float(x["ts"])+sec<=ts]
    return data[-limit:] if limit else data


def _score_at(c1,c4,cd,oi_rows,ts):
    # Raw 1H rows carry their OPEN timestamp, unlike resampled bars that
    # have end_ts. Never expose the current unfinished hour to a 15m decision.
    h1=_closed_raw_before(c1,ts,3600,80)
    h4=_slice_before(c4,ts,80)
    d1=_slice_before(cd,ts,40)
    if len(h1)<25 or len(h4)<25 or len(d1)<25:
        return None
    t1=_trend(h1); t4=_trend(h4); td=_trend(d1)
    oi1=_oi_change_at(oi_rows,ts,1)
    oi4=_oi_change_at(oi_rows,ts,4)
    if not all(x.get("ready") for x in (t1,t4,td,oi1,oi4)):
        return None
    scored=_score(t1,t4,td,oi1,oi4,None,_volume_state(h1),short_term=None)
    bias=scored.get("bias")
    aligned=(
        bias=="BULLISH" and t1.get("regime")=="UP" and t4.get("regime")=="UP"
    ) or (
        bias=="BEARISH" and t1.get("regime")=="DOWN" and t4.get("regime")=="DOWN"
    )
    if not aligned:
        return None
    if bias=="BULLISH" and int(scored.get("score",50))<70:
        return None
    if bias=="BEARISH" and int(scored.get("score",50))>30:
        return None
    return {**scored,"h1":h1}


def build_ready_entries(symbol,c15,c1,oi_rows):
    if not c15 or not c1 or not oi_rows:
        return []
    c4=_resample(c1,4)
    cd=_resample(c1,24)
    out=[]
    last_entry_ts={"LONG":None,"SHORT":None}

    import swing_strength_engine as engine
    engine._breakout_memory.pop(symbol,None)

    for i in range(80,len(c15)-1):
        decision_ts=c15[i]["ts"]+900
        history15=c15[max(0,i-79):i+1]
        scored=_score_at(c1,c4,cd,oi_rows,decision_ts)
        if not scored:
            continue

        bias=scored["bias"]
        side="LONG" if bias=="BULLISH" else "SHORT"
        t15=_trend(history15)
        expected="UP" if side=="LONG" else "DOWN"
        if not t15.get("ready") or t15.get("regime")!=expected:
            continue

        breakout=_breakout_state(scored["h1"],bias,symbol=symbol,now=decision_ts)
        if breakout.get("phase")!="BREAKOUT_CONFIRMED":
            continue
        setup=_entry_setup(history15,bias)
        if setup.get("phase")!="READY_ENTRY":
            continue

        nxt=c15[i+1]
        if nxt["ts"]!=decision_ts:
            continue
        entry=_num(nxt["o"])
        if entry is None or entry<=0:
            continue
        plan=_trade_plan(history15,bias,entry)
        if plan.get("ready") is not True:
            continue

        prior=last_entry_ts.get(side)
        if prior is not None and decision_ts-prior<6*3600:
            continue
        last_entry_ts[side]=decision_ts
        out.append({
            "symbol":symbol,"decision_ts":decision_ts,"entry_ts":nxt["ts"],
            "direction":side,"score":int(scored["score"]),
            "entry":entry,"stop":plan["stop"],"tp1":plan["tp1"],"tp2":plan["tp2"],
            "stop_pct":plan["stop_pct"],"breakout_level":breakout.get("level"),
        })
    return out


def simulate_trade(event,c15,max_hours=72):
    start=float(event["entry_ts"])
    end=start+max_hours*3600
    path=[x for x in c15 if start<=x["ts"]<end]
    if not path or path[0]["ts"]!=start:
        return None

    side=event["direction"]
    stop=float(event["stop"]); tp1=float(event["tp1"]); tp2=float(event["tp2"])
    entry=float(event["entry"])
    tp1_at=None; tp2_at=None; stop_at=None

    for c in path:
        if side=="LONG":
            stop_hit=c["l"]<=stop; tp1_hit=c["h"]>=tp1; tp2_hit=c["h"]>=tp2
        else:
            stop_hit=c["h"]>=stop; tp1_hit=c["l"]<=tp1; tp2_hit=c["l"]<=tp2
        if stop_hit:
            stop_at=c["ts"]+900
            break
        if tp1_at is None and tp1_hit:
            tp1_at=c["ts"]+900
        if tp2_at is None and tp2_hit:
            tp2_at=c["ts"]+900
            break

    if stop_at is not None:
        r=-1.0; reason="STOP"; exit_ts=stop_at
    elif tp2_at is not None:
        r=3.0; reason="TP2"; exit_ts=tp2_at
    elif tp1_at is not None:
        r=2.0; reason="TP1_ONLY"; exit_ts=tp1_at
    else:
        final_px=path[-1]["c"]
        risk=abs(entry-stop)
        if risk<=0:
            return None
        signed=(final_px-entry) if side=="LONG" else (entry-final_px)
        r=signed/risk
        reason="TIME"; exit_ts=path[-1]["ts"]+900

    return {**event,"reason":reason,"r":round(r,4),"exit_ts":exit_ts,
            "tp1_hit":tp1_at is not None,"tp2_hit":tp2_at is not None,
            "stop_hit":stop_at is not None}


def summarize(trades):
    if not trades:
        return {"n":0,"status":"INSUFFICIENT_DATA"}
    rs=[float(x["r"]) for x in trades]
    return {
        "n":len(trades),
        "wins":sum(x>0 for x in rs),
        "losses":sum(x<0 for x in rs),
        "win_rate_pct":round(100*sum(x>0 for x in rs)/len(rs),2),
        "expectancy_r":round(statistics.fmean(rs),3),
        "median_r":round(statistics.median(rs),3),
        "tp1_or_better":sum(x.get("tp1_hit") for x in trades),
        "tp2":sum(x.get("tp2_hit") for x in trades),
        "stops":sum(x.get("stop_hit") for x in trades),
    }


def backtest_symbol(symbol,days=DEFAULT_DAYS,now=None):
    now=time.time() if now is None else float(now)
    # OKX 1H OI exposes ~60 days (1440 points). Keep enough warm-up
    # for 1D trend while reserving 30 days for evaluation.
    warmup_days=30
    start_ms=int((now-(days+warmup_days)*86400)*1000)
    end_ms=int(now*1000)
    c15=fetch_okx_klines(symbol,"15m",start_ms,end_ms)
    c1=fetch_okx_klines(symbol,"1H",start_ms,end_ms)
    c15=[x for x in c15 if x["ts"]+900<=now]
    c1=[x for x in c1 if x["ts"]+3600<=now]
    oi=fetch_okx_oi_1h(symbol,start_ms,end_ms)

    cutoff=now-days*86400
    events=build_ready_entries(symbol,c15,c1,oi)
    events=[x for x in events if x["entry_ts"]>=cutoff]
    trades=[simulate_trade(e,c15,72) for e in events]
    trades=[x for x in trades if x is not None]
    return {"symbol":symbol,"days":days,"candles15":len(c15),
            "candles1h":len(c1),"oi_points":len(oi),
            "ready_entries":len(events),"stats":summarize(trades),"trades":trades}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("symbols",nargs="*",default=list(DEFAULT_SYMBOLS))
    p.add_argument("--days",type=int,default=DEFAULT_DAYS)
    p.add_argument("--trades",action="store_true")
    args=p.parse_args()
    if not 15<=args.days<=30:
        p.error("days must be 15..30 for free OKX 1H OI history")

    reports=[]; combined=[]; now=time.time()
    for raw in args.symbols:
        symbol=raw.upper()
        if not symbol.endswith("USDT"):
            symbol+="USDT"
        r=backtest_symbol(symbol,args.days,now=now)
        combined.extend(r.get("trades") or [])
        if not args.trades:
            r=dict(r,trades=[])
        reports.append(r)

    print(json.dumps({
        "parameters":{"days":args.days,"symbols":[x.upper() for x in args.symbols]},
        "symbols":reports,
        "combined":summarize(combined),
        "limitations":[
            "Historical replay excludes live-only Spot CVD, futures taker-flow and liquidations.",
            "OKX USDT-SWAP candles and contract OI history are used for the replay.",
            "Entry is next 15m open after a closed READY_ENTRY candle.",
            "If stop and target touch the same 15m candle, stop is assumed first."
        ]
    },ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
