"""Historical walk-forward validation for the medium-term SWING engine.

Purpose
-------
Evaluate whether a point-in-time SWING score predicts the next 1h/4h/12h/24h/
72h/7d without look-ahead bias.

Only public historical Bybit candle/OI data are used. Taker flow, spot CVD and
liquidations are intentionally excluded from historical scoring unless they are
available from our own stored database. This prevents invented history.

Usage:
    python swing_backtest.py ARIAUSDT
    python swing_backtest.py ARIAUSDT EVAAUSDT --days 30
"""
import argparse
import json
import math
import statistics
import time
from collections import defaultdict

import requests

from swing_strength_engine import _score, _trend, _volume_state

BYBIT_BASE="https://api.bybit.com"
HORIZONS_H=(1,4,12,24,72,168)


def _num(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError,OverflowError):
        return None


def _bybit(path,params,request_get=None):
    request_get=request_get or requests.get
    r=request_get(BYBIT_BASE+path,params=params,timeout=10)
    r.raise_for_status()
    data=r.json()
    if not isinstance(data,dict) or data.get("retCode")!=0:
        raise RuntimeError("BYBIT_API")
    result=data.get("result") or {}
    if not isinstance(result,dict):
        raise RuntimeError("BYBIT_SCHEMA")
    return result


def fetch_klines(symbol,interval,start_ms,end_ms,request_get=None):
    """Fetch historical closed candles oldest->newest, paginated backwards."""
    out={}
    cursor=end_ms
    loops=0
    while cursor>start_ms and loops<200:
        loops+=1
        result=_bybit("/v5/market/kline",{
            "category":"linear","symbol":symbol,"interval":str(interval),
            "start":int(start_ms),"end":int(cursor),"limit":1000,
        },request_get)
        rows=result.get("list") or []
        if not rows:
            break
        oldest=None
        for row in rows:
            if not isinstance(row,list) or len(row)<6:
                continue
            ts=_num(row[0]); o=_num(row[1]); h=_num(row[2]); l=_num(row[3])
            c=_num(row[4]); v=_num(row[5])
            if ts is None or any(x is None or x<=0 for x in (o,h,l,c)) or v is None or v<0:
                continue
            ts=int(ts)
            if not (start_ms<=ts<=end_ms):
                continue
            if not l<=min(o,c)<=max(o,c)<=h:
                continue
            out[ts]={"ts":ts/1000.0,"o":o,"h":h,"l":l,"c":c,"v":v}
            oldest=ts if oldest is None else min(oldest,ts)
        if oldest is None or oldest<=start_ms:
            break
        cursor=oldest-1
        time.sleep(0.03)
    return [out[k] for k in sorted(out)]


def fetch_oi_1h(symbol,start_ms,end_ms,request_get=None):
    """Fetch historical 1h OI snapshots oldest->newest."""
    out={}
    cursor=end_ms
    loops=0
    while cursor>start_ms and loops<200:
        loops+=1
        result=_bybit("/v5/market/open-interest",{
            "category":"linear","symbol":symbol,"intervalTime":"1h",
            "startTime":int(start_ms),"endTime":int(cursor),"limit":200,
        },request_get)
        rows=result.get("list") or []
        if not rows:
            break
        oldest=None
        for row in rows:
            if not isinstance(row,dict):
                continue
            ts=_num(row.get("timestamp")); value=_num(row.get("openInterest"))
            if ts is None or value is None or value<=0:
                continue
            ts=int(ts)
            if start_ms<=ts<=end_ms:
                out[ts]=value
                oldest=ts if oldest is None else min(oldest,ts)
        if oldest is None or oldest<=start_ms:
            break
        cursor=oldest-1
        time.sleep(0.03)
    return [(k/1000.0,out[k]) for k in sorted(out)]


def _resample(c1,hours):
    """Build closed N-hour candles strictly from 1h candles."""
    groups={}
    sec=hours*3600
    for row in c1:
        bucket=int(row["ts"]//sec)*sec
        groups.setdefault(bucket,[]).append(row)
    out=[]
    for ts,rows in sorted(groups.items()):
        if len(rows)!=hours:
            continue
        rows=sorted(rows,key=lambda x:x["ts"])
        expected=[ts+i*3600 for i in range(hours)]
        actual=[int(x["ts"]) for x in rows]
        if actual!=expected:
            continue
        out.append({
            "ts":float(ts),"end_ts":float(ts+sec),
            "o":rows[0]["o"],"h":max(x["h"] for x in rows),
            "l":min(x["l"] for x in rows),"c":rows[-1]["c"],
            "v":sum(x["v"] for x in rows),
        })
    return out


def _daily_from_hourly(c1):
    return _resample(c1,24)


def _slice_before(rows,ts,limit=None):
    # A resampled 4H/1D bar is usable only after its full interval has closed.
    # This prevents a 00:00-04:00 bar from leaking 02:00/03:00 data into a
    # signal evaluated at 01:00.
    data=[
        x for x in rows
        if (x.get("end_ts") is not None and x["end_ts"]<=ts)
        or (x.get("end_ts") is None and x["ts"]<ts)
    ]
    return data[-limit:] if limit else data


def _oi_change_at(oi_rows,ts,hours):
    pts=[x for x in oi_rows if x[0]<ts]
    if len(pts)<hours+1:
        return {"ready":False}
    current=pts[-1]
    previous=pts[-1-hours]
    period=current[0]-previous[0]
    if not (hours*3600-900<=period<=hours*3600+900):
        return {"ready":False}
    return {
        "ready":True,"change_pct":100*(current[1]/previous[1]-1),
        "current":current[1],"period_sec":period,
    }


def _weekly_position_at(c1,ts,current):
    rows=_slice_before(c1,ts,24*7)
    if len(rows)<24*7 or current is None:
        return None
    lo=min(x["l"] for x in rows); hi=max(x["h"] for x in rows)
    return 100*(current-lo)/(hi-lo) if hi>lo else None


def _future_markout(c1,index,hours,direction):
    if index>=len(c1):
        return None
    entry=c1[index]["o"]
    end=index+hours
    if end>len(c1) or entry<=0:
        return None
    future=c1[index:end]
    exit_px=future[-1]["c"]
    raw=100*(exit_px/entry-1)
    signed=raw if direction=="LONG" else -raw
    favorable=max(100*(x["h"]/entry-1) for x in future)
    adverse=min(100*(x["l"]/entry-1) for x in future)
    if direction=="SHORT":
        mfe=-adverse
        mae=-favorable
    else:
        mfe=favorable
        mae=adverse
    return {
        "return_pct":signed,
        "mfe_pct":mfe,
        "mae_pct":mae,
        "exit":exit_px,
    }


def build_signals(symbol,c1,oi_rows,step_hours=1,min_score=70,max_score=30):
    """Walk forward one hour at a time; every score uses only prior closed data."""
    c4=_resample(c1,4)
    cd=_daily_from_hourly(c1)
    by_ts={int(x["ts"]):i for i,x in enumerate(c1)}
    rows=[]
    warmup=30*24
    for i in range(warmup,len(c1)-max(HORIZONS_H)):
        if i%max(1,step_hours):
            continue
        ts=c1[i]["ts"]  # entry is this hour open; evidence is strictly before ts
        h1=_slice_before(c1,ts,80)
        h4=_slice_before(c4,ts,80)
        d1=_slice_before(cd,ts,40)
        if len(h1)<25 or len(h4)<25 or len(d1)<25:
            continue
        t1=_trend(h1); t4=_trend(h4); td=_trend(d1)
        oi1=_oi_change_at(oi_rows,ts,1)
        oi4=_oi_change_at(oi_rows,ts,4)
        weekly=_weekly_position_at(c1,ts,h1[-1]["c"])
        vol=_volume_state(h1)
        scored=_score(t1,t4,td,oi1,oi4,weekly,vol,short_term=None)
        score=scored["score"]
        if score>=min_score:
            direction="LONG"
        elif score<=max_score:
            direction="SHORT"
        else:
            continue
        event={
            "symbol":symbol,"ts":ts,"direction":direction,"score":score,
            "bias":scored["bias"],"reasons":scored["reasons"],
            "risks":scored["risks"],"entry":c1[i]["o"],
            "outcomes":{},
        }
        for h in HORIZONS_H:
            m=_future_markout(c1,i,h,direction)
            if m is not None:
                event["outcomes"][str(h)]=m
        rows.append(event)
    return rows


def summarize(events):
    buckets=defaultdict(list)
    for e in events:
        score=e["score"]
        if score>=90: bucket="90-100"
        elif score>=80: bucket="80-89"
        elif score>=70: bucket="70-79"
        elif score<=10: bucket="0-10"
        elif score<=20: bucket="11-20"
        else: bucket="21-30"
        buckets[(e["direction"],bucket)].append(e)

    result=[]
    for (direction,bucket),rows in sorted(buckets.items()):
        for h in HORIZONS_H:
            vals=[r["outcomes"].get(str(h)) for r in rows]
            vals=[x for x in vals if x is not None]
            if not vals:
                continue
            returns=[x["return_pct"] for x in vals]
            mfes=[x["mfe_pct"] for x in vals]
            maes=[x["mae_pct"] for x in vals]
            result.append({
                "direction":direction,"score_bucket":bucket,"horizon_h":h,
                "n":len(vals),
                "win_pct":round(100*sum(x>0 for x in returns)/len(returns),1),
                "avg_return_pct":round(statistics.fmean(returns),3),
                "median_return_pct":round(statistics.median(returns),3),
                "avg_mfe_pct":round(statistics.fmean(mfes),3),
                "avg_mae_pct":round(statistics.fmean(maes),3),
            })
    return result


def backtest_symbol(symbol,days=60,request_get=None,now=None):
    now=time.time() if now is None else float(now)
    end_ms=int(now*1000)
    # Warm-up: daily trend needs ~25 days before first evaluated signal.
    start_ms=int((now-(days+35)*86400)*1000)
    c1=fetch_klines(symbol,"60",start_ms,end_ms,request_get)
    oi=fetch_oi_1h(symbol,start_ms,end_ms,request_get)
    events=build_signals(symbol,c1,oi)
    cutoff=now-days*86400
    events=[e for e in events if e["ts"]>=cutoff]
    return {
        "symbol":symbol,"days":days,
        "hourly_candles":len(c1),"oi_points":len(oi),
        "events":events,"summary":summarize(events),
    }


def main():
    p=argparse.ArgumentParser()
    p.add_argument("symbols",nargs="+")
    p.add_argument("--days",type=int,default=60)
    p.add_argument("--events",action="store_true")
    args=p.parse_args()
    reports=[]
    for raw in args.symbols:
        symbol=raw.upper()
        if not symbol.endswith("USDT"):
            symbol+="USDT"
        report=backtest_symbol(symbol,args.days)
        if not args.events:
            report=dict(report,events=[])
        reports.append(report)
    print(json.dumps(reports,ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
