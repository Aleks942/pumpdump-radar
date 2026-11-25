"""Price-only exploratory research, no orders and no production imports."""
import csv, datetime as dt, hashlib, json, math, statistics, time, urllib.parse, urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DAY=86400000
BAR=300000
START=int(dt.datetime(2025,11,25,tzinfo=dt.timezone.utc).timestamp()*1000)
END=int(dt.datetime(2026,11,25,tzinfo=dt.timezone.utc).timestamp()*1000)
BASE="https://www.okx.com"
def get(path, **params):
    url=BASE+path+"?"+urllib.parse.urlencode(params)
    for retry in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"pump-research/1.0"}),timeout=15) as f:
                j=json.load(f)
            if j.get("code")=="0": return j["data"]
            raise ValueError(str(j)[:200])
        except Exception:
            if retry==3: raise
            time.sleep(1+retry*2)
def candles(sym,bar,start,end):
    result={}
    cursor=end
    while cursor>start:
        rows=get("/api/v5/market/history-candles",instId=sym,bar=bar,after=cursor,limit=300)
        if not rows: break
        low=min(int(r[0]) for r in rows)
        if low>=cursor: raise RuntimeError("nonadvancing pagination")
        for r in rows:
            ts=int(r[0])
            if start<=ts<end and r[-1]=="1":
                o,h,l,c=map(float,r[1:5])
                if min(o,h,l,c)>0: result[ts]=(ts,o,h,l,c)
        cursor=low
    return sorted(result.values())
def candidate_windows(daily):
    ranges=[]
    for i in range(1,len(daily)):
        p,x=daily[i-1],daily[i]
        if x[0]-p[0]!=DAY: continue
        # Broad filter: a 24h gain can span neighboring UTC days. Use lows,
        # not just opens, to avoid missing potentially eligible intraday gains.
        if max(p[2],x[2])/min(p[3],x[3])>=1.30:
            lo=max(START-DAY,x[0]-2*DAY)
            hi=min(END,x[0]+3*DAY)
            if ranges and lo<=ranges[-1][1]: ranges[-1]=(ranges[-1][0],max(hi,ranges[-1][1]))
            else: ranges.append((lo,hi))
    return ranges
def episodes(sym,rows):
    out=[]; state=None; prev=None; by_ts={}
    for ts,o,h,l,c in rows:
        if prev is not None and ts-prev!=BAR:
            state=None; by_ts={}
        prev=ts; by_ts[ts]=o
        base=by_ts.get(ts-DAY)
        by_ts.pop(ts-DAY-BAR,None)
        if state is None:
            if base and START<=ts<END and c/base>=1.30:
                state={"symbol":sym,"trigger":ts,"base":base,"peak":c,"peak_at":ts,"trough":c,"trough_at":ts,"rebound_at":None,"bounce":0.0}
            continue
        s=state
        # Closes only, to make ordering observable without guessing intrabar paths.
        if s["rebound_at"] is None:
            if c>s["peak"]:
                s.update(peak=c,peak_at=ts,trough=c,trough_at=ts)
            elif c<s["trough"]:
                s.update(trough=c,trough_at=ts)
            elif s["trough"]<s["peak"] and c/s["trough"]>=1.02:
                s.update(rebound_at=ts,bounce=100*(c/s["trough"]-1),rebound_high=c,min_after=c)
            if ts-s["trigger"]>=3*DAY:
                state=None
            continue
        s["rebound_high"]=max(s["rebound_high"],c)
        s["min_after"]=min(s["min_after"],c)
        outcome=None
        if c>=s["peak"]: outcome="NEW_HIGH"
        elif c<=s["trough"]*.95: outcome="LOWER_LOW_5"
        elif ts-s["rebound_at"]>=DAY: outcome="NO_RESOLUTION_24H"
        if outcome:
            out.append(dict(symbol=sym,trigger=s["trigger"],peak_at=s["peak_at"],peak_gain_pct=100*(s["peak"]/s["base"]-1),first_pullback_pct=100*(1-s["trough"]/s["peak"]),bounce_pct=100*(s["rebound_high"]/s["trough"]-1),further_drop_pct=100*(1-s["min_after"]/s["trough"]),outcome=outcome))
            state=None
    return out
def scan(sym):
    try:
        daily=candles(sym,"1Dutc",START-2*DAY,END)
        ranges=candidate_windows(daily)
        rows=[]
        for lo,hi in ranges: rows.extend(candles(sym,"5m",lo,hi))
        data=episodes(sym,sorted({r[0]:r for r in rows}.values()))
        return {"symbol":sym,"daily_bars":len(daily),"five_minute_bars":len(rows),"windows":len(ranges),"events":data}
    except Exception as e:
        return {"symbol":sym,"error":str(e),"events":[]}
def main():
    universe=[r["instId"] for r in get("/api/v5/public/instruments",instType="SWAP") if r["instId"].endswith("-USDT-SWAP")]
    # Fixed hash sample, no hindsight selection based on pump outcome.
    syms=sorted(set(universe),key=lambda s:hashlib.sha256(("pump-first-v1"+s).encode()).hexdigest())[:60]
    results=[]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for item in pool.map(scan,syms):
            results.append(item)
            print("COVERAGE",json.dumps({k:v for k,v in item.items() if k!="events"}),"events",len(item["events"]),flush=True)
    events=[e for r in results for e in r["events"]]
    report={"start":"2025-11-25 UTC","end_exclusive":"2026-11-25 UTC","universe_current":len(universe),"sample":len(syms),"successful_symbols":sum("error" not in r for r in results),"events":len(events),"outcomes":dict(Counter(e["outcome"] for e in events)),"groups":{}}
    for name,lo,hi in [("all",30,math.inf),("30-50",30,50),("50-100",50,100),("100+",100,math.inf)]:
        es=[e for e in events if lo<=e["peak_gain_pct"]<hi]
        group={"n":len(es)}
        for outcome in ["LOWER_LOW_5","NEW_HIGH","NO_RESOLUTION_24H"]:
            values=[e["first_pullback_pct"] for e in es if e["outcome"]==outcome]
            group[outcome]={"n":len(values),"median_first_pullback":round(statistics.median(values),3) if values else None,"threshold_counts":{str(t):sum(v>=t for v in values) for t in [2.5,5,7,10,15]}}
        report["groups"][name]=group
    Path("research-output").mkdir(exist_ok=True)
    Path("research-output/summary.json").write_text(json.dumps(report,indent=2))
    Path("research-output/coverage.json").write_text(json.dumps([{k:v for k,v in r.items() if k!="events"} for r in results],indent=2))
    with open("research-output/events.csv","w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=["symbol","trigger","peak_at","peak_gain_pct","first_pullback_pct","bounce_pct","further_drop_pct","outcome"])
        w.writeheader();w.writerows(events)
    print("YEAR_REPORT",json.dumps(report),flush=True)
if __name__=="__main__": main()
