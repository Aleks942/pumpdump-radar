"""Descriptive annual OKX study. No orders, production imports, or future-based entries."""
import argparse, csv, datetime as dt, hashlib, json, math, statistics, time, threading
import urllib.parse, urllib.request
from collections import deque, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

H=3600000
D=24*H
M=300000
START=int(dt.datetime(2025,11,25,tzinfo=dt.timezone.utc).timestamp()*1000)
END=int(dt.datetime(2026,11,25,tzinfo=dt.timezone.utc).timestamp()*1000)
HORIZON=30*D
LOCK=threading.Lock()
NEXT=0.0

def get(path, **params):
    global NEXT
    url="https://www.okx.com"+path+"?"+urllib.parse.urlencode(params)
    for attempt in range(5):
        with LOCK:
            now=time.monotonic()
            wait=max(0,NEXT-now)
            NEXT=max(now,NEXT)+0.20
        if wait: time.sleep(wait)
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"pump-full-cycle-research/1.0"}),timeout=20) as f:
                j=json.load(f)
            if j.get("code")=="0": return j["data"]
            raise RuntimeError(str(j)[:200])
        except Exception:
            if attempt==4: raise
            time.sleep(2**attempt)

def candles(sym,bar,start,end):
    out={}; cursor=end
    while cursor>start:
        rows=get("/api/v5/market/history-candles",instId=sym,bar=bar,after=cursor,limit=300)
        if not rows: break
        oldest=min(int(r[0]) for r in rows)
        if oldest>=cursor: raise RuntimeError("nonadvancing pagination")
        for r in rows:
            ts=int(r[0])
            if start<=ts<end and r[-1]=="1":
                o,h,l,c=map(float,r[1:5])
                if min(o,h,l,c)>0 and l<=min(o,c)<=max(o,c)<=h:
                    out[ts]=(ts,o,h,l,c)
        cursor=oldest
    return sorted(out.values())

def windows(daily):
    out=[]
    for a,b in zip(daily,daily[1:]):
        if b[0]-a[0]!=D: continue
        if max(a[2],b[2])/min(a[3],b[3])<1.30: continue
        lo=max(START-D,b[0]-D)
        hi=min(END,b[0]+D+HORIZON)
        if lo>=hi: continue
        if out and lo<=out[-1][1]: out[-1]=(out[-1][0],max(hi,out[-1][1]))
        else: out.append((lo,hi))
    return out

def cycles(sym,rows,start=START,end=END,horizon=HORIZON):
    hist=deque(maxlen=24); state=None; prev=None; eligible_before=False; out=[]
    def finish(s,status,t):
        q=dict(s)
        q.update(status=status,end_at=t,
                 peak_gain_pct=100*(s["peak"]/s["base"]-1),
                 drop_to_base_pct=100*(1-s["base"]/s["peak"]),
                 max_post_peak_close_drop_pct=100*(1-s["post_peak_min_close"]/s["peak"]),
                 peak_to_end_hours=(t-(s["peak_bar"]+H))/H,
                 trigger_to_end_hours=(t-s["trigger_at"])/H)
        if status=="ABSORBED":
            q["absorbed_at"]=t
        else: q["absorbed_at"]=None
        out.append(q)
    for ts,o,hi,lo,c in rows:
        t=ts+H
        if t>end: break
        if prev is not None and ts!=prev+H:
            if state: finish(state,"GAP",prev+H)
            state=None; hist.clear(); eligible_before=False
        prev=ts
        # hist contains 24 closed hours, oldest close exactly 24h before current close.
        eligible=len(hist)==24 and c>=hist[0][4]*1.30
        crossed=eligible and not eligible_before
        eligible_before=eligible
        if state:
            s=state
            if hi>s["peak"]:
                s.update(peak=hi,peak_bar=ts,post_peak_min_close=c)
            else:
                s["post_peak_min_close"]=min(s["post_peak_min_close"],c)
            if c<=s["base"]:
                finish(s,"ABSORBED",t); state=None
            elif t-s["trigger_at"]>=horizon:
                finish(s,"NOT_ABSORBED_30D",t); state=None
        elif crossed and start<t<=end:
            # Baseline defined prospectively: lowest hourly close in prior 24 hours.
            # Do not include highs preceding that closing price.
            b=min(hist,key=lambda r:r[4])
            path=[r for r in hist if r[0]>b[0]]+[(ts,o,hi,lo,c)]
            p=max(path,key=lambda r:r[2])
            state=dict(symbol=sym,base=b[4],base_at=b[0]+H,
                       trigger_at=t,trigger_price=c,trigger_gain_pct=100*(c/hist[0][4]-1),
                       peak=p[2],peak_bar=p[0],post_peak_min_close=min(r[4] for r in path if r[0]>=p[0]))
        hist.append((ts,o,hi,lo,c))
    if state:
        last=prev+H
        finish(state,"CENSORED_END" if last>=end else "GAP",last)
    return out

def refine(s):
    # Describe the first decline from the ex-post peak, confirmed by a 2% rebound
    # on 5m closes. This is NOT an entry signal or a causal backtest.
    hi=min(END,s["end_at"],s["peak_bar"]+D+H)
    lo=s["peak_bar"]
    s.update(first_pullback_pct=None,first_rebound_pct=None,refine_status="NO_5M_DATA")
    if hi<=lo: return
    rows=candles(s["symbol"],"5m",lo,hi)
    peaks=[r for r in rows if r[0]<lo+H and abs(r[2]-s["peak"])<=max(1e-12,s["peak"]*1e-8)]
    if not peaks: return
    p=peaks[-1]
    s["peak_5m_at"]=p[0]
    trough=p[4]; prev=p[0]
    for r in rows:
        if r[0]<=p[0]: continue
        if r[0]!=prev+M:
            s["refine_status"]="GAP_5M";return
        prev=r[0]; c=r[4]
        if c<trough: trough=c
        elif trough<s["peak"] and c>=trough*1.02:
            s.update(first_pullback_pct=100*(1-trough/s["peak"]),
                     first_rebound_pct=100*(c/trough-1),refine_status="REBOUND_2_CONFIRMED")
            return
    s["refine_status"]="NO_REBOUND_BEFORE_END_OR_25H"
    s["unconfirmed_drop_pct"]=100*(1-trough/s["peak"])

def scan(sym):
    try:
        daily=candles(sym,"1Dutc",START-2*D,END)
        ws=windows(daily)
        rows={}
        for lo,hi in ws:
            rows.update({r[0]:r for r in candles(sym,"1H",lo,hi)})
        events=cycles(sym,sorted(rows.values()))
        for e in events:
            if e["status"]=="ABSORBED":
                try: refine(e)
                except Exception as ex: e["refine_status"]="ERROR: "+str(ex)[:120]
        return dict(symbol=sym,daily_bars=len(daily),hourly_bars=len(rows),windows=len(ws),events=events)
    except Exception as ex:
        return dict(symbol=sym,error=str(ex)[:300],events=[])

def stats(values):
    v=sorted(x for x in values if x is not None and math.isfinite(x))
    if not v:return {"n":0}
    return dict(n=len(v),min=round(v[0],4),mean=round(statistics.mean(v),4),
                median=round(statistics.median(v),4),max=round(v[-1],4),
                p90=round(v[math.ceil(.9*len(v))-1],4))

def summarize(events,coverage):
    r=dict(instruments=len(coverage),errors=sum("error" in x for x in coverage),
           full_daily_history=sum(x.get("daily_bars",0)>=365 for x in coverage),
           events=len(events),outcomes=dict(Counter(e["status"] for e in events)),groups={})
    for name,lo,hi in [("all",30,math.inf),("30-50",30,50),("50-100",50,100),("100-200",100,200),("200-500",200,500),("500+",500,math.inf)]:
        es=[e for e in events if lo<=e["peak_gain_pct"]<hi]
        a=[e for e in es if e["status"]=="ABSORBED"]
        n=[e for e in es if e["status"]=="NOT_ABSORBED_30D"]
        r["groups"][name]=dict(n=len(es),outcomes=dict(Counter(e["status"] for e in es)),
             absorption_observed_rate=round(len(a)/(len(a)+len(n))*100,3) if a or n else None,
             peak_gain_all=stats([e["peak_gain_pct"] for e in es]),
             peak_gain_absorbed=stats([e["peak_gain_pct"] for e in a]),
             peak_to_base_drop=stats([e["drop_to_base_pct"] for e in a]),
             hours_peak_to_absorption=stats([e["peak_to_end_hours"] for e in a]),
             first_pullback_absorbed=stats([e.get("first_pullback_pct") for e in a]),
             refinement=dict(Counter(e.get("refine_status","NOT_RUN") for e in a)),
             thresholds={str(t):sum(e.get("first_pullback_pct") is not None and e["first_pullback_pct"]>=t for e in a) for t in [2.5,5,7,10,15]})
    r["largest_absorbed"]=sorted([e for e in events if e["status"]=="ABSORBED"],key=lambda e:e["peak_gain_pct"],reverse=True)[:10]
    return r

def self_test():
    t=START-D
    def bar(i,c,hi=None):
        return (t+i*H,c,hi or c,c,c)
    rows=[bar(i,100) for i in range(24)]+[bar(24,130),bar(25,150,160),bar(26,140),bar(27,170,180),bar(28,100)]
    e=cycles("TEST",rows)[0]
    assert e["status"]=="ABSORBED" and e["peak"]==180 and e["base"]==100
    assert abs(e["peak_gain_pct"]-80)<1e-8
    assert abs(e["drop_to_base_pct"]-44.4444444444)<1e-7
    assert cycles("TEST",rows[:-1],end=rows[-2][0]+H)[0]["status"]=="CENSORED_END"
    assert cycles("TEST",rows[:-1],horizon=2*H)[0]["status"]=="NOT_ABSORBED_30D"
    assert cycles("TEST",rows[:26]+rows[27:])[0]["status"]=="GAP"
    print("SELF_TEST_OK",flush=True)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--shard",type=int,default=0)
    p.add_argument("--shards",type=int,default=8)
    p.add_argument("--aggregate",action="store_true")
    args=p.parse_args()
    self_test()
    if args.aggregate:
        paths=sorted(Path("shards").rglob("shard-*.json"))
        if len(paths)!=args.shards: raise RuntimeError(f"Expected {args.shards} shards, got {len(paths)}")
        results=[r for f in paths for r in json.loads(f.read_text())]
        if len({r["symbol"] for r in results})!=len(results): raise RuntimeError("Duplicate instruments")
        events=[e for r in results for e in r["events"]]
        coverage=[{k:v for k,v in r.items() if k!="events"} for r in results]
        report=summarize(events,coverage)
        Path("full-cycle-output").mkdir(exist_ok=True)
        Path("full-cycle-output/summary.json").write_text(json.dumps(report,indent=2))
        Path("full-cycle-output/coverage.json").write_text(json.dumps(coverage,indent=2))
        fields=sorted({k for e in events for k in e})
        if fields:
            with open("full-cycle-output/events.csv","w",newline="") as f:
                w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(events)
        print("FULL_CYCLE_REPORT",json.dumps(report),flush=True)
        if not events: raise RuntimeError("No usable events")
        return
    universe=sorted({r["instId"] for r in get("/api/v5/public/instruments",instType="SWAP") if r["instId"].endswith("-USDT-SWAP")})
    syms=[s for s in universe if int(hashlib.sha256(s.encode()).hexdigest(),16)%args.shards==args.shard]
    print("UNIVERSE",len(universe),"SHARD",args.shard,"COUNT",len(syms),flush=True)
    results=[]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed([pool.submit(scan,s) for s in syms]):
            r=f.result();results.append(r)
            print("COVERAGE",json.dumps({k:v for k,v in r.items() if k!="events"}),"events",len(r["events"]),flush=True)
    Path("shard-output").mkdir(exist_ok=True)
    Path(f"shard-output/shard-{args.shard}.json").write_text(json.dumps(results))
    print("SHARD_COMPLETE",args.shard,len(results),flush=True)
if __name__=="__main__": main()
