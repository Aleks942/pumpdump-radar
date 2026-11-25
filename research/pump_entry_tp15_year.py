"""Price-only causal entry study using ALL hourly trigger episodes from the annual full-cycle scan.
Uses only trigger/base timestamps known at trigger, never ex-post peak or outcome.
No production imports or orders."""
import argparse,csv,hashlib,json,statistics
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path
from pump_full_cycle_year import candles,END,M,H,D
THRESHOLDS=(2.5,5,7,10,15)
ENTRY_WINDOW=4*D
HOLD=3*D
COST=0.20

def simulate(e,rows,threshold,end=END):
    trigger=int(e["trigger_at"])
    sym=e["symbol"]
    out=dict(symbol=sym,trigger_at=trigger,threshold=threshold,entry=None,stop=None,target=None,
             outcome="NO_ENTRY",net_pct=None,net_r=None)
    pre=[r for r in rows if int(e["base_at"])<=r[0]<trigger]
    base=int(e["base_at"])
    if len(pre)!=(trigger-base)//M or any(r[0]!=base+i*M for i,r in enumerate(pre)):
        out["outcome"]="DATA_GAP";return out
    peak=max([float(e["trigger_price"])]+[r[2] for r in pre])
    phase="DROP";trough=peak; rebound=None; broken=None; pending=None
    expected=trigger
    for ts,o,hi,lo,c in rows:
        if ts<trigger: continue
        if ts>=end: break
        if ts!=expected:
            out["outcome"]="DATA_GAP";return out
        expected=ts+M
        if out["entry"] is None and ts>=trigger+ENTRY_WINDOW:
            return out
        if pending is not None and out["entry"] is None:
            stop=pending
            if o>=stop or not 0.5<=100*(stop/o-1)<=9:
                out["outcome"]="RISK_BLOCKED";return out
            entry=o; risk=100*(stop/o-1)
            out.update(entry=entry,entry_at=ts,stop=stop,target=entry*.85,risk_pct=risk,
                       confirmed_peak=peak,first_drop_pct=100*(1-trough/peak))
        if out["entry"] is not None:
            entry=out["entry"];stop=out["stop"];target=out["target"]
            if hi>=stop: # conservative when both stop and target are touched
                price=max(o,stop);result="SL"
            elif lo<=target:price=target;result="TP15"
            elif ts+M>=out["entry_at"]+HOLD:price=c;result="TIME_EXIT"
            else:continue
            net=100*(1-price/entry)-COST
            out.update(outcome=result,exit_at=ts+M,exit_price=price,net_pct=net,net_r=net/out["risk_pct"])
            return out
        if hi>peak:
            peak=hi;trough=c;phase="DROP";rebound=None;broken=None
            continue
        if phase=="DROP":
            trough=min(trough,c)
            if 100*(1-trough/peak)>=threshold and c>=trough*1.02:
                phase="BOUNCE";rebound=hi
        elif phase=="BOUNCE":
            rebound=max(rebound,hi)
            if c<trough:
                phase="RETEST";broken=ts
        elif phase=="RETEST":
            if ts-broken>12*H:
                out["outcome"]="NO_RETEST";return out
            if hi>=trough*.995 and c<trough:
                pending=max(rebound,hi)*1.005
    out["outcome"]="CENSORED" if expected>=end else "DATA_GAP"
    return out

def merged(windows):
    out=[]
    for lo,hi in sorted(windows):
        if out and lo<=out[-1][1]:out[-1]=(out[-1][0],max(out[-1][1],hi))
        else:out.append((lo,hi))
    return out

def scan(sym,episodes):
    try:
        data={}
        for lo,hi in merged([(int(e["base_at"]),min(END,int(e["trigger_at"])+ENTRY_WINDOW+HOLD)) for e in episodes]):
            data.update({r[0]:r for r in candles(sym,"5m",lo,hi)})
        rows=sorted(data.values())
        return [simulate(e,[r for r in rows if int(e["base_at"])<=r[0]<min(END,int(e["trigger_at"])+ENTRY_WINDOW+HOLD)],t) for e in episodes for t in THRESHOLDS]
    except Exception as ex:
        return [dict(symbol=sym,trigger_at=e["trigger_at"],threshold=t,outcome="FETCH_ERROR",error=str(ex)[:160],entry=None,net_pct=None,net_r=None) for e in episodes for t in THRESHOLDS]

def self_test():
    e=dict(symbol="TEST",trigger_at=0,base_at=0,trigger_price=100)
    # decline 8%, bounce, break, retest, enter at next open, target hit
    rows=[(0,100,100,92,92),(M,92,95,92,95),(2*M,95,95,91,91),(3*M,91,92,90,91),(4*M,91,91,76,78)]
    a=simulate(e,rows,7,end=10*D)
    assert a["outcome"]=="TP15" and a["entry_at"]==4*M and abs(a["target"]-77.35)<1e-8,a
    b=simulate(e,rows[:-1]+[(4*M,91,96,76,78)],7,end=10*D)
    assert b["outcome"]=="SL",b
    c=simulate(e,rows,10,end=10*D)
    assert c["entry"] is None,c
    print("SELF_TEST_OK",flush=True)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--shard",type=int,default=0)
    p.add_argument("--shards",type=int,default=8)
    p.add_argument("--aggregate",action="store_true")
    args=p.parse_args()
    self_test()
    if args.aggregate:
        files=sorted(Path("entry-shards").rglob("entry-*.json"))
        if len(files)!=args.shards:raise RuntimeError("Missing shard")
        results=[r for f in files for r in json.loads(f.read_text())]
        report={}
        for t in THRESHOLDS:
            es=[r for r in results if r["threshold"]==t]
            finished=[r for r in es if r["outcome"] in ("TP15","SL","TIME_EXIT")]
            count=Counter(r["outcome"] for r in es)
            report[str(t)]=dict(episodes=len(es),outcomes=dict(count),entries=sum(r.get("entry") is not None for r in es),
                resolved=len(finished),target_rate=round(100*count["TP15"]/len(finished),2) if finished else None,
                mean_net_pct=round(statistics.mean(r["net_pct"] for r in finished),4) if finished else None,
                mean_net_r=round(statistics.mean(r["net_r"] for r in finished),4) if finished else None,
                target_hits_per_all=round(100*count["TP15"]/len(es),2) if es else None)
        Path("entry-output").mkdir(exist_ok=True)
        Path("entry-output/summary.json").write_text(json.dumps(report,indent=2))
        fields=sorted({k for r in results for k in r})
        with open("entry-output/trades.csv","w",newline="") as f:
            w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(results)
        print("TP15_REPORT",json.dumps(report),flush=True)
        return
    with open("full-cycle-input/events.csv") as f:episodes=list(csv.DictReader(f))
    groups={}
    for e in episodes:
        s=e["symbol"]
        if int(hashlib.sha256(s.encode()).hexdigest(),16)%args.shards==args.shard:groups.setdefault(s,[]).append(e)
    results=[]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed([pool.submit(scan,s,es) for s,es in groups.items()]):
            rr=f.result();results.extend(rr);print("COVERAGE",rr[0]["symbol"],len(rr),dict(Counter(r["outcome"] for r in rr)),flush=True)
    Path("entry-shard-output").mkdir(exist_ok=True)
    Path(f"entry-shard-output/entry-{args.shard}.json").write_text(json.dumps(results))
if __name__=="__main__":main()
