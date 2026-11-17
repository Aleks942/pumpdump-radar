"""Conservative candle-based trade simulation for historical SWING events.
Entry at the first hour open after a signal. Fixed stop/target relative to entry.
If stop and target touch in the same candle, stop is assumed first.
"""
import argparse
import json
import statistics
from swing_backtest import backtest_symbol, MODELED_ROUND_TRIP_COST_PCT

def simulate(event, candles, stop_pct=2.0, target_pct=4.0, max_hours=72,
             cost_pct=MODELED_ROUND_TRIP_COST_PCT):
    direction=event["direction"]
    entry=float(event["entry"])
    start=float(event["ts"])
    if direction not in ("LONG","SHORT") or entry<=0:
        raise ValueError("Invalid trade")
    path=[c for c in candles if start<=c["ts"]<start+max_hours*3600]
    if not path or path[0]["ts"]!=start:
        return None
    sign=1 if direction=="LONG" else -1
    stop=entry*(1-sign*stop_pct/100)
    target=entry*(1+sign*target_pct/100)
    exit_px=None
    reason="TIME"
    for c in path:
        stop_hit=c["l"]<=stop if sign==1 else c["h"]>=stop
        target_hit=c["h"]>=target if sign==1 else c["l"]<=target
        if stop_hit:
            exit_px=stop
            reason="STOP"
            break
        if target_hit:
            exit_px=target
            reason="TARGET"
            break
    if exit_px is None:
        exit_px=path[-1]["c"]
    gross=sign*100*(exit_px/entry-1)
    return {"symbol":event["symbol"],"ts":start,"direction":direction,
            "score":event["score"],"entry":entry,"exit":exit_px,
            "reason":reason,"gross_pct":round(gross,5),
            "net_pct":round(gross-cost_pct,5)}

def report(trades):
    if not trades:
        return {"n":0,"status":"INSUFFICIENT_DATA"}
    values=[t["net_pct"] for t in trades]
    equity=1.0
    peak=1.0
    drawdown=0.0
    for t in sorted(trades,key=lambda x:x["ts"]):
        equity*=1+t["net_pct"]/100
        peak=max(peak,equity)
        drawdown=max(drawdown,100*(1-equity/peak))
    return {"n":len(trades),"win_rate_pct":round(100*sum(v>0 for v in values)/len(values),2),
            "avg_net_pct":round(statistics.mean(values),3),
            "median_net_pct":round(statistics.median(values),3),
            "compounded_pct_illustrative":round(100*(equity-1),2),
            "max_drawdown_pct_illustrative":round(drawdown,2),
            "stops":sum(t["reason"]=="STOP" for t in trades),
            "targets":sum(t["reason"]=="TARGET" for t in trades),
            "note":"Overlapping trades and position sizing are not modeled; compounded figures are illustrative."}

def main():
    from swing_backtest import fetch_klines
    import time
    p=argparse.ArgumentParser()
    p.add_argument("symbols",nargs="+")
    p.add_argument("--days",type=int,default=90)
    p.add_argument("--stop",type=float,default=2.0)
    p.add_argument("--target",type=float,default=4.0)
    p.add_argument("--hours",type=int,default=72)
    args=p.parse_args()
    if not (1<=args.days<=365 and 0<args.stop<100 and 0<args.target<100 and 1<=args.hours<=168):
        p.error("Invalid parameters")
    now=time.time()
    all_trades=[]
    per_symbol=[]
    for raw in args.symbols:
        symbol=raw.upper()
        if not symbol.endswith("USDT"):symbol+="USDT"
        base=backtest_symbol(symbol,args.days,now=now)
        candles=fetch_klines(symbol,"60",int((now-(args.days+35)*86400)*1000),int(now*1000))
        trades=[simulate(e,candles,args.stop,args.target,args.hours) for e in base["events"]]
        trades=[t for t in trades if t]
        all_trades.extend(trades)
        per_symbol.append({"symbol":symbol,"candles":len(candles),
                           "oi_points":base["oi_points"],"stats":report(trades)})
    print(json.dumps({"parameters":vars(args),"symbols":per_symbol,
                      "combined":report(all_trades),
                      "limitations":"Bybit price/OI only; no historical CVD/liquidations. No slippage, funding or intrabar gap modeling."},
                     ensure_ascii=False,indent=2))

if __name__=="__main__":main()
