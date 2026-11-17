"""Conservative candle-based trade simulation for historical SWING events.
Entry at the first hour open after a signal. Fixed stop/target relative to entry.
If stop and target touch in the same candle, stop is assumed first.
"""
import argparse
import json
import statistics
import math
from swing_backtest import backtest_symbol, MODELED_ROUND_TRIP_COST_PCT

def simulate(event, candles, stop_pct=2.0, target_pct=4.0, max_hours=72,
             cost_pct=MODELED_ROUND_TRIP_COST_PCT, slippage_pct=0.05):
    direction=event["direction"]
    entry=float(event["entry"])
    start=float(event["ts"])
    if direction not in ("LONG","SHORT") or entry<=0:
        raise ValueError("Invalid trade")
    path=[c for c in candles if start<=c["ts"]<start+max_hours*3600]
    if not path or path[0]["ts"]!=start or len(path)<max_hours:
        return None
    if any(path[i]["ts"]-path[i-1]["ts"]!=3600 for i in range(1,len(path))):
        return None
    sign=1 if direction=="LONG" else -1
    stop=entry*(1-sign*stop_pct/100)
    target=entry*(1+sign*target_pct/100)
    exit_px=None
    reason="TIME"
    exit_ts=path[-1]["ts"]+3600
    for c in path:
        stop_hit=c["l"]<=stop if sign==1 else c["h"]>=stop
        target_hit=c["h"]>=target if sign==1 else c["l"]<=target
        if stop_hit:
            exit_px=stop
            reason="STOP"
            exit_ts=c["ts"]+3600
            break
        if target_hit:
            exit_px=target
            reason="TARGET"
            exit_ts=c["ts"]+3600
            break
    if exit_px is None:
        exit_px=path[-1]["c"]
    gross=sign*100*(exit_px/entry-1)
    # Slippage modeled on both sides of the transaction.
    gross-=2*slippage_pct
    return {"symbol":event["symbol"],"ts":start,"direction":direction,
            "score":event["score"],"entry":entry,"exit":exit_px,"exit_ts":exit_ts,
            "reason":reason,"gross_pct":round(gross,5),
            "net_pct":round(gross-cost_pct,5)}

def report(trades):
    if not trades:
        return {"n":0,"status":"INSUFFICIENT_DATA"}
    values=[t["net_pct"] for t in trades]
    # Risk-normalized, non-overlapping portfolio proxy: one trade at a time.
    equity=1.0
    peak=1.0
    drawdown=0.0
    selected=[]
    next_entry=0.0
    for t in sorted(trades,key=lambda x:(x["ts"],x["symbol"])):
        if t["ts"]<next_entry:
            continue
        selected.append(t)
        next_entry=t["exit_ts"]
        equity*=1+t["net_pct"]/100*0.02
        peak=max(peak,equity)
        drawdown=max(drawdown,100*(1-equity/peak))
    return {"n":len(trades),"win_rate_pct":round(100*sum(v>0 for v in values)/len(values),2),
            "avg_net_pct":round(statistics.mean(values),3),
            "median_net_pct":round(statistics.median(values),3),
            "portfolio_trades_nonoverlap":len(selected),
            "compounded_pct_illustrative":round(100*(equity-1),2),
            "max_drawdown_pct_illustrative":round(drawdown,2),
            "stops":sum(t["reason"]=="STOP" for t in trades),
            "targets":sum(t["reason"]=="TARGET" for t in trades),
            "note":"Illustrative one-position portfolio, 2% notional allocation per trade; excludes funding and gaps."}

def main():
    from swing_backtest import fetch_klines
    import time
    p=argparse.ArgumentParser()
    p.add_argument("symbols",nargs="+")
    p.add_argument("--days",type=int,default=90)
    p.add_argument("--stop",type=float,default=2.0)
    p.add_argument("--target",type=float,default=4.0)
    p.add_argument("--hours",type=int,default=72)
    p.add_argument("--slippage",type=float,default=0.05)
    p.add_argument("--min-oi-coverage",type=float,default=0.90)
    args=p.parse_args()
    if not (1<=args.days<=365 and 0<args.stop<100 and 0<args.target<100 and 1<=args.hours<=168 and 0<=args.slippage<10 and 0<args.min_oi_coverage<=1):
        p.error("Invalid parameters")
    now=time.time()
    all_trades=[]
    per_symbol=[]
    for raw in args.symbols:
        symbol=raw.upper()
        if not symbol.endswith("USDT"):symbol+="USDT"
        base=backtest_symbol(symbol,args.days,now=now)
        candles=fetch_klines(symbol,"60",int((now-(args.days+35)*86400)*1000),int(now*1000))
        coverage=base["oi_points"]/max(1,base["hourly_candles"])
        if coverage<args.min_oi_coverage:
            per_symbol.append({"symbol":symbol,"status":"INSUFFICIENT_OI_HISTORY",
                "oi_coverage":round(coverage,3),"candles":len(candles),
                "oi_points":base["oi_points"]})
            continue
        trades=[simulate(e,candles,args.stop,args.target,args.hours,
                         slippage_pct=args.slippage) for e in base["events"]]
        trades=[t for t in trades if t]
        all_trades.extend(trades)
        per_symbol.append({"symbol":symbol,"candles":len(candles),
                           "oi_points":base["oi_points"],"stats":report(trades)})
    print(json.dumps({"parameters":vars(args),"symbols":per_symbol,
                      "combined":report(all_trades),
                      "limitations":"Bybit price/OI only; no historical CVD/liquidations. Slippage estimated, funding and price gaps not modeled. Stop-first if both touched."},
                     ensure_ascii=False,indent=2))

if __name__=="__main__":main()
