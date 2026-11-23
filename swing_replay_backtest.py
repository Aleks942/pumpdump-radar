"""Point-in-time historical replay of the current SWING core lifecycle.

Uses only historically reproducible data:
- 15m/1H/4H/1D price/volume structure
- OKX 1H open interest
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
from swing_market_scanner import _compression_snapshot
from swing_strength_engine import (
    _score, _trend, _volume_state, _entry_setup, _trade_plan, _breakout_state, entry_price_allowed,
)

DEFAULT_DAYS=30
MODELED_ROUND_TRIP_COST_PCT=0.25
MAX_HOURS=72
DEFAULT_SYMBOLS=("BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT",
                 "ADAUSDT","LINKUSDT","AVAXUSDT","SUIUSDT","APTUSDT")


OKX_BASE="https://www.okx.com/api/v5"


def _okx_get(path,params=None,request_get=None):
    getter=request_get or requests.get
    for attempt in range(4):
        r = getter(OKX_BASE + path, params=params, timeout=15)
        if r.status_code not in (429, 500, 502, 503, 504) or attempt == 3:
            break
        retry_after = _num(r.headers.get("Retry-After"))
        time.sleep(min(30, max(2 ** attempt, retry_after or 0)))
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
    return {**scored,"h1":h1,"oi_1h_change_pct":oi1["change_pct"]}


def build_ready_entries(symbol,c15,c1,oi_rows,stages=None,cutoff=None):
    if not c15 or not c1 or not oi_rows:
        return []
    c4=_resample(c1,4)
    cd=_resample(c1,24)
    out=[]
    if stages is not None:
        for index, candle in enumerate(c1):
            close_at = candle["ts"] + 3600
            if close_at >= cutoff and _compression_snapshot(c1[max(0,index-35):index+1])["active"]:
                stages["WATCH"].add(close_at)

    for i in range(79,len(c15)-1):
        decision_ts=c15[i]["ts"]+900
        history15=c15[max(0,i-79):i+1]
        scored=_score_at(c1,c4,cd,oi_rows,decision_ts)
        if not scored:
            continue

        bias=scored["bias"]
        side="LONG" if bias=="BULLISH" else "SHORT"
        breakout=_breakout_state(scored["h1"],bias,now=decision_ts)
        if breakout.get("phase")!="BREAKOUT_CONFIRMED":
            continue
        tracked = stages is not None and decision_ts >= cutoff
        identity = (side, breakout.get("closed_at"))
        if tracked:
            stages["BREAKOUT"].add(identity)
        setup=_entry_setup(history15,bias,breakout)
        phase = setup.get("phase")
        if tracked and phase in ("RETEST_FORMING", "READY_ENTRY"):
            stages["RETEST"].add(identity)
        if phase != "READY_ENTRY":
            continue
        expected = "UP" if side == "LONG" else "DOWN"
        if _trend(history15).get("regime") != expected:
            continue

        nxt=c15[i+1]
        if nxt["ts"]!=decision_ts:
            continue
        entry=_num(nxt["o"])
        if not entry_price_allowed(entry, side, breakout, setup):
            continue
        plan=_trade_plan(history15,bias,entry)
        if plan.get("ready") is not True:
            continue

        hour = next((index for index, row in enumerate(scored["h1"])
                     if row["ts"] + 3600 == breakout.get("closed_at")), None)
        breakout_volume_ratio = None
        if hour is not None and hour >= 12:
            average = statistics.fmean(row["v"] for row in scored["h1"][hour-12:hour])
            if average > 0:
                breakout_volume_ratio = scored["h1"][hour]["v"] / average

        out.append({
            "symbol":symbol,"decision_ts":decision_ts,"entry_ts":nxt["ts"],
            "direction":side,"score":int(scored["score"]),
            "entry":entry,"stop":plan["stop"],"tp1":plan["tp1"],"tp2":plan["tp2"],
            "stop_pct":plan["stop_pct"],"breakout_level":breakout.get("level"),
            "setup_at":breakout.get("closed_at"),
            "breakout_volume_ratio":breakout_volume_ratio,
            "oi_1h_change_pct":scored.get("oi_1h_change_pct"),
        })
    return out


def simulate_trade(event,c15,max_hours=MAX_HOURS,
                   cost_pct=MODELED_ROUND_TRIP_COST_PCT):
    start=float(event["entry_ts"])
    end=start+max_hours*3600
    path=[x for x in c15 if start<=x["ts"]<end]
    if not path or path[0]["ts"]!=start:
        return None

    side=event["direction"]
    stop=float(event["stop"]); tp1=float(event["tp1"]); tp2=float(event["tp2"])
    entry=float(event["entry"])
    stop_pct=float(event.get("stop_pct") or 0)
    if stop_pct<=0:
        return None
    tp1_at=None; tp2_at=None; stop_at=None
    terminal=None

    for index, candle in enumerate(path):
        if candle["ts"] != start + index * 900:
            return None
        if side=="LONG":
            stop_hit=candle["l"]<=stop
            tp1_hit=candle["h"]>=tp1
            tp2_hit=candle["h"]>=tp2
        else:
            stop_hit=candle["h"]>=stop
            tp1_hit=candle["l"]<=tp1
            tp2_hit=candle["l"]<=tp2
        if stop_hit:
            stop_fill = min(stop, candle["o"]) if side == "LONG" else max(stop, candle["o"])
            signed_loss = (stop_fill - entry) if side == "LONG" else (entry - stop_fill)
            stop_r = signed_loss / abs(entry - stop)
            stop_at=candle["ts"]+900
            terminal=("STOP",stop_r,stop_at)
            break
        if tp1_at is None and tp1_hit:
            tp1_at=candle["ts"]+900
        if tp2_at is None and tp2_hit:
            tp2_at=candle["ts"]+900
            terminal=("TP2",3.0,tp2_at)
            break

    full_horizon_end=start+max_hours*3600
    if terminal is not None:
        reason,gross_r,exit_ts=terminal
    else:
        expected_last=start+(max_hours*3600)-900
        if path[-1]["ts"]<expected_last:
            return None
        final_px=float(path[-1]["c"])
        risk=abs(entry-stop)
        if risk<=0:
            return None
        signed=(final_px-entry) if side=="LONG" else (entry-final_px)
        gross_r=signed/risk
        reason="TIME"
        exit_ts=full_horizon_end

    cost_r=float(cost_pct)/stop_pct
    net_r=gross_r-cost_r
    return {
        **event,
        "reason":reason,
        "gross_r":round(gross_r,4),
        "cost_r":round(cost_r,4),
        "r":round(net_r,4),
        "exit_ts":exit_ts,
        "tp1_hit":tp1_at is not None,
        "tp2_hit":tp2_at is not None,
        "stop_hit":stop_at is not None,
    }


def select_nonoverlapping_active_entries(events,c15,max_hours=MAX_HOURS):
    """Mimic live tracker: one active sample per symbol and side."""
    active_until={}
    trades=[]
    sampled_setups = set()
    for event in sorted(events,key=lambda x:x["entry_ts"]):
        key = (event["symbol"], event["direction"])
        setup = (*key, event.get("setup_at"))
        if setup[-1] is not None and setup in sampled_setups:
            continue
        if float(event["entry_ts"]) < active_until.get(key, float("-inf")):
            continue
        trade=simulate_trade(event,c15,max_hours=max_hours)
        if trade is None:
            continue
        trades.append(trade)
        sampled_setups.add(setup)
        if trade["reason"] in ("STOP","TP2"):
            active_until[key]=float(trade["exit_ts"])
        else:
            active_until[key]=float(event["entry_ts"])+max_hours*3600
    return trades

def summarize(trades):
    if not trades:
        return {"n":0,"status":"INSUFFICIENT_DATA"}
    net=[float(x["r"]) for x in trades]
    gross=[float(x["gross_r"]) for x in trades]
    gains = sum(value for value in net if value > 0)
    losses = -sum(value for value in net if value < 0)
    equity = peak = 0.0
    max_drawdown = 0.0
    for trade in sorted(trades, key=lambda row: row.get("exit_ts", 0)):
        equity += trade["r"]
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "n":len(trades),
        "profit_factor_net": round(gains / losses, 3) if losses else None,
        "max_drawdown_r_net": round(max_drawdown, 3),
        "wins_net":sum(x>0 for x in net),
        "losses_net":sum(x<0 for x in net),
        "win_rate_pct_net":round(100*sum(x>0 for x in net)/len(net),2),
        "expectancy_r_net":round(statistics.fmean(net),3),
        "expectancy_r_gross":round(statistics.fmean(gross),3),
        "median_r_net":round(statistics.median(net),3),
        "tp1_touched":sum(x.get("tp1_hit") for x in trades),
        "tp2":sum(x.get("tp2_hit") for x in trades),
        "stops":sum(x.get("stop_hit") for x in trades),
        "time_exits":sum(x.get("reason")=="TIME" for x in trades),
        "modeled_round_trip_cost_pct":MODELED_ROUND_TRIP_COST_PCT,
    }


def backtest_symbol(symbol,days=DEFAULT_DAYS,now=None):
    now=time.time() if now is None else float(now)
    # OKX 1H OI exposes ~60 days (1440 points). Keep enough warm-up
    # for 1D trend while reserving 30 days for evaluation.
    warmup_days=30
    start_ms=int((now-(days+warmup_days)*86400)*1000)
    end_ms=int(now*1000)
    with requests.Session() as session:
        c15 = fetch_okx_klines(symbol, "15m", start_ms, end_ms, request_get=session.get)
        c1 = fetch_okx_klines(symbol, "1H", start_ms, end_ms, request_get=session.get)
        oi = fetch_okx_oi_1h(symbol, start_ms, end_ms, request_get=session.get)
    c15=[x for x in c15 if x["ts"]+900<=now]
    c1=[x for x in c1 if x["ts"]+3600<=now]

    cutoff=now-days*86400
    stages = {name: set() for name in ("WATCH", "BREAKOUT", "RETEST")}
    events=build_ready_entries(symbol,c15,c1,oi,stages=stages,cutoff=cutoff)
    events=[x for x in events if x["entry_ts"]>=cutoff]
    # Equal follow-up for every setup avoids counting recent fast exits while
    # their still-open peers are missing from the sample.
    last_complete_entry = now - MAX_HOURS * 3600
    trades = select_nonoverlapping_active_entries(
        [event for event in events if event["entry_ts"] <= last_complete_entry], c15, MAX_HOURS
    )
    evaluation_hours = {row["ts"] for row in c1 if row["ts"] >= cutoff}
    oi_hours = {ts for ts, _ in oi}
    return {"symbol":symbol,"days":days,
            "period_start_ts": cutoff, "period_end_ts": now,
            "oi_coverage": round(len(evaluation_hours & oi_hours) / max(1, len(evaluation_hours)), 3),
            "complete_followup_cutoff_ts": last_complete_entry,"candles15":len(c15),
            "candles1h":len(c1),"oi_points":len(oi),
            "stages": {**{name: len(samples) for name, samples in stages.items()},
                       "ENTRY": len({(row["direction"], row["setup_at"]) for row in events}),
                       "RESULT": len(trades)},
            "ready_candidates":len(events),"sampled_trades":len(trades),
            "stats":summarize(trades),"trades":trades,
            "_hourly_closes": {int(row["ts"] + 3600): row["c"] for row in c1}}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("symbols",nargs="*",default=list(DEFAULT_SYMBOLS))
    p.add_argument("--days",type=int,default=DEFAULT_DAYS)
    p.add_argument("--trades",action="store_true")
    args=p.parse_args()
    if not 15<=args.days<=30:
        p.error("days must be 15..30 for free OKX 1H OI history")

    reports=[]; combined=[]; now=time.time()
    hourly_closes = {}
    for raw in args.symbols:
        symbol=raw.upper()
        if not symbol.endswith("USDT"):
            symbol+="USDT"
        r=backtest_symbol(symbol,args.days,now=now)
        hourly_closes[symbol] = r.pop("_hourly_closes")
        combined.extend(r.get("trades") or [])
        if not args.trades:
            r=dict(r,trades=[])
        reports.append(r)

    regimes = {"BTC_UP": [], "BTC_DOWN": [], "BTC_FLAT": [], "BTC_UNAVAILABLE": [],
               "ALTS_STRONG": [], "ALTS_WEAK": [], "ALTS_MIXED": [], "ALTS_UNAVAILABLE": []}
    for trade in combined:
        hour = int(trade["decision_ts"] // 3600) * 3600
        returns = {symbol: closes[hour] / closes[hour - 86400] - 1
                   for symbol, closes in hourly_closes.items()
                   if hour in closes and hour - 86400 in closes}
        btc = returns.pop("BTCUSDT", None)
        btc_key = ("BTC_UNAVAILABLE" if btc is None else
                   "BTC_UP" if btc > 0.01 else "BTC_DOWN" if btc < -0.01 else "BTC_FLAT")
        regimes[btc_key].append(trade)
        # Exclude the traded instrument so it cannot classify its own context.
        returns.pop(trade["symbol"], None)
        median = statistics.median(returns.values()) if len(returns) >= 3 else None
        alt_key = ("ALTS_UNAVAILABLE" if median is None else
                   "ALTS_STRONG" if median > 0.01 else "ALTS_WEAK" if median < -0.01 else "ALTS_MIXED")
        regimes[alt_key].append(trade)

    print(json.dumps({
        "parameters":{"days":args.days,"symbols":[x.upper() for x in args.symbols]},
        "symbols":reports,
        "combined":summarize(combined),
        "by_market_regime": {key: summarize(rows) for key, rows in regimes.items()},
        "by_direction": {side: summarize([row for row in combined if row["direction"] == side])
                         for side in ("LONG", "SHORT")},
        "by_breakout_volume": {
            label: summarize([row for row in combined if predicate(row.get("breakout_volume_ratio"))])
            for label, predicate in (
                ("BELOW_BASELINE", lambda value: value is not None and value < 1),
                ("AT_OR_ABOVE_BASELINE", lambda value: value is not None and value >= 1),
                ("UNAVAILABLE", lambda value: value is None),
            )
        },
        "by_oi_at_entry": {
            label: summarize([row for row in combined if predicate(row.get("oi_1h_change_pct"))])
            for label, predicate in (
                ("FALLING", lambda value: value is not None and value < -0.15),
                ("FLAT", lambda value: value is not None and -0.15 <= value <= 0.15),
                ("RISING", lambda value: value is not None and value > 0.15),
            )
        },
        "by_directional_score": {
            f"{floor}–{floor + 9}" if floor < 100 else "100": summarize([
                row for row in combined
                if floor <= (row["score"] if row["direction"] == "LONG" else 100 - row["score"]) < floor + 10
            ])
            for floor in (70, 80, 90, 100)
        },
        "limitations":[
            "Historical replay excludes live-only Spot CVD, futures taker-flow and liquidations.",
            "OKX USDT-SWAP candles and contract OI history are used for the replay.",
            "Entry is next 15m open after a closed READY_ENTRY candle.",
            "If stop and target touch the same 15m candle, stop is assumed first.",
            "Stops that gap through the level fill at the worse open. Paths with missing bars before exit are excluded.",
            "TP1 is a milestone only; only STOP/TP2 are terminal before the 72h time exit.",
            "Results model 0.25% round-trip cost/slippage and report net R.",
            "Drawdown is measured on cumulative R in exit order, with each trade assigned one unit of risk.",
            "WATCH counts closed compression hours independently of directional setups.",
            "RESULT statistics require a full 72 hours of potential follow-up at the data cutoff.",
            "Regimes use the prior 24h return: BTC and the median of at least three other sampled alts, with thresholds of ±1%."
        ]
    },ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
