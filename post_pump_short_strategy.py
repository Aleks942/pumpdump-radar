"""Complete POST-PUMP SHORT decision pipeline inside existing PumpDump Radar.

Research/paper-only: no orders, no automatic READY/ELITE Telegram alerts.
Finds *all* eligible +30% OKX-USDT-SWAP tickers directly, including those that
already pulled back >15%; does not depend on early Pump Watch/cycle seeding.
Closed OKX OHLC 5m/15m/1H, sequential structural break and later retest,
real 5m spot taker flow with overlap-verified sampling, cached futures/OI,
and stale-data/anti-late risk gates. Bounded to 3 active market candidates,
no new service or paid API. No claim of validated profitability.
"""
import json
import math
import os
import re
import sqlite3
import time
from contextlib import closing

from post_pump_short_evidence import take_cached_evidence
from ticker_metrics import approx_quote_volume_usdt, spread_bps
from post_pump_spot_stream import (
    watch as watch_spot_stream,
    verified as verified_spot_stream,
)

DB_PATH=os.getenv("PUMP_WATCH_DB","/data/pump_watch.sqlite3")
MIN_RISE_PCT=30.0
MIN_NOTIONAL=10_000_000.0
MAX_SPREAD_BPS=40.0
MAX_ACTIVE=3
CONTEXT_INTERVAL_SEC=120
SPOT_POLL_INTERVAL_SEC=65
STALE_SYMBOL_SEC=24*3600
SPOT_WINDOW=300
MAX_CONTEXT_MISSING_SEC=10*60
MAX_SCANS_ROWS=12000
ROUND_TRIP_COST_PCT=0.22
RISK_FRACTION=0.005
MAX_LATE_ATR=1.0
WINDOWS=(5,15,30,60)
last_status_at=0.0


def num(value):
    try:
        n=float(value)
        return n if math.isfinite(n) else None
    except (TypeError,ValueError,OverflowError):
        return None


def safe_profile(t):
    if not isinstance(t,dict):
        return None
    inst=t.get("instId")
    if not isinstance(inst,str) or re.fullmatch(r"[A-Z0-9]{2,25}-USDT-SWAP",inst) is None:
        return None
    last,opening,high=map(num,(t.get("last"),t.get("open24h"),t.get("high24h")))
    vol,spread=approx_quote_volume_usdt(t),spread_bps(t)
    if (any(x is None or x<=0 for x in (last,opening,high,vol))
            or high<max(last,opening) or spread is None):
        return None
    rise=100*(high/opening-1)
    if rise<MIN_RISE_PCT:
        return None
    return dict(symbol=inst[:-10]+"USDT",inst=inst,last=last,
                high=high,opening=opening,rise=rise,notional=vol,
                spread=spread,retrace=100*(1-last/high),
                band="100_PLUS" if rise>=100 else
                     "50_TO_99" if rise>=50 else "30_TO_49",
                liquid=(vol>=MIN_NOTIONAL and spread<=MAX_SPREAD_BPS))


def setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_strategy(
        symbol TEXT PRIMARY KEY,
        episode_at REAL NOT NULL, last_seen_at REAL NOT NULL,
        phase TEXT NOT NULL, phase_at REAL NOT NULL,
        peak REAL NOT NULL, rise_max REAL NOT NULL,
        band TEXT NOT NULL, reference_open REAL NOT NULL,
        break_level REAL, break_candle_ts REAL,
        watch_reason TEXT, last_context_at REAL,
        last_spot_poll_at REAL, last_plan_at REAL,
        last_price REAL, last_retrace_pct REAL,
        last_evidence_json TEXT, last_structure_json TEXT
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_spot_trades(
        symbol TEXT NOT NULL, trade_id TEXT NOT NULL,
        ts REAL NOT NULL, side TEXT NOT NULL, quote REAL NOT NULL,
        PRIMARY KEY(symbol,trade_id)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_short_spot_ts
        ON post_pump_short_spot_trades(ts)""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_spot_coverage(
        symbol TEXT PRIMARY KEY, first_at REAL NOT NULL,
        last_at REAL NOT NULL, newest_trade_id TEXT NOT NULL,
        continuous INTEGER NOT NULL DEFAULT 0
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_strategy_audit(
        id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
        episode_at REAL NOT NULL, at REAL NOT NULL,
        phase TEXT NOT NULL, reason TEXT NOT NULL,
        rise_pct REAL NOT NULL, retrace_pct REAL NOT NULL,
        evidence_json TEXT, structure_json TEXT,
        entry REAL, stop REAL, tp1 REAL, tp2 REAL,
        risk_fraction REAL, quantity REAL,
        paper_only INTEGER NOT NULL DEFAULT 1,
        UNIQUE(symbol,episode_at,phase)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_strategy_outcomes(
        audit_id INTEGER NOT NULL, horizon_min INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        quote_at REAL, price REAL, model_net_short_pct REAL,
        PRIMARY KEY(audit_id,horizon_min)
    )""")


def parse_closed_candles(rows,seconds,now):
    """Strictly closed and monotonic OHLC, current forming candle excluded."""
    good=[]
    for row in rows or []:
        if not isinstance(row,dict) or row.get("confirmed") is not True:
            continue
        ts=num(row.get("ts"))
        o,h,l,c=(num(row.get(k)) for k in ("open","high","low","close"))
        if any(x is None or x<=0 for x in (ts,o,h,l,c)):
            continue
        ts/=1000 if ts>100000000000 else 1
        if ts+seconds>now+3 or not l<=min(o,c)<=max(o,c)<=h:
            continue
        good.append(dict(ts=ts,o=o,h=h,l=l,c=c))
    good.sort(key=lambda x:x["ts"])
    if len(good)<27 or any(
        b["ts"]-a["ts"]!=seconds for a,b in zip(good,good[1:])):
        return []
    if now-good[-1]["ts"]-seconds>seconds+60:
        return []
    return good


def ema(values,n):
    if len(values)<n:
        return None
    a=2/(n+1)
    v=sum(values[:n])/n
    for q in values[n:]:
        v=v+a*(q-v)
    return v


def swings(candles,kind):
    out=[]
    for i in range(2,len(candles)-2):
        v=candles[i][kind]
        neighbors=[candles[j][kind] for j in (i-2,i-1,i+1,i+2)]
        if (v<min(neighbors) if kind=="l" else v>max(neighbors)):
            out.append((candles[i]["ts"],v))
    return out


def analyze_structure(c5,c15,c60,now):
    """Closed-candle multi-timeframe evidence; no future or current bar."""
    a=parse_closed_candles(c5,300,now)
    b=parse_closed_candles(c15,900,now)
    c=parse_closed_candles(c60,3600,now)
    if not a or not b or not c:
        return dict(ready=False,reason="CANDLES_NOT_READY")
    e5=ema([x["c"] for x in a],20)
    e15=ema([x["c"] for x in b],20)
    e60=ema([x["c"] for x in c],20)
    e60prev=ema([x["c"] for x in c[:-1]],20)
    recent=a[-15:]
    true_ranges=[max(x["h"]-x["l"],abs(x["h"]-prev["c"]),abs(x["l"]-prev["c"]))
                 for prev,x in zip(a[-16:-1],a[-15:])]
    atr=sum(true_ranges)/len(true_ranges)
    lo=swings(a,"l")
    hi=swings(a,"h")
    support=lo[-1][1] if lo else None
    recent_hi=max(x["h"] for x in a[-6:-1])
    lower_highs=(len(hi)>=2 and hi[-1][1]<hi[-2][1])
    bearish5=(a[-1]["c"]<e5 and a[-1]["c"]<a[-3]["c"] and
              (lower_highs or a[-1]["h"]<max(x["h"] for x in a[-8:-2])))
    bearish15=(b[-1]["c"]<e15 and b[-1]["c"]<b[-3]["c"])
    # H1 may still be recovering from a pump; a strong upward H1 blocks READY.
    h1_down=(c[-1]["c"]<e60 and c[-1]["c"]<c[-2]["c"])
    h1_up=(c[-1]["c"]>e60 and e60>e60prev and c[-1]["c"]>=c[-2]["c"])
    h1_regime="DOWN" if h1_down else "UP" if h1_up else "MIXED"
    return dict(ready=True,reason="OK",support=support,
                broken=bool(support is not None and a[-1]["c"]<support*0.998),
                bearish5=bool(bearish5),bearish15=bool(bearish15),
                h1=h1_regime,atr=atr,recent_hi=recent_hi,
                last5=a[-1],last15=b[-1],last1h=c[-1],
                last5_close=a[-1]["c"],
                trend_shift=bool(bearish5 and bearish15 and h1_regime!="UP"))


def parse_spot_trades(data,symbol,now):
    """Trades are sorted separately. Do not confuse last 100 with 5m CVD."""
    if not isinstance(data,list):
        return []
    seen=set()
    out=[]
    for trade in data:
        if not isinstance(trade,dict):
            continue
        side=trade.get("side")
        ident=str(trade.get("tradeId") or "")
        ts=num(trade.get("ts"))
        px=num(trade.get("px"))
        size=num(trade.get("sz"))
        if (not ident or ident in seen or side not in ("buy","sell")
                or any(x is None or x<=0 for x in (ts,px,size))):
            continue
        ts=ts/1000 if ts>100000000000 else ts
        if not now-900<=ts<=now+3:
            continue
        seen.add(ident)
        out.append((ident,ts,side,px*size))
    return sorted(out,key=lambda row:row[1])


def store_spot_poll(db,symbol,now,trades):
    """Overlap between successive 100-trade pages proves no missed trade page."""
    sample=parse_spot_trades(trades,symbol,now)
    if not sample:
        return "NO_SPOT_DATA"
    previous=db.execute("""SELECT first_at,last_at,newest_trade_id,continuous
        FROM post_pump_short_spot_coverage WHERE symbol=?""",(symbol,)).fetchone()
    page_ids={x[0] for x in sample}
    overlap=bool(previous and previous["newest_trade_id"] in page_ids
                 and 0<now-previous["last_at"]<=180)
    if not overlap:
        db.execute("DELETE FROM post_pump_short_spot_trades WHERE symbol=?",(symbol,))
    for ident,ts,side,quote in sample:
        db.execute("""INSERT OR IGNORE INTO post_pump_short_spot_trades
            (symbol,trade_id,ts,side,quote) VALUES (?,?,?,?,?)""",
            (symbol,ident,ts,side,quote))
    newest=max(sample,key=lambda x:x[1])[0]
    beginning=previous["first_at"] if overlap else now
    db.execute("""INSERT INTO post_pump_short_spot_coverage(
        symbol,first_at,last_at,newest_trade_id,continuous)
        VALUES (?,?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET
        first_at=excluded.first_at,last_at=excluded.last_at,
        newest_trade_id=excluded.newest_trade_id,continuous=excluded.continuous""",
        (symbol,beginning,now,newest,1 if overlap else 0))
    db.execute("DELETE FROM post_pump_short_spot_trades WHERE symbol=? AND ts<?",
               (symbol,now-SPOT_WINDOW-45))
    return "OVERLAP" if overlap else "WARMING"


def spot_status(db,symbol,now):
    p=db.execute("""SELECT first_at,last_at,continuous
        FROM post_pump_short_spot_coverage WHERE symbol=?""",(symbol,)).fetchone()
    if not p or not p["continuous"] or now-p["last_at"]>150:
        return dict(ready=False,reason="SPOT_NOT_CONTINUOUS")
    if now-p["first_at"]<SPOT_WINDOW:
        return dict(ready=False,reason="SPOT_WARMING")
    rows=db.execute("""SELECT side,quote,ts FROM post_pump_short_spot_trades
        WHERE symbol=? AND ts>=?""",(symbol,now-SPOT_WINDOW)).fetchall()
    if len(rows)<15 or max((r["ts"] for r in rows),default=0)<now-120:
        return dict(ready=False,reason="SPOT_LOW_SAMPLE")
    buy=sum(r["quote"] for r in rows if r["side"]=="buy")
    sell=sum(r["quote"] for r in rows if r["side"]=="sell")
    total=buy+sell
    if total<1000:
        return dict(ready=False,reason="SPOT_LOW_NOTIONAL")
    return dict(ready=True,reason="OVERLAP_VERIFIED_5M",
                count=len(rows),imbalance_pct=round(100*(buy-sell)/total,4),
                delta_quote=round(buy-sell,4))


def _evidence_ok(evidence,spot):
    f=num(evidence.get("futures_taker_5m_imbalance_pct"))
    local=num(evidence.get("local_oi_5m_pct"))
    agg=num(evidence.get("agg_oi_5m_pct"))
    # Both futures and spot aggressive sellers; OI can expand (new short) or
    # contract (long liquidation), but at least one reliable OI is required.
    return (f is not None and f<=-10 and spot.get("ready") is True
            and num(spot.get("imbalance_pct")) is not None
            and spot["imbalance_pct"]<=-10
            and (local is not None or agg is not None))


def _plan(price,structure,level,retrace,spread,equity):
    atr=num(structure.get("atr"))
    high=num(structure.get("recent_hi"))
    if (atr is None or atr<=0 or high is None or level is None
            or price<=0 or spread>MAX_SPREAD_BPS):
        return None
    if price<level-atr*MAX_LATE_ATR:
        return None
    stop=max(high,level+0.35*atr)*(1+0.002)
    risk=stop-price
    if risk<=0 or risk/price>0.09 or risk/price<0.003:
        return None
    tp1=price-2*risk
    tp2=price-3*risk
    if tp1<=0:
        return None
    # Conservative bound on risk: <=0.5% equity; equity unknown => no qty.
    equity=num(equity)
    # Loss budget includes exit fees and a slippage allowance.
    # Conservative but still a model, not an executable order guarantee.
    fee_buffer=price*(ROUND_TRIP_COST_PCT+0.10)/100
    qty=(equity*RISK_FRACTION/(risk+fee_buffer)
         if equity is not None and equity>0 else None)
    return dict(entry=price,stop=stop,tp1=tp1,tp2=tp2,
                rr1=2.0,risk_fraction=RISK_FRACTION,quantity=qty,
                validity="PAPER_ONLY_NOT_BACKTESTED",fee_model_pct=ROUND_TRIP_COST_PCT)


def _record_phase(db,state,now,phase,reason,evidence=None,structure=None,plan=None):
    evidence=evidence or {}
    structure=structure or {}
    payload=json.dumps(evidence,default=str,sort_keys=True)
    structur=json.dumps(structure,default=str,sort_keys=True)
    new=phase!=state["phase"]
    if not new:
        return False
    old=state["phase"]
    state["phase"]=phase
    state["phase_at"]=now
    state["watch_reason"]=reason
    db.execute("""INSERT OR IGNORE INTO post_pump_short_strategy_audit(
        symbol,episode_at,at,phase,reason,rise_pct,retrace_pct,
        evidence_json,structure_json,entry,stop,tp1,tp2,
        risk_fraction,quantity)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (state["symbol"],state["episode_at"],now,phase,reason,
         state["rise_max"],state["last_retrace_pct"],payload,structur,
         plan["entry"] if plan else None,plan["stop"] if plan else None,
         plan["tp1"] if plan else None,plan["tp2"] if plan else None,
         plan["risk_fraction"] if plan else None,
         plan["quantity"] if plan else None))
    audit_id=db.execute("""SELECT id FROM post_pump_short_strategy_audit
        WHERE symbol=? AND episode_at=? AND phase=?""",
        (state["symbol"],state["episode_at"],phase)).fetchone()["id"]
    db.executemany("""INSERT OR IGNORE INTO post_pump_short_strategy_outcomes
        (audit_id,horizon_min) VALUES (?,?)""",
        [(audit_id,h) for h in WINDOWS])
    print(f"[SHORT_STRATEGY_STAGE] symbol={state['symbol']} "
          f"{old}->{phase} reason={reason} rise={state['rise_max']:.1f}% "
          f"retrace={state['last_retrace_pct']:.1f}% "
          "research_only=true no_orders=true",flush=True)
    return True


def _fetch_context(symbol):
    # Bounded to MAX_ACTIVE symbols per scan; all candles use the existing
    # OKX market data helper. No unconfirmed/unfinished bars used.
    from market_data_v3 import get_candles
    return {label:get_candles(symbol,label,limit=70)
            for label in ("5m","15m","1H")}


def _fetch_spot(symbol):
    # The existing get_spot_cvd() last-100 API has no 5m coverage guarantee.
    # Retrieve trade IDs to verify pagination continuity across polls instead.
    import requests
    raw=symbol[:-4]+"-USDT"
    r=requests.get("https://www.okx.com/api/v5/market/trades",
                   params={"instId":raw,"limit":"100"},timeout=8)
    r.raise_for_status()
    data=r.json()
    return data.get("data") if data.get("code")=="0" else None


def _advance(db,st,profile,now,context,spot_reader,evidence_reader,equity):
    sym=st["symbol"]
    ws_spot=(verified_spot_stream(sym) if spot_reader is _fetch_spot
             else {"ready":False,"reason":"INJECTED_SPOT_SOURCE"})
    st["last_seen_at"]=now
    st["last_price"]=profile["last"]
    st["rise_max"]=max(st["rise_max"],profile["rise"])
    st["peak"]=max(st["peak"],profile["high"])
    st["band"]=profile["band"]
    st["last_retrace_pct"]=100*(1-profile["last"]/st["peak"])
    if not ws_spot.get("ready") and now-st["last_spot_poll_at"]>=SPOT_POLL_INTERVAL_SEC:
        try:
            trades=spot_reader(sym)
            status=store_spot_poll(db,sym,now,trades)
        except Exception as exc:
            status=type(exc).__name__
        st["last_spot_poll_at"]=now
    else:
        status="WAIT_POLL"
    spot=ws_spot if ws_spot.get("ready") else spot_status(db,sym,now)
    if now-st["last_context_at"]<CONTEXT_INTERVAL_SEC:
        return status,None
    st["last_context_at"]=now
    try:
        c=context(sym)
        struct=analyze_structure(c.get("5m"),c.get("15m"),c.get("1H"),now)
    except Exception as exc:
        struct=dict(ready=False,reason=type(exc).__name__)
    try:
        ev=evidence_reader(sym,now) or {}
    except Exception as exc:
        ev=dict(evidence_error=type(exc).__name__)
    ev["verified_spot"]=spot
    st["last_evidence_json"]=json.dumps(ev,sort_keys=True,default=str)
    st["last_structure_json"]=json.dumps(struct,sort_keys=True,default=str)
    if not struct.get("ready"):
        return status,struct
    if (profile["last"]>=st["peak"]*1.001 and st["phase"]!="WATCH"):
        st["break_level"]=None
        st["break_candle_ts"]=None
        _record_phase(db,st,now,"WATCH","BUYERS_RECLAIMED_NEW_HIGH",ev,struct)
    if st["last_retrace_pct"]>=2.5 and st["phase"]=="WATCH":
        if struct["bearish5"]:
            _record_phase(db,st,now,"WEAKENING","RETRACE_AND_5M_LOWER_PRICE",ev,struct)
    # A reversal stage requires a NEW closed candle after weakening, not
    # two labels from the same market snapshot.
    if (st["phase"]=="WEAKENING" and now-st["phase_at"]>=CONTEXT_INTERVAL_SEC
            and struct["trend_shift"] and struct["broken"]
            and struct["last5"]["ts"]+300>st["phase_at"]):
        st["break_level"]=struct["support"]
        st["break_candle_ts"]=struct["last5"]["ts"]
        _record_phase(db,st,now,"TREND_SHIFT","CLOSED_5M_BREAK_15M_H1_ALIGNED",ev,struct)
    if st["phase"]=="TREND_SHIFT" and now-st["phase_at"]>=CONTEXT_INTERVAL_SEC:
        level=st["break_level"]
        last5=struct["last5"]
        if (level is not None and st["break_candle_ts"] is not None
                and last5["ts"]>st["break_candle_ts"]):
            if last5["c"]>level*1.012:
                st["break_level"]=None
                st["break_candle_ts"]=None
                _record_phase(db,st,now,"WEAKENING","BREAK_INVALIDATED",ev,struct)
            elif (last5["h"]>=level*0.997 and last5["c"]<level
                  and struct["trend_shift"]):
                _record_phase(db,st,now,"RETEST","CLOSED_5M_RETEST_REJECTED",ev,struct)
    if st["phase"]=="RETEST" and now-st["phase_at"]>=CONTEXT_INTERVAL_SEC:
        if not struct["trend_shift"] or struct["h1"]=="UP":
            _record_phase(db,st,now,"WEAKENING","TREND_NOT_ALIGNED",ev,struct)
        elif not _evidence_ok(ev,spot):
            # Remain research watch while waiting for validated spot and OI.
            st["watch_reason"]="WAITING_FOR_INDEPENDENT_FLOW_OI"
        else:
            plan=_plan(profile["last"],struct,st["break_level"],
                       st["last_retrace_pct"],profile["spread"],equity)
            if plan is not None:
                if _record_phase(db,st,now,"READY_RESEARCH",
                                 "STRUCTURE_RETEST_SPOT_FUTURES_OI",ev,struct,plan):
                    st["last_plan_at"]=now
                    print(f"[SHORT_PLAN_PAPER] {sym} "
                          f"entry={plan['entry']:.8g} stop={plan['stop']:.8g} "
                          f"tp1={plan['tp1']:.8g} tp2={plan['tp2']:.8g} "
                          f"qty={plan['quantity']} risk={100*RISK_FRACTION:.2f}% "
                          "NO_LIVE_ORDER=true NOT_PROVEN=true",flush=True)
    return status,struct


def observe(tickers,now=None,db_path=None,fetch_context=None,
            fetch_spot=None,evidence_reader=None,equity_usdt=None):
    """One bounded live scan, direct discovery and entirely paper decisions."""
    now=time.time() if now is None else float(now)
    path=DB_PATH if db_path is None else db_path
    context=_fetch_context if fetch_context is None else fetch_context
    spot_fetch=_fetch_spot if fetch_spot is None else fetch_spot
    evidence=take_cached_evidence if evidence_reader is None else evidence_reader
    equity_usdt=equity_usdt if equity_usdt is not None else os.getenv("POST_PUMP_EQUITY_USDT")
    quotes={}
    eligible={}
    diagnostics=dict(tickers=0,over30=0,liquid=0,active=0,
                     context_checks=0,new=0,outcomes=0)
    for ticker in tickers or ():
        if not isinstance(ticker,dict):
            continue
        inst=ticker.get("instId","")
        last=num(ticker.get("last"))
        if isinstance(inst,str) and inst.endswith("-USDT-SWAP") and last and last>0:
            quotes[inst[:-10]+"USDT"]=last
        diagnostics["tickers"]+=1
        item=safe_profile(ticker)
        if not item:
            continue
        diagnostics["over30"]+=1
        if item["liquid"]:
            diagnostics["liquid"]+=1
            eligible[item["symbol"]]=item
    try:
        os.makedirs(os.path.dirname(path) or ".",exist_ok=True)
        with closing(sqlite3.connect(path,timeout=7)) as db:
            db.row_factory=sqlite3.Row
            db.execute("PRAGMA busy_timeout=7000")
            setup(db)
            # All detected +30% symbols enter watch regardless of earlier pullback.
            for symbol,item in eligible.items():
                old=db.execute("""SELECT * FROM post_pump_short_strategy
                                  WHERE symbol=?""",(symbol,)).fetchone()
                if old is None or now-old["last_seen_at"]>STALE_SYMBOL_SEC:
                    if old is not None:
                        db.execute("DELETE FROM post_pump_short_strategy WHERE symbol=?",
                                   (symbol,))
                    st=dict(symbol=symbol,episode_at=now,last_seen_at=now,
                            phase="WATCH",phase_at=now,peak=item["high"],
                            rise_max=item["rise"],band=item["band"],
                            reference_open=item["opening"],
                            break_level=None,break_candle_ts=None,
                            watch_reason="PUMP_30_PERCENT_DETECTED",
                            last_context_at=0,last_spot_poll_at=0,
                            last_plan_at=None,last_price=item["last"],
                            last_retrace_pct=item["retrace"],
                            last_evidence_json=None,last_structure_json=None)
                    db.execute("""INSERT INTO post_pump_short_strategy(
                        symbol,episode_at,last_seen_at,phase,phase_at,
                        peak,rise_max,band,reference_open,break_level,
                        break_candle_ts,watch_reason,last_context_at,
                        last_spot_poll_at,last_plan_at,last_price,
                        last_retrace_pct,last_evidence_json,last_structure_json)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        tuple(st.values()))
                    _record_phase(db,st,now,"DISCOVERED","OBSERVED_RISE_30PCT")
                    st["phase"]="WATCH"
                    diagnostics["new"]+=1
                else:
                    # Keep state even if the ticker rose in a different band.
                    db.execute("""UPDATE post_pump_short_strategy SET
                        last_seen_at=?,peak=MAX(peak,?),rise_max=MAX(rise_max,?),
                        band=?,last_price=?,last_retrace_pct=? WHERE symbol=?""",
                        (now,item["high"],item["rise"],item["band"],
                         item["last"],item["retrace"],symbol))
            # Evaluate existing + fresh assets even after a 24h window rolled.
            candidates=db.execute("""SELECT * FROM post_pump_short_strategy
                WHERE last_seen_at>=? AND phase NOT IN ('READY_RESEARCH','EXPIRED')
                ORDER BY CASE phase WHEN 'RETEST' THEN 0
                         WHEN 'TREND_SHIFT' THEN 1 WHEN 'WEAKENING' THEN 2
                         ELSE 3 END, last_context_at ASC LIMIT 120""",
                (now-STALE_SYMBOL_SEC,)).fetchall()
            if fetch_spot is None:
                selected=watch_spot_stream([z["symbol"] for z in candidates])
                rows=[z for z in candidates
                      if z["symbol"][:-4]+"-USDT" in selected]
            else:
                rows=candidates[:MAX_ACTIVE]  # deterministic offline tests
            diagnostics["active"]=len(rows)
            for row in rows:
                st=dict(row)
                symbol=st["symbol"]
                profile=eligible.get(symbol)
                if profile is None:
                    price=quotes.get(symbol)
                    if not price or not st["peak"]:
                        continue
                    profile=dict(symbol=symbol,inst=symbol[:-4]+"-USDT-SWAP",
                                 last=price,high=st["peak"],
                                 opening=st["reference_open"],
                                 rise=st["rise_max"],band=st["band"],
                                 spread=0,notional=0,liquid=False)
                status,structure=_advance(db,st,profile,now,context,spot_fetch,
                                           evidence,equity_usdt)
                if structure is not None:
                    diagnostics["context_checks"]+=1
                db.execute("""UPDATE post_pump_short_strategy SET
                    last_seen_at=?,phase=?,phase_at=?,peak=?,rise_max=?,band=?,
                    break_level=?,break_candle_ts=?,watch_reason=?,
                    last_context_at=?,last_spot_poll_at=?,last_plan_at=?,
                    last_price=?,last_retrace_pct=?,
                    last_evidence_json=?,last_structure_json=? WHERE symbol=?""",
                    (st["last_seen_at"],st["phase"],st["phase_at"],
                     st["peak"],st["rise_max"],st["band"],
                     st["break_level"],st["break_candle_ts"],
                     st["watch_reason"],st["last_context_at"],
                     st["last_spot_poll_at"],st["last_plan_at"],
                     st["last_price"],st["last_retrace_pct"],
                     st["last_evidence_json"],st["last_structure_json"],symbol))
            # Forward quote markouts; not TP/SL first-touch execution.
            due=db.execute("""SELECT o.audit_id,o.horizon_min,
                a.symbol,a.at,a.entry FROM post_pump_short_strategy_outcomes o
                JOIN post_pump_short_strategy_audit a ON a.id=o.audit_id
                WHERE o.status='PENDING' AND a.at<=? LIMIT 2000""",(now,)).fetchall()
            for row in due:
                lag=now-row["at"]-60*row["horizon_min"]
                if lag<0:
                    continue
                quote=quotes.get(row["symbol"])
                if lag>300 or quote is None:
                    db.execute("""UPDATE post_pump_short_strategy_outcomes
                        SET status='MISSING' WHERE audit_id=? AND horizon_min=?""",
                        (row["audit_id"],row["horizon_min"]))
                else:
                    # The reference quote is available from event's original
                    # stage timestamp, never retroactively replaced.
                    ref=db.execute("""SELECT last_price FROM
                        post_pump_short_strategy WHERE symbol=?""",
                        (row["symbol"],)).fetchone()
                    # Audit tables are descriptive only if no entry quote saved.
                    if row["entry"] is None:
                        db.execute("""UPDATE post_pump_short_strategy_outcomes
                          SET status='NO_ENTRY' WHERE audit_id=? AND horizon_min=?""",
                          (row["audit_id"],row["horizon_min"]))
                    else:
                        net=100*(row["entry"]-quote)/row["entry"]-ROUND_TRIP_COST_PCT
                        db.execute("""UPDATE post_pump_short_strategy_outcomes SET
                            status='DONE',quote_at=?,price=?,model_net_short_pct=?
                            WHERE audit_id=? AND horizon_min=?""",
                            (now,quote,round(net,6),row["audit_id"],row["horizon_min"]))
                        diagnostics["outcomes"]+=1
            cutoff=now-30*86400
            db.execute("DELETE FROM post_pump_short_spot_trades WHERE ts<?",(now-3600,))
            db.execute("DELETE FROM post_pump_short_spot_coverage WHERE last_at<?",
                       (now-STALE_SYMBOL_SEC,))
            db.execute("DELETE FROM post_pump_short_strategy WHERE last_seen_at<?",
                       (now-STALE_SYMBOL_SEC,))
            db.execute("""DELETE FROM post_pump_short_strategy_outcomes WHERE audit_id IN(
               SELECT id FROM post_pump_short_strategy_audit WHERE at<?)""",(cutoff,))
            db.execute("DELETE FROM post_pump_short_strategy_audit WHERE at<?",(cutoff,))
            db.commit()
    except (sqlite3.Error,ValueError,TypeError,OverflowError) as exc:
        print("[SHORT_STRATEGY_ERROR]",type(exc).__name__,str(exc)[:170],flush=True)
        return diagnostics
    global last_status_at
    if now-last_status_at>=3600:
        last_status_at=now
        print("[SHORT_STRATEGY_FUNNEL]",diagnostics,
              "30pct_uses_24h_high_open=true paper_only=true",flush=True)
    return diagnostics
