"""Complete POST-PUMP SHORT decision pipeline inside existing PumpDump Radar.

Research/paper-only: no orders. Telegram lifecycle uses a separate ACK outbox.
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
from post_pump_short_flow import dispatch as dispatch_post_pump_flow
from post_pump_short_paper import (
    setup as setup_paper_audit,
    register as register_paper_plan,
    advance as advance_paper_audit,
    report as paper_audit_report,
    prune as prune_paper_audit,
)
from post_pump_context_v2 import enrich as enrich_research_evidence
from post_pump_context_v2 import maybe_report_cohorts
from ticker_metrics import approx_quote_volume_usdt, spread_bps
from post_pump_early_weakening import is_early_weakening
from post_pump_spot_stream import (
    watch as watch_spot_stream,
    verified as verified_spot_stream,
    snapshot_fallback as spot_snapshot_fallback,
)

DB_PATH=os.getenv("PUMP_WATCH_DB","/data/pump_watch.sqlite3")
MIN_RISE_PCT=30.0
MIN_NOTIONAL=10_000_000.0
MAX_SPREAD_BPS=40.0
MAX_ACTIVE=3
PRIORITY_RETRACE_PCT=5.0
CONTEXT_INTERVAL_SEC=120
HOT_CONTEXT_INTERVAL_SEC=60
MAX_EPISODE_SEC=72*3600
ENTRY_ZONE_BELOW_ATR=0.70
ENTRY_ZONE_ABOVE_ATR=0.15
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


def _watch_bybit_liquidations(symbol):
    """Lazily attach a pump candidate to the existing Bybit liquidation stream."""
    from liquidation_engine import watch_bybit_symbol
    return watch_bybit_symbol(symbol)


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



def live_market_profile(t):
    """Current quote/liquidity for an already-observed pump episode.

    Unlike safe_profile(), this does not require the rolling 24h move to remain
    above +30%. Once an episode has been discovered, a later pullback must keep
    using the real ticker volume/spread instead of an artificial illiquid
    placeholder. Invalid/missing market fields still fail closed.
    """
    if not isinstance(t,dict):
        return None
    inst=t.get("instId")
    if not isinstance(inst,str) or re.fullmatch(r"[A-Z0-9]{2,25}-USDT-SWAP",inst) is None:
        return None
    last=num(t.get("last"))
    high=num(t.get("high24h"))
    opening=num(t.get("open24h"))
    vol=approx_quote_volume_usdt(t)
    spread=spread_bps(t)
    if (last is None or last<=0 or vol is None or vol<=0
            or spread is None or spread<0):
        return None
    rise=(100*(high/opening-1)
          if high is not None and opening is not None
          and high>0 and opening>0 else None)
    return dict(symbol=inst[:-10]+"USDT",inst=inst,last=last,
                high=high,opening=opening,rise=rise,
                notional=vol,spread=spread,
                liquid=(vol>=MIN_NOTIONAL and spread<=MAX_SPREAD_BPS),
                liquidity_source="LIVE_TICKER")


_PHASE_PRIORITY = {
    "ENTRY_ZONE": 0,
    "READY_RESEARCH": 0,  # legacy rows from older deployments
    "RETEST": 1,
    "SHORT_CONFIRMED": 2,
    "TREND_SHIFT": 3,
    "WEAKENING": 4,
    "EARLY_WEAKENING": 5,
    "WATCH": 6,
}


def _candidate_priority_snapshot(row, eligible, live_profiles):
    """Rank active pumps using the freshest quote without creating new data.

    Mature structural phases stay first. Within the same phase, a pump that has
    already retraced >=5% gets priority so a fresh +30%/+50% pump cannot be
    starved behind three quiet WATCH symbols.
    """
    symbol = row["symbol"]
    phase = str(row["phase"] or "WATCH")
    peak = num(row["peak"]) or 0.0
    rise = num(row["rise_max"]) or 0.0
    price = num(row["last_price"])

    fresh = eligible.get(symbol)
    if fresh is not None:
        peak = max(peak, num(fresh.get("high")) or 0.0)
        rise = max(rise, num(fresh.get("rise")) or 0.0)
        price = num(fresh.get("last")) or price
    else:
        live = live_profiles.get(symbol)
        if live is not None:
            price = num(live.get("last")) or price

    retrace = max(0.0, 100.0 * (1.0 - price / peak)) if price and peak > 0 else 0.0
    urgent = (
        phase in ("ENTRY_ZONE", "READY_RESEARCH", "RETEST",
                  "SHORT_CONFIRMED", "TREND_SHIFT",
                  "WEAKENING", "EARLY_WEAKENING")
        or retrace >= PRIORITY_RETRACE_PCT
    )
    return {
        "phase_rank": _PHASE_PRIORITY.get(phase, 9),
        "retrace": retrace,
        "rise": rise,
        "urgent": urgent,
    }


def _candidate_sort_key(row, eligible, live_profiles):
    p = _candidate_priority_snapshot(row, eligible, live_profiles)
    last_context = num(row["last_context_at"]) or 0.0
    # A newly discovered hot pump must receive one deep-context pass even when
    # all three expensive slots are occupied. After that first pass, normal
    # lifecycle priority resumes so mature RETEST/CONFIRMED setups stay first.
    first_deep_pass = (
        last_context <= 0.0
        and p["urgent"]
        and p["retrace"] >= PRIORITY_RETRACE_PCT
    )
    return (
        0 if first_deep_pass else 1,
        p["phase_rank"],
        0 if p["urgent"] else 1,
        -p["retrace"],
        -p["rise"],
        last_context,
    )


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
        last_evidence_json TEXT, last_structure_json TEXT,
        peak_at REAL, entry_zone_at REAL
    )""")
    cols={row[1] for row in db.execute(
        "PRAGMA table_info(post_pump_short_strategy)")}
    if "peak_at" not in cols:
        db.execute("ALTER TABLE post_pump_short_strategy ADD COLUMN peak_at REAL")
    if "entry_zone_at" not in cols:
        db.execute("ALTER TABLE post_pump_short_strategy ADD COLUMN entry_zone_at REAL")
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
        reference_price REAL NOT NULL,
        entry REAL, stop REAL, tp1 REAL, tp2 REAL,
        risk_fraction REAL, quantity REAL,
        paper_only INTEGER NOT NULL DEFAULT 1,
        UNIQUE(symbol,episode_at,phase,at)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_short_strategy_outcomes(
        audit_id INTEGER NOT NULL, horizon_min INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        quote_at REAL, price REAL, model_net_short_pct REAL,
        PRIMARY KEY(audit_id,horizon_min)
    )""")
    setup_paper_audit(db)


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
    # ENTRY ZONE is deliberately tighter than the old anti-late bound.
    # Do not chase a candle that has already fallen far below the retested level.
    zone_low=max(0.0,level-atr*ENTRY_ZONE_BELOW_ATR)
    zone_high=level+atr*ENTRY_ZONE_ABOVE_ATR
    if price<zone_low or price>zone_high:
        return None
    stop=max(high,level+0.35*atr)*(1+0.002)
    risk=stop-price
    if risk<=0 or risk/price>0.09 or risk/price<0.003:
        return None
    tp1=price-2*risk
    tp2=price-3*risk
    if tp1<=0 or tp2<=0:
        return None
    # Conservative bound on risk: <=0.5% equity; equity unknown => no qty.
    equity=num(equity)
    # Loss budget includes exit fees and a slippage allowance.
    # Conservative but still a model, not an executable order guarantee.
    fee_buffer=price*(ROUND_TRIP_COST_PCT+0.10)/100
    qty=(equity*RISK_FRACTION/(risk+fee_buffer)
         if equity is not None and equity>0 else None)
    return dict(entry=price,stop=stop,tp1=tp1,tp2=tp2,
                zone_low=zone_low,zone_high=zone_high,break_level=level,
                distance_from_break_pct=100*(price/level-1),
                rr1=2.0,risk_fraction=RISK_FRACTION,quantity=qty,
                validity="DECISION_SUPPORT_NOT_BACKTESTED",
                fee_model_pct=ROUND_TRIP_COST_PCT)


def _record_phase(db,state,now,phase,reason,evidence=None,structure=None,plan=None):
    evidence=dict(evidence or {})
    structure=structure or {}
    peak_at=num(state.get("peak_at")) or num(state.get("episode_at"))
    lifecycle=dict(evidence.get("lifecycle") or {})
    lifecycle.update({
        "peak":num(state.get("peak")),
        "peak_at":peak_at,
        "peak_age_sec":max(0.0,now-peak_at) if peak_at is not None else None,
        "retrace_pct":num(state.get("last_retrace_pct")),
        "break_level":num(state.get("break_level")),
    })
    evidence["lifecycle"]=lifecycle
    if plan is not None:
        evidence["entry_zone"]={
            "zone_low":plan.get("zone_low"),
            "zone_high":plan.get("zone_high"),
            "entry":plan.get("entry"),
            "stop":plan.get("stop"),
            "tp1":plan.get("tp1"),
            "tp2":plan.get("tp2"),
            "break_level":plan.get("break_level"),
            "distance_from_break_pct":plan.get("distance_from_break_pct"),
        }
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
        evidence_json,structure_json,reference_price,entry,stop,tp1,tp2,
        risk_fraction,quantity)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (state["symbol"],state["episode_at"],now,phase,reason,
         state["rise_max"],state["last_retrace_pct"],payload,structur,
         state["last_price"],
         plan["entry"] if plan else None,plan["stop"] if plan else None,
         plan["tp1"] if plan else None,plan["tp2"] if plan else None,
         plan["risk_fraction"] if plan else None,
         plan["quantity"] if plan else None))
    audit_id=db.execute("""SELECT id FROM post_pump_short_strategy_audit
        WHERE symbol=? AND episode_at=? AND phase=? AND at=?""",
        (state["symbol"],state["episode_at"],phase,now)).fetchone()["id"]
    db.executemany("""INSERT OR IGNORE INTO post_pump_short_strategy_outcomes
        (audit_id,horizon_min) VALUES (?,?)""",
        [(audit_id,h) for h in WINDOWS])
    if phase in ("ENTRY_ZONE","READY_RESEARCH") and plan is not None:
        register_paper_plan(db,audit_id,state["symbol"],now,
                            plan["entry"],plan["stop"],plan["tp1"])
    print(f"[SHORT_STRATEGY_STAGE] symbol={state['symbol']} "
          f"{old}->{phase} reason={reason} rise={state['rise_max']:.1f}% "
          f"retrace={state['last_retrace_pct']:.1f}% "
          "research_only=true no_orders=true",flush=True)
    return True


def _context_interval(phase):
    return (HOT_CONTEXT_INTERVAL_SEC
            if phase in ("EARLY_WEAKENING","WEAKENING","TREND_SHIFT",
                         "SHORT_CONFIRMED","RETEST")
            else CONTEXT_INTERVAL_SEC)


def _refresh_passive_state(db,row,profile,now):
    """Cheap persistent peak/retrace memory for candidates outside 3 deep slots."""
    if profile is None:
        return False
    st=dict(row)
    price=num(profile.get("last"))
    if price is None or price<=0:
        return False
    old_peak=num(st.get("peak")) or price
    observed_high=num(profile.get("high"))
    new_peak=(observed_high is not None and observed_high>old_peak*1.001)
    if new_peak:
        st["peak"]=observed_high
        st["peak_at"]=now
        opening=num(profile.get("opening")) or num(st.get("reference_open"))
        if opening and opening>0:
            st["rise_max"]=max(num(st.get("rise_max")) or 0.0,
                               100*(observed_high/opening-1))
    elif num(st.get("peak_at")) is None:
        st["peak_at"]=num(st.get("episode_at")) or now
    st["last_seen_at"]=now
    st["last_price"]=price
    st["last_retrace_pct"]=100*(1-price/st["peak"])
    if new_peak and st["phase"]!="WATCH":
        st["break_level"]=None
        st["break_candle_ts"]=None
        _record_phase(db,st,now,"WATCH","BUYERS_RECLAIMED_NEW_HIGH",
                      {"passive_lifecycle":True})
        st["entry_zone_at"]=None
    db.execute("""UPDATE post_pump_short_strategy SET
        last_seen_at=?,phase=?,phase_at=?,peak=?,peak_at=?,rise_max=?,
        break_level=?,break_candle_ts=?,watch_reason=?,
        last_price=?,last_retrace_pct=?,entry_zone_at=? WHERE symbol=?""",
        (st["last_seen_at"],st["phase"],st["phase_at"],st["peak"],
         st["peak_at"],st["rise_max"],st["break_level"],
         st["break_candle_ts"],st.get("watch_reason"),st["last_price"],
         st["last_retrace_pct"],st.get("entry_zone_at"),st["symbol"]))
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


def _advance(db,st,profile,now,context,spot_reader,evidence_reader,equity,db_path=None):
    sym=st["symbol"]
    ws_spot=(verified_spot_stream(sym) if spot_reader is _fetch_spot
             else {"ready":False,"reason":"INJECTED_SPOT_SOURCE"})
    # Snapshot the preceding market observation before overwriting it.
    # Comparing a rolling 24h peak alone would give false early warnings.
    previous_price=num(st.get("last_price"))
    try:
        previous_structure=json.loads(st.get("last_structure_json") or "{}")
    except (TypeError,ValueError):
        previous_structure={}
    st["last_seen_at"]=now
    st["last_price"]=profile["last"]
    previous_peak=st["peak"]
    observed_high=num(profile.get("high")) or previous_peak
    new_peak=observed_high>previous_peak*1.001
    if new_peak:
        st["peak_at"]=now
    elif num(st.get("peak_at")) is None:
        st["peak_at"]=num(st.get("episode_at")) or now
    rise_now=num(profile.get("rise"))
    if rise_now is not None:
        st["rise_max"]=max(st["rise_max"],rise_now)
    st["peak"]=max(st["peak"],observed_high)
    st["band"]=profile["band"]
    st["last_retrace_pct"]=100*(1-profile["last"]/st["peak"])
    # A published paper plan must keep a live cancellation path. This cheap
    # check also runs while candle/flow collectors are warming or unavailable.
    if st["phase"] in ("ENTRY_ZONE","READY_RESEARCH"):
        plan_row = db.execute("""SELECT stop FROM post_pump_short_strategy_audit
            WHERE symbol=? AND episode_at=?
              AND phase IN ('ENTRY_ZONE','READY_RESEARCH')
            ORDER BY id DESC LIMIT 1""", (sym, st["episode_at"])).fetchone()
        stop = num(plan_row["stop"]) if plan_row else None
        level = num(st.get("break_level"))
        reason = None
        if stop is not None and profile["last"] >= stop:
            reason = "PAPER_STOP_REACHED"
        elif level is not None and profile["last"] > level * 1.012:
            reason = "BREAK_INVALIDATED"
        elif now - st["phase_at"] >= 3600:
            reason = "PAPER_PLAN_EXPIRED"
        if reason:
            _record_phase(db,st,now,"CANCELLED",reason)
            return "CANCELLED",None
    if not ws_spot.get("ready") and now-st["last_spot_poll_at"]>=SPOT_POLL_INTERVAL_SEC:
        try:
            trades=spot_reader(sym)
            status=store_spot_poll(db,sym,now,trades)
        except Exception as exc:
            status=type(exc).__name__
        st["last_spot_poll_at"]=now
    else:
        status="WAIT_POLL"
    if ws_spot.get("ready"):
        spot=ws_spot
    else:
        overlap_spot=spot_status(db,sym,now)
        snapshot=(spot_snapshot_fallback(sym) if spot_reader is _fetch_spot
                  else {"ready":False,"reason":"INJECTED_SPOT_SOURCE"})
        spot=(overlap_spot if overlap_spot.get("ready")
              else snapshot if snapshot.get("ready")
              else overlap_spot)
    if now-st["last_context_at"]<_context_interval(st["phase"]):
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
    # Existing liquidation stream cache; no new REST or WebSocket.
    # Incomplete coverage is reported as partial, never "zero liquidation".
    try:
        from liquidation_engine import get_liquidation_summary
        liq=get_liquidation_summary(sym) or {}
        ev["liquidations"]={
            "quality":liq.get("quality","UNAVAILABLE"),
            "available":liq.get("available") is True,
            "complete":liq.get("complete") is True,
            "event_count":liq.get("event_count"),
            "sources":liq.get("sources", {}),
            "exchanges":liq.get("exchanges"),
            "observed_at":now,
            "long_usdt":liq.get("long_liq") if liq.get("available") else None,
            "short_usdt":liq.get("short_liq") if liq.get("available") else None,
        }
    except Exception as exc:
        ev["liquidations"]={"available":False,
                            "quality":type(exc).__name__}
    # Enrich the immutable research snapshot without changing the existing
    # READY_RESEARCH gate. These features are measured first, promoted only
    # after cohort evidence proves they add value.
    ev=enrich_research_evidence(
        sym,now,ev,spot,struct,liquidations=ev.get("liquidations"),
        db_path=db_path,episode_at=st["episode_at"])
    st["last_evidence_json"]=json.dumps(ev,sort_keys=True,default=str)
    st["last_structure_json"]=json.dumps(struct,sort_keys=True,default=str)
    if not struct.get("ready"):
        return status,struct
    if st["phase"] in ("ENTRY_ZONE","READY_RESEARCH"):
        if not struct["trend_shift"] or struct["h1"] == "UP":
            _record_phase(db,st,now,"CANCELLED","TREND_NOT_ALIGNED",ev,struct)
        return status,struct
    if (new_peak and st["phase"]!="WATCH"):
        st["break_level"]=None
        st["break_candle_ts"]=None
        _record_phase(db,st,now,"WATCH","BUYERS_RECLAIMED_NEW_HIGH",ev,struct)
    # EARLY_WEAKENING is informational only. Demand a fresh closed 5m bar
    # and a lower observed quote, not merely a stale rolling 24h high.
    if (st["phase"]=="EARLY_WEAKENING" and previous_price is not None
            and profile["last"]>=previous_price*1.005
            and not struct["bearish5"]):
        _record_phase(db,st,now,"WATCH","BUYERS_RECOVERED_EARLY_WEAKENING",ev,struct)
    if (st["phase"]=="WATCH" and is_early_weakening(
            previous_price,profile["last"],st["peak"],
            previous_structure,struct)):
        _record_phase(db,st,now,"EARLY_WEAKENING",
                      "LOWER_LIVE_QUOTE_AND_FRESH_CLOSED_5M",ev,struct)
    if st["last_retrace_pct"]>=2.5 and st["phase"] in ("WATCH","EARLY_WEAKENING"):
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
    promoted_confirmed=False
    # Seller takeover is useful context, but it is not mandatory here.
    # SHORT_CONFIRMED still requires bearish Spot + Futures, reliable OI,
    # aligned closed-candle structure, and a locally confirmed seller response.
    strict_short=(
        _evidence_ok(ev, spot)
        and (ev.get("order_flow") or {}).get("status")=="CONFIRMED"
        and (ev.get("order_flow") or {}).get("ready") is True
    )
    if (st["phase"]=="TREND_SHIFT"
            and now-st["phase_at"]>=HOT_CONTEXT_INTERVAL_SEC
            and strict_short):
        promoted_confirmed=_record_phase(
            db,st,now,"SHORT_CONFIRMED",
            "SPOT_FUTURES_OI_STRUCTURE_SELLERS_CONFIRMED_WAIT_RETEST",ev,struct)
    if (not promoted_confirmed
            and st["phase"] in ("TREND_SHIFT","SHORT_CONFIRMED")
            and now-st["phase_at"]>=HOT_CONTEXT_INTERVAL_SEC):
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
    if st["phase"]=="RETEST" and now-st["phase_at"]>=HOT_CONTEXT_INTERVAL_SEC:
        if not profile.get("liquid"):
            st["watch_reason"]="WAITING_FOR_LIQUID_MARKET"
        elif not struct["trend_shift"] or struct["h1"]=="UP":
            _record_phase(db,st,now,"WEAKENING","TREND_NOT_ALIGNED",ev,struct)
        elif not _evidence_ok(ev,spot):
            # Remain research watch while waiting for validated spot and OI.
            st["watch_reason"]="WAITING_FOR_INDEPENDENT_FLOW_OI"
        elif ((ev.get("order_flow") or {}).get("status")!="CONFIRMED"
              or (ev.get("order_flow") or {}).get("ready") is not True):
            # Autonomous local verifier: fresh 1m/5m taker aggression plus
            # Spot confirmation and bearish price response. It is deliberately
            # not represented as independent order-book reconstruction.
            st["watch_reason"]="WAITING_FOR_LOCAL_AGGRESSION_RESPONSE"
        else:
            plan=_plan(profile["last"],struct,st["break_level"],
                       st["last_retrace_pct"],profile["spread"],equity)
            if plan is not None:
                if _record_phase(db,st,now,"ENTRY_ZONE",
                                 "RETEST_REJECTION_SPOT_FUTURES_OI_SELLERS_CONFIRMED",
                                 ev,struct,plan):
                    st["last_plan_at"]=now
                    st["entry_zone_at"]=now
                    print(f"[SHORT_ENTRY_ZONE] {sym} "
                          f"zone={plan['zone_low']:.8g}-{plan['zone_high']:.8g} "
                          f"entry={plan['entry']:.8g} stop={plan['stop']:.8g} "
                          f"tp1={plan['tp1']:.8g} tp2={plan['tp2']:.8g} "
                          f"retrace={st['last_retrace_pct']:.2f}% "
                          f"qty={plan['quantity']} risk={100*RISK_FRACTION:.2f}% "
                          "NO_LIVE_ORDER=true DECISION_SUPPORT=true",flush=True)
    return status,struct


def _fetch_paper_1m(symbol):
    from market_data_v3 import get_candles
    return get_candles(symbol,bar="1m",limit=100)


def _cancel_published_plans(db, quotes, now):
    """All paper plans get cheap invalidation, regardless of the 3-symbol cap."""
    for row in db.execute("""SELECT * FROM post_pump_short_strategy
        WHERE phase IN ('ENTRY_ZONE','READY_RESEARCH')""").fetchall():
        st = dict(row)
        px = quotes.get(st['symbol'])
        plan = db.execute("""SELECT stop FROM post_pump_short_strategy_audit
            WHERE symbol=? AND episode_at=?
              AND phase IN ('ENTRY_ZONE','READY_RESEARCH')
            ORDER BY id DESC LIMIT 1""", (st['symbol'],st['episode_at'])).fetchone()
        stop = num(plan['stop']) if plan else None
        level = num(st.get('break_level'))
        reason = None
        if px is not None and stop is not None and px >= stop:
            reason = 'PAPER_STOP_REACHED'
        elif px is not None and level is not None and px > level * 1.012:
            reason = 'BREAK_INVALIDATED'
        elif now-st['phase_at'] >= 3600:
            reason = 'PAPER_PLAN_EXPIRED'
        if reason:
            if px is not None:
                st['last_price'] = px
                st['last_retrace_pct'] = 100*(1-px/st['peak'])
            _record_phase(db,st,now,'CANCELLED',reason)
            db.execute("""UPDATE post_pump_short_strategy SET phase=?,phase_at=?,
                watch_reason=?,last_price=?,last_retrace_pct=? WHERE symbol=?""",
                (st['phase'],st['phase_at'],reason,st['last_price'],st['last_retrace_pct'],st['symbol']))


def observe(tickers,now=None,db_path=None,fetch_context=None,
            fetch_spot=None,evidence_reader=None,equity_usdt=None,
            fetch_paper_1m=None):
    """One bounded live scan, direct discovery and entirely paper decisions."""
    now=time.time() if now is None else float(now)
    path=DB_PATH if db_path is None else db_path
    context=_fetch_context if fetch_context is None else fetch_context
    spot_fetch=_fetch_spot if fetch_spot is None else fetch_spot
    evidence=take_cached_evidence if evidence_reader is None else evidence_reader
    paper_source=_fetch_paper_1m if fetch_paper_1m is None else fetch_paper_1m
    equity_usdt=equity_usdt if equity_usdt is not None else os.getenv("POST_PUMP_EQUITY_USDT")
    quotes={}
    eligible={}
    live_profiles={}
    diagnostics=dict(tickers=0,over30=0,liquid=0,active=0,
                     context_checks=0,new=0,outcomes=0,paper_checks=0)
    pending_flow=[]
    for ticker in tickers or ():
        if not isinstance(ticker,dict):
            continue
        inst=ticker.get("instId","")
        last=num(ticker.get("last"))
        if isinstance(inst,str) and inst.endswith("-USDT-SWAP") and last and last>0:
            symbol=inst[:-10]+"USDT"
            quotes[symbol]=last
            live=live_market_profile(ticker)
            if live is not None:
                live_profiles[symbol]=live
        diagnostics["tickers"]+=1
        item=safe_profile(ticker)
        if not item:
            continue
        diagnostics["over30"]+=1
        if item["liquid"]:
            diagnostics["liquid"]+=1
            eligible[item["symbol"]]=item
    # Extend Bybit liquidation coverage to every live +30% pump candidate.
    # The stream itself remains read-only and deduplicates subscriptions.
    for symbol in eligible:
        try:
            _watch_bybit_liquidations(symbol)
        except Exception as liq_watch_exc:
            print("[BYBIT_LIQ_WATCH_ERROR]",symbol,
                  type(liq_watch_exc).__name__,flush=True)

    try:
        os.makedirs(os.path.dirname(path) or ".",exist_ok=True)
        with closing(sqlite3.connect(path,timeout=7)) as db:
            db.row_factory=sqlite3.Row
            db.execute("PRAGMA busy_timeout=7000")
            setup(db)
            _cancel_published_plans(db, quotes, now)
            # All detected +30% symbols enter watch regardless of earlier pullback.
            for symbol,item in eligible.items():
                old=db.execute("""SELECT * FROM post_pump_short_strategy
                                  WHERE symbol=?""",(symbol,)).fetchone()
                if (old is None or old["phase"] in ("CANCELLED","EXPIRED")
                        or now-old["last_seen_at"]>STALE_SYMBOL_SEC
                        or now-old["episode_at"]>MAX_EPISODE_SEC):
                    if old is not None:
                        db.execute("DELETE FROM post_pump_short_strategy WHERE symbol=?",
                                   (symbol,))
                    try:
                        discovery_evidence=evidence(symbol,now) or {}
                    except Exception as discovery_exc:
                        discovery_evidence={"evidence_error":type(discovery_exc).__name__}
                    st=dict(symbol=symbol,episode_at=now,last_seen_at=now,
                            phase="WATCH",phase_at=now,peak=item["high"],
                            rise_max=item["rise"],band=item["band"],
                            reference_open=item["opening"],
                            break_level=None,break_candle_ts=None,
                            watch_reason="PUMP_30_PERCENT_DETECTED",
                            last_context_at=0,last_spot_poll_at=0,
                            last_plan_at=None,last_price=item["last"],
                            last_retrace_pct=item["retrace"],
                            last_evidence_json=json.dumps(
                                discovery_evidence,sort_keys=True,default=str),
                            last_structure_json=None,peak_at=now,
                            entry_zone_at=None)
                    db.execute("""INSERT INTO post_pump_short_strategy(
                        symbol,episode_at,last_seen_at,phase,phase_at,
                        peak,rise_max,band,reference_open,break_level,
                        break_candle_ts,watch_reason,last_context_at,
                        last_spot_poll_at,last_plan_at,last_price,
                        last_retrace_pct,last_evidence_json,last_structure_json,
                        peak_at,entry_zone_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        tuple(st.values()))
                    _record_phase(db,st,now,"DISCOVERED","OBSERVED_RISE_30PCT",
                                  discovery_evidence)
                    st["phase"]="WATCH"
                    diagnostics["new"]+=1
                else:
                    # Keep state even if the ticker rose in a different band.
                    # Preserve the *previous* peak until the stage engine
                    # compares the fresh price against it. Prematurely
                    # updating peak here would make BUYERS_RECLAIMED_NEW_HIGH
                    # mathematically unreachable.
                    db.execute("""UPDATE post_pump_short_strategy SET
                        last_seen_at=? WHERE symbol=?""",(now,symbol))
            # Evaluate existing + fresh assets even after the 24h window rolls;
            # at most one 72h research episode, never immortal on stale momentum.
            candidates=list(db.execute("""SELECT * FROM post_pump_short_strategy
                WHERE episode_at>=? AND phase NOT IN ('CANCELLED','EXPIRED')
                LIMIT 120""",(now-MAX_EPISODE_SEC,)).fetchall())
            candidates=[z for z in candidates if z["symbol"] in live_profiles]
            candidates.sort(
                key=lambda z:_candidate_sort_key(z,eligible,live_profiles)
            )
            if fetch_spot is None:
                priority=[
                    z["symbol"] for z in candidates
                    if _candidate_priority_snapshot(
                        z,eligible,live_profiles
                    )["urgent"]
                ]
                selected=watch_spot_stream(
                    [z["symbol"] for z in candidates],
                    urgent_symbols=priority,
                )
                from post_pump_market_context import watch as watch_market_context
                watch_market_context([p.replace("-USDT", "USDT") for p in selected])
                rows=[z for z in candidates
                      if z["symbol"][:-4]+"-USDT" in selected]
            else:
                rows=candidates[:MAX_ACTIVE]  # deterministic offline tests
            selected_symbols={z["symbol"] for z in rows}
            for passive in candidates:
                if passive["symbol"] in selected_symbols:
                    continue
                symbol=passive["symbol"]
                profile=eligible.get(symbol) or live_profiles.get(symbol)
                _refresh_passive_state(db,passive,profile,now)
            diagnostics["tracked"]=len(candidates)
            diagnostics["active"]=len(rows)
            for row in rows:
                st=dict(row)
                symbol=st["symbol"]
                profile=eligible.get(symbol)
                if profile is None:
                    live=live_profiles.get(symbol)
                    if live is None or not st["peak"]:
                        continue
                    # The +30% condition is a DISCOVERY gate, not a permanent
                    # liquidity gate. Keep the episode alive after pullback and
                    # re-check the real current ticker volume/spread every scan.
                    live_high=num(live.get("high")) or st["peak"]
                    live_open=num(live.get("opening")) or st["reference_open"]
                    live_rise=num(live.get("rise"))
                    profile=dict(live,
                                 high=max(st["peak"],live_high),
                                 opening=live_open,
                                 rise=max(st["rise_max"],live_rise)
                                      if live_rise is not None else st["rise_max"],
                                 band=st["band"],
                                 retrace=100*(1-live["last"]/st["peak"]))
                status,structure=_advance(db,st,profile,now,context,spot_fetch,
                                           evidence,equity_usdt,db_path=path)
                if structure is not None:
                    diagnostics["context_checks"]+=1
                # The autonomous local verifier reads only in-process caches
                # and is refreshed while the setup is structurally relevant.
                # Cooldown inside dispatch bounds repeated work.
                if st["phase"] in ("TREND_SHIFT","SHORT_CONFIRMED","RETEST"):
                    pending_flow.append((symbol,profile["last"],st["episode_at"]))
                db.execute("""UPDATE post_pump_short_strategy SET
                    last_seen_at=?,phase=?,phase_at=?,peak=?,rise_max=?,band=?,
                    break_level=?,break_candle_ts=?,watch_reason=?,
                    last_context_at=?,last_spot_poll_at=?,last_plan_at=?,
                    last_price=?,last_retrace_pct=?,
                    last_evidence_json=?,last_structure_json=?,
                    peak_at=?,entry_zone_at=? WHERE symbol=?""",
                    (st["last_seen_at"],st["phase"],st["phase_at"],
                     st["peak"],st["rise_max"],st["band"],
                     st["break_level"],st["break_candle_ts"],
                     st["watch_reason"],st["last_context_at"],
                     st["last_spot_poll_at"],st["last_plan_at"],
                     st["last_price"],st["last_retrace_pct"],
                     st["last_evidence_json"],st["last_structure_json"],
                     st.get("peak_at"),st.get("entry_zone_at"),symbol))
            # Forward quote markouts; not TP/SL first-touch execution.
            due=db.execute("""SELECT o.audit_id,o.horizon_min,
                a.symbol,a.at,a.reference_price FROM post_pump_short_strategy_outcomes o
                JOIN post_pump_short_strategy_audit a ON a.id=o.audit_id
                WHERE o.status='PENDING' AND a.at<=? LIMIT 2000""",(now,)).fetchall()
            for row in due:
                lag=now-row["at"]-60*row["horizon_min"]
                if lag<0:
                    continue
                quote=quotes.get(row["symbol"])
                if lag>300:
                    db.execute("""UPDATE post_pump_short_strategy_outcomes
                        SET status='MISSING' WHERE audit_id=? AND horizon_min=?""",
                        (row["audit_id"],row["horizon_min"]))
                elif quote is None:
                    # A temporarily unavailable quote is not a confirmed
                    # missing 5/15/30/60m outcome until grace expires.
                    continue
                else:
                    # Markouts at every causal phase are descriptive counterfactuals,
                    # not trades, first-touch TP/SL fills, or execution results.
                    baseline=row["reference_price"]
                    if baseline is None or baseline<=0:
                        db.execute("""UPDATE post_pump_short_strategy_outcomes
                          SET status='NO_BASELINE' WHERE audit_id=? AND horizon_min=?""",
                          (row["audit_id"],row["horizon_min"]))
                    else:
                        net=100*(baseline-quote)/baseline-ROUND_TRIP_COST_PCT
                        db.execute("""UPDATE post_pump_short_strategy_outcomes SET
                            status='DONE',quote_at=?,price=?,model_net_short_pct=?
                            WHERE audit_id=? AND horizon_min=?""",
                            (now,quote,round(net,6),row["audit_id"],row["horizon_min"]))
                        diagnostics["outcomes"]+=1
            diagnostics["paper_checks"]=advance_paper_audit(
                db,now,paper_source)
            cutoff=now-30*86400
            prune_paper_audit(db,cutoff)
            db.execute("DELETE FROM post_pump_short_spot_trades WHERE ts<?",(now-3600,))
            db.execute("DELETE FROM post_pump_short_spot_coverage WHERE last_at<?",
                       (now-STALE_SYMBOL_SEC,))
            db.execute("""DELETE FROM post_pump_short_strategy WHERE
                 last_seen_at<? OR episode_at<?""",
                       (now-STALE_SYMBOL_SEC,now-72*3600))
            db.execute("""DELETE FROM post_pump_short_strategy_outcomes WHERE audit_id IN(
               SELECT id FROM post_pump_short_strategy_audit WHERE at<?)""",(cutoff,))
            db.execute("DELETE FROM post_pump_short_strategy_audit WHERE at<?",(cutoff,))
            maybe_report_cohorts(db,now)
            db.commit()
    except (sqlite3.Error,ValueError,TypeError,OverflowError) as exc:
        print("[SHORT_STRATEGY_ERROR]",type(exc).__name__,str(exc)[:170],flush=True)
        return diagnostics
    for symbol,price,episode in pending_flow:
        dispatch_post_pump_flow(symbol,price,episode,db_path=path,now=now)
    global last_status_at
    if now-last_status_at>=3600:
        last_status_at=now
        print("[SHORT_STRATEGY_FUNNEL]",diagnostics,
              "30pct_uses_24h_high_open=true paper_only=true",flush=True)
        try:
            with closing(sqlite3.connect(path,timeout=5)) as report_db:
                print("[POST_PUMP_PAPER_REPORT]",paper_audit_report(report_db),
                      "first_touch_closed_1m=true no_live_orders=true",
                      flush=True)
                # Lightweight hourly funnel: diagnose blocked research candidates
                # using the state already persisted in SQLite. No extra API,
                # notifications, tables or changes to the SHORT safety gates.
                report_db.row_factory=sqlite3.Row
                stalled=report_db.execute("""SELECT phase,
                    COALESCE(NULLIF(watch_reason,''),'NO_REASON') AS reason,
                    COUNT(*) AS n
                    FROM post_pump_short_strategy
                    WHERE last_seen_at>=? AND phase NOT IN ('EXPIRED','CANCELLED')
                    GROUP BY phase,reason ORDER BY n DESC LIMIT 20""",
                    (now-STALE_SYMBOL_SEC,)).fetchall()
                for item in stalled:
                    print("[SHORT_REJECTION_FUNNEL]",
                          "phase=",item["phase"],"reason=",item["reason"],
                          "candidates=",item["n"],
                          "descriptive_only=true no_gate_change=true",flush=True)
        except sqlite3.Error:
            print("[POST_PUMP_PAPER_REPORT_ERROR] sqlite",flush=True)
    return diagnostics
