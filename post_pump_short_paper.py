"""Strict first-touch PAPER audit for POST-PUMP SHORT READY_RESEARCH.

A *model*, not proof of an executable fill. Enter on the NEXT minute open,
not the previously observed quote. Use 60 consecutive confirmed OKX 1m OHLC
bars, account for worst-case same-bar stop/TP ambiguity, fees and slippage.
One bounded REST candle request only when a completed research plan matures.
Missing data is not a loss, a win, or an invented candle path.
"""
import math
import sqlite3

HORIZON_MIN=60
BAR_SEC=60
LAG_GRACE_SEC=90
MAX_RETRY_AFTER_WINDOW_SEC=30*60
MODEL_COST_PCT=0.22+0.10
ENTRY_MAX_DEVIATION_PCT=0.5
MAX_REQUESTS_PER_SCAN=2


def finite(x):
    try:
        f=float(x)
        return f if math.isfinite(f) else None
    except (TypeError,ValueError,OverflowError):
        return None


def setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS post_pump_paper_trades(
       audit_id INTEGER PRIMARY KEY,
       symbol TEXT NOT NULL, signal_at REAL NOT NULL,
       signal_price REAL NOT NULL, stop REAL NOT NULL, tp1 REAL NOT NULL,
       status TEXT NOT NULL DEFAULT 'PENDING',
       next_try_at REAL NOT NULL DEFAULT 0,
       evaluated_at REAL, fill_open REAL, exit_price REAL,
       exit_bar_ts REAL, modeled_net_pct REAL, detail TEXT,
       model_only INTEGER NOT NULL DEFAULT 1
    )""")


def register(db, audit_id, symbol, signal_at, signal_price, stop, tp1):
    """A confirmed research-stage plan is recorded exactly once."""
    setup(db)
    numbers=[finite(x) for x in (signal_at,signal_price,stop,tp1)]
    if any(x is None or x<=0 for x in numbers):
        return False
    stamp,entry,loss,target=numbers
    if not target<entry<loss:
        return False
    cursor=db.execute("""INSERT OR IGNORE INTO post_pump_paper_trades(
        audit_id,symbol,signal_at,signal_price,stop,tp1)
        VALUES (?,?,?,?,?,?)""",(audit_id,symbol,stamp,entry,loss,target))
    return cursor.rowcount==1


def _closed_bars(rows,signal_at,now):
    """Require an exact, real, minute-by-minute candle path after signal."""
    first=(int(signal_at)//60+1)*60
    end=first+HORIZON_MIN*BAR_SEC
    if now<end+LAG_GRACE_SEC:
        return None,"TOO_EARLY"
    seen={}
    for row in rows or []:
        if not isinstance(row,dict) or row.get("confirmed") is not True:
            continue
        timestamp=finite(row.get("ts"))
        if timestamp is None:
            continue
        ts=timestamp/1000 if timestamp>1e11 else timestamp
        ts=int(ts)
        if ts<first or ts>=end or (ts-first)%BAR_SEC:
            continue
        values=[finite(row.get(k)) for k in ("open","high","low","close")]
        if any(x is None or x<=0 for x in values):
            return None,"BAD_BAR"
        o,h,l,c=values
        if l>min(o,c) or h<max(o,c):
            return None,"BAD_BAR"
        item=(o,h,l,c)
        if ts in seen and seen[ts]!=item:
            return None,"CONFLICTING_BAR"
        seen[ts]=item
    expected=[first+i*BAR_SEC for i in range(HORIZON_MIN)]
    if any(ts not in seen for ts in expected):
        return None,"MISSING_CANDLES"
    return [(ts,)+seen[ts] for ts in expected],"OK"


def first_touch(signal_at,signal_price,stop,tp1,rows,now):
    """Report STOP, TP1, TIME, AMBIGUOUS, ENTRY_GAP or MISSING.

    The first bar OPEN is an illustrative fill with a <=0.5% maximum gap.
    Both stop/target touched within the same 1m bar => AMBIGUOUS, not a win.
    Stop gaps are marked conservatively at max(stop, that bar's open).
    """
    entry=finite(signal_price)
    target=finite(tp1)
    sl=finite(stop)
    if any(x is None or x<=0 for x in (entry,target,sl)) or not target<entry<sl:
        return {"status":"INVALID_PLAN","detail":"BAD_RISK_GEOMETRY"}
    bars,reason=_closed_bars(rows,signal_at,now)
    if bars is None:
        return {"status":"MISSING" if reason!="TOO_EARLY" else "PENDING",
                "detail":reason}
    filled=bars[0][1]
    if (abs(filled/entry-1)*100>ENTRY_MAX_DEVIATION_PCT
            or filled<=target or filled>=sl):
        return {"status":"ENTRY_GAP","fill_open":filled,
                "detail":"NO_RELIABLE_NEXT_MINUTE_FILL"}
    for ts,o,h,l,c in bars:
        hit_stop=h>=sl
        hit_tp=l<=target
        if hit_stop and hit_tp:
            return {"status":"AMBIGUOUS","fill_open":filled,
                    "exit_bar_ts":ts,"detail":"TP_AND_SL_SAME_MINUTE"}
        if hit_stop:
            price=max(sl,o)
            state="SL"
        elif hit_tp:
            price=target  # no favorable price improvement assumption
            state="TP1"
        else:
            continue
        return {"status":state,"fill_open":filled,"exit_price":price,
                "exit_bar_ts":ts,
                "modeled_net_pct":round(100*(filled-price)/filled-MODEL_COST_PCT,6),
                "detail":"NEXT_MINUTE_OPEN_PROXY"}
    return {"status":"TIME","fill_open":filled,
            "exit_price":bars[-1][4],"exit_bar_ts":bars[-1][0],
            "modeled_net_pct":round(100*(filled-bars[-1][4])/filled-MODEL_COST_PCT,6),
            "detail":"FULL_60_MINUTE_HOLD"}


def advance(db,now,fetch_1m,max_calls=MAX_REQUESTS_PER_SCAN):
    """Bounded final settlement. Calls fetch_1m only after the full 60m."""
    setup(db)
    due=db.execute("""SELECT * FROM post_pump_paper_trades
        WHERE status='PENDING' AND next_try_at<=?
        ORDER BY signal_at LIMIT ?""",(now,max_calls*3)).fetchall()
    n=0
    for row in due:
        first=(int(row["signal_at"])//60+1)*60
        end=first+HORIZON_MIN*BAR_SEC
        if now<end+LAG_GRACE_SEC:
            continue
        if n>=max_calls:
            break
        n+=1
        try:
            candles=fetch_1m(row["symbol"])
            outcome=first_touch(row["signal_at"],row["signal_price"],
                                row["stop"],row["tp1"],candles,now)
        except Exception as exc:
            outcome={"status":"MISSING","detail":"FETCH_"+type(exc).__name__}
        status=outcome["status"]
        if status=="PENDING":
            continue
        if status=="MISSING" and now<end+MAX_RETRY_AFTER_WINDOW_SEC:
            db.execute("""UPDATE post_pump_paper_trades SET next_try_at=?,
                detail=? WHERE audit_id=?""",(now+180,
                outcome.get("detail"),row["audit_id"]))
            continue
        vals=(status,now,outcome.get("fill_open"),
              outcome.get("exit_price"),outcome.get("exit_bar_ts"),
              outcome.get("modeled_net_pct"),outcome.get("detail"),
              row["audit_id"])
        db.execute("""UPDATE post_pump_paper_trades SET
            status=?,evaluated_at=?,fill_open=?,exit_price=?,exit_bar_ts=?,
            modeled_net_pct=?,detail=? WHERE audit_id=?""",vals)
        print(f"[POST_PUMP_PAPER_FIRST_TOUCH] symbol={row['symbol']} "
              f"outcome={status} net={outcome.get('modeled_net_pct')}% "
              "model_only=true no_live_orders=true",flush=True)
    return n


def report(db):
    setup(db)
    rows=db.execute("""SELECT status,COUNT(*) AS n,
        AVG(modeled_net_pct) AS mean_net FROM post_pump_paper_trades
        GROUP BY status ORDER BY status""").fetchall()
    return [{"status":status,"n":n,
             "mean_net":round(mean,6) if mean is not None else None}
            for status,n,mean in rows]


def prune(db,cutoff):
    db.execute("""DELETE FROM post_pump_paper_trades
        WHERE signal_at<? AND status!='PENDING'""",(cutoff,))
