"""Shared forward markout statistics for PumpDump LONG research stages.

Research-only. Measures the observed ticker quote at 5/10/15/30/60/120 minutes
after a strategy stage. This is NOT first-touch execution, TP/SL simulation, or
proof of tradable fills.
"""
import sqlite3
import time

HORIZONS=(5,10,15,30,60,120)
MAX_LAG_SEC=300
REPORT_INTERVAL_SEC=3600
_last_report={}


def setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS long_forward_outcomes(
        source TEXT NOT NULL,
        audit_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        phase TEXT NOT NULL,
        signal_at REAL NOT NULL,
        reference_price REAL NOT NULL,
        horizon_min INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        quote_at REAL,
        quote_lag_sec REAL,
        price REAL,
        markout_pct REAL,
        PRIMARY KEY(source,audit_id,horizon_min)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_long_forward_due
        ON long_forward_outcomes(source,status,signal_at,horizon_min)""")


def register(db,source,audit_id,symbol,phase,signal_at,price):
    if not source or not symbol or price is None or float(price)<=0:
        return 0
    setup(db)
    rows=[(source,int(audit_id),symbol,phase,float(signal_at),float(price),h)
          for h in HORIZONS]
    db.executemany("""INSERT OR IGNORE INTO long_forward_outcomes(
        source,audit_id,symbol,phase,signal_at,reference_price,horizon_min)
        VALUES(?,?,?,?,?,?,?)""",rows)
    return len(rows)


def advance(db,source,quotes,now=None):
    now=time.time() if now is None else float(now)
    setup(db)
    rows=db.execute("""SELECT * FROM long_forward_outcomes
        WHERE source=? AND status='PENDING'
        AND signal_at + horizon_min*60 <= ?
        ORDER BY signal_at LIMIT 1000""",(source,now)).fetchall()
    done=0
    for row in rows:
        # sqlite.Row in production/tests; index fallback keeps helper reusable.
        get=(lambda k: row[k]) if hasattr(row,"keys") else (lambda k: row[
            ["source","audit_id","symbol","phase","signal_at","reference_price",
             "horizon_min","status","quote_at","quote_lag_sec","price","markout_pct"].index(k)])
        target=float(get("signal_at"))+int(get("horizon_min"))*60
        lag=now-target
        if lag>MAX_LAG_SEC:
            db.execute("""UPDATE long_forward_outcomes SET status='MISSING',
                quote_at=?,quote_lag_sec=? WHERE source=? AND audit_id=? AND horizon_min=?""",
                (now,lag,source,get("audit_id"),get("horizon_min")))
            continue
        px=quotes.get(get("symbol"))
        if px is None or float(px)<=0:
            continue
        ref=float(get("reference_price"))
        markout=100.0*(float(px)/ref-1.0)
        db.execute("""UPDATE long_forward_outcomes SET
            status='DONE',quote_at=?,quote_lag_sec=?,price=?,markout_pct=?
            WHERE source=? AND audit_id=? AND horizon_min=?""",
            (now,lag,float(px),round(markout,6),source,get("audit_id"),get("horizon_min")))
        done+=1
    return done


def report_rows(db,source,days=30,now=None):
    now=time.time() if now is None else float(now)
    setup(db)
    cutoff=now-days*86400
    rows=db.execute("""SELECT phase,horizon_min,COUNT(*) AS n,
        AVG(markout_pct) AS avg_markout,
        100.0*AVG(CASE WHEN markout_pct>0 THEN 1.0 ELSE 0.0 END) AS positive_pct,
        100.0*AVG(CASE WHEN markout_pct>=2 THEN 1.0 ELSE 0.0 END) AS gain_ge_2_pct,
        100.0*AVG(CASE WHEN markout_pct>=4 THEN 1.0 ELSE 0.0 END) AS gain_ge_4_pct
        FROM long_forward_outcomes
        WHERE source=? AND status='DONE' AND signal_at>=?
        GROUP BY phase,horizon_min ORDER BY phase,horizon_min""",
        (source,cutoff)).fetchall()
    out=[]
    for r in rows:
        if hasattr(r,"keys"):
            get=lambda k:r[k]
        else:
            names=("phase","horizon_min","n","avg_markout","positive_pct",
                   "gain_ge_2_pct","gain_ge_4_pct")
            get=lambda k:r[names.index(k)]
        out.append({
            "phase":get("phase"),"horizon_min":int(get("horizon_min")),
            "n":int(get("n")),"avg_markout_pct":round(float(get("avg_markout")),4),
            "positive_pct":round(float(get("positive_pct")),1),
            "gain_ge_2_pct":round(float(get("gain_ge_2_pct")),1),
            "gain_ge_4_pct":round(float(get("gain_ge_4_pct")),1),
        })
    return out


def maybe_log_report(db,source,now=None,days=30):
    now=time.time() if now is None else float(now)
    if now-_last_report.get(source,0)<REPORT_INTERVAL_SEC:
        return []
    _last_report[source]=now
    rows=report_rows(db,source,days=days,now=now)
    if not rows:
        print("[LONG_FORWARD_STATS]",source,
              "no_completed_samples=true days=",days,
              "quote_markout_only=true no_orders=true",flush=True)
        return []
    for x in rows:
        print("[LONG_FORWARD_STATS]",
              "source=",source,
              "phase=",x["phase"],
              "horizon=",str(x["horizon_min"])+"m",
              "n=",x["n"],
              "avg=",str(x["avg_markout_pct"])+"%",
              "positive=",str(x["positive_pct"])+"%",
              "gain_ge_2=",str(x["gain_ge_2_pct"])+"%",
              "gain_ge_4=",str(x["gain_ge_4_pct"])+"%",
              "days=",days,
              "quote_markout_only=true no_orders=true",flush=True)
    return rows
