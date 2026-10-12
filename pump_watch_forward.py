"""Forward-only price observations of confirmed or silent Pump Watch events.

Uses OKX tickers ALREADY retrieved by the Radar scan. No HTTP requests,
orders, signal changes, background threads, or look-ahead data.
For each stage (WATCH/WEAKENING/POSSIBLE_DUMP), record the FIRST available
ticker snapshot at 5/10/30/60/120m after delivery (<=5m late).
This is neither historical intrabar low nor executed short PnL.
"""
import math
import statistics
import time
from contextlib import closing

import pump_peak_watch as watch

HORIZONS = (5, 10, 30, 60, 120)
MAX_LATE_SEC = 300
BATCH = 250
RETENTION = 30 * 86400
_last_report = 0.0
_last_cleanup = 0.0


def _setup(db):
    watch._setup(db)
    db.execute("""CREATE TABLE IF NOT EXISTS pump_watch_forward (
        alert_id INTEGER NOT NULL,
        horizon_min INTEGER NOT NULL,
        due_at REAL NOT NULL,
        observed_at REAL,
        price REAL,
        down_pct REAL,
        status TEXT NOT NULL,
        PRIMARY KEY (alert_id, horizon_min)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_pump_forward_due
        ON pump_watch_forward(due_at)""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_pump_forward_status
        ON pump_watch_forward(status)""")


def _quotes(tickers):
    quotes = {}
    for ticker in tickers or ():
        try:
            pair = str(ticker.get("instId") or "")
            if not pair.endswith("-USDT-SWAP"):
                continue
            price = float(ticker.get("last"))
            if math.isfinite(price) and price > 0:
                quotes[pair.replace("-USDT-SWAP", "USDT")] = price
        except (ValueError, TypeError, OverflowError, AttributeError):
            continue
    return quotes


def _pending(db, now):
    # Legacy alerts with NULL entry_price are deliberately excluded:
    # reconstructing their price retrospectively would introduce lookahead.
    return db.execute("""WITH horizon(h) AS (
            VALUES (5),(10),(30),(60),(120)
        )
        SELECT a.id, a.symbol, a.level, a.ts, a.entry_price,
               h.h, a.ts + h.h * 60.0 AS due_at
        FROM pump_watch_alerts AS a
        CROSS JOIN horizon AS h
        LEFT JOIN pump_watch_forward AS o
               ON o.alert_id=a.id AND o.horizon_min=h.h
        WHERE a.entry_price > 0 AND a.ts >= ? AND a.ts <= ?
          AND a.ts + h.h * 60.0 <= ?
          AND o.alert_id IS NULL
        ORDER BY due_at ASC, a.id ASC LIMIT ?""",
        (now - RETENTION, now, now, BATCH)).fetchall()


def observe(tickers, now=None):
    """Evaluate matured horizons with quotes observed only after alert time.

    If the first available quote arrives too late, record MISSING rather
    than reporting a misleading point-in-time outcome.
    """
    global _last_cleanup
    now = time.time() if now is None else float(now)
    quotes = _quotes(tickers)
    processed = 0
    ok = 0
    missed = 0
    try:
        with closing(watch._db()) as db:
            _setup(db)
            clean_now = now - _last_cleanup >= 3600
            if clean_now:
                # Delete only orphaned research outcomes after the watch
                # alert retention policy expires. Never delete legacy data.
                db.execute("""DELETE FROM pump_watch_forward
                    WHERE NOT EXISTS (
                        SELECT 1 FROM pump_watch_alerts a
                        WHERE a.id = pump_watch_forward.alert_id
                    )""")
            rows = _pending(db, now)
            for alert_id, symbol, level, ts, entry, minutes, due in rows:
                quote = quotes.get(symbol)
                timely = now - due <= MAX_LATE_SEC
                if timely and quote is None:
                    # Wait within grace window for a later valid quote.
                    continue
                if timely and quote is not None:
                    down = round((1.0 - quote / float(entry))*100.0, 5)
                    status = "OK"
                    ok += 1
                else:
                    quote = None
                    down = None
                    status = "MISSING"
                    missed += 1
                db.execute("""INSERT OR IGNORE INTO pump_watch_forward
                    (alert_id,horizon_min,due_at,observed_at,price,down_pct,status)
                    VALUES (?,?,?,?,?,?,?)""",
                    (alert_id, minutes, due, now if status == "OK" else None,
                     quote, down, status))
                processed += 1
                if status == "OK":
                    print(f"[PUMP_WATCH_FORWARD] {symbol} level={level} "
                          f"horizon={minutes}m down={down:+.3f}% "
                          f"delay_sec={now-due:.0f} model=observed_quote_no_orders",
                          flush=True)
            db.commit()
            if clean_now:
                _last_cleanup = now
        if processed:
            print(f"[PUMP_WATCH_FORWARD_BATCH] processed={processed} "
                  f"ok={ok} missing={missed}", flush=True)
        return processed
    except Exception as exc:
        print(f"[PUMP_WATCH_FORWARD_ERROR] {type(exc).__name__}: "
              f"{str(exc)[:100]}", flush=True)
        return 0


def summary(now=None):
    """Return descriptive grouped research, not a probability of future falls."""
    now = time.time() if now is None else float(now)
    result = []
    try:
        with closing(watch._db()) as db:
            _setup(db)
            rows = db.execute("""SELECT a.level, o.horizon_min, o.status,
                    o.down_pct, a.symbol, CAST(a.ts / 86400 AS INTEGER),
                    a.delivered
                FROM pump_watch_forward o
                JOIN pump_watch_alerts a ON a.id=o.alert_id
                WHERE a.ts >= ? AND o.horizon_min IN (5,10,30,60,120)""",
                (now - RETENTION,)).fetchall()
        grouped = {}
        for level, horizon, status, value, symbol, day, delivered in rows:
            key = (int(delivered), level, horizon)
            entry = grouped.setdefault(key, {"down":[],"missing":0,
                                            "symbols":set(),"days":set()})
            if status == "OK" and value is not None:
                entry["down"].append(float(value))
                entry["symbols"].add(symbol)
                entry["days"].add(day)
            else:
                entry["missing"] += 1
        for delivered in (1, 0):
          for level in (1,2,3):
            for minutes in HORIZONS:
                item = grouped.get((delivered,level,minutes), {})
                drops = item.get("down",[])
                n = len(drops)
                n_missing = item.get("missing",0)
                result.append({
                    "cohort": "DELIVERED" if delivered else "SILENT",
                    "level":level, "horizon_min":minutes,
                    "n":n, "missing":n_missing,
                    "symbols":len(item.get("symbols",set())),
                    "days":len(item.get("days",set())),
                    "mean_down_pct":round(statistics.mean(drops),4) if n else None,
                    "median_down_pct":round(statistics.median(drops),4) if n else None,
                    "fell_2pct":sum(1 for v in drops if v >= 2),
                    "fell_4pct":sum(1 for v in drops if v >= 4),
                    "status":"PRELIMINARY" if n >= 30 and
                         len(item.get("days",set())) >= 7 else "COLLECTING"
                })
    except Exception as exc:
        print(f"[PUMP_WATCH_FORWARD_ERROR] stage=report "
              f"{type(exc).__name__}: {str(exc)[:100]}", flush=True)
    return result


def hourly_report(now=None):
    global _last_report
    now = time.time() if now is None else float(now)
    if now - _last_report < 3600:
        return
    _last_report = now
    rows = summary(now)
    if not rows:
        return
    for r in rows:
        if r["n"] == 0 and r["missing"] == 0:
            continue
        n = r["n"]
        print(f"[PUMP_WATCH_EDGE] cohort={r['cohort']} level={r['level']} "
              f"horizon={r['horizon_min']}m n={n} "
              f"missing={r['missing']} symbols={r['symbols']} "
              f"days={r['days']} avg_down={r['mean_down_pct']}% "
              f"median_down={r['median_down_pct']}% "
              f"fall_ge_2={r['fell_2pct']}/{n} "
              f"fall_ge_4={r['fell_4pct']}/{n} "
              f"status={r['status']} model=point_quote_no_orders",
              flush=True)
