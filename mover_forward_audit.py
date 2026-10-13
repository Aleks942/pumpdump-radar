"""Bounded, research-only early-mover forward QUOTE markouts: 5m / 15m / 30m.

No new exchange requests, threads, services, orders or Telegram messages.
Entry and exits are observed LAST prices, NOT executable fills or TP/SL
first-touch paths. Assumed round-trip cost = 0.22% before funding/slippage.
Only observations sampled within 180s after a horizon are scored; missing
or late samples are marked MISSING, never assumed profitable.
"""
import math
import os
import sqlite3
import time

DB_PATH = os.getenv("MOVER_FORWARD_DB", "/data/mover_forward.sqlite3")
HORIZONS = (5, 15, 30)
ROUNDTRIP_COST_PCT = 0.22
MIN_5M_PCT = 1.0
MAX_5M_PCT = 5.5
MIN_DELAY_SEC = 0
MAX_DELAY_SEC = 180
COOLDOWN_SEC = 90 * 60
RETENTION_SEC = 30 * 86400
MAX_EVENTS = 2400
MAX_NEW_PER_SCAN = 12
MAX_DUE_PER_SCAN = 250
REPORT_EVERY_SEC = 3600
_last_report = None
_last_cleanup = None


def _connect():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=3)
    db.execute("""CREATE TABLE IF NOT EXISTS mover_cases (
        id INTEGER PRIMARY KEY, ts REAL NOT NULL, symbol TEXT NOT NULL,
        side TEXT NOT NULL, band TEXT NOT NULL, change5 REAL NOT NULL,
        entry REAL NOT NULL, spread_bps REAL NOT NULL
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS mover_recent ON mover_cases(symbol,side,ts)")
    db.execute("""CREATE TABLE IF NOT EXISTS mover_outcomes (
        case_id INTEGER NOT NULL, horizon_min INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        markout_pct REAL, net_pct REAL, sampled_ts REAL, lag_sec REAL,
        PRIMARY KEY (case_id,horizon_min)
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS mover_outcome_pending ON mover_outcomes(status,horizon_min)")
    return db


def _valid_row(row):
    if not isinstance(row, dict) or not row.get("eligible_research"):
        return None
    symbol = row.get("symbol")
    try:
        pct = float(row.get("change5"))
        price = float(row.get("price"))
        spread = float(row.get("spread_bps"))
    except (TypeError, ValueError, OverflowError):
        return None
    if (not isinstance(symbol, str) or not symbol.endswith("USDT")
            or len(symbol) > 35 or not all(math.isfinite(x) for x in (pct,price,spread))
            or price <= 0 or spread < 0 or spread > 40
            or not MIN_5M_PCT <= abs(pct) <= MAX_5M_PCT):
        return None
    side = "LONG" if pct > 0 else "SHORT"
    band = "1_TO_2P5" if abs(pct) < 2.5 else "2P5_TO_5P5"
    return symbol, side, band, pct, price, spread


def observe(rows, now=None):
    """Journal fresh candidates and settle due point-quote markouts.

    Only rows already obtained in mover_discovery.inspect() are accepted.
    Repeated triggers on same symbol+side are deduped for 90 minutes.
    """
    global _last_cleanup
    now = time.time() if now is None else float(now)
    if not math.isfinite(now):
        return
    try:
        quotes = {}
        candidates = []
        for row in rows or ():
            if not isinstance(row, dict):
                continue
            sym = row.get("symbol")
            try:
                px = float(row.get("price"))
            except (TypeError, ValueError, OverflowError):
                continue
            if isinstance(sym, str) and math.isfinite(px) and px > 0:
                quotes[sym] = px
            valid = _valid_row(row)
            if valid:
                candidates.append(valid)

        with _connect() as db:
            if _last_cleanup is None or now-_last_cleanup >= 600:
                # Keep the existing database bounded even if left unattended.
                db.execute("""DELETE FROM mover_outcomes WHERE case_id IN (
                    SELECT id FROM mover_cases WHERE ts < ?
                )""", (now-RETENTION_SEC,))
                db.execute("DELETE FROM mover_cases WHERE ts < ?", (now-RETENTION_SEC,))
                db.execute("""DELETE FROM mover_outcomes WHERE case_id IN (
                    SELECT id FROM mover_cases ORDER BY ts DESC LIMIT -1 OFFSET ?
                )""", (MAX_EVENTS,))
                db.execute("""DELETE FROM mover_cases WHERE id IN (
                    SELECT id FROM mover_cases ORDER BY ts DESC LIMIT -1 OFFSET ?
                )""", (MAX_EVENTS,))
                _last_cleanup = now

            # Settle outcomes FIRST, so today's new cases never receive
            # contemporaneous or future markouts.
            due = db.execute("""SELECT c.id,c.ts,c.symbol,c.side,c.entry,
                        o.horizon_min FROM mover_cases c
                JOIN mover_outcomes o ON o.case_id=c.id
                WHERE o.status='PENDING' AND c.ts+o.horizon_min*60 <= ?
                ORDER BY c.ts,o.horizon_min LIMIT ?""",
                (now,MAX_DUE_PER_SCAN)).fetchall()
            for case_id, start, symbol, side, entry, horizon in due:
                target_time = start + horizon*60
                lag = now-target_time
                px = quotes.get(symbol)
                if lag < MIN_DELAY_SEC:
                    continue
                if lag > MAX_DELAY_SEC or px is None:
                    # Do not mark unavailable quotes MISSING until the
                    # allowable snapshot window is actually exhausted.
                    if lag <= MAX_DELAY_SEC:
                        continue
                    db.execute("""UPDATE mover_outcomes SET status='MISSING'
                        WHERE case_id=? AND horizon_min=? AND status='PENDING'""",
                        (case_id,horizon))
                    continue
                sign = 1 if side == "LONG" else -1
                markout = sign*(px/entry-1)*100
                net = markout-ROUNDTRIP_COST_PCT
                db.execute("""UPDATE mover_outcomes SET status='DONE',
                        markout_pct=?,net_pct=?,sampled_ts=?,lag_sec=?
                        WHERE case_id=? AND horizon_min=? AND status='PENDING'""",
                        (markout,net,now,lag,case_id,horizon))
                print(f"[MOVER_FWD_OUTCOME] {symbol} side={side} horizon={horizon}m "
                      f"markout={markout:+.3f}% net={net:+.3f}% lag_sec={lag:.0f} "
                      "observed_quote_only=true not_tp_sl_first_touch=true "
                      "no_orders=true", flush=True)

            # Strongest in-band changes first. Selection is deliberately
            # research-only; MAX_NEW_PER_SCAN introduces selection bias.
            new = 0
            for symbol,side,band,pct,price,spread in sorted(
                    candidates,key=lambda r:abs(r[3]),reverse=True):
                if new >= MAX_NEW_PER_SCAN:
                    break
                found = db.execute("""SELECT 1 FROM mover_cases
                    WHERE symbol=? AND side=? AND ts>? LIMIT 1""",
                    (symbol,side,now-COOLDOWN_SEC)).fetchone()
                if found:
                    continue
                cur=db.execute("""INSERT INTO mover_cases
                    (ts,symbol,side,band,change5,entry,spread_bps)
                    VALUES (?,?,?,?,?,?,?)""",
                    (now,symbol,side,band,pct,price,spread))
                db.executemany("""INSERT INTO mover_outcomes (case_id,horizon_min)
                    VALUES (?,?)""",[(cur.lastrowid,h) for h in HORIZONS])
                new += 1
                print(f"[MOVER_FWD_CASE] {symbol} side={side} move5={pct:+.2f}% "
                      f"band={band} spread_bps={spread:.2f} "
                      "research_only=true no_orders=true",flush=True)
            db.commit()
    except (sqlite3.Error,OSError) as exc:
        print(f"[MOVER_FWD_ERROR] stage=observe kind={type(exc).__name__}",
              flush=True)


def quality_snapshot(db):
    """Diagnostic only. Never use this output to approve a live trade."""
    data=db.execute("""SELECT c.side,c.band,o.horizon_min,
         COUNT(*),
         SUM(CASE WHEN o.status='DONE' THEN 1 ELSE 0 END),
         SUM(CASE WHEN o.status='PENDING' THEN 1 ELSE 0 END),
         SUM(CASE WHEN o.status='MISSING' THEN 1 ELSE 0 END),
         SUM(CASE WHEN o.status='DONE' AND o.net_pct>0 THEN 1 ELSE 0 END),
         AVG(CASE WHEN o.status='DONE' THEN o.net_pct END),
         AVG(CASE WHEN o.status='DONE' THEN o.lag_sec END),
         COUNT(DISTINCT CASE WHEN o.status='DONE'
                       THEN CAST(c.ts/86400 AS INTEGER) END),
         COUNT(DISTINCT CASE WHEN o.status='DONE' THEN c.symbol END)
         FROM mover_cases c JOIN mover_outcomes o ON o.case_id=c.id
         GROUP BY c.side,c.band,o.horizon_min
         ORDER BY c.side,c.band,o.horizon_min""").fetchall()
    return [
        {"side":side,"band":band,"horizon":int(horizon),
         "n":int(n),"done":int(done or 0),"pending":int(pending or 0),
         "missing":int(missing or 0),"positive":int(positive or 0),
         "mean_net":float(mean_net) if mean_net is not None else None,
         "mean_lag":float(mean_lag) if mean_lag is not None else None,
         "days":int(days),"symbols":int(symbols)}
        for (side,band,horizon,n,done,pending,missing,positive,
             mean_net,mean_lag,days,symbols) in data]


def report(now=None):
    global _last_report
    now=time.time() if now is None else float(now)
    if _last_report is not None and now-_last_report < REPORT_EVERY_SEC:
        return
    try:
        with _connect() as db:
            n=db.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0]
            stats=quality_snapshot(db)
        print(f"[MOVER_FWD_SUMMARY] cases={n} cohorts={len(stats)} "
              "research_only=true no_trade_approval=true "
              "execution_not_proven=true",flush=True)
        for row in stats:
            avg="NA" if row["mean_net"] is None else f'{row["mean_net"]:+.4f}%'
            delay="NA" if row["mean_lag"] is None else f'{row["mean_lag"]:.0f}'
            print(f'[MOVER_FWD_QUALITY] side={row["side"]} band={row["band"]} '
                  f'horizon={row["horizon"]}m n={row["n"]} '
                  f'done={row["done"]} pending={row["pending"]} '
                  f'missing={row["missing"]} positive_net={row["positive"]} '
                  f'mean_net={avg} mean_lag_sec={delay} '
                  f'days={row["days"]} symbols={row["symbols"]} '
                  'quote_only=true cost_pct=0.22 '
                  'descriptive_only=true no_trade_approval=true',flush=True)
        _last_report=now
    except (sqlite3.Error,OSError) as exc:
        print(f"[MOVER_FWD_ERROR] stage=report kind={type(exc).__name__}",
              flush=True)
