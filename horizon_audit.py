"""Bounded 1h/4h/24h/48h research for already screened PumpDump candidates.

Stores quote-anchored samples and scalar outcomes in existing SQLite. Fetches
15-minute OKX bars only when each checkpoint matures: no in-RAM candle cache,
new service, orders or changes to Telegram/first-touch research gate.
15m intrabar ordering AND the partial candle at signal time are unknowable:
verdicts are explicitly observational and NEVER authorize live alerts.
"""
import math
import sqlite3
from contextlib import closing
import time

import requests

import trend_probability_gate as gate

HORIZONS_H = (1, 4, 24, 48)
BAR_SEC = 900
MAX_CASES = 2400
RETENTION_SEC = 30 * 86400
MIN_INDEPENDENT_SEC = 90 * 60
MAX_LATENESS_SEC = 60 * 60
MAX_CALLS_PER_SCAN = 1
MIN_REQUEST_GAP_SEC = 120
_last_request = 0.0
_last_report = 0.0
_last_cleanup = 0.0


def _conn():
    conn = gate._db()
    conn.execute("""CREATE TABLE IF NOT EXISTS horizon_cases (
        id INTEGER PRIMARY KEY,
        ts REAL NOT NULL,
        symbol TEXT NOT NULL,
        pattern TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_price REAL NOT NULL
    )""")
    conn.execute("""CREATE INDEX IF NOT EXISTS horizon_cases_symbol
        ON horizon_cases(symbol,side,ts)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS horizon_outcomes (
        case_id INTEGER NOT NULL,
        horizon_h INTEGER NOT NULL,
        status TEXT NOT NULL,
        verdict TEXT,
        net_close_pct REAL,
        mfe_pct REAL,
        mae_pct REAL,
        sampled_ts REAL NOT NULL,
        PRIMARY KEY(case_id,horizon_h)
    )""")
    return conn


def _valid_price(price):
    try:
        result = float(price)
        return result if math.isfinite(result) and result > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def record(signal, now=None):
    """Journal one fresh, trend-aligned candidate; never make a trade decision."""
    now = time.time() if now is None else float(now)
    symbol = signal.get("symbol")
    decision = signal.get("decision") or {}
    side = {"UP": "LONG", "DOWN": "SHORT"}.get(decision.get("direction"))
    pattern = decision.get("pattern")
    price = _valid_price(signal.get("price"))
    if (not isinstance(symbol, str) or not symbol.endswith("USDT")
            or not side or not isinstance(pattern, str) or not price):
        return False
    # The newly refreshed quote must be recent, and timestamped before study.
    entry_time = signal.get("entry_observed_at")
    try:
        stamp = entry_time.timestamp() if entry_time is not None else now
        if not (now - 120 <= stamp <= now + 5):
            stamp = now
    except (TypeError, ValueError, OverflowError, AttributeError):
        stamp = now
    try:
        with closing(_conn()) as db:
            seen = db.execute("""SELECT 1 FROM horizon_cases
                WHERE symbol=? AND side=? AND ts>=? LIMIT 1""",
                (symbol,side,stamp-MIN_INDEPENDENT_SEC)).fetchone()
            if seen:
                return False
            db.execute("""INSERT INTO horizon_cases
                (ts,symbol,pattern,side,entry_price) VALUES(?,?,?,?,?)""",
                (stamp,symbol,pattern,side,price))
            db.commit()
            return True
    except (sqlite3.Error, OSError) as exc:
        print("[HORIZON_AUDIT_ERROR] stage=record kind=" +
              type(exc).__name__, flush=True)
        return False


def _analyze(rows, ts, due, side, entry):
    """Fully CLOSED 15m bars starting AFTER the signal's partial candle.

    End price is the last completed 15m close at/before deadline (up to 15m
    early). Gaps, unconfirmed candles and future bars invalidate the outcome.
    First touch cannot be proven for the omitted initial partial candle.
    """
    first = (int(ts // BAR_SEC) + 1) * BAR_SEC
    last = int(due // BAR_SEC) * BAR_SEC - BAR_SEC
    if last < first:
        return None
    bars = {}
    for row in rows or ():
        try:
            at = int(row[0]) // 1000
            op, high, low, close = map(float, row[1:5])
            if (str(row[8]) != "1" or not all(
                math.isfinite(x) and x > 0 for x in (op,high,low,close))
                    or not low <= min(op,close) <= max(op,close) <= high):
                continue
            if first <= at <= last:
                bars[at] = (op,high,low,close)
        except (ValueError, TypeError, IndexError, OverflowError):
            continue
    path = [bars.get(at) for at in range(first,last+1,BAR_SEC)]
    if not path or any(x is None for x in path):
        return None
    sign = 1 if side == "LONG" else -1
    target = entry * (1 + sign*gate.TP_PCT/100)
    stop = entry * (1 - sign*gate.SL_PCT/100)
    verdict = "NO_OBSERVED_TOUCH"
    mfe, mae = -float("inf"), float("inf")
    for op, high, low, close in path:
        favor = (high/entry - 1)*100 if sign == 1 else (1-low/entry)*100
        adverse = (low/entry - 1)*100 if sign == 1 else (1-high/entry)*100
        mfe = max(mfe, favor)
        mae = min(mae, adverse)
        tp_hit = high >= target if sign == 1 else low <= target
        sl_hit = low <= stop if sign == 1 else high >= stop
        if verdict == "NO_OBSERVED_TOUCH":
            if tp_hit and sl_hit:
                verdict = "AMBIGUOUS_SAME_BAR"
            elif tp_hit:
                verdict = "TP_OBSERVED_AFTER_GAP"
            elif sl_hit:
                verdict = "SL_OBSERVED_AFTER_GAP"
    close_net = sign * (path[-1][3]/entry - 1)*100 - gate.COST_PCT
    return verdict, round(close_net,5),round(mfe,5),round(mae,5)


def _cleanup(db, now):
    db.execute("DELETE FROM horizon_cases WHERE ts < ?",
               (now-RETENTION_SEC,))
    db.execute("""DELETE FROM horizon_cases WHERE id IN
        (SELECT id FROM horizon_cases ORDER BY ts DESC LIMIT -1 OFFSET ?)""",
        (MAX_CASES,))
    db.execute("""DELETE FROM horizon_outcomes WHERE NOT EXISTS
        (SELECT 1 FROM horizon_cases c WHERE c.id=horizon_outcomes.case_id)""")


def advance(now=None):
    """At most one historical request per scan, and no more than one / 120s."""
    global _last_request, _last_cleanup
    now = time.time() if now is None else float(now)
    try:
        with closing(_conn()) as db:
            if now - _last_cleanup >= 3600:
                _cleanup(db, now)
                db.commit()
                _last_cleanup = now
            tasks = db.execute("""WITH horizons(h) AS
                (VALUES(1),(4),(24),(48))
                SELECT c.id,c.ts,c.symbol,c.side,c.entry_price,
                       h.h,c.ts+h.h*3600 AS due
                FROM horizon_cases c CROSS JOIN horizons h
                LEFT JOIN horizon_outcomes o
                  ON o.case_id=c.id AND o.horizon_h=h.h
                WHERE o.case_id IS NULL AND c.ts+h.h*3600<=?
                ORDER BY due,c.id LIMIT 1""", (now-60,)).fetchall()
            if not tasks:
                return 0
            ident,ts,symbol,side,entry,hour,due = tasks[0]
            if now - due > MAX_LATENESS_SEC:
                db.execute("""INSERT OR IGNORE INTO horizon_outcomes
                    (case_id,horizon_h,status,sampled_ts)
                    VALUES(?,?,'MISSING_LATE',?)""",(ident,hour,now))
                db.commit()
                return 1
            if now - _last_request < MIN_REQUEST_GAP_SEC:
                return 0
            _last_request = now
            try:
                candles = gate._okx(symbol,"15m",200)
            except (requests.RequestException, ValueError, TypeError,
                    OverflowError) as exc:
                print("[HORIZON_AUDIT_ERROR] stage=history kind=" +
                      type(exc).__name__, flush=True)
                return 0
            result = _analyze(candles,ts,due,side,entry)
            if result is None:
                # Do not invent missing bars or evaluate with later prices.
                if now - due < 30*60:
                    return 0
                db.execute("""INSERT OR IGNORE INTO horizon_outcomes
                    (case_id,horizon_h,status,sampled_ts)
                    VALUES(?,?,'MISSING_BARS',?)""",(ident,hour,now))
            else:
                verdict,net,mfe,mae = result
                db.execute("""INSERT OR IGNORE INTO horizon_outcomes
                    (case_id,horizon_h,status,verdict,net_close_pct,
                     mfe_pct,mae_pct,sampled_ts)
                    VALUES(?,?,'OK',?,?,?,?,?)""",
                    (ident,hour,verdict,net,mfe,mae,now))
                print(f"[HORIZON_AUDIT] {symbol} side={side} h={hour} "
                      f"verdict={verdict} close_net={net:+.3f}% "
                      f"mfe={mfe:+.3f}% mae={mae:+.3f}% "
                      "model=15m_post_initial_gap_research_only",
                      flush=True)
            db.commit()
            return 1
    except (sqlite3.Error, OSError) as exc:
        print("[HORIZON_AUDIT_ERROR] stage=database kind=" +
              type(exc).__name__, flush=True)
        return 0


def hourly_report(now=None):
    global _last_report
    now = time.time() if now is None else float(now)
    if now-_last_report < 3600:
        return
    _last_report=now
    try:
        with closing(_conn()) as db:
            rows=db.execute("""SELECT horizon_h,status,COUNT(*)
                FROM horizon_outcomes GROUP BY horizon_h,status
                ORDER BY horizon_h,status""").fetchall()
        print(f"[HORIZON_AUDIT_SUMMARY] counts={rows} "
              f"horizons_hours={HORIZONS_H} max_cases={MAX_CASES} "
              "last_horizon_hours=48 research_only=true",
              flush=True)
    except (sqlite3.Error, OSError) as exc:
        print("[HORIZON_AUDIT_ERROR] stage=summary kind=" +
              type(exc).__name__, flush=True)
