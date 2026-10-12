"""Deferred, disk-backed observational checkpoints for PumpDump candidates.

NO new worker, NO stored candles, NO orders/Telegram, NO effect on trend gate.
A checkpoint compares the signal-time reference price with the CLOSE of the
first fully closed minute beginning AFTER its due time. It is NOT a first-touch
TP/SL outcome or a claim of executable profit. Existing 30m trade study stays
separate. Each scan fetches at most two historical candles from OKX.
"""
import math
import time
from contextlib import closing

import requests

import trend_probability_gate as gate

HORIZONS_H = (1, 4, 24, 72, 168)
MAX_REQUESTS_PER_SCAN = 2
RETRY_INTERVAL = 3600
MAX_ATTEMPTS = 5
MAX_RECORD_AGE = 25 * 86400
ROUNDTRIP_COST_PCT = 0.22
API = "https://www.okx.com/api/v5/market/history-candles"
_last_report = 0.0


def _setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS deferred_checkpoints (
        candidate_id INTEGER NOT NULL,
        horizon_h INTEGER NOT NULL,
        due_ts REAL NOT NULL,
        state TEXT NOT NULL DEFAULT 'PENDING',
        price REAL,
        gross_pct REAL,
        after_ref_cost_pct REAL,
        confirmed INTEGER,
        checked_ts REAL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_ts REAL NOT NULL DEFAULT 0,
        PRIMARY KEY(candidate_id, horizon_h)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_deferred_state
        ON deferred_checkpoints(state, next_retry_ts)""")


def _target_minute(due_ts):
    # Never use the price of an unfinished candle or round backwards.
    return (int(due_ts // 60) + 1) * 60


def _read_closed_close(symbol, minute_ts, now):
    """Read precisely one 1m bar from OKX historical API, never a nearby bar."""
    if not isinstance(symbol, str) or not symbol.endswith("USDT"):
        raise ValueError("INVALID_SYMBOL")
    response = requests.get(API, params={
        "instId": symbol[:-4] + "-USDT-SWAP",
        "bar": "1m",
        # OKX 'after' paginates to older candles. Include the target minute.
        "after": str((minute_ts + 60) * 1000 + 1),
        "limit": "5",
    }, timeout=8)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("code") != "0":
        raise ValueError("INVALID_RESPONSE")
    for row in payload.get("data", []):
        try:
            if (int(row[0]) != minute_ts * 1000 or str(row[8]) != "1"
                    or minute_ts + 60 > now):
                continue
            close = float(row[4])
            if math.isfinite(close) and close > 0:
                return close
        except (ValueError, TypeError, IndexError, OverflowError):
            continue
    raise ValueError("EXACT_CLOSED_MINUTE_NOT_AVAILABLE")


def _due(db, now):
    # Read from durable candidates, not from process memory.
    return db.execute("""WITH horizons(h) AS (
            VALUES (1),(4),(24),(72),(168)
        )
        SELECT c.id, c.ts, c.symbol, c.side, c.entry_price,
               h.h, c.ts + h.h * 3600.0,
               COALESCE(cp.attempts, 0)
        FROM candidates AS c
        CROSS JOIN horizons AS h
        LEFT JOIN deferred_checkpoints AS cp
               ON cp.candidate_id=c.id AND cp.horizon_h=h.h
        WHERE c.entry_price > 0
          AND c.ts >= ? AND c.ts <= ?
          AND c.ts + h.h * 3600.0 + 120 <= ?
          AND (cp.candidate_id IS NULL
               OR (cp.state='PENDING' AND cp.next_retry_ts <= ?))
        ORDER BY c.ts + h.h * 3600.0 ASC, c.id ASC
        LIMIT ?""", (now - MAX_RECORD_AGE, now, now, now,
                      MAX_REQUESTS_PER_SCAN)).fetchall()


def advance(now=None):
    """One bounded slice per existing scan; retry missing history, no lookahead."""
    now = time.time() if now is None else float(now)
    try:
        with closing(gate._db()) as db:
            _setup(db)
            # The gate already has a 2400-record/30-day cap; no leaked orphans.
            db.execute("""DELETE FROM deferred_checkpoints
                WHERE candidate_id NOT IN (SELECT id FROM candidates)""")
            due = _due(db, now)
            db.commit()
        processed = 0
        for ident, ts, symbol, side, entry, hours, due_ts, attempts in due:
            attempts += 1
            state, price, gross, net, confirmed = "PENDING", None, None, None, None
            minute = _target_minute(due_ts)
            try:
                price = _read_closed_close(symbol, minute, now)
                gross = (1 if side == "LONG" else -1) * (
                    price / float(entry) - 1.0) * 100.0
                net = gross - ROUNDTRIP_COST_PCT
                confirmed = int(net > 0)
                state = "DONE"
            except (requests.RequestException, ValueError, TypeError,
                    OverflowError, IndexError) as exc:
                if attempts >= MAX_ATTEMPTS:
                    state = "MISSING"
                print(f"[DEFERRED_CHECK_ERROR] {symbol} h={hours} "
                      f"attempt={attempts} state={state} "
                      f"kind={type(exc).__name__}", flush=True)
            with closing(gate._db()) as db:
                _setup(db)
                db.execute("""INSERT INTO deferred_checkpoints
                    (candidate_id,horizon_h,due_ts,state,price,gross_pct,
                     after_ref_cost_pct,confirmed,checked_ts,attempts,next_retry_ts)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(candidate_id,horizon_h) DO UPDATE SET
                        state=excluded.state, price=excluded.price,
                        gross_pct=excluded.gross_pct,
                        after_ref_cost_pct=excluded.after_ref_cost_pct,
                        confirmed=excluded.confirmed,
                        checked_ts=excluded.checked_ts,
                        attempts=excluded.attempts,
                        next_retry_ts=excluded.next_retry_ts""",
                    (ident, hours, due_ts, state, price, gross, net,
                     confirmed, now, attempts, now + RETRY_INTERVAL))
                db.commit()
            processed += 1
            if state == "DONE":
                print(f"[DEFERRED_CHECK] {symbol} side={side} h={hours} "
                      f"gross={gross:+.4f}% after_ref_cost={net:+.4f}% "
                      f"confirmed={bool(confirmed)} "
                      "model=checkpoint_close_not_first_touch_no_orders",
                      flush=True)
        return processed
    except (OSError, ValueError, TypeError, OverflowError) as exc:
        print(f"[DEFERRED_CHECK_ERROR] stage=advance kind={type(exc).__name__}",
              flush=True)
        return 0
    except Exception as exc:
        # A research failure must never break the live scan.
        print(f"[DEFERRED_CHECK_ERROR] stage=db kind={type(exc).__name__}",
              flush=True)
        return 0


def hourly_report(now=None):
    global _last_report
    now = time.time() if now is None else float(now)
    if now - _last_report < 3600:
        return
    _last_report = now
    try:
        with closing(gate._db()) as db:
            _setup(db)
            counts = db.execute("""SELECT horizon_h, state, COUNT(*),
                SUM(COALESCE(confirmed, 0))
                FROM deferred_checkpoints
                GROUP BY horizon_h,state ORDER BY horizon_h,state""").fetchall()
        print(f"[DEFERRED_SUMMARY] rows={counts} "
              "confirmed_means_checkpoint_close_after_reference_cost "
              "not_trade_win=true", flush=True)
    except Exception as exc:
        print(f"[DEFERRED_CHECK_ERROR] stage=report kind={type(exc).__name__}",
              flush=True)
