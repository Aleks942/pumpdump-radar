"""Silent pump lifecycle and causal reversal-candidate research.

Only chronological sampled OKX swap prices. Does not infer OI, Flow, or
seller identity. Does NOT send Telegram, approve SHORT, or alter scanners.
A candidate is not 'REVERSAL CONFIRMED': independent Flow/structure needed.
"""
import json
import math
import os
import sqlite3
import time

DB_PATH = os.getenv("MARKET_MEMORY_DB", "market_memory.db")
MAX_ACTIVE = 250
WATCH_SECONDS = 4 * 3600
MIN_SAMPLE_SECONDS = 20
MIN_RETRACE_PCT = 0.40
MIN_BREAK_PCT = 0.15
STALE_GAP_SECONDS = 180


def _good(x):
    try:
        y = float(x)
        return y if math.isfinite(y) and y > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS pump_lifecycle (
        symbol TEXT PRIMARY KEY, start_ts REAL NOT NULL,
        updated_ts REAL NOT NULL, anchor_price REAL NOT NULL,
        peak_price REAL NOT NULL, current_price REAL NOT NULL,
        prior_low REAL NOT NULL, state TEXT NOT NULL,
        weak_ts REAL, break_ts REAL, observations INTEGER NOT NULL,
        falling_steps INTEGER NOT NULL, move_pct REAL NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS pump_lifecycle_events (
        id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, ts REAL NOT NULL,
        state TEXT NOT NULL, price REAL NOT NULL,
        peak_price REAL NOT NULL, retrace_pct REAL NOT NULL,
        note TEXT NOT NULL
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_pump_lifecycle_event_ts
        ON pump_lifecycle_events(ts)""")


def _event(db, symbol, now, state, price, peak, note):
    retrace = (1-price/peak)*100
    db.execute("""INSERT INTO pump_lifecycle_events
        (symbol,ts,state,price,peak_price,retrace_pct,note)
        VALUES (?,?,?,?,?,?,?)""",
        (symbol,now,state,price,peak,round(retrace,5),note))


def observe(rows, threshold_pct, now=None, db_path=None):
    """Maintain bounded study episodes; return new research-only events."""
    now = time.time() if now is None else float(now)
    threshold = _good(threshold_pct)
    if not math.isfinite(now) or threshold is None:
        return []
    db_path = DB_PATH if db_path is None else db_path
    incoming = {}
    for row in rows or ():
        if not isinstance(row, dict):
            continue
        symbol = row.get("symbol")
        price = _good(row.get("price"))
        if not isinstance(symbol, str) or not symbol.endswith("USDT") or price is None:
            continue
        try:
            move = float(row.get("change5"))
            age = float(row.get("sample_age_sec"))
        except (ValueError, TypeError, OverflowError):
            continue
        if math.isfinite(move) and math.isfinite(age) and 240 <= age <= 420:
            incoming[symbol] = (price, move)

    changes = []
    try:
        with sqlite3.connect(db_path, timeout=5) as db:
            _schema(db)
            db.execute("DELETE FROM pump_lifecycle WHERE updated_ts < ?",
                       (now-STALE_GAP_SECONDS,))
            db.execute("DELETE FROM pump_lifecycle_events WHERE ts < ?",
                       (now-30*86400,))
            active = {r[0]: r for r in db.execute(
                "SELECT symbol,start_ts,updated_ts,anchor_price,peak_price,"
                "current_price,prior_low,state,weak_ts,break_ts,observations,"
                "falling_steps,move_pct FROM pump_lifecycle")}
            for symbol, (price, move) in incoming.items():
                old = active.get(symbol)
                if old is None:
                    if move < threshold or len(active) >= MAX_ACTIVE:
                        continue
                    # The real trigger is the sampled ~5m price change.
                    # Anchor is algebraic and not reconstructed from future data.
                    anchor = price/(1+move/100)
                    db.execute("""INSERT INTO pump_lifecycle
                        (symbol,start_ts,updated_ts,anchor_price,peak_price,
                         current_price,prior_low,state,observations,
                         falling_steps,move_pct)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (symbol,now,now,anchor,price,price,price,
                         "PUMP_START",1,0,move))
                    active[symbol] = (symbol,now,now,anchor,price,price,
                                      price,"PUMP_START",None,None,1,0,move)
                    _event(db,symbol,now,"PUMP_START",price,price,
                           "observed_5m_threshold_research_only")
                    changes.append((symbol,"PUMP_START"))
                    continue
                (_,started,updated,anchor,peak,previous,prior_low,
                 state,weak_ts,break_ts,samples,falls,initial_move) = old
                if now - started > WATCH_SECONDS or now-updated < MIN_SAMPLE_SECONDS:
                    continue
                # A new high resumes continuation, invalidating a weakening
                # hypothesis; never keep 'reversal' after price regains peak.
                if price > peak:
                    peak = price
                    state,falls,weak_ts,break_ts = "PUMP_CONTINUATION",0,None,None
                    prior_low = min(previous,price)
                elif state == "PUMP_CONTINUATION" and price >= previous:
                    # A higher low is observable only after post-start samples.
                    prior_low = min(prior_low,previous)
                    falls = 0
                else:
                    falls = falls+1 if price < previous else 0
                retrace = 100*(1-price/peak)
                # Only an *observed* prior local floor, never a future low.
                if (state in ("PUMP_START","PUMP_CONTINUATION") and samples >= 3
                        and falls >= 2 and retrace >= MIN_RETRACE_PCT):
                    state,weak_ts = "PUMP_WEAKENING",now
                    _event(db,symbol,now,state,price,peak,
                           "two_lower_samples_and_retrace_no_short")
                    changes.append((symbol,state))
                elif (state == "PUMP_WEAKENING" and weak_ts is not None
                      and now-weak_ts >= MIN_SAMPLE_SECONDS and falls >= 3
                      and price < prior_low*(1-MIN_BREAK_PCT/100)):
                    state,break_ts = "PRICE_BREAK_CANDIDATE",now
                    _event(db,symbol,now,state,price,peak,
                           "observed_floor_break_without_flow_confirmation")
                    changes.append((symbol,state))
                # After a snapback above prior floor a candidate is voided;
                # this avoids declaring a permanent reversal from one wick.
                elif (state == "PRICE_BREAK_CANDIDATE" and price > prior_low):
                    state,falls,break_ts = "PUMP_WEAKENING",0,None
                    _event(db,symbol,now,"PRICE_BREAK_INVALIDATED",price,peak,
                           "reclaimed_prebreak_floor")
                    changes.append((symbol,"PRICE_BREAK_INVALIDATED"))
                db.execute("""UPDATE pump_lifecycle SET updated_ts=?,
                    peak_price=?,current_price=?,prior_low=?,state=?,
                    weak_ts=?,break_ts=?,observations=?,falling_steps=?
                    WHERE symbol=?""",(now,peak,price,prior_low,state,
                                     weak_ts,break_ts,samples+1,falls,symbol))
            db.execute("""DELETE FROM pump_lifecycle_events WHERE id IN (
                SELECT id FROM pump_lifecycle_events ORDER BY id DESC
                LIMIT -1 OFFSET 3000)""")
        for symbol,state in changes[:15]:
            print("[PUMP_LIFECYCLE]",symbol,"state=",state,
                  "source=sampled_OKX_quotes research_only=true "
                  "no_trade_approval=true",flush=True)
    except (sqlite3.Error, ValueError, TypeError, OverflowError) as exc:
        print("[PUMP_LIFECYCLE_ERROR]",type(exc).__name__,flush=True)
        return []
    return changes
