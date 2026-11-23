"""Persistent forward result tracker for live SWING entries.

Uses already-fetched market prices only. No orders.
Stores:
- fixed 1/4/12/24/72h markouts;
- observed touches of SL / TP1 / TP2 from the live scan stream;
- one active sample per symbol/side so the same READY_ENTRY is not counted
  again every hour.
"""
import math
import os
import sqlite3
import time

CHECKPOINTS_H = (1, 4, 12, 24, 72)
ACTIVE_TTL_SEC = 72 * 3600
MAX_MARKOUT_DELAY_SEC = 15 * 60
VALIDATION_VERSION = "swing-retest-2026-11-23"


def _db_path():
    return os.getenv("SWING_RESULT_DB") or (
        "/data/swing_results.db" if os.path.isdir("/data") else "swing_results.db"
    )


def _conn():
    path = _db_path()
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    db = sqlite3.connect(path, timeout=5)
    db.execute("BEGIN IMMEDIATE")
    db.execute(
        """CREATE TABLE IF NOT EXISTS swing_entries(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            entry_at REAL NOT NULL,
            score INTEGER NOT NULL,
            stop REAL,
            tp1 REAL,
            tp2 REAL,
            UNIQUE(symbol, side, entry_at)
        )"""
    )
    # Backward-compatible migration for existing production DBs.
    cols={row[1] for row in db.execute("PRAGMA table_info(swing_entries)").fetchall()}
    for name in ("stop","tp1","tp2"):
        if name not in cols:
            db.execute(f"ALTER TABLE swing_entries ADD COLUMN {name} REAL")
    if "strategy_version" not in cols:
        db.execute("ALTER TABLE swing_entries ADD COLUMN strategy_version TEXT")

    if "setup_at" not in cols:
        db.execute("ALTER TABLE swing_entries ADD COLUMN setup_at REAL")
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS swing_entry_setup "
        "ON swing_entries(symbol, side, strategy_version, setup_at)"
    )

    db.execute(
        """CREATE TABLE IF NOT EXISTS swing_results(
            entry_id INTEGER NOT NULL,
            checkpoint_h INTEGER NOT NULL,
            measured_at REAL NOT NULL,
            price REAL NOT NULL,
            return_pct REAL NOT NULL,
            PRIMARY KEY(entry_id, checkpoint_h)
        )"""
    )
    db.execute(
        """CREATE TABLE IF NOT EXISTS swing_events(
            entry_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            observed_at REAL NOT NULL,
            price REAL NOT NULL,
            r_multiple REAL,
            PRIMARY KEY(entry_id, event_type)
        )"""
    )
    db.commit()
    return db


def _num(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError, OverflowError):
        return None


def register_entry(symbol, side, entry_price, score, now=None, trade_plan=None, strategy_version=VALIDATION_VERSION, setup_at=None):
    now = time.time() if now is None else float(now)
    price=_num(entry_price)
    try:
        score=int(score)
    except (TypeError, ValueError, OverflowError):
        return False
    if not symbol or side not in ("LONG","SHORT"):
        return False
    if price is None or price<=0 or not math.isfinite(now):
        return False

    plan=trade_plan or {}
    stop=_num(plan.get("stop"))
    tp1=_num(plan.get("tp1"))
    tp2=_num(plan.get("tp2"))
    if trade_plan is not None:
        if plan.get("ready") is not True:
            return False
        if side=="LONG":
            valid=stop is not None and tp1 is not None and tp2 is not None and stop<price<tp1<tp2
        else:
            valid=stop is not None and tp1 is not None and tp2 is not None and stop>price>tp1>tp2
        if not valid or min(stop, tp1, tp2) <= 0:
            return False

    db=_conn()
    try:
        db.execute("BEGIN IMMEDIATE")
        # A READY_ENTRY can persist for many scans. Count one active setup only.
        active_since=now-ACTIVE_TTL_SEC
        existing=db.execute(
            "SELECT id FROM swing_entries WHERE symbol=? AND side=? AND entry_at>=? AND strategy_version=? "
            "AND NOT EXISTS (SELECT 1 FROM swing_events e "
            "WHERE e.entry_id=swing_entries.id AND e.event_type IN ('STOP','TP2')) "
            "ORDER BY entry_at DESC LIMIT 1",
            (symbol,side,active_since,strategy_version),
        ).fetchone()
        if existing:
            return False

        inserted = db.execute(
            "INSERT INTO swing_entries(symbol,side,entry_price,entry_at,score,stop,tp1,tp2,strategy_version,setup_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
            (symbol,side,price,now,score,stop,tp1,tp2,strategy_version,setup_at),
        ).rowcount
        db.commit()
        return inserted == 1
    finally:
        db.close()


def current_setup_plan(symbol, side, setup_at):
    """Return the journaled entry prices for an active setup Telegram may retry."""
    db = _conn()
    try:
        row = db.execute(
            "SELECT entry_price, stop, tp1, tp2 FROM swing_entries "
            "WHERE symbol=? AND side=? AND strategy_version=? AND setup_at=? "
            "AND entry_at > ? AND NOT EXISTS (SELECT 1 FROM swing_events "
            "WHERE entry_id=swing_entries.id AND event_type IN ('STOP','TP2','TIME'))",
            (symbol, side, VALIDATION_VERSION, setup_at, time.time() - ACTIVE_TTL_SEC),
        ).fetchone()
    finally:
        db.close()
    if row is None:
        return None
    entry, stop, tp1, tp2 = row
    return {"ready": True, "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
            "stop_pct": 100 * abs(entry - stop) / entry}


def _event_if_new(db, entry_id, event_type, now, price, r_multiple):
    exists=db.execute(
        "SELECT 1 FROM swing_events WHERE entry_id=? AND event_type=?",
        (entry_id,event_type),
    ).fetchone()
    if exists:
        return None
    db.execute(
        "INSERT INTO swing_events(entry_id,event_type,observed_at,price,r_multiple) "
        "VALUES(?,?,?,?,?)",
        (entry_id,event_type,now,price,r_multiple),
    )
    return {
        "entry_id":entry_id,
        "event_type":event_type,
        "observed_at":now,
        "price":price,
        "r_multiple":r_multiple,
    }


def update_results(current_prices, now=None, legacy_prices=None):
    now=time.time() if now is None else float(now)
    if not isinstance(current_prices,dict) or not math.isfinite(now):
        return []
    db=_conn()
    completed=[]
    try:
        db.execute("BEGIN IMMEDIATE")
        rows=db.execute(
            "SELECT id,symbol,side,entry_price,entry_at,stop,tp1,tp2,strategy_version, "
            "EXISTS (SELECT 1 FROM swing_events WHERE entry_id=swing_entries.id "
            "AND event_type IN ('STOP','TP2','TIME')) "
            "FROM swing_entries WHERE entry_at BETWEEN ? AND ?",
            (now - ACTIVE_TTL_SEC - MAX_MARKOUT_DELAY_SEC, now),
        ).fetchall()

        for entry_id,symbol,side,entry,entry_at,stop,tp1,tp2,version,terminal in rows:
            prices = legacy_prices if legacy_prices is not None and version != VALIDATION_VERSION else current_prices
            price=_num(prices.get(symbol))
            if price is None or price<=0 or entry<=0:
                continue

            sign=1.0 if side=="LONG" else -1.0
            markout=sign*100.0*(price/entry-1.0)

            # Record observed plan touches from the same live price stream.
            stop=_num(stop); tp1=_num(tp1); tp2=_num(tp2)
            if not terminal and stop is not None and tp1 is not None and tp2 is not None:
                risk = abs(entry - stop)
                elapsed = now - entry_at
                if risk <= 0:
                    checks = ()
                elif elapsed >= ACTIVE_TTL_SEC:
                    checks = (("TIME", True, sign * (price - entry) / risk),)
                else:
                    checks = (
                        ("STOP", sign * (price - stop) <= 0, -1.0),
                        ("TP1", sign * (price - tp1) >= 0, sign * (tp1 - entry) / risk),
                        ("TP2", sign * (price - tp2) >= 0, sign * (tp2 - entry) / risk),
                    )
                for event_type, hit, r_mult in checks:
                    if hit:
                        event = _event_if_new(db, entry_id, event_type, now, price, r_mult)
                        if event:
                            completed.append({"kind": "EVENT", "symbol": symbol, "side": side, **event})

            # Keep independent horizon markouts for research.
            if now-entry_at < 3600:
                continue
            for hours in CHECKPOINTS_H:
                due=entry_at+hours*3600
                if not due <= now <= due + MAX_MARKOUT_DELAY_SEC:
                    continue
                exists=db.execute(
                    "SELECT 1 FROM swing_results WHERE entry_id=? AND checkpoint_h=?",
                    (entry_id,hours),
                ).fetchone()
                if exists:
                    continue
                db.execute(
                    "INSERT INTO swing_results(entry_id,checkpoint_h,measured_at,price,return_pct) "
                    "VALUES(?,?,?,?,?)",
                    (entry_id,hours,now,price,markout),
                )
                completed.append({
                    "kind":"MARKOUT","entry_id":entry_id,"symbol":symbol,
                    "side":side,"checkpoint_h":hours,"price":price,
                    "return_pct":markout,
                })
        db.commit()
        return completed
    finally:
        db.close()
