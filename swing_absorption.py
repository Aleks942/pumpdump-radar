"""Research-only journal for possible post-pump buy absorption."""
from contextlib import closing
import sqlite3
import time

from bybit_taker_stream import get_flow
from swing_result_tracker import VALIDATION_VERSION, _db_path
from swing_strength_engine import _entry_setup

HOUR = 3600
MAX_SETUP_AGE = 6 * HOUR
MIN_DELTA_PCT = 10


def detect_watch(hourly, oi_points):
    """Three tight hours near the high after a six-hour impulse."""
    if len(hourly) < 21:
        return None
    window = hourly[-21:]
    if any(right["ts"] - left["ts"] != HOUR for left, right in zip(window, window[1:])):
        return None
    baseline, impulse, plateau = window[:12], window[12:18], window[18:]
    high = max(row["h"] for row in plateau)
    low = min(row["l"] for row in plateau)
    midpoint = (high + low) / 2
    range_pct = 100 * (high - low) / midpoint
    pump_pct = 100 * (impulse[-1]["c"] / impulse[0]["o"] - 1)
    progress_pct = 100 * (plateau[-1]["c"] / plateau[0]["o"] - 1)
    normal_volume = sum(row["v"] for row in baseline) / len(baseline)
    if normal_volume <= 0:
        return None
    volume_ratio = sum(row["v"] for row in plateau) / (3 * normal_volume)
    if (pump_pct < 5 or not 0 < range_pct <= 3 or abs(progress_pct) > 1
            or low < max(row["h"] for row in impulse) * 0.98 or volume_ratio < 1.25):
        return None
    start = plateau[0]["ts"]
    end = plateau[-1]["ts"] + HOUR
    interest = dict(oi_points)
    if start not in interest or end not in interest or interest[start] <= 0:
        return None
    oi_pct = 100 * (interest[end] / interest[start] - 1)
    if oi_pct < 0.5:
        return None
    return {"closed_at": end, "range_low": low, "range_high": high, "pump_pct": pump_pct,
            "range_pct": range_pct, "volume_ratio": volume_ratio, "oi_pct": oi_pct}


def _connect():
    db = sqlite3.connect(_db_path(), timeout=0.15)
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS swing_absorption (
        symbol TEXT NOT NULL, closed_at REAL NOT NULL,
        range_low REAL NOT NULL, range_high REAL NOT NULL,
        pump_pct REAL NOT NULL, range_pct REAL NOT NULL,
        volume_ratio REAL NOT NULL, oi_pct REAL NOT NULL,
        phase TEXT NOT NULL, delta_pct REAL, delta_at REAL,
        positive_delta_at REAL, breakdown_at REAL, retest_at REAL,
        entry_id INTEGER UNIQUE, PRIMARY KEY (symbol, closed_at)
    )""")
    return db


def record_observation(symbol, result, now=None, flow_get=get_flow):
    now = time.time() if now is None else now
    hourly = result.get("_research_h1")
    if not hourly:
        return
    with closing(_connect()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "UPDATE swing_absorption SET phase='EXPIRED' "
            "WHERE symbol=? AND phase IN ('WATCH','BREAKDOWN') AND closed_at<=?",
            (symbol, now - MAX_SETUP_AGE),
        )
        row = db.execute(
            "SELECT * FROM swing_absorption WHERE symbol=? AND phase IN ('WATCH','BREAKDOWN') "
            "ORDER BY closed_at DESC LIMIT 1", (symbol,),
        ).fetchone()
        if row is None:
            watch = detect_watch(hourly, (result.get("oi4h") or {}).get("samples", ()))
            if watch is None or not 0 <= now - watch["closed_at"] < HOUR:
                return
            db.execute(
                "INSERT OR IGNORE INTO swing_absorption "
                "(symbol, closed_at, range_low, range_high, pump_pct, range_pct, volume_ratio, oi_pct, phase) "
                "VALUES (:symbol,:closed_at,:range_low,:range_high,:pump_pct,:range_pct,:volume_ratio,:oi_pct,'WATCH')",
                dict(watch, symbol=symbol),
            )
            return

        low, high = row["range_low"], row["range_high"]
        key = (symbol, row["closed_at"])
        breakdown_at = row["breakdown_at"]
        for candle in hourly:
            close_at = candle["ts"] + HOUR
            if not row["closed_at"] < close_at <= now:
                continue
            if candle["c"] > high or (breakdown_at is not None and close_at > breakdown_at and candle["c"] >= low):
                db.execute("UPDATE swing_absorption SET phase='INVALIDATED' WHERE symbol=? AND closed_at=?", key)
                return
            if breakdown_at is None and candle["c"] < low:
                breakdown_at = close_at
                db.execute(
                    "UPDATE swing_absorption SET phase='BREAKDOWN', breakdown_at=? WHERE symbol=? AND closed_at=?",
                    (breakdown_at, *key),
                )

        if breakdown_at is not None:
            setup = _entry_setup(result.get("_research_m15") or [], "BEARISH", {
                "phase": "BREAKOUT_CONFIRMED", "level": low, "closed_at": breakdown_at,
            })
            if setup["phase"] == "READY_ENTRY":
                db.execute(
                    "UPDATE swing_absorption SET phase='RETEST', retest_at=? WHERE symbol=? AND closed_at=?",
                    (result["_research_m15"][-1]["ts"] + 900, *key),
                )
            return

        price = result.get("price")
        if price is None or not low <= price <= high:
            return
        flow = flow_get(symbol, category="linear", now=now, since=row["closed_at"])
        if flow.get("ready") is not True:
            return
        delta = flow["imbalance_pct"]
        db.execute(
            "UPDATE swing_absorption SET delta_pct=?, delta_at=?, "
            "positive_delta_at=CASE WHEN ?>=? THEN ? ELSE positive_delta_at END "
            "WHERE symbol=? AND closed_at=? AND (delta_at IS NULL OR delta_at<?)",
            (delta, now, delta, MIN_DELTA_PCT, now, *key, now),
        )


def link_short_entry(symbol, setup_at):
    """Link a registered SHORT only if both setups used the same breakdown hour."""
    with closing(_connect()) as db, db:
        db.execute("""
            UPDATE swing_absorption SET entry_id=(
                SELECT id FROM swing_entries WHERE symbol=? AND side='SHORT'
                AND strategy_version=? AND setup_at=?
                AND entry_at>=swing_absorption.retest_at
            )
            WHERE symbol=? AND breakdown_at=? AND phase='RETEST' AND entry_id IS NULL
              AND closed_at=(SELECT MAX(closed_at) FROM swing_absorption
                             WHERE symbol=? AND breakdown_at=? AND phase='RETEST')
        """, (symbol, VALIDATION_VERSION, setup_at, symbol, setup_at, symbol, setup_at))
