"""Persistent forward result tracker for live SWING entries.

Uses already-fetched market prices only. No exchange requests, no orders,
no Telegram. Checkpoints are observational markouts for strategy validation.
"""
import math
import os
import sqlite3
import time

CHECKPOINTS_H = (1, 4, 12, 24, 72)


def _db_path():
    return os.getenv("SWING_RESULT_DB", "/data/swing_results.db")


def _conn():
    path = _db_path()
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    db = sqlite3.connect(path, timeout=5)
    db.execute(
        """CREATE TABLE IF NOT EXISTS swing_entries(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            entry_at REAL NOT NULL,
            score INTEGER NOT NULL,
            UNIQUE(symbol, side, entry_at)
        )"""
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
    db.commit()
    return db


def register_entry(symbol, side, entry_price, score, now=None):
    now = time.time() if now is None else float(now)
    try:
        price = float(entry_price)
        score = int(score)
    except (TypeError, ValueError, OverflowError):
        return False
    if not symbol or side not in ("LONG", "SHORT"):
        return False
    if not math.isfinite(price) or price <= 0 or not math.isfinite(now):
        return False
    db = _conn()
    try:
        # One live entry sample per symbol/side per hour prevents duplicate
        # Telegram transitions from creating duplicate research cohorts.
        bucket = int(now // 3600) * 3600
        existing = db.execute(
            "SELECT id FROM swing_entries WHERE symbol=? AND side=? "
            "AND entry_at>=? AND entry_at<? LIMIT 1",
            (symbol, side, bucket, bucket + 3600),
        ).fetchone()
        if existing:
            return False
        db.execute(
            "INSERT INTO swing_entries(symbol,side,entry_price,entry_at,score) "
            "VALUES(?,?,?,?,?)",
            (symbol, side, price, now, score),
        )
        db.commit()
        return True
    finally:
        db.close()


def update_results(current_prices, now=None):
    now = time.time() if now is None else float(now)
    if not isinstance(current_prices, dict) or not math.isfinite(now):
        return []
    db = _conn()
    completed = []
    try:
        rows = db.execute(
            "SELECT id,symbol,side,entry_price,entry_at FROM swing_entries "
            "WHERE entry_at<=?",
            (now - 3600,),
        ).fetchall()
        for entry_id, symbol, side, entry, entry_at in rows:
            raw = current_prices.get(symbol)
            try:
                price = float(raw)
            except (TypeError, ValueError, OverflowError):
                continue
            if not math.isfinite(price) or price <= 0 or entry <= 0:
                continue
            sign = 1.0 if side == "LONG" else -1.0
            markout = sign * 100.0 * (price / entry - 1.0)
            for hours in CHECKPOINTS_H:
                due = entry_at + hours * 3600
                if now < due:
                    continue
                exists = db.execute(
                    "SELECT 1 FROM swing_results WHERE entry_id=? AND checkpoint_h=?",
                    (entry_id, hours),
                ).fetchone()
                if exists:
                    continue
                db.execute(
                    "INSERT INTO swing_results(entry_id,checkpoint_h,measured_at,price,return_pct) "
                    "VALUES(?,?,?,?,?)",
                    (entry_id, hours, now, price, markout),
                )
                completed.append({
                    "entry_id": entry_id,
                    "symbol": symbol,
                    "side": side,
                    "checkpoint_h": hours,
                    "price": price,
                    "return_pct": markout,
                })
        db.commit()
        return completed
    finally:
        db.close()
