"""Persistent SWING Telegram alert deduplication.

Keeps the last delivered bias/score per symbol in SQLite so Railway restarts do
not resend the same medium-term idea inside the cooldown window.
"""
import os
import sqlite3
import threading
import time

_LOCK = threading.RLock()


def _db_path():
    explicit = os.getenv("SWING_ALERT_DB")
    if explicit:
        return explicit
    if os.path.isdir("/data"):
        return "/data/swing_alert_state.sqlite3"
    return "swing_alert_state.sqlite3"


def _connect():
    conn = sqlite3.connect(_db_path(), timeout=5)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS swing_alert_state (
            symbol TEXT PRIMARY KEY,
            bias TEXT NOT NULL,
            score REAL NOT NULL,
            sent_at REAL NOT NULL,
            entry_state TEXT NOT NULL DEFAULT 'UNKNOWN'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS swing_compression_alert_state (
            symbol TEXT PRIMARY KEY,
            range_low REAL NOT NULL,
            range_high REAL NOT NULL,
            strength REAL NOT NULL,
            sent_at REAL NOT NULL
        )
        """
    )
    cols={row[1] for row in conn.execute("PRAGMA table_info(swing_alert_state)")}
    if "entry_state" not in cols:
        conn.execute(
            "ALTER TABLE swing_alert_state ADD COLUMN entry_state TEXT NOT NULL DEFAULT 'UNKNOWN'"
        )
    return conn


def alert_allowed(symbol, bias, score, cooldown_sec, now=None, entry_state="UNKNOWN"):
    now = time.time() if now is None else float(now)
    with _LOCK:
        with _connect() as conn:
            row = conn.execute(
                "SELECT bias, score, sent_at, entry_state FROM swing_alert_state WHERE symbol=?",
                (str(symbol),),
            ).fetchone()
    if row is None:
        return True
    old_bias, old_score, sent_at, old_entry_state = row
    if old_bias != bias:
        return True
    if str(old_entry_state) != str(entry_state):
        return True
    if abs(float(score) - float(old_score)) >= 8:
        return True
    return now - float(sent_at) >= int(cooldown_sec)


def mark_alert(symbol, bias, score, now=None, entry_state="UNKNOWN"):
    now = time.time() if now is None else float(now)
    with _LOCK:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO swing_alert_state(symbol, bias, score, sent_at, entry_state)
                VALUES(?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    bias=excluded.bias,
                    score=excluded.score,
                    sent_at=excluded.sent_at,
                    entry_state=excluded.entry_state
                """,
                (str(symbol), str(bias), float(score), now, str(entry_state)),
            )


def compression_alert_allowed(
    symbol, range_low, range_high, strength, cooldown_sec, now=None
):
    now = time.time() if now is None else float(now)
    low = float(range_low)
    high = float(range_high)
    strength = float(strength)
    if low <= 0 or high <= low:
        return False

    with _LOCK:
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT range_low, range_high, strength, sent_at
                FROM swing_compression_alert_state
                WHERE symbol=?
                """,
                (str(symbol),),
            ).fetchone()
    if row is None:
        return True

    old_low, old_high, old_strength, sent_at = map(float, row)
    if now - sent_at >= int(cooldown_sec):
        return True

    low_shift = abs(low / old_low - 1.0) * 100.0 if old_low > 0 else 100.0
    high_shift = abs(high / old_high - 1.0) * 100.0 if old_high > 0 else 100.0
    if max(low_shift, high_shift) >= 1.0:
        return True
    if strength - old_strength >= 15.0:
        return True
    return False


def mark_compression_alert(
    symbol, range_low, range_high, strength, now=None
):
    now = time.time() if now is None else float(now)
    with _LOCK:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO swing_compression_alert_state(
                    symbol, range_low, range_high, strength, sent_at
                )
                VALUES(?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    range_low=excluded.range_low,
                    range_high=excluded.range_high,
                    strength=excluded.strength,
                    sent_at=excluded.sent_at
                """,
                (
                    str(symbol),
                    float(range_low),
                    float(range_high),
                    float(strength),
                    now,
                ),
            )


def clear_state():
    with _LOCK:
        with _connect() as conn:
            conn.execute("DELETE FROM swing_alert_state")
            conn.execute("DELETE FROM swing_compression_alert_state")
