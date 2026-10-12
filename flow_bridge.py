"""Research-only durable handoff of PumpDump candidates to Order Flow.

This queue never approves or blocks a trade. Consumers must verify that a
candidate is fresh and that the symbol exists on their exchange.
"""
import json
import math
import os
import sqlite3
import time

DB_PATH = os.getenv("PUMP_FLOW_BRIDGE_DB", "/data/pump_flow_bridge.sqlite3")
TTL_SECONDS = 120
MAX_ROWS = 5000


def enqueue_candidate(signal, now=None, db_path=None):
    now = time.time() if now is None else float(now)
    db_path = DB_PATH if db_path is None else db_path
    symbol = str(signal.get("symbol") or "").upper()
    direction = str((signal.get("decision") or {}).get("direction") or "")
    side = {"UP": "LONG", "DOWN": "SHORT"}.get(direction)
    try:
        price = float(signal.get("price"))
    except (TypeError, ValueError, OverflowError):
        return False
    if (not symbol.endswith("USDT") or not symbol[:-4].isalnum()
            or not side or not math.isfinite(price) or price <= 0):
        return False
    payload = {
        "schema": 1,
        "source": "pumpdump-radar",
        "symbol": symbol,
        "side": side,
        "observed_at": now,
        "expires_at": now + TTL_SECONDS,
        "price": price,
        "pattern": str((signal.get("decision") or {}).get("pattern") or "")[:80],
        "change_pct": signal.get("change"),
    }
    directory = os.path.dirname(db_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with sqlite3.connect(db_path, timeout=3) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS flow_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING'
        )""")
        db.execute("CREATE INDEX IF NOT EXISTS flow_candidates_pending ON flow_candidates(status, expires_at)")
        db.execute("DELETE FROM flow_candidates WHERE expires_at < ?", (now - 86400,))
        db.execute("""INSERT INTO flow_candidates
            (observed_at, expires_at, symbol, side, payload)
            VALUES (?, ?, ?, ?, ?)""",
            (now, now + TTL_SECONDS, symbol, side, json.dumps(payload, allow_nan=False)))
        db.execute("""DELETE FROM flow_candidates WHERE id IN (
            SELECT id FROM flow_candidates ORDER BY id DESC LIMIT -1 OFFSET ?
        )""", (MAX_ROWS,))
    return True


def pending_candidates(now=None, db_path=None, limit=20):
    """Local diagnostic reader; cross-service transport is a separate step."""
    now = time.time() if now is None else float(now)
    db_path = DB_PATH if db_path is None else db_path
    if not os.path.isfile(db_path):
        return []
    with sqlite3.connect(db_path, timeout=3) as db:
        rows = db.execute("""SELECT payload FROM flow_candidates
            WHERE status='PENDING' AND expires_at>=?
            ORDER BY id DESC LIMIT ?""", (now, max(1, min(int(limit), 100)))).fetchall()
    return [json.loads(row[0]) for row in rows]


def deliver_candidate(signal, now=None):
    """Best-effort HTTPS delivery; never gates the PumpDump scan."""
    import requests
    url = os.getenv("PUMP_FLOW_URL", "").strip()
    token = os.getenv("PUMP_FLOW_SHARED_TOKEN", "")
    if not url or not token or not url.startswith("https://"):
        return False, "NOT_CONFIGURED"
    now = time.time() if now is None else float(now)
    symbol = str(signal.get("symbol") or "").upper()
    side = {"UP": "LONG", "DOWN": "SHORT"}.get(
        str((signal.get("decision") or {}).get("direction") or ""))
    if not side or not symbol.endswith("USDT"):
        return False, "INVALID_SIGNAL"
    try:
        price = float(signal.get("price"))
        if not math.isfinite(price) or price <= 0:
            return False, "INVALID_PRICE"
        payload = {"schema": 1, "source": "pumpdump-radar",
                   "symbol": symbol, "side": side, "price": price,
                   "observed_at": now, "expires_at": now + TTL_SECONDS,
                   "pattern": str((signal.get("decision") or {}).get("pattern") or "")[:80]}
        response = requests.post(url, json=payload,
                                 headers={"X-Pump-Flow-Token": token}, timeout=2.5)
        return response.status_code == 202, str(response.status_code)
    except Exception as exc:
        return False, type(exc).__name__
