import math
import threading
import time
from contextlib import closing

import requests
from market_memory import get_connection

_start_lock = threading.Lock()
_thread = None


def initialize():
    with closing(get_connection()) as db:
        with db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS candle_path_meta (
                    key TEXT PRIMARY KEY,
                    value REAL NOT NULL
                )
            """)

            db.execute("""
                INSERT OR IGNORE INTO candle_path_meta
                VALUES ('start_ts_v1', ?)
            """, (time.time(),))

            db.execute("""
                CREATE TABLE IF NOT EXISTS candle_path_jobs (
                    context_id TEXT PRIMARY KEY,
                    first_ms INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    next_try REAL NOT NULL DEFAULT 0,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                )
            """)

            db.execute("""
                CREATE TABLE IF NOT EXISTS entry_candles_1m (
                    context_id TEXT NOT NULL,
                    open_ms INTEGER NOT NULL,
                    o REAL NOT NULL,
                    h REAL NOT NULL,
                    l REAL NOT NULL,
                    c REAL NOT NULL,
                    fetched_at REAL NOT NULL,
                    PRIMARY KEY (context_id, open_ms)
                )
            """)

        return db.execute(
            "SELECT value FROM candle_path_meta "
            "WHERE key='start_ts_v1'"
        ).fetchone()[0]


def parse_candles(rows, first_ms):
    expected = set(range(
        first_ms,
        first_ms + 1800000,
        60000,
    ))

    found = {}

    for row in rows:
        ts = int(row[0])

        if ts not in expected:
            continue

        if len(row) < 9 or str(row[8]) != "1":
            continue

        o, h, l, c = map(float, row[1:5])

        if not all(
            math.isfinite(v) and v > 0
            for v in (o, h, l, c)
        ):
            raise ValueError("invalid OHLC")

        if not l <= min(o, c) <= max(o, c) <= h:
            raise ValueError("inconsistent OHLC")

        value = (ts, o, h, l, c)

        if ts in found and found[ts] != value:
            raise ValueError("conflicting candles")

        found[ts] = value

    if set(found) != expected:
        raise ValueError(
            "closed candles: %s/30" % len(found)
        )

    return [found[ts] for ts in sorted(found)]


def collect_once(session, start_ts):
    now = time.time()

    with closing(get_connection()) as db:
        exists = db.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='entry_contexts'"
        ).fetchone()

        if not exists:
            return

        contexts = db.execute("""
            SELECT context_id, entry_ts
            FROM entry_contexts
            WHERE entry_ts >= ?
              AND entry_ts <= ?
        """, (start_ts, now - 1870)).fetchall()

        with db:
            for context_id, entry_ts in contexts:
                first_ms = math.ceil(entry_ts / 60) * 60000

                db.execute("""
                    INSERT OR IGNORE INTO candle_path_jobs
                    (context_id, first_ms)
                    VALUES (?, ?)
                """, (context_id, first_ms))

        jobs = db.execute("""
            SELECT j.context_id, j.first_ms, c.symbol
            FROM candle_path_jobs j
            JOIN entry_contexts c
              ON c.context_id = j.context_id
            WHERE j.status != 'READY'
              AND j.next_try <= ?
            ORDER BY j.next_try, j.first_ms
            LIMIT 10
        """, (now,)).fetchall()

    for context_id, first_ms, symbol in jobs:
        try:
            if not symbol.endswith("USDT") or "-" in symbol:
                raise ValueError(
                    "unsupported symbol: " + symbol
                )

            inst_id = symbol[:-4] + "-USDT-SWAP"

            response = session.get(
                "https://www.okx.com/api/v5/market/history-candles",
                params={
                    "instId": inst_id,
                    "bar": "1m",
                    "limit": "100",
                    "after": str(first_ms + 1800000),
                },
                timeout=(5, 15),
            )

            response.raise_for_status()
            data = response.json()

            if data.get("code") != "0":
                raise ValueError(
                    "OKX code=" + str(data.get("code"))
                )

            candles = parse_candles(
                data.get("data") or [],
                first_ms,
            )

            fetched_at = time.time()

            with closing(get_connection()) as db:
                with db:
                    db.executemany("""
                        INSERT OR REPLACE INTO entry_candles_1m
                        (
                            context_id, open_ms,
                            o, h, l, c, fetched_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [
                        (context_id, *candle, fetched_at)
                        for candle in candles
                    ])

                    db.execute("""
                        UPDATE candle_path_jobs
                        SET status = 'READY',
                            attempts = attempts + 1,
                            last_error = NULL
                        WHERE context_id = ?
                    """, (context_id,))

            print(
                "[CANDLE_PATH_SAVED]",
                symbol,
                "id=", context_id,
                "candles=30",
                "first_ms=", first_ms,
                flush=True,
            )

        except Exception as error:
            message = (
                type(error).__name__ + ": " + str(error)
            )

            with closing(get_connection()) as db:
                with db:
                    db.execute("""
                        UPDATE candle_path_jobs
                        SET status = 'RETRY',
                            attempts = attempts + 1,
                            next_try = ?,
                            last_error = ?
                        WHERE context_id = ?
                    """, (
                        time.time() + 300,
                        message[:500],
                        context_id,
                    ))

            print(
                "[CANDLE_PATH_RETRY]",
                symbol,
                message,
                flush=True,
            )

        finally:
            time.sleep(0.3)


def run_candle_path():
    start_ts = None

    with requests.Session() as session:
        while True:
            try:
                if start_ts is None:
                    start_ts = initialize()

                    print(
                        "[CANDLE_PATH_STARTED]",
                        "start_ts=", start_ts,
                        "closed_1m=30",
                        flush=True,
                    )

                collect_once(session, start_ts)

            except Exception as error:
                print(
                    "[CANDLE_PATH_ERROR]",
                    type(error).__name__,
                    str(error),
                    flush=True,
                )

            time.sleep(30)


def start_candle_path():
    global _thread

    with _start_lock:
        if _thread is None or not _thread.is_alive():
            _thread = threading.Thread(
                target=run_candle_path,
                name="candle-path",
                daemon=True,
            )
            _thread.start()
