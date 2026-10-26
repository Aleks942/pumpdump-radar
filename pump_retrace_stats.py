"""Forward-only post-pump retrace statistics for PumpDump Radar.

Research only. No orders, no Telegram, no signal gating and no extra market
HTTP requests. The module consumes ticker snapshots already fetched by main.py.

Two independent bases are tracked:
1) Rolling 5m/10m/20m/30m pump windows. T0 is the first observed >=15% event.
2) UTC day open using OKX ticker field sodUtc0. This is NOT open24h.

Milestones: +15/+20/+30/+50% for rolling windows and additionally +70/+100%
for UTC-day moves. For every reached milestone we keep the latest sampled peak
and reset post-peak retrace clocks whenever a new high is observed. Therefore,
when an episode closes, retrace4/6/8/10/15/20 describe the correction after the
final sampled peak of that episode rather than an earlier temporary pullback.
"""
import math
import os
import sqlite3
import statistics
import time
from contextlib import closing

DB_PATH = os.getenv("PUMP_RETRACE_STATS_DB", "/data/pump_retrace_stats.sqlite3")
ROLLING_MILESTONES = (15, 20, 30, 50)
DAY_MILESTONES = (15, 20, 30, 50, 70, 100)
RETRACE_LEVELS = (4, 6, 8, 10, 15, 20)
VALID_WINDOWS = ("5m", "10m", "20m", "30m")
MAX_EPISODE_SEC = 6 * 3600
CLOSE_AFTER_20_RETRACE_SEC = 15 * 60
RETENTION_SEC = 35 * 86400
REPORT_EVERY_SEC = 3600
_last_report = 0.0


def _finite(value, positive=False):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(value):
        return None
    if positive and value <= 0:
        return None
    return value


def _window(raw):
    value = str(raw or "").strip().lower().lstrip("~")
    return value if value in VALID_WINDOWS else None


def _connect(db_path=None):
    path = DB_PATH if db_path is None else db_path
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    db = sqlite3.connect(path, timeout=5)
    db.row_factory = sqlite3.Row
    return db


def _setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS pump_retrace_episode (
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        basis TEXT NOT NULL,
        exchange TEXT NOT NULL,
        session_key INTEGER,
        started_at REAL NOT NULL,
        sample_age_sec REAL,
        reference_price REAL NOT NULL,
        t0_price REAL NOT NULL,
        t0_move_pct REAL NOT NULL,
        last_at REAL NOT NULL,
        last_price REAL NOT NULL,
        peak_price REAL NOT NULL,
        peak_at REAL NOT NULL,
        peak_move_pct REAL NOT NULL,
        max_retrace_pct REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'ACTIVE',
        close_reason TEXT,
        closed_at REAL
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_pump_retrace_active
        ON pump_retrace_episode(status, exchange, symbol, basis)""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_pump_retrace_started
        ON pump_retrace_episode(started_at)""")
    db.execute("""CREATE TABLE IF NOT EXISTS pump_retrace_milestone (
        episode_id INTEGER NOT NULL,
        threshold_pct INTEGER NOT NULL,
        reached_at REAL NOT NULL,
        reach_mode TEXT NOT NULL,
        threshold_price REAL NOT NULL,
        observed_reach_price REAL NOT NULL,
        max_price REAL NOT NULL,
        max_price_at REAL NOT NULL,
        post_peak_low REAL NOT NULL,
        retrace_pct REAL NOT NULL DEFAULT 0,
        retrace4_at REAL,
        retrace6_at REAL,
        retrace8_at REAL,
        retrace10_at REAL,
        retrace15_at REAL,
        retrace20_at REAL,
        PRIMARY KEY (episode_id, threshold_pct)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_pump_retrace_threshold
        ON pump_retrace_milestone(threshold_pct, reached_at)""")


def _threshold_column(level):
    return {
        4: "retrace4_at",
        6: "retrace6_at",
        8: "retrace8_at",
        10: "retrace10_at",
        15: "retrace15_at",
        20: "retrace20_at",
    }[int(level)]


def _add_milestones(db, episode_id, basis, reference_price, price,
                    move_pct, now, initial):
    levels = DAY_MILESTONES if basis == "UTC_DAY" else ROLLING_MILESTONES
    added = 0
    for threshold in levels:
        if move_pct + 1e-12 < threshold:
            continue
        exists = db.execute(
            "SELECT 1 FROM pump_retrace_milestone "
            "WHERE episode_id=? AND threshold_pct=?",
            (episode_id, threshold),
        ).fetchone()
        if exists:
            continue
        threshold_price = reference_price * (1.0 + threshold / 100.0)
        mode = (
            "FIRST_OBSERVED_AT_OR_ABOVE"
            if initial else
            "OBSERVED_FORWARD_CROSSING"
        )
        db.execute("""INSERT INTO pump_retrace_milestone
            (episode_id,threshold_pct,reached_at,reach_mode,threshold_price,
             observed_reach_price,max_price,max_price_at,post_peak_low,
             retrace_pct)
            VALUES (?,?,?,?,?,?,?,?,?,0)""",
            (episode_id, threshold, now, mode, threshold_price,
             price, price, now, price))
        added += 1
    return added


def _new_episode(db, symbol, basis, exchange, session_key, now,
                 reference_price, price, move_pct, sample_age_sec=None):
    cur = db.execute("""INSERT INTO pump_retrace_episode
        (symbol,basis,exchange,session_key,started_at,sample_age_sec,
         reference_price,t0_price,t0_move_pct,last_at,last_price,
         peak_price,peak_at,peak_move_pct,max_retrace_pct,status)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'ACTIVE')""",
        (symbol, basis, exchange, session_key, now, sample_age_sec,
         reference_price, price, move_pct, now, price,
         price, now, move_pct))
    episode_id = int(cur.lastrowid)
    _add_milestones(
        db, episode_id, basis, reference_price, price,
        move_pct, now, initial=True,
    )
    print(
        "[PUMP_RETRACE_T0]",
        symbol,
        "basis=", basis,
        "exchange=", exchange,
        "move=", round(move_pct, 3),
        "price=", price,
        "research_only=true",
        flush=True,
    )
    return episode_id


def _active_episode(db, symbol, basis, exchange, session_key=None):
    if basis == "UTC_DAY":
        return db.execute("""SELECT * FROM pump_retrace_episode
            WHERE status='ACTIVE' AND symbol=? AND basis=? AND exchange=?
              AND session_key=?
            ORDER BY id DESC LIMIT 1""",
            (symbol, basis, exchange, session_key)).fetchone()
    return db.execute("""SELECT * FROM pump_retrace_episode
        WHERE status='ACTIVE' AND symbol=? AND basis=? AND exchange=?
        ORDER BY id DESC LIMIT 1""",
        (symbol, basis, exchange)).fetchone()


def _close(db, episode_id, now, reason):
    db.execute("""UPDATE pump_retrace_episode
        SET status='CLOSED', close_reason=?, closed_at=?, last_at=?
        WHERE id=? AND status='ACTIVE'""",
        (reason, now, now, episode_id))


def _update_milestone(db, row, price, now):
    episode_id = int(row["episode_id"])
    threshold = int(row["threshold_pct"])
    old_peak = float(row["max_price"])
    if price > old_peak:
        # New high invalidates the earlier pullback as the "final peak"
        # correction. Start measuring retrace from the new sampled high.
        db.execute("""UPDATE pump_retrace_milestone
            SET max_price=?, max_price_at=?, post_peak_low=?, retrace_pct=0,
                retrace4_at=NULL,retrace6_at=NULL,retrace8_at=NULL,
                retrace10_at=NULL,retrace15_at=NULL,retrace20_at=NULL
            WHERE episode_id=? AND threshold_pct=?""",
            (price, now, price, episode_id, threshold))
        return 0.0

    low = min(float(row["post_peak_low"]), price)
    retrace = max(0.0, (1.0 - low / old_peak) * 100.0)
    assignments = ["post_peak_low=?", "retrace_pct=?"]
    values = [low, retrace]
    for level in RETRACE_LEVELS:
        column = _threshold_column(level)
        if row[column] is None and retrace + 1e-12 >= level:
            assignments.append(f"{column}=?")
            values.append(now)
    values.extend([episode_id, threshold])
    db.execute(
        "UPDATE pump_retrace_milestone SET " + ",".join(assignments) +
        " WHERE episode_id=? AND threshold_pct=?",
        values,
    )
    return retrace


def _update_episode(db, episode, price, now):
    episode_id = int(episode["id"])
    reference = float(episode["reference_price"])
    basis = str(episode["basis"])
    move_pct = (price / reference - 1.0) * 100.0
    peak = float(episode["peak_price"])
    peak_at = float(episode["peak_at"])

    if price > peak:
        peak = price
        peak_at = now
    peak_move = (peak / reference - 1.0) * 100.0
    retrace = max(0.0, (1.0 - price / peak) * 100.0)
    max_retrace = max(float(episode["max_retrace_pct"]), retrace)

    db.execute("""UPDATE pump_retrace_episode
        SET last_at=?,last_price=?,peak_price=?,peak_at=?,
            peak_move_pct=?,max_retrace_pct=?
        WHERE id=?""",
        (now, price, peak, peak_at, peak_move, max_retrace, episode_id))

    _add_milestones(
        db, episode_id, basis, reference, price,
        move_pct, now, initial=False,
    )

    rows = db.execute(
        "SELECT * FROM pump_retrace_milestone WHERE episode_id=?",
        (episode_id,),
    ).fetchall()
    current_max_retrace = 0.0
    for row in rows:
        current_max_retrace = max(
            current_max_retrace,
            _update_milestone(db, row, price, now),
        )

    if now - float(episode["started_at"]) >= MAX_EPISODE_SEC:
        _close(db, episode_id, now, "MAX_6H")
    elif (current_max_retrace >= 20.0
          and now - peak_at >= CLOSE_AFTER_20_RETRACE_SEC):
        _close(db, episode_id, now, "RETRACE_20")
    return move_pct


def register_event(event, now=None, db_path=None):
    """Register a rolling-window PUMP event at its first observed >=15%."""
    now = time.time() if now is None else _finite(now)
    if now is None or not isinstance(event, dict):
        return False
    if event.get("type") != "PUMP":
        return False
    direction = (event.get("decision") or {}).get("direction")
    if direction != "UP":
        return False

    basis = _window(event.get("window"))
    symbol = str(event.get("symbol") or "")
    exchange = str(event.get("exchange") or "OKX").strip() or "OKX"
    price = _finite(event.get("price"), positive=True)
    move = _finite(event.get("change"))
    age = _finite(event.get("sample_age_sec"))
    if (basis is None or not symbol.endswith("USDT") or price is None
            or move is None or move < 15):
        return False

    reference = price / (1.0 + move / 100.0)
    if not math.isfinite(reference) or reference <= 0:
        return False

    with closing(_connect(db_path)) as db:
        _setup(db)
        episode = _active_episode(db, symbol, basis, exchange)
        if episode is not None:
            _update_episode(db, episode, price, now)
            db.commit()
            return False
        _new_episode(
            db, symbol, basis, exchange, None, now,
            reference, price, move, sample_age_sec=age,
        )
        db.commit()
    return True


def _ticker_rows(tickers, exchange):
    for ticker in tickers or ():
        if not isinstance(ticker, dict):
            continue
        if exchange == "OKX":
            inst = str(ticker.get("instId") or "")
            if not inst.endswith("-USDT-SWAP"):
                continue
            symbol = inst[:-10] + "USDT"
            price = _finite(ticker.get("last"), positive=True)
            day_open = _finite(ticker.get("sodUtc0"), positive=True)
        elif exchange == "Bybit":
            symbol = str(ticker.get("symbol") or "")
            if not symbol.endswith("USDT"):
                continue
            price = _finite(ticker.get("lastPrice"), positive=True)
            day_open = None
        else:
            return
        if price is None:
            continue
        yield symbol, price, day_open


def observe_tickers(tickers, exchange, now=None, db_path=None):
    """Advance active episodes and discover UTC-day milestones from OKX."""
    now = time.time() if now is None else _finite(now)
    if now is None or exchange not in ("OKX", "Bybit"):
        return 0
    rows = list(_ticker_rows(tickers, exchange))
    if not rows:
        return 0

    touched = 0
    utc_day = int(now // 86400)
    with closing(_connect(db_path)) as db:
        _setup(db)

        # Close stale active episodes even if the symbol disappeared.
        stale = db.execute("""SELECT id FROM pump_retrace_episode
            WHERE status='ACTIVE' AND ? - started_at >= ?""",
            (now, MAX_EPISODE_SEC)).fetchall()
        for item in stale:
            _close(db, int(item["id"]), now, "MAX_6H")

        for symbol, price, day_open in rows:
            active = db.execute("""SELECT * FROM pump_retrace_episode
                WHERE status='ACTIVE' AND symbol=? AND exchange=?""",
                (symbol, exchange)).fetchall()
            for episode in active:
                _update_episode(db, episode, price, now)
                touched += 1

            # A true day-open study uses sodUtc0 from the already fetched
            # OKX ticker. open24h is intentionally never used here.
            if exchange == "OKX" and day_open:
                day_move = (price / day_open - 1.0) * 100.0
                if day_move >= 15:
                    daily = _active_episode(
                        db, symbol, "UTC_DAY", "OKX", utc_day
                    )
                    if daily is None:
                        _new_episode(
                            db, symbol, "UTC_DAY", "OKX", utc_day,
                            now, day_open, price, day_move, sample_age_sec=None,
                        )
                        touched += 1
                    else:
                        _update_episode(db, daily, price, now)
                        touched += 1

        # Keep only closed research older than retention; active rows survive.
        old_ids = [
            int(row["id"]) for row in db.execute(
                """SELECT id FROM pump_retrace_episode
                   WHERE status='CLOSED' AND closed_at < ?""",
                (now - RETENTION_SEC,),
            ).fetchall()
        ]
        for episode_id in old_ids:
            db.execute(
                "DELETE FROM pump_retrace_milestone WHERE episode_id=?",
                (episode_id,),
            )
            db.execute(
                "DELETE FROM pump_retrace_episode WHERE id=?",
                (episode_id,),
            )
        db.commit()
    return touched


def summary(now=None, db_path=None):
    """Closed-episode descriptive stats by basis and milestone."""
    now = time.time() if now is None else _finite(now)
    if now is None:
        return []
    with closing(_connect(db_path)) as db:
        _setup(db)
        rows = db.execute("""SELECT
                e.basis,e.exchange,e.reference_price,e.started_at,e.closed_at,
                m.threshold_pct,m.reached_at,m.threshold_price,m.max_price,
                m.max_price_at,m.retrace_pct,
                m.retrace4_at,m.retrace6_at,m.retrace8_at,m.retrace10_at,
                m.retrace15_at,m.retrace20_at
            FROM pump_retrace_milestone m
            JOIN pump_retrace_episode e ON e.id=m.episode_id
            WHERE e.status='CLOSED' AND e.closed_at>=?""",
            (now - RETENTION_SEC,),
        ).fetchall()

    groups = {}
    for row in rows:
        key = (row["basis"], int(row["threshold_pct"]))
        groups.setdefault(key, []).append(row)

    output = []
    for (basis, threshold), items in sorted(groups.items()):
        extra = [
            (float(r["max_price"]) / float(r["threshold_price"]) - 1.0) * 100.0
            for r in items
        ]
        peak_minutes = [
            max(0.0, (float(r["max_price_at"]) - float(r["reached_at"])) / 60.0)
            for r in items
        ]
        result = {
            "basis": basis,
            "threshold_pct": threshold,
            "n": len(items),
            "median_extra_after_threshold_pct": round(statistics.median(extra), 3),
            "median_time_to_peak_min": round(statistics.median(peak_minutes), 2),
            "median_final_retrace_pct": round(
                statistics.median(float(r["retrace_pct"]) for r in items), 3
            ),
        }
        for level in RETRACE_LEVELS:
            column = _threshold_column(level)
            hits = [r for r in items if r[column] is not None]
            result[f"retrace{level}_hits"] = len(hits)
            result[f"retrace{level}_pct"] = round(
                100.0 * len(hits) / len(items), 1
            ) if items else 0.0
            waits = [
                max(0.0, (float(r[column]) - float(r["max_price_at"])) / 60.0)
                for r in hits
            ]
            result[f"median_retrace{level}_after_peak_min"] = (
                round(statistics.median(waits), 2) if waits else None
            )
        output.append(result)
    return output


def hourly_report(now=None, db_path=None):
    global _last_report
    now = time.time() if now is None else _finite(now)
    if now is None or now - _last_report < REPORT_EVERY_SEC:
        return
    _last_report = now
    rows = summary(now=now, db_path=db_path)
    for row in rows:
        print(
            "[PUMP_RETRACE_STATS]",
            "basis=", row["basis"],
            "threshold=", row["threshold_pct"],
            "n=", row["n"],
            "extra_med=", row["median_extra_after_threshold_pct"],
            "peak_min_med=", row["median_time_to_peak_min"],
            "r6=", f'{row["retrace6_hits"]}/{row["n"]}',
            "r8=", f'{row["retrace8_hits"]}/{row["n"]}',
            "r10=", f'{row["retrace10_hits"]}/{row["n"]}',
            "r15=", f'{row["retrace15_hits"]}/{row["n"]}',
            "r20=", f'{row["retrace20_hits"]}/{row["n"]}',
            "research_only=true",
            flush=True,
        )
