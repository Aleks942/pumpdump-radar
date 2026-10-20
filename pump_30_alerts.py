"""Low-cost +30% pump notifications without Order Flow or additional API calls.

Uses the ticker batch already fetched by PumpDump Radar. Informational only:
not a short-entry recommendation and never places orders.
"""
import os
import sqlite3
import time
from contextlib import closing
from html import escape

from ticker_metrics import approx_quote_volume_usdt, spread_bps

DB_PATH = os.getenv("PUMP_WATCH_DB", "/data/pump_watch.sqlite3")
MIN_RISE_PCT = 30.0
MIN_NOTIONAL_USDT = 10_000_000
MAX_SPREAD_BPS = 40.0
MAX_PER_SCAN = 4
MAX_DAILY_ALERTS = 18
STATE_TTL = 72 * 3600


def _float(value):
    try:
        val = float(value)
        if val != val or val in (float("inf"), float("-inf")):
            return None
        return val
    except (TypeError, ValueError, OverflowError):
        return None


def _profile(row):
    if not isinstance(row, dict):
        return None
    instrument = row.get("instId")
    if not isinstance(instrument, str) or not instrument.endswith("-USDT-SWAP"):
        return None
    price, opening, high = (_float(row.get(k)) for k in ("last", "open24h", "high24h"))
    notional = approx_quote_volume_usdt(row)
    spread = spread_bps(row)
    if (any(v is None or v <= 0 for v in (price, opening, high, notional))
            or high < max(price, opening) or notional < MIN_NOTIONAL_USDT):
        return None
    if price < 0.01 and (spread is None or spread > MAX_SPREAD_BPS):
        return None
    rise = 100.0 * (high / opening - 1.0)
    if rise < MIN_RISE_PCT:
        return None
    return dict(symbol=instrument.replace("-USDT-SWAP", "USDT"), price=price,
                peak=high, opening=opening, rise=rise, retrace=100.0 * (1.0 - price / high))


def _connect():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    return sqlite3.connect(DB_PATH, timeout=4)


def _setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS pump_30_watch (
        symbol TEXT PRIMARY KEY, episode_at REAL NOT NULL, updated_at REAL NOT NULL,
        peak REAL NOT NULL, last_price REAL NOT NULL, down_count INTEGER NOT NULL,
        notified_stage INTEGER NOT NULL DEFAULT 0, retry_after REAL NOT NULL DEFAULT 0
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS pump_30_alerts (
        id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, at REAL NOT NULL,
        stage INTEGER NOT NULL, rise_pct REAL NOT NULL, retrace_pct REAL NOT NULL,
        delivered INTEGER NOT NULL
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_pump_30_alerts_at ON pump_30_alerts(at)")


def observe(tickers, now=None):
    """Discover ordered stages 1=pump +30%, 2=weakening, 3=dump risk.

    Reuses one batch of tickers. Alerts are not acknowledged until delivery succeeds.
    """
    now = time.time() if now is None else float(now)
    events = []
    with closing(_connect()) as db:
        _setup(db)
        db.execute("DELETE FROM pump_30_watch WHERE updated_at < ?", (now - STATE_TTL,))
        db.execute("DELETE FROM pump_30_alerts WHERE at < ?", (now - 7 * 86400,))
        existing = {r[0]: r for r in db.execute(
            "SELECT symbol,episode_at,updated_at,peak,last_price,down_count,"
            "notified_stage,retry_after FROM pump_30_watch")}
        day_start = int(now // 86400) * 86400
        sent_today = db.execute(
            "SELECT count(*) FROM pump_30_alerts WHERE at>=? AND delivered=1",
            (day_start,)).fetchone()[0]
        for row in tickers or ():
            p = _profile(row)
            if p is None:
                continue
            symbol = p["symbol"]
            current = existing.get(symbol)
            if current is None:
                # Don't initiate stale episodes after an already completed dump.
                if p["retrace"] > 15.0 or p["price"] < p["opening"] * 1.30:
                    continue
                peak, down, notified, retry = p["peak"], 0, 0, 0.0
                db.execute("""INSERT INTO pump_30_watch
                    (symbol,episode_at,updated_at,peak,last_price,down_count)
                    VALUES (?,?,?,?,?,0)""",
                    (symbol, now, now, peak, p["price"]))
            else:
                (_, _episode, _updated, peak, previous, down, notified, retry) = current
                peak = max(peak, p["peak"])
                down = min(100, down + 1) if p["price"] < previous else 0
                db.execute("""UPDATE pump_30_watch
                    SET updated_at=?,peak=?,last_price=?,down_count=?
                    WHERE symbol=?""",
                    (now, peak, p["price"], down, symbol))
            retrace = max(0.0, 100.0 * (1 - p["price"] / peak))
            stage = 1
            if down >= 2 and retrace >= max(3.0, p["rise"] * 0.10):
                stage = 2
            if down >= 3 and retrace >= max(6.0, p["rise"] * 0.20):
                stage = 3
            if (stage > notified and retry <= now and
                    sent_today + len(events) < MAX_DAILY_ALERTS):
                events.append(dict(symbol=symbol, stage=stage, rise=p["rise"],
                                   retrace=retrace, price=p["price"], peak=peak))
        db.commit()
    events.sort(key=lambda item: (-item["stage"], -item["rise"]))
    return events[:MAX_PER_SCAN]


def acknowledge(event, delivered, now=None):
    """Persist successful delivery, or retry later without flooding Telegram."""
    now = time.time() if now is None else float(now)
    with closing(_connect()) as db:
        _setup(db)
        if delivered is True:
            db.execute("""UPDATE pump_30_watch SET notified_stage=MAX(notified_stage,?),
                retry_after=0 WHERE symbol=?""",
                (event["stage"], event["symbol"]))
            db.execute("""INSERT INTO pump_30_alerts
                (symbol,at,stage,rise_pct,retrace_pct,delivered)
                VALUES (?,?,?,?,?,1)""",
                (event["symbol"], now, event["stage"], event["rise"], event["retrace"]))
        else:
            db.execute("UPDATE pump_30_watch SET retry_after=? WHERE symbol=?",
                       (now + 300, event["symbol"]))
        db.commit()


def message(event):
    title = {
        1: "🛰️ ПАМП +30% — НАБЛЮДЕНИЕ",
        2: "⚠️ ПАМП — ЦЕНА ОСЛАБЕВАЕТ",
        3: "🔴 ПАМП — РИСК ПРОДОЛЖЕНИЯ ПАДЕНИЯ",
    }[event["stage"]]
    return (
        f"{title} — <b>{escape(event['symbol'])}</b>\n"
        f"Рост от открытия 24ч до максимума: +{event['rise']:.1f}%\n"
        f"Откат от максимума: −{event['retrace']:.1f}%\n"
        f"Максимум: {event['peak']:.8g}; сейчас: {event['price']:.8g}\n"
        "Сила покупателей по споту здесь НЕ подтверждена. "
        "Это наблюдение, а НЕ подтверждённый сигнал SHORT."
    )
