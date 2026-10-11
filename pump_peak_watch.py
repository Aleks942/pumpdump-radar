"""Read-only pump watchlist for PumpDump Radar, independent of trading logic.

Sources: the existing OKX tickers call and in-process rolling observations.
No extra exchange HTTP, background thread, automatic orders, or filter changes.
"""
import math
import os
import sqlite3
import time
from collections import deque
from contextlib import closing
from html import escape

DB_PATH = os.getenv("PUMP_WATCH_DB", "/data/pump_watch.sqlite3")
PUMP_THRESHOLD = 10.0
WEAK_RETRACE_MIN = 2.5
RISK_RETRACE_MIN = 5.0
MIN_VOLUME = 10_000_000
MAX_WATCHES = 300
MAX_ALERTS_PER_SCAN = 4
DAILY_ALERTS_PER_LEVEL = 12
HISTORY_SEC = 48 * 3600
STATE_SEC = 72 * 3600
_observed = {}
_ready = False


def _number(value):
    try:
        x = float(value)
        return x if math.isfinite(x) and x > 0 else None
    except (ValueError, TypeError, OverflowError):
        return None


def _setup(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS pump_watch_state (
        symbol TEXT PRIMARY KEY, created_at REAL NOT NULL,
        updated_at REAL NOT NULL, anchor REAL NOT NULL,
        peak REAL NOT NULL, price REAL NOT NULL,
        rise_pct REAL NOT NULL, falling INTEGER NOT NULL DEFAULT 0,
        notified INTEGER NOT NULL DEFAULT 0,
        retry_after REAL NOT NULL DEFAULT 0
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pump_watch_alerts (
        id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
        ts REAL NOT NULL, level INTEGER NOT NULL,
        rise_pct REAL NOT NULL, retrace_pct REAL NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pump_watch_alert_ts ON pump_watch_alerts(ts)")


def _db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    return sqlite3.connect(DB_PATH, timeout=4)


def _roll_prices(symbol, price, now):
    observations = _observed.get(symbol)
    if observations is None:
        observations = deque()
        _observed[symbol] = observations
    while observations and observations[0][0] < now - HISTORY_SEC:
        observations.popleft()
    if not observations or now - observations[-1][0] >= 600:
        observations.append((now, price))
    if len(_observed) > 1500:
        for old_symbol in list(_observed):
            if old_symbol != symbol and (not _observed[old_symbol] or
                   _observed[old_symbol][-1][0] < now-3600):
                _observed.pop(old_symbol, None)
    # This is chronological: the trough always precedes the observed peak.
    low = None
    best = (0.0, None, None)
    for _ts, px in observations:
        if low is None or px < low:
            low = px
        else:
            change = (px / low - 1.0) * 100.0
            if change > best[0]:
                best = (change, low, px)
    return best


def _ticker(ticker):
    symbol = str(ticker.get("instId") or "")
    if not symbol.endswith("-USDT-SWAP"):
        return None
    price = _number(ticker.get("last"))
    opening = _number(ticker.get("open24h"))
    high = _number(ticker.get("high24h"))
    volume = _number(ticker.get("volCcy24h"))
    if not all((price,opening,high,volume)) or high < price:
        return None
    return (symbol.replace("-USDT-SWAP", "USDT"), price, opening, high, volume)


def observe(tickers, now=None):
    """Return phase transitions to notify; never modifies old signal pipeline."""
    global _ready
    now = time.time() if now is None else float(now)
    notifications = []
    try:
        with closing(_db()) as conn:
            _setup(conn)
            # Existing research and alert tables are never touched.
            conn.execute("DELETE FROM pump_watch_state WHERE created_at < ?", (now-STATE_SEC,))
            conn.execute("DELETE FROM pump_watch_alerts WHERE ts < ?", (now-30*86400,))
            states = {r[0]: r for r in conn.execute(
                "SELECT symbol,created_at,updated_at,anchor,peak,price,"
                "rise_pct,falling,notified,retry_after FROM pump_watch_state")}
            start_of_day = int(now // 86400) * 86400
            counts = dict(conn.execute(
                "SELECT level, COUNT(*) FROM pump_watch_alerts "
                "WHERE ts >= ? GROUP BY level", (start_of_day,)).fetchall())
            for ticker in tickers or ():
                parsed = _ticker(ticker)
                if parsed is None:
                    continue
                symbol, price, opening, high, volume = parsed
                if price < 0.01 or volume < MIN_VOLUME:
                    continue
                historical_pct, history_low, history_peak = _roll_prices(symbol, price, now)
                # The 24h opening quote precedes its high; it is not a
                # same-candle unordered high/low comparison.
                rise24 = max(0, (high/opening-1)*100)
                sample_pct = max(0, historical_pct)
                current = states.get(symbol)
                if current is None:
                    if max(rise24, sample_pct) < PUMP_THRESHOLD:
                        continue
                    if sample_pct > rise24 and history_low and history_peak:
                        anchor, peak = history_low, history_peak
                    else:
                        anchor, peak = opening, high
                    if peak < price:
                        peak = price
                    rise = max(0.0, (peak/anchor-1)*100)
                    falling, notified, retry_after = 0, 0, 0.0
                    created = now
                    conn.execute("""INSERT OR IGNORE INTO pump_watch_state
                        (symbol,created_at,updated_at,anchor,peak,price,rise_pct)
                        VALUES (?,?,?,?,?,?,?)""",
                        (symbol,created,now,anchor,peak,price,rise))
                else:
                    (_,created,_,anchor,peak,previous,rise,falling,
                     notified,retry_after) = current
                    if price > peak:
                        peak = price
                        falling = 0
                    else:
                        falling = min(100, falling+1) if price < previous else 0
                    rise = max(rise, (peak/anchor-1)*100)
                    conn.execute("""UPDATE pump_watch_state SET updated_at=?,
                        peak=?,price=?,rise_pct=?,falling=? WHERE symbol=?""",
                        (now,peak,price,rise,falling,symbol))
                retrace = max(0, (1-price/peak)*100)
                level = 1
                if falling >= 2 and retrace >= max(WEAK_RETRACE_MIN, rise*0.15):
                    level = 2
                if falling >= 3 and retrace >= max(RISK_RETRACE_MIN, rise*0.30):
                    level = 3
                if (level > notified and retry_after <= now and
                        counts.get(level, 0) < DAILY_ALERTS_PER_LEVEL and
                        (level > 1 or now-created <= 6*3600)):
                    notifications.append(dict(symbol=symbol,level=level,
                        rise=round(rise,2), retrace=round(retrace,2),
                        price=price, peak=peak, falling=falling))
            conn.commit()
            _ready = True
        notifications.sort(key=lambda x: (-x["level"],-x["rise"]))
        return notifications[:MAX_ALERTS_PER_SCAN]
    except Exception as exc:
        print(f"[PUMP_WATCH_ERROR] stage=observe "
              f"kind={type(exc).__name__}: {str(exc)[:100]}",flush=True)
        return []


def acknowledge(event, sent, now=None):
    """Advance notification stage only on confirmed Telegram delivery."""
    now = time.time() if now is None else float(now)
    try:
        with closing(_db()) as conn:
            _setup(conn)
            if sent is True:
                conn.execute("""UPDATE pump_watch_state
                    SET notified=MAX(notified, ?), retry_after=0
                    WHERE symbol=?""",(event["level"],event["symbol"]))
                conn.execute("""INSERT INTO pump_watch_alerts
                    (symbol,ts,level,rise_pct,retrace_pct)
                    VALUES (?,?,?,?,?)""",(event["symbol"],now,
                        event["level"],event["rise"],event["retrace"]))
            else:
                conn.execute("""UPDATE pump_watch_state
                    SET retry_after=? WHERE symbol=?""",(now+300,event["symbol"]))
            conn.commit()
    except Exception as exc:
        print(f"[PUMP_WATCH_ERROR] stage=ack "
              f"kind={type(exc).__name__}",flush=True)


def message(event, oi=None, imbalance=None):
    level = event["level"]
    heading = {1:"🛰️ PUMP WATCH", 2:"⚠️ PUMP WEAKENING",
               3:"🔴 POSSIBLE DUMP"}[level]
    details = {
        1:"Монета сильно выросла. Наблюдаем развитие пампа и признаки усталости.",
        2:"После роста появился заметный откат и последовательное снижение котировок.",
        3:"Откат усилился. Риск продолжения падения вырос; требуется подтверждение.",
    }[level]
    extra = ""
    if level >= 2:
        if isinstance(oi, dict) and oi.get("ready") and oi.get("change_pct") is not None:
            extra += f"\\nOI 5м: {float(oi['change_pct']):+.2f}%"
        if imbalance is not None:
            try:
                extra += f"\\nFutures imbalance: {float(imbalance):+.1f}%"
            except (ValueError, TypeError):
                pass
    return (f"{heading} — <b>{escape(event['symbol'])}</b>\\n"
            f"Рост до максимума: +{event['rise']:.2f}%\\n"
            f"Откат от пика: −{event['retrace']:.2f}%\\n"
            f"Максимум: {event['peak']:.8g}\\n"
            f"Цена: {event['price']:.8g}\\n"
            f"{details}{extra}\\n"
            "Это наблюдение, НЕ подтверждённый сигнал SHORT.")


def status():
    try:
        with closing(_db()) as conn:
            _setup(conn)
            rows = conn.execute(
                "SELECT COUNT(*), SUM(notified>=1), SUM(notified>=2), "
                "SUM(notified>=3) FROM pump_watch_state").fetchone()
            return tuple(int(x or 0) for x in rows)
    except Exception:
        return (0,0,0,0)
