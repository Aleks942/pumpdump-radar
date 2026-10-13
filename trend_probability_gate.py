"""Conservative trend + first-touch probability gate for PumpDump Radar.

Research-first. Until independently completed forward cases establish an edge,
trading Telegram messages remain silent. No orders are executed. Uses two
bounded OKX calls only for new candidates and up to two for mature outcomes
per scan. Never trains on the current or future event.
"""
import math
import os
import sqlite3
import time
from collections import OrderedDict
from datetime import datetime, timezone
from statistics import NormalDist

import requests

TREND_TTL_SEC = 15 * 60
MAX_CACHE = 160
MAX_PENDING_PER_SCAN = 2
MAX_STORED = 2400
RETENTION_SEC = 30 * 86400
MIN_INDEPENDENT_SEC = 90 * 60
MIN_N = 80
MIN_DAYS = 5
MIN_SYMBOLS = 15
COST_PCT = 0.22
TP_PCT = 1.5
SL_PCT = 1.0
HORIZON = 30
MIN_EDGE_BUFFER = 0.03
DB_PATH = os.getenv("PUMP_TREND_STUDY_DB", "/data/pump_trend_study.sqlite3")
_trend_cache = OrderedDict()
_last_report = 0


def _okx(symbol, bar, limit):
    if not isinstance(symbol, str) or not symbol.endswith("USDT"):
        return []
    inst_id = symbol[:-4] + "-USDT-SWAP"
    res = requests.get(
        "https://www.okx.com/api/v5/market/candles",
        params={"instId": inst_id, "bar": bar, "limit": str(limit)},
        timeout=12,
    )
    res.raise_for_status()
    payload = res.json()
    if payload.get("code") != "0":
        return []
    return payload.get("data") if isinstance(payload.get("data"), list) else []


def _ema(values, period):
    if len(values) < period:
        return None
    value = sum(values[:period]) / period
    alpha = 2 / (period + 1)
    results = [value]
    for price in values[period:]:
        value += (price - value) * alpha
        results.append(value)
    return results


def _side_from_candles(rows, frame_seconds, now):
    """Use only confirmed, complete, chronological bars (no forming candle)."""
    parsed = {}
    for row in rows:
        try:
            stamp = int(row[0]) / 1000
            close = float(row[4])
            if (str(row[8]) != "1" or not math.isfinite(close)
                    or close <= 0 or stamp + frame_seconds > now):
                continue
            parsed[stamp] = close
        except (ValueError, TypeError, IndexError, OverflowError):
            continue
    stamps = sorted(parsed)
    if (len(stamps) < 57 or now - stamps[-1] > 2*frame_seconds + 120
            or any(
        round(stamps[i] - stamps[i-1]) != frame_seconds
        for i in range(len(stamps)-56, len(stamps))
    )):
        return "UNKNOWN"
    closes = [parsed[s] for s in stamps[-90:]]
    fast = _ema(closes, 20)
    slow = _ema(closes, 50)
    if not fast or not slow or len(fast) < 4:
        return "UNKNOWN"
    last, f, s, previous_f = closes[-1], fast[-1], slow[-1], fast[-4]
    if last > f > s and f > previous_f:
        return "UP"
    if last < f < s and f < previous_f:
        return "DOWN"
    return "FLAT"


def trend_status(symbol, now=None):
    """Fail closed if either H1 or H4 cannot be validated as aligned."""
    now = time.time() if now is None else float(now)
    prior = _trend_cache.get(symbol)
    if prior is not None and now - prior[0] <= TREND_TTL_SEC:
        _trend_cache.move_to_end(symbol)
        return prior[1]
    try:
        h1 = _side_from_candles(_okx(symbol, "1H", 100), 3600, now)
        h4 = _side_from_candles(_okx(symbol, "4H", 100), 14400, now)
        if h1 == h4 and h1 in ("UP", "DOWN"):
            state = h1
        elif h1 in ("UP", "DOWN") and h4 == "FLAT":
            # Modest relaxation: H1 trend + genuinely FLAT H4 (never
            # stale/unavailable/conflicting H4). Separate OOS evidence bucket.
            state = f"{h1}_H4_FLAT"
        else:
            state = "UNKNOWN"
    except (requests.RequestException, ValueError, TypeError, OverflowError):
        state = "UNKNOWN"
    _trend_cache[symbol] = (now, state)
    _trend_cache.move_to_end(symbol)
    while len(_trend_cache) > MAX_CACHE:
        _trend_cache.popitem(last=False)
    return state


def _db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=3)
    conn.execute("""CREATE TABLE IF NOT EXISTS candidates (
        id INTEGER PRIMARY KEY,
        ts REAL NOT NULL,
        symbol TEXT NOT NULL,
        pattern TEXT NOT NULL,
        side TEXT NOT NULL,
        trend TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        verdict TEXT,
        net_pct REAL,
        evaluated_ts REAL
    )""")
    conn.execute("""CREATE INDEX IF NOT EXISTS candidate_pending
        ON candidates (status, ts)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS candidate_cohort
        ON candidates (pattern, side, trend, status, ts)""")
    return conn


def _wilson_lower(successes, n):
    if n <= 0:
        return 0.0
    z = NormalDist().inv_cdf(0.95)  # one-sided 95% lower bound
    p = successes / n
    z2 = z * z
    return max(0.0, (p + z2/(2*n) - z * math.sqrt(
        (p*(1-p) + z2/(4*n))/n)) / (1 + z2/n))


def _edge(conn, pattern, side, trend, now):
    # Cutoff prevents decisions based on results within the same UTC day.
    cutoff = min(now - 86400, int(now // 86400) * 86400)
    n, wins, days, symbols = conn.execute("""
        SELECT COUNT(*),
               COALESCE(SUM(verdict='TP_FIRST'),0),
               COUNT(DISTINCT CAST(ts/86400 AS INTEGER)),
               COUNT(DISTINCT symbol)
        FROM candidates
        WHERE pattern=? AND side=? AND trend=? AND status='DONE'
          AND evaluated_ts < ? AND ts >= ?
    """, (pattern, side, trend, cutoff, now-RETENTION_SEC)).fetchone()
    n, wins, days, symbols = int(n), int(wins), int(days), int(symbols)
    lower = _wilson_lower(wins, n)
    breakeven = (SL_PCT+COST_PCT)/(TP_PCT+SL_PCT)
    # Treat all timeouts as full-stop losses for conservative lower-bound EV.
    lower_ev = lower*(TP_PCT-COST_PCT) - (1-lower)*(SL_PCT+COST_PCT)
    approved = (n >= MIN_N and days >= MIN_DAYS and symbols >= MIN_SYMBOLS
                and lower > breakeven + MIN_EDGE_BUFFER and lower_ev > 0)
    return {
        "n": n, "wins": wins, "days": days, "symbols": symbols,
        "lower_win_probability": lower,
        "breakeven_probability": breakeven,
        "lower_ev_pct": lower_ev,
        "approved": approved,
    }


def _direction(signal):
    d = (signal.get("decision") or {}).get("direction")
    return {"UP":"LONG", "DOWN":"SHORT"}.get(d)


def consider(signal, now=None):
    """Record isolated research case and return strict Telegram decision.

    Always fail closed on invalid data, nonaligned trend, insufficient OOS
    evidence, network failures, or research database errors.
    """
    now = time.time() if now is None else float(now)
    symbol = signal.get("symbol")
    side = _direction(signal)
    pattern = (signal.get("decision") or {}).get("pattern")
    if (not isinstance(symbol, str) or not symbol.endswith("USDT")
            or side is None or pattern not in (
                "NEW_LONG_BUILDUP", "NEW_SHORT_BUILDUP",
                "SHORT_SQUEEZE", "LONG_LIQUIDATION")):
        return False, "INVALID_PATTERN"
    trend = trend_status(symbol, now)
    expected = "UP" if side == "LONG" else "DOWN"
    if trend not in (expected, f"{expected}_H4_FLAT"):
        print(f"[TREND_REJECT] {symbol} side={side} trend={trend} "
              "reason=HTF_NOT_ALIGNED", flush=True)
        return False, "TREND_NOT_ALIGNED"
    if trend == f"{expected}_H4_FLAT":
        print(f"[TREND_H4_FLAT_RESEARCH] {symbol} side={side} "
              "separate_cohort=true original_gate_preserved=true", flush=True)
    try:
        with _db() as conn:
            edge = _edge(conn, pattern, side, trend, now)
            last = conn.execute("""SELECT MAX(ts) FROM candidates
                WHERE symbol=? AND side=? AND ts>?""",
                (symbol,side,now-MIN_INDEPENDENT_SEC)).fetchone()[0]
            if last is None:
                conn.execute("""INSERT INTO candidates
                    (ts,symbol,pattern,side,trend) VALUES (?,?,?,?,?)""",
                    (now,symbol,pattern,side,trend))
            conn.commit()
        print(f"[TREND_EDGE] {symbol} pattern={pattern} side={side} trend={trend} "
              f"n={edge['n']} wins={edge['wins']} days={edge['days']} "
              f"symbols={edge['symbols']} lower_p={edge['lower_win_probability']:.4f} "
              f"breakeven={edge['breakeven_probability']:.4f} "
              f"lower_ev={edge['lower_ev_pct']:+.4f}% "
              f"approved={edge['approved']} "
              "mode=FIRST_TOUCH_RESEARCH_NO_ORDERS", flush=True)
        return edge["approved"], "CONFIRMED" if edge["approved"] else "EDGE_UNPROVEN"
    except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
        print(f"[TREND_STUDY_ERROR] stage=consider kind={type(exc).__name__}",
              flush=True)
        return False, "RESEARCH_UNAVAILABLE"


def _evaluate_rows(rows, ts):
    """Next full minute; require a complete 30m path with closed 1m bars."""
    first = (math.floor(ts/60) + 1) * 60
    bars = {}
    for row in rows:
        try:
            at = int(row[0])//1000
            op, hi, lo, close = map(float, row[1:5])
            if (str(row[8]) != "1" or not all(
                math.isfinite(v) and v > 0 for v in (op,hi,lo,close)
            ) or not lo <= min(op,close) <= max(op,close) <= hi):
                continue
            bars[at] = (op,hi,lo,close)
        except (ValueError, TypeError, IndexError, OverflowError):
            continue
    path = [bars.get(first + k*60) for k in range(HORIZON)]
    if any(v is None for v in path):
        return None
    return path


def _outcome(path, side):
    entry = path[0][0]
    sign = 1 if side == "LONG" else -1
    target = entry * (1 + sign*TP_PCT/100)
    stop = entry * (1 - sign*SL_PCT/100)
    for _op, hi, lo, close in path:
        hit_target = hi >= target if sign == 1 else lo <= target
        hit_stop = lo <= stop if sign == 1 else hi >= stop
        if hit_stop:
            return "SL_FIRST", -SL_PCT-COST_PCT
        if hit_target:
            return "TP_FIRST", TP_PCT-COST_PCT
    net = sign * (path[-1][3]/entry - 1)*100 - COST_PCT
    return "TIME_EXIT", net


def advance(now=None):
    """Max two mature REST histories per existing scan, bounded memory+disk."""
    now = time.time() if now is None else float(now)
    try:
        with _db() as conn:
            conn.execute("DELETE FROM candidates WHERE ts < ?", (now-RETENTION_SEC,))
            # Oldest mature cases first; invalid/unavailable history can expire.
            entries = conn.execute("""SELECT id,ts,symbol,side FROM candidates
                WHERE status='PENDING' AND ts <= ?
                ORDER BY ts LIMIT ?""", (now-(HORIZON+3)*60, MAX_PENDING_PER_SCAN)).fetchall()
            for ident, ts, symbol, side in entries:
                if now-ts > 90*60:
                    conn.execute("""UPDATE candidates SET status='MISSING',
                        evaluated_ts=? WHERE id=?""", (now,ident))
                    continue
                try:
                    path = _evaluate_rows(_okx(symbol, "1m", 100), ts)
                    if path is None:
                        continue
                    verdict, net = _outcome(path, side)
                    conn.execute("""UPDATE candidates SET status='DONE',
                        verdict=?,net_pct=?,evaluated_ts=? WHERE id=?""",
                        (verdict,net,now,ident))
                    print(f"[TREND_OUTCOME] {symbol} side={side} "
                          f"verdict={verdict} net={net:+.4f}% "
                          "model=next_full_1m_first_touch_no_orders", flush=True)
                except (requests.RequestException, ValueError, TypeError,
                        OverflowError) as exc:
                    print(f"[TREND_STUDY_ERROR] stage=advance kind={type(exc).__name__}",
                          flush=True)
            # Hard cap using oldest-first deletion, regardless of status.
            conn.execute("""DELETE FROM candidates WHERE id IN (
                SELECT id FROM candidates ORDER BY ts DESC LIMIT -1 OFFSET ?
            )""", (MAX_STORED,))
            conn.commit()
    except (sqlite3.Error, OSError) as exc:
        print(f"[TREND_STUDY_ERROR] stage=advance_db kind={type(exc).__name__}",
              flush=True)


def hourly_report(now=None):
    global _last_report
    now = time.time() if now is None else float(now)
    if now - _last_report < 3600:
        return
    _last_report = now
    try:
        with _db() as conn:
            rows = conn.execute("""SELECT status,COUNT(*) FROM candidates
                GROUP BY status""").fetchall()
            print(f"[TREND_STUDY_SUMMARY] counts={dict(rows)} "
                  f"retention_days=30 max_rows={MAX_STORED} "
                  "proof_not_assumed=true", flush=True)
    except (sqlite3.Error,OSError) as exc:
        print(f"[TREND_STUDY_ERROR] stage=report kind={type(exc).__name__}",
              flush=True)
