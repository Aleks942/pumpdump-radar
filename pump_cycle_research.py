"""Research-only pump lifecycle correlated with authenticated Order Flow evidence.

Uses existing pump_watch_state and the same OKX ticker batch, never affects
market-watch Telegram, price thresholds, Smart Money or trade approval.
A price pivot is an observed chronological swing, not a 24h OHLC guess.
Only authenticated, fresh Bybit SELL taker flow can confirm stage 4.
"""
import json
import math
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque


DB_PATH = os.getenv("PUMP_WATCH_DB", "/data/pump_watch.sqlite3")
OBS_INTERVAL = 25
PRICE_LOOKBACK = 45 * 60
STRUCTURE_CONFIRM_SAMPLES = 2
FLOW_TTL = 180
MAX_EVENTS = 10000
_prices = defaultdict(lambda: deque(maxlen=90))
_flow_slots = threading.BoundedSemaphore(2)
_flow_cooldown = {}
_flow_lock = threading.Lock()

# Stages deliberately track evidence, NOT a permission to trade.
STAGES = ("PUMP_START", "PUMP_CONTINUATION", "PUMP_WEAKENING",
          "REVERSAL_CONFIRMED", "DUMP_CONTINUATION", "DUMP_EXHAUSTION")


def _num(value):
    try:
        x = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return x if math.isfinite(x) else None


def _init(db):
    db.execute("""CREATE TABLE IF NOT EXISTS pump_cycle_state (
        symbol TEXT PRIMARY KEY, start_at REAL NOT NULL, last_at REAL NOT NULL,
        stage TEXT NOT NULL, peak REAL NOT NULL, peak_at REAL NOT NULL,
        observed_peak REAL NOT NULL, observed_peak_at REAL NOT NULL,
        last_price REAL NOT NULL, support REAL,
        weakening_at REAL, break_price REAL, break_at REAL,
        break_count INTEGER NOT NULL DEFAULT 0,
        trough REAL NOT NULL, stage_changed_at REAL NOT NULL,
        flow_delta REAL, flow_status TEXT, flow_verified_at REAL,
        flow_observed_at REAL, flow_evidence TEXT, last_flow_request REAL,
        anchor_estimate REAL NOT NULL, seed_source TEXT NOT NULL,
        initial_move_pct REAL NOT NULL, down_count INTEGER NOT NULL DEFAULT 0
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS pump_cycle_events (
        id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, at REAL NOT NULL,
        stage TEXT NOT NULL, price REAL NOT NULL, support REAL,
        peak REAL NOT NULL, reason TEXT NOT NULL,
        flow_delta REAL, research_only INTEGER NOT NULL DEFAULT 1
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_cycle_event_symbol_at "
               "ON pump_cycle_events(symbol,at)")
    db.execute("""CREATE TABLE IF NOT EXISTS pump_cycle_outcomes (
        event_id INTEGER NOT NULL, horizon_sec INTEGER NOT NULL,
        status TEXT NOT NULL, sampled_at REAL, lag_sec REAL,
        quote_price REAL, gross_short_pct REAL, net_short_pct REAL,
        PRIMARY KEY(event_id,horizon_sec)
    )""")


def _swing_support(points, peak_at):
    """Last 2+2 confirmed pivot before an OBSERVED peak, never future data."""
    valid = [(t, p) for t, p in points if t <= peak_at]
    if len(valid) < 6:
        return None
    pivots = []
    for i in range(2, len(valid)-2):
        ts, low = valid[i]
        if (low < valid[i-2][1] and low < valid[i-1][1]
                and low < valid[i+1][1] and low < valid[i+2][1]
                and max(p for _, p in valid[i+1:]) >= low*1.004):
            pivots.append((ts, low))
    return pivots[-1][1] if pivots else None


def _quote_rows(tickers):
    out = {}
    for ticker in tickers or ():
        if not isinstance(ticker, dict):
            continue
        inst = ticker.get("instId")
        if not isinstance(inst, str) or not inst.endswith("-USDT-SWAP"):
            continue
        price = _num(ticker.get("last"))
        if price is None or price <= 0:
            continue
        out[inst[:-10]+"USDT"] = price
    return out


def _transition(db, state, stage, price, now, reason, emitted):
    if state["stage"] == stage:
        return
    state["stage"] = stage
    state["stage_changed_at"] = now
    db.execute("""INSERT INTO pump_cycle_events
        (symbol,at,stage,price,support,peak,reason,flow_delta)
        VALUES (?,?,?,?,?,?,?,?)""", (
        state["symbol"], now, stage, price, state.get("support"),
        state["peak"], reason, state.get("flow_delta")))
    emitted.append((state["symbol"], stage, reason))


def _fresh_sell(state, now):
    return (state["flow_status"] == "CONFIRMED"
            and state["flow_delta"] is not None
            and state["flow_delta"] <= -0.20
            and state["flow_verified_at"] is not None
            and 0 <= now-state["flow_verified_at"] <= FLOW_TTL
            and state["flow_observed_at"] is not None
            and state["weakening_at"] is not None
            and state["flow_observed_at"] >= state["weakening_at"])


def _record_flow(symbol, observed_at, result, now=None, db_path=None):
    """Only bounded verified results, tied to exact request timestamp."""
    now = time.time() if now is None else float(now)
    db_path = DB_PATH if db_path is None else db_path
    if not isinstance(result, dict) or result.get("status") not in (
            "CONFIRMED", "CONTRADICTION", "INSUFFICIENT_DATA"):
        return False
    status = result["status"]
    finished = _num(result.get("finished_at"))
    delta = _num(result.get("delta_ratio"))
    observed_at = _num(observed_at)
    if not (finished is not None and observed_at is not None
            and observed_at <= finished <= now
            and finished-observed_at <= 90
            and now-finished <= 15):
        return False
    if delta is not None and not -1 <= delta <= 1:
        return False
    evidence = result.get("evidence_event")
    if evidence not in ("NONE", "BUY_ABSORPTION_CANDIDATE"):
        evidence = None
    with sqlite3.connect(db_path, timeout=4) as db:
        _init(db)
        row = db.execute("""UPDATE pump_cycle_state
            SET flow_delta=?, flow_status=?, flow_verified_at=?,
                flow_observed_at=?, flow_evidence=?
            WHERE symbol=? AND weakening_at IS NOT NULL
              AND weakening_at<=? AND stage IN ('PUMP_WEAKENING',
                 'REVERSAL_CONFIRMED', 'DUMP_CONTINUATION')""",
            (delta, status, finished, observed_at, evidence,
             symbol, observed_at))
    return row.rowcount == 1


def _dispatch_flow(symbol, price, now, db_path):
    """Bounded, authenticated Flow query when a pump weakens/breaks."""
    base = os.getenv("PUMP_FLOW_URL", "").strip()
    token = os.getenv("PUMP_FLOW_SHARED_TOKEN", "")
    if (os.getenv("PUMP_FLOW_MODE", "OFF").upper() != "RESEARCH"
            or not base.startswith("https://")
            or not base.endswith("/api/pump-candidate") or not token):
        return False
    with _flow_lock:
        if now-_flow_cooldown.get(symbol, -float("inf")) < 120:
            return False
        if not _flow_slots.acquire(blocking=False):
            return False
        _flow_cooldown[symbol] = now

    def worker():
        try:
            import requests
            observed = time.time()
            payload = {"schema": 1, "source": "pumpdump-radar",
                       "symbol": symbol, "side": "SHORT", "price": price,
                       "observed_at": observed, "expires_at": observed+90}
            headers = {"X-Pump-Flow-Token": token}
            result = requests.post(base, json=payload, headers=headers, timeout=3)
            if result.status_code != 202:
                print("[PUMP_CYCLE_FLOW] rejected", symbol, result.status_code,
                      flush=True)
                return
            url = base.rsplit("/", 1)[0]+"/pump-result"
            for _ in range(10):  # max ~36s; verification usually ~20s
                time.sleep(3)
                response = requests.get(url, headers=headers,
                                        params={"symbol": symbol,
                                                "observed_at": observed},
                                        timeout=3)
                if response.status_code != 200:
                    return
                answer = response.json()
                if answer.get("status") == "PENDING":
                    continue
                saved = _record_flow(symbol, observed, answer, db_path=db_path)
                print("[PUMP_CYCLE_FLOW]", symbol,
                      "status=", answer.get("status"),
                      "delta=", answer.get("delta_ratio"),
                      "absorption=", answer.get("evidence_event"),
                      "saved=", saved, "research_only=true", flush=True)
                return
        except Exception as exc:
            print("[PUMP_CYCLE_FLOW_ERROR]", symbol, type(exc).__name__, flush=True)
        finally:
            _flow_slots.release()
    try:
        threading.Thread(target=worker, daemon=True).start()
        return True
    except Exception:
        _flow_slots.release()
        return False


def observe(tickers, now=None, db_path=None, flow_sender=None,
            seeded_pumps=()):
    """Record evidence-based phases; pass event times from current live scans."""
    now = time.time() if now is None else float(now)
    db_path = DB_PATH if db_path is None else db_path
    if not math.isfinite(now):
        return []
    prices = _quote_rows(tickers)
    # Evict old observation buffers, including delisted/long-absent assets.
    if len(_prices) > 500:
        for old_symbol in list(_prices):
            recent = _prices[old_symbol]
            if not recent or now-recent[-1][0] > PRICE_LOOKBACK:
                _prices.pop(old_symbol, None)
    emitted, needs_flow = [], []
    with sqlite3.connect(db_path, timeout=5) as db:
        db.row_factory = sqlite3.Row
        _init(db)
        try:
            watches = list(db.execute("""SELECT symbol,anchor,peak,rise_pct,falling
                FROM pump_watch_state WHERE updated_at>=?""", (now-180,)))
        except sqlite3.OperationalError:
            # The old Pump Watch initializes its table on the first scan.
            return []
        watch_map = {row["symbol"]: dict(row, seed_source="WATCH") for row in watches}
        # Continue previously triggered early pumps even below the 10% watch.
        tracked = db.execute("""SELECT symbol,peak,anchor_estimate,
            initial_move_pct,seed_source,down_count FROM pump_cycle_state
            WHERE last_at>=?""",(now-72*3600,)).fetchall()
        for row in tracked:
            if row["symbol"] not in watch_map:
                watch_map[row["symbol"]] = {
                    "symbol":row["symbol"], "peak":row["peak"],
                    "anchor":row["anchor_estimate"],
                    "rise_pct": max(0,100*(row["peak"]/row["anchor_estimate"]-1)),
                    "falling":row["down_count"],"seed_source":row["seed_source"],
                    "initial_move_pct":row["initial_move_pct"]}
        # Existing early continuation selector already checks real 5m quotes
        # and two post-trigger directional advances.
        for seed in seeded_pumps or ():
            if not isinstance(seed,dict) or seed.get("type")!="PUMP":
                continue
            symbol = seed.get("symbol")
            change = _num(seed.get("change"))
            px = _num(seed.get("price"))
            if (not isinstance(symbol,str) or
                    re.fullmatch(r"[A-Z0-9]{2,25}USDT",symbol) is None
                    or change is None or not 0.5 <= change <= 100
                    or px is None or px<=0):
                continue
            if symbol not in watch_map:
                anchor = px/(1+change/100)
                watch_map[symbol] = {"symbol":symbol,"peak":px,
                    "anchor":anchor,"rise_pct":change,"falling":0,
                    "seed_source":"EARLY","initial_move_pct":change}
        for watch in watch_map.values():
            symbol = watch["symbol"]
            price = prices.get(symbol)
            if price is None:
                continue
            history = _prices[symbol]
            if not history or now-history[-1][0] >= OBS_INTERVAL:
                history.append((now, price))
            while history and now-history[0][0] > PRICE_LOOKBACK:
                history.popleft()
            existing = db.execute(
                "SELECT * FROM pump_cycle_state WHERE symbol=?", (symbol,)
            ).fetchone()
            if existing is None:
                state = {"symbol": symbol, "start_at": now, "last_at": now,
                         "stage": "PUMP_START", "peak": max(price, watch["peak"]),
                         "peak_at": now, "observed_peak": price,
                         "observed_peak_at": now, "last_price": price,
                         "support": None, "weakening_at": None,
                         "break_price": None, "break_at": None,
                         "break_count": 0, "trough": price,
                         "stage_changed_at": now, "flow_delta": None,
                         "flow_status": None, "flow_verified_at": None,
                         "flow_observed_at": None, "flow_evidence": None,
                         "last_flow_request": None,
                         "anchor_estimate": watch["anchor"],
                         "seed_source":watch.get("seed_source","WATCH"),
                         "initial_move_pct":watch.get("initial_move_pct",watch["rise_pct"]),
                         "down_count":0}
                db.execute("""INSERT INTO pump_cycle_state
                    (symbol,start_at,last_at,stage,peak,peak_at,
                     observed_peak,observed_peak_at,last_price,
                     trough,stage_changed_at,anchor_estimate,
                     seed_source,initial_move_pct,down_count)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (symbol,now,now,state["stage"],state["peak"],now,
                     price,now,price,price,now,state["anchor_estimate"],
                     state["seed_source"],state["initial_move_pct"],0))
                db.execute("""INSERT INTO pump_cycle_events
                    (symbol,at,stage,price,peak,reason)
                    VALUES (?,?,?,?,?,?)""",
                    (symbol,now,"PUMP_START",price,state["peak"],
                     "WATCHED_STRONG_PUMP" if state["seed_source"]=="WATCH"
                     else "EARLY_CONTINUATION_OBSERVED"))
                emitted.append((symbol,"PUMP_START",
                                "WATCHED_STRONG_PUMP" if state["seed_source"]=="WATCH"
                                else "EARLY_CONTINUATION_OBSERVED"))
                continue
            state = dict(existing)
            if now <= state["last_at"]:
                continue  # no recycled or out-of-order input
            old_price = state["last_price"]
            state["down_count"] = (min(100,state["down_count"]+1)
                                   if price < old_price else 0)
            if price > state["observed_peak"]:
                state["observed_peak"], state["observed_peak_at"] = price, now
            if price > state["peak"]:
                state["peak"], state["peak_at"] = price, now
            state["trough"] = min(state["trough"], price)
            rise = max(watch["rise_pct"],100*(state["peak"]/state["anchor_estimate"]-1))
            retrace = max(0, 100*(1-price/state["peak"]))
            # Research starting values; NOT optimized entry thresholds.
            if state["seed_source"]=="EARLY":
                weak_limit = max(0.8,0.4*state["initial_move_pct"])
                falling_count = state["down_count"]
            else:
                weak_limit = max(2.5,0.15*rise)
                falling_count = max(watch["falling"],state["down_count"])
            weak = falling_count >= 2 and retrace >= weak_limit

            # Invalidate any bearish hypothesis on an observed fresh peak.
            if (state["stage"] != "PUMP_START"
                    and price >= state["observed_peak"]*0.9999
                    and price > old_price*1.002
                    and state["stage"] not in ("DUMP_CONTINUATION", "DUMP_EXHAUSTION")):
                state.update(support=None,weakening_at=None,break_price=None,
                             break_at=None,break_count=0,flow_delta=None,
                             flow_status=None,flow_verified_at=None,
                             flow_observed_at=None,flow_evidence=None)
                _transition(db,state,"PUMP_CONTINUATION",price,now,
                            "BUYERS_RECLAIMED_HIGH",emitted)
            elif state["stage"] == "PUMP_START":
                if price > old_price*1.002:
                    _transition(db,state,"PUMP_CONTINUATION",price,now,
                                "OBSERVED_HIGHER_PRICE",emitted)

            if weak and state["stage"] in ("PUMP_START", "PUMP_CONTINUATION"):
                support = _swing_support(history, state["observed_peak_at"])
                state.update(support=support,weakening_at=now,
                             break_price=None,break_at=None,break_count=0)
                _transition(db,state,"PUMP_WEAKENING",price,now,
                            "RETRACE_AND_CONSECUTIVE_LOWER_QUOTES",emitted)
                needs_flow.append((symbol,price))
            elif state["stage"] == "PUMP_WEAKENING":
                support = state["support"]
                if support is not None and price < support*0.998:
                    state["break_count"] += 1
                    if state["break_count"] >= STRUCTURE_CONFIRM_SAMPLES:
                        if state["break_at"] is None:
                            state["break_at"] = now
                            state["break_price"] = price
                            emitted.append((symbol,"TREND_BREAK_CANDIDATE",
                                            "OBSERVED_PIVOT_LOST_NO_TRADE"))
                        if _fresh_sell(state, now):
                            _transition(db,state,"REVERSAL_CONFIRMED",price,now,
                                        "PRICE_STRUCTURE_AND_FRESH_SELL_FLOW",
                                        emitted)
                        elif (state["last_flow_request"] is None or
                              now-state["last_flow_request"] >= 120):
                            needs_flow.append((symbol,price))
                else:
                    state["break_count"] = 0
                    state["break_at"] = None
                    state["break_price"] = None
            elif state["stage"] == "REVERSAL_CONFIRMED":
                if (state["break_price"] is not None and
                        price <= state["break_price"]*0.99):
                    _transition(db,state,"DUMP_CONTINUATION",price,now,
                                "FURTHER_1PCT_BELOW_BREAK",emitted)
            elif state["stage"] == "DUMP_CONTINUATION":
                if price >= state["trough"]*1.015:
                    _transition(db,state,"DUMP_EXHAUSTION",price,now,
                                "PRICE_REBOUND_ONLY_NO_LONG_APPROVAL",emitted)
            state["last_at"] = now
            state["last_price"] = price
            # A temporary request marker is updated after sender accepts.
            db.execute("""UPDATE pump_cycle_state SET
                last_at=?,stage=?,peak=?,peak_at=?,observed_peak=?,
                observed_peak_at=?,last_price=?,support=?,weakening_at=?,
                break_price=?,break_at=?,break_count=?,trough=?,
                stage_changed_at=?,flow_delta=?,flow_status=?,
                flow_verified_at=?,flow_observed_at=?,flow_evidence=?,
                down_count=? WHERE symbol=?""",
                (now,state["stage"],state["peak"],state["peak_at"],
                 state["observed_peak"],state["observed_peak_at"],price,
                 state["support"],state["weakening_at"],state["break_price"],
                 state["break_at"],state["break_count"],state["trough"],
                 state["stage_changed_at"],state["flow_delta"],state["flow_status"],
                 state["flow_verified_at"],state["flow_observed_at"],
                 state["flow_evidence"],state["down_count"],symbol))
        db.execute("DELETE FROM pump_cycle_state WHERE last_at<?",
                   (now-72*3600,))
        db.execute("""DELETE FROM pump_cycle_events WHERE id IN (
            SELECT id FROM pump_cycle_events ORDER BY id DESC
            LIMIT -1 OFFSET ?)""", (MAX_EVENTS,))
    sender = _dispatch_flow if flow_sender is None else flow_sender
    for symbol, price in needs_flow[:4]:
        try:
            accepted = sender(symbol,price,now,db_path)
            if accepted:
                with sqlite3.connect(db_path,timeout=4) as db:
                    db.execute("""UPDATE pump_cycle_state
                        SET last_flow_request=? WHERE symbol=?""",(now,symbol))
        except Exception as exc:
            print("[PUMP_CYCLE_FLOW_DISPATCH_ERROR]",symbol,
                  type(exc).__name__,flush=True)
    for symbol, stage, reason in emitted[:15]:
        print("[PUMP_CYCLE]",symbol,"stage=",stage,
              "reason=",reason,"research_only=true no_trade_approval=true",
              flush=True)
    return emitted


# Research forward markouts are point-in-time sampled quotes, NOT exact
# intra-candle TP/SL first touches or executable trade fills.
OUTCOME_HORIZONS = (300, 900, 1800)
MAX_MARKOUT_LAG_SEC = 120
ASSUMED_ROUNDTRIP_COST_PCT = 0.22
_last_cycle_quality_report = None


def observe_outcomes(tickers, now=None, db_path=None):
    """Auditable forward markouts after genuine stage-4 events.

    At most one snapshot near each horizon. If none exists in the permitted
    interval, record MISSING rather than synthesizing an exit price.
    """
    now = time.time() if now is None else float(now)
    db_path = DB_PATH if db_path is None else db_path
    if not math.isfinite(now):
        return []
    quotes = _quote_rows(tickers)
    finished = []
    with sqlite3.connect(db_path, timeout=5) as db:
        _init(db)
        cases = db.execute("""SELECT id,symbol,at,price
            FROM pump_cycle_events WHERE stage='REVERSAL_CONFIRMED'
                AND at>=? AND at<=?
            ORDER BY id DESC LIMIT 2000""",
            (now-10*86400, now)).fetchall()
        for ident,symbol,start,entry in cases:
            if not math.isfinite(entry) or entry<=0:
                continue
            for horizon in OUTCOME_HORIZONS:
                target = start+horizon
                if now<target:
                    continue
                already = db.execute("""SELECT 1 FROM pump_cycle_outcomes
                    WHERE event_id=? AND horizon_sec=?""",
                    (ident,horizon)).fetchone()
                if already:
                    continue
                quote = quotes.get(symbol)
                lag = now-target
                if lag<=MAX_MARKOUT_LAG_SEC and quote is not None:
                    gross = 100*(entry/quote-1)  # Percent P&L on a short is (entry-exit)/entry, not entry/exit-1.
                    gross = 100*(entry-quote)/entry
                    net = gross-ASSUMED_ROUNDTRIP_COST_PCT
                    db.execute("""INSERT INTO pump_cycle_outcomes
                        (event_id,horizon_sec,status,sampled_at,lag_sec,
                         quote_price,gross_short_pct,net_short_pct)
                        VALUES (?,?, 'DONE',?,?,?,?,?)""",
                        (ident,horizon,now,lag,quote,round(gross,6),round(net,6)))
                    finished.append((symbol,horizon,round(net,4),"DONE"))
                elif lag>MAX_MARKOUT_LAG_SEC:
                    db.execute("""INSERT INTO pump_cycle_outcomes
                        (event_id,horizon_sec,status,lag_sec)
                        VALUES (?,?,'MISSING',?)""",
                        (ident,horizon,lag))
                    finished.append((symbol,horizon,None,"MISSING"))
        db.execute("""DELETE FROM pump_cycle_outcomes WHERE event_id IN (
            SELECT id FROM pump_cycle_events WHERE at<?
        )""",(now-10*86400,))
    for symbol,horizon,net,status in finished[:12]:
        print("[PUMP_CYCLE_OUTCOME]", symbol,
              "horizon=",horizon//60,"min status=",status,
              "net_short_pct=",net,
              "sampled_quotes_only=true roundtrip_assumption_pct=0.22",
              "no_orders=true no_trade_approval=true",flush=True)
    return finished


def report_outcomes(now=None, db_path=None):
    """Hourly group-level evaluation, not an automatic trading approval."""
    global _last_cycle_quality_report
    now = time.time() if now is None else float(now)
    if _last_cycle_quality_report is not None and (
            now-_last_cycle_quality_report < 3600):
        return []
    _last_cycle_quality_report = now
    db_path = DB_PATH if db_path is None else db_path
    results = []
    try:
        with sqlite3.connect(db_path, timeout=4) as db:
            _init(db)
            rows = db.execute("""SELECT o.horizon_sec,
                  count(*) AS samples,
                  sum(CASE WHEN o.status='DONE' THEN 1 ELSE 0 END),
                  count(DISTINCT CASE WHEN o.status='DONE' THEN c.symbol END),
                  count(DISTINCT CASE WHEN o.status='DONE'
                    THEN CAST(c.at/86400 AS INTEGER) END),
                  avg(CASE WHEN o.status='DONE' THEN o.net_short_pct END),
                  sum(CASE WHEN o.status='DONE' AND o.net_short_pct>0
                    THEN 1 ELSE 0 END)
                FROM pump_cycle_outcomes o
                JOIN pump_cycle_events c ON c.id=o.event_id
                GROUP BY o.horizon_sec ORDER BY o.horizon_sec""").fetchall()
        for horizon,total,done,symbols,days,mean_net,positive in rows:
            result = {"horizon_min":horizon//60,"total":total,
                      "done":done or 0,"symbols":symbols or 0,
                      "days":days or 0,"mean_net":mean_net,
                      "positive":positive or 0}
            results.append(result)
            print("[PUMP_CYCLE_QUALITY]",result,
                  "research_only=true future_leakage=false",
                  "sampled_quotes_only=true not_first_touch=true",
                  "no_trade_approval=true",flush=True)
    except Exception as exc:
        print("[PUMP_CYCLE_QUALITY_ERROR]",type(exc).__name__,flush=True)
    return results
