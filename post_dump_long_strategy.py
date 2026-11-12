"""POST-DUMP LONG / squeeze reversal research strategy.

Research-only. No orders. Uses existing PumpDump Radar ticker/OI/flow caches plus
a lightweight cached OKX funding read. Designed for cases where a strong prior
pump has largely returned to its base and a squeeze/reversal may form.

Lifecycle:
POST_DUMP_WATCH -> DUMP_EXHAUSTED -> BULLS_GAINING -> SQUEEZE_SETUP
-> LONG_CONFIRMED -> RETEST -> ENTRY_ZONE

A price dump alone is never enough. LONG confirmation requires bullish futures
aggression, rising OI, negative funding, and no strong contradictory spot flow.
"""
import json
import math
import os
import sqlite3
import time
from contextlib import closing

from post_pump_short_evidence import take_cached_evidence
from ticker_metrics import approx_quote_volume_usdt, spread_bps

DB_PATH = os.getenv("PUMP_WATCH_DB", "/data/pump_watch.sqlite3")
MIN_PRIOR_PUMP_PCT = 30.0
MIN_DROP_FROM_HIGH_PCT = 25.0
MAX_VS_BASE_PCT = 5.0
MIN_NOTIONAL = 10_000_000.0
MAX_SPREAD_BPS = 40.0
MIN_RECOVERY_PCT = 1.5
DUMP_EXHAUSTED_RECOVERY_PCT = 0.8
MIN_OI_PCT = 0.10
MIN_FUTURES_IMB = 10.0
CONFIRM_FUTURES_IMB = 15.0
BLOCK_SPOT_IMB = -15.0
MAX_ACTIVE = 6
FUNDING_TTL_SEC = 300
NEGATIVE_FUNDING_PCT = -0.03
EPISODE_TTL_SEC = 36 * 3600
PRICE_LOOKBACK_SEC = 15 * 60
PRICE_CONFIRM_SEC = 5 * 60
MIN_PRICE_CONFIRM_PCT = 0.40
MIN_STRUCTURE_BREAK_PCT = 0.20

_funding_cache = {}


def _num(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _profile(t):
    if not isinstance(t, dict):
        return None
    inst = str(t.get("instId") or "")
    if not inst.endswith("-USDT-SWAP"):
        return None
    last = _num(t.get("last"))
    high = _num(t.get("high24h"))
    opening = _num(t.get("open24h"))
    low = _num(t.get("low24h"))
    vol = approx_quote_volume_usdt(t)
    spread = spread_bps(t)
    if any(x is None or x <= 0 for x in (last, high, opening, vol)):
        return None
    if spread is None or spread < 0:
        return None
    prior_pump = 100.0 * (high / opening - 1.0)
    drop_from_high = 100.0 * (1.0 - last / high)
    vs_base = 100.0 * (last / opening - 1.0)
    return {
        "symbol": inst[:-10] + "USDT",
        "inst": inst,
        "last": last,
        "high": high,
        "low24h": low,
        "opening": opening,
        "prior_pump_pct": prior_pump,
        "drop_from_high_pct": drop_from_high,
        "vs_base_pct": vs_base,
        "notional": vol,
        "spread": spread,
        "liquid": vol >= MIN_NOTIONAL and spread <= MAX_SPREAD_BPS,
    }


def _candidate(p):
    return (
        p is not None
        and p["liquid"]
        and p["prior_pump_pct"] >= MIN_PRIOR_PUMP_PCT
        and p["drop_from_high_pct"] >= MIN_DROP_FROM_HIGH_PCT
        and p["vs_base_pct"] <= MAX_VS_BASE_PCT
    )


def _setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS post_dump_long_strategy(
        symbol TEXT PRIMARY KEY,
        episode_at REAL NOT NULL,
        last_seen_at REAL NOT NULL,
        phase TEXT NOT NULL,
        phase_at REAL NOT NULL,
        base_price REAL NOT NULL,
        prior_peak REAL NOT NULL,
        episode_low REAL NOT NULL,
        confirm_level REAL,
        last_price REAL NOT NULL,
        last_evidence_json TEXT,
        last_reason TEXT
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_dump_long_audit(
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        episode_at REAL NOT NULL,
        at REAL NOT NULL,
        phase TEXT NOT NULL,
        reason TEXT NOT NULL,
        price REAL NOT NULL,
        prior_pump_pct REAL NOT NULL,
        drop_from_high_pct REAL NOT NULL,
        recovery_pct REAL NOT NULL,
        evidence_json TEXT,
        paper_only INTEGER NOT NULL DEFAULT 1
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_dump_long_notify(
        audit_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        phase TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        sent_at REAL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_dump_long_quotes(
        symbol TEXT NOT NULL,
        ts REAL NOT NULL,
        price REAL NOT NULL,
        PRIMARY KEY(symbol,ts)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_post_dump_long_quotes_ts
        ON post_dump_long_quotes(symbol,ts)""")


def _funding(inst, now=None, request_get=None):
    now = time.time() if now is None else float(now)
    cached = _funding_cache.get(inst)
    if cached and 0 <= now - cached["at"] <= FUNDING_TTL_SEC:
        return dict(cached["data"])
    try:
        if request_get is None:
            import requests
            request_get = requests.get
        r = request_get(
            "https://www.okx.com/api/v5/public/funding-rate",
            params={"instId": inst},
            timeout=(3, 6),
        )
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("data") if payload.get("code") == "0" else None
        if not isinstance(rows, list) or len(rows) != 1:
            raise ValueError("FUNDING_SCHEMA")
        rate = _num(rows[0].get("fundingRate"))
        pct = rate * 100.0 if rate is not None else None
        data = {"available": pct is not None, "funding_pct": pct, "source": "OKX"}
    except Exception as exc:
        data = {"available": False, "funding_pct": None, "source": "OKX",
                "reason": type(exc).__name__}
    _funding_cache[inst] = {"at": now, "data": dict(data)}
    return data


def _oi_value(e):
    agg = _num(e.get("agg_oi_5m_pct"))
    local = _num(e.get("local_oi_5m_pct"))
    return agg if agg is not None else local


def _recovery(price, low):
    if low <= 0:
        return 0.0
    return max(0.0, 100.0 * (price / low - 1.0))


def _price_state(db, symbol, now, price, episode_low):
    rows = db.execute("""SELECT ts,price FROM post_dump_long_quotes
        WHERE symbol=? AND ts>=? AND ts<? ORDER BY ts""",
        (symbol, now-PRICE_LOOKBACK_SEC, now)).fetchall()
    samples=[(float(r["ts"]),float(r["price"])) for r in rows if _num(r["price"]) is not None]
    prior_prices=[p for _,p in samples]
    recent=[(ts,p) for ts,p in samples if ts>=now-PRICE_CONFIRM_SEC]
    base=recent[0][1] if recent else None
    change5=(100.0*(price/base-1.0)) if base and base>0 else None
    local_ceiling=max(prior_prices[-5:]) if prior_prices else None
    structure_break=(
        local_ceiling is not None
        and price >= local_ceiling*(1.0+MIN_STRUCTURE_BREAK_PCT/100.0)
        and price >= episode_low*(1.0+MIN_RECOVERY_PCT/100.0)
    )
    price_confirming=change5 is not None and change5>=MIN_PRICE_CONFIRM_PCT
    return {
        "sample_count":len(samples),
        "change_5m_pct":round(change5,4) if change5 is not None else None,
        "local_ceiling":local_ceiling,
        "structure_break":structure_break,
        "price_confirming":price_confirming,
    }


def _record_price(db,symbol,now,price):
    db.execute("""INSERT OR REPLACE INTO post_dump_long_quotes(symbol,ts,price)
        VALUES(?,?,?)""",(symbol,now,price))
    db.execute("DELETE FROM post_dump_long_quotes WHERE ts<?",
               (now-2*PRICE_LOOKBACK_SEC,))


def _conditions(evidence, funding, recovery_pct, price_state):
    fut = _num(evidence.get("futures_taker_5m_imbalance_pct"))
    spot = _num(evidence.get("spot_taker_5m_imbalance_pct"))
    oi = _oi_value(evidence)
    f = _num(funding.get("funding_pct"))
    futures_buy = fut is not None and fut >= MIN_FUTURES_IMB
    strong_futures_buy = fut is not None and fut >= CONFIRM_FUTURES_IMB
    oi_rising = oi is not None and oi >= MIN_OI_PCT
    funding_negative = f is not None and f <= NEGATIVE_FUNDING_PCT
    spot_block = spot is not None and spot <= BLOCK_SPOT_IMB
    dump_exhausted = (
        recovery_pct >= DUMP_EXHAUSTED_RECOVERY_PCT
        and (fut is None or fut > -10.0)
    )
    buyers = futures_buy and (oi_rising or recovery_pct >= MIN_RECOVERY_PCT)
    price_confirming = price_state.get("price_confirming") is True
    structure_break = price_state.get("structure_break") is True
    absorption_risk = futures_buy and oi_rising and not price_confirming
    bulls_gaining = buyers and price_confirming and not spot_block
    squeeze = (futures_buy and oi_rising and funding_negative and
               price_confirming and structure_break and not spot_block)
    confirmed = (strong_futures_buy and oi_rising and funding_negative and
                 price_confirming and structure_break and not spot_block)
    return {
        "futures_imbalance_pct": fut,
        "spot_imbalance_pct": spot,
        "oi_5m_pct": oi,
        "funding_pct": f,
        "price_state": price_state,
        "price_confirming": price_confirming,
        "structure_break": structure_break,
        "absorption_risk": absorption_risk,
        "dump_exhausted": dump_exhausted,
        "buyers": buyers,
        "bulls_gaining": bulls_gaining,
        "squeeze": squeeze,
        "confirmed": confirmed,
        "spot_block": spot_block,
    }


def _audit(db, st, p, now, phase, reason, recovery_pct, snapshot):
    cur = db.execute("""INSERT INTO post_dump_long_audit(
        symbol,episode_at,at,phase,reason,price,prior_pump_pct,
        drop_from_high_pct,recovery_pct,evidence_json,paper_only)
        VALUES(?,?,?,?,?,?,?,?,?,?,1)""",
        (p["symbol"], st["episode_at"], now, phase, reason, p["last"],
         p["prior_pump_pct"], p["drop_from_high_pct"], recovery_pct,
         json.dumps(snapshot, sort_keys=True)))
    audit_id = int(cur.lastrowid)
    if phase in ("DUMP_EXHAUSTED", "BULLS_GAINING", "SQUEEZE_SETUP",
                 "LONG_CONFIRMED", "RETEST", "ENTRY_ZONE"):
        payload = {
            "audit_id": audit_id, "symbol": p["symbol"], "phase": phase,
            "at": now, "price": p["last"], "prior_pump_pct": p["prior_pump_pct"],
            "drop_from_high_pct": p["drop_from_high_pct"],
            "recovery_pct": recovery_pct, "reason": reason,
            "evidence": snapshot,
        }
        db.execute("""INSERT OR IGNORE INTO post_dump_long_notify
            (audit_id,symbol,phase,payload) VALUES(?,?,?,?)""",
            (audit_id, p["symbol"], phase, json.dumps(payload)))
    return audit_id


def _set_phase(db, st, p, now, phase, reason, recovery_pct, snapshot, confirm_level=None):
    db.execute("""UPDATE post_dump_long_strategy
        SET phase=?,phase_at=?,last_seen_at=?,last_price=?,
            episode_low=?,confirm_level=COALESCE(?,confirm_level),
            last_evidence_json=?,last_reason=?
        WHERE symbol=?""",
        (phase, now, now, p["last"], min(st["episode_low"], p["last"]),
         confirm_level, json.dumps(snapshot, sort_keys=True), reason, p["symbol"]))
    _audit(db, st, p, now, phase, reason, recovery_pct, snapshot)


def observe(tickers, now=None, db_path=None, evidence_reader=None, funding_reader=None):
    now = time.time() if now is None else float(now)
    path = DB_PATH if db_path is None else db_path
    evidence_reader = evidence_reader or take_cached_evidence
    funding_reader = funding_reader or _funding
    profiles = {p["symbol"]: p for p in map(_profile, tickers or ()) if p is not None}

    with closing(sqlite3.connect(path, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        _setup(db)
        db.execute("DELETE FROM post_dump_long_strategy WHERE ?-last_seen_at>?",
                   (now, EPISODE_TTL_SEC))

        states = {r["symbol"]: r for r in db.execute(
            "SELECT * FROM post_dump_long_strategy ORDER BY phase_at DESC").fetchall()}

        candidates = [p for p in profiles.values() if _candidate(p)]
        candidates.sort(key=lambda x: (-x["drop_from_high_pct"], -x["prior_pump_pct"]))

        for p in candidates:
            if p["symbol"] in states:
                continue
            if len(states) >= MAX_ACTIVE:
                break
            db.execute("""INSERT INTO post_dump_long_strategy(
                symbol,episode_at,last_seen_at,phase,phase_at,base_price,
                prior_peak,episode_low,last_price,last_reason)
                VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (p["symbol"], now, now, "POST_DUMP_WATCH", now, p["opening"],
                 p["high"], p["last"], p["last"], "STRONG_DUMP_RETURNED_TO_BASE"))
            st = db.execute("SELECT * FROM post_dump_long_strategy WHERE symbol=?",
                            (p["symbol"],)).fetchone()
            _audit(db, st, p, now, "POST_DUMP_WATCH",
                   "STRONG_DUMP_RETURNED_TO_BASE", 0.0,
                   {"funding": None, "evidence": {}})
            states[p["symbol"]] = st
            print("[POST_DUMP_LONG_DISCOVERED]", p["symbol"],
                  "prior_pump=", round(p["prior_pump_pct"], 2),
                  "drop=", round(p["drop_from_high_pct"], 2), flush=True)

        for symbol, st in list(states.items()):
            p = profiles.get(symbol)
            if p is None:
                continue
            old_low = float(st["episode_low"])
            made_new_low = p["last"] < old_low * 0.998
            low = min(old_low, p["last"])
            recovery_pct = _recovery(p["last"], low)
            ev = evidence_reader(symbol, now) or {}
            fund = funding_reader(p["inst"], now) or {}
            price_state = _price_state(db, symbol, now, p["last"], low)
            cond = _conditions(ev, fund, recovery_pct, price_state)
            snapshot = {"evidence": ev, "funding": fund,
                        "price_state": price_state, "conditions": cond}

            db.execute("""UPDATE post_dump_long_strategy
                SET last_seen_at=?,last_price=?,episode_low=?,last_evidence_json=?
                WHERE symbol=?""",
                (now, p["last"], low, json.dumps(snapshot, sort_keys=True), symbol))

            phase = st["phase"]
            if made_new_low and phase != "POST_DUMP_WATCH":
                _set_phase(db, st, p, now, "POST_DUMP_WATCH",
                           "NEW_LOW_AFTER_BUYING_INVALIDATED_LONG",
                           recovery_pct, snapshot)
            elif phase == "POST_DUMP_WATCH" and cond["dump_exhausted"]:
                _set_phase(db, st, p, now, "DUMP_EXHAUSTED",
                           "SELL_PRESSURE_FADING_AND_PRICE_STOPS_MAKING_LOW",
                           recovery_pct, snapshot)
            elif phase in ("DUMP_EXHAUSTED", "BULLS_GAINING") and cond["absorption_risk"]:
                reason = "BUYING_ABSORBED_PRICE_NOT_CONFIRMING"
                if st["last_reason"] != reason:
                    db.execute("""UPDATE post_dump_long_strategy
                        SET last_reason=?,last_evidence_json=? WHERE symbol=?""",
                        (reason, json.dumps(snapshot, sort_keys=True), symbol))
                    _audit(db, st, p, now, "ABSORPTION_RISK", reason,
                           recovery_pct, snapshot)
                    print("[POST_DUMP_LONG_ABSORPTION_RISK]", symbol,
                          "futures_buy=", cond["futures_imbalance_pct"],
                          "oi=", cond["oi_5m_pct"],
                          "price5m=", price_state.get("change_5m_pct"),
                          "structure_break=", price_state.get("structure_break"),
                          flush=True)
            elif phase == "DUMP_EXHAUSTED" and cond["bulls_gaining"]:
                _set_phase(db, st, p, now, "BULLS_GAINING",
                           "FUTURES_BUYERS_AND_PRICE_CONFIRM",
                           recovery_pct, snapshot)
            elif phase in ("DUMP_EXHAUSTED", "BULLS_GAINING") and cond["squeeze"] and recovery_pct >= MIN_RECOVERY_PCT:
                _set_phase(db, st, p, now, "SQUEEZE_SETUP",
                           "NEGATIVE_FUNDING_OI_AND_BUYERS_ALIGN", recovery_pct, snapshot)
            elif phase == "SQUEEZE_SETUP" and cond["confirmed"] and recovery_pct >= MIN_RECOVERY_PCT:
                _set_phase(db, st, p, now, "LONG_CONFIRMED",
                           "FUTURES_OI_FUNDING_LONG_CONFIRMED", recovery_pct, snapshot,
                           confirm_level=p["last"])
            elif phase == "LONG_CONFIRMED":
                level = _num(st["confirm_level"]) or p["last"]
                if p["last"] < low * 0.998:
                    _set_phase(db, st, p, now, "POST_DUMP_WATCH",
                               "NEW_LOW_INVALIDATED_LONG", recovery_pct, snapshot)
                elif p["last"] <= level * 0.995 and p["last"] >= low * 1.003:
                    _set_phase(db, st, p, now, "RETEST",
                               "PULLBACK_HELD_ABOVE_DUMP_LOW", recovery_pct, snapshot)
            elif phase == "RETEST":
                level = _num(st["confirm_level"]) or p["last"]
                if cond["confirmed"] and p["last"] >= level * 0.997:
                    _set_phase(db, st, p, now, "ENTRY_ZONE",
                               "RETEST_HELD_AND_BUYERS_RECONFIRMED", recovery_pct, snapshot)
                elif p["last"] <= low * 1.001:
                    _set_phase(db, st, p, now, "POST_DUMP_WATCH",
                               "RETEST_FAILED_NEAR_NEW_LOW", recovery_pct, snapshot)
            elif phase == "ENTRY_ZONE":
                if cond["spot_block"] or (_num(cond["futures_imbalance_pct"]) is not None
                                           and cond["futures_imbalance_pct"] < 0):
                    _set_phase(db, st, p, now, "POST_DUMP_WATCH",
                               "BUYER_CONFIRMATION_LOST", recovery_pct, snapshot)

            _record_price(db, symbol, now, p["last"])

        db.commit()


def _message(e):
    phase = e["phase"]
    title = {
        "DUMP_EXHAUSTED": "🟠 ДАМП ВЫДЫХАЕТСЯ — ПРОДАВЕЦ ТЕРЯЕТ СИЛУ",
        "BULLS_GAINING": "🐂 БЫКИ НАБИРАЮТ СИЛУ — ПОКУПАТЕЛЬ ВКЛЮЧАЕТСЯ",
        "SQUEEZE_SETUP": "🟡 POST-DUMP SQUEEZE — ГОТОВИМСЯ К LONG",
        "LONG_CONFIRMED": "🟢 LONG ПОДТВЕРЖДЁН — ЖДЁМ РЕТЕСТ",
        "RETEST": "🔎 LONG RETEST — ПОДДЕРЖКА ДЕРЖИТСЯ",
        "ENTRY_ZONE": "🎯 ЗОНА ВХОДА LONG",
    }[phase]
    c = (e.get("evidence") or {}).get("conditions") or {}
    lines = [
        title,
        f"<b>{e['symbol']}</b>",
        f"Цена: {e['price']:.8g}",
        f"Предыдущий памп: +{e['prior_pump_pct']:.1f}%",
        f"Падение от вершины: -{e['drop_from_high_pct']:.1f}%",
        f"Отскок от дна: +{e['recovery_pct']:.1f}%",
        f"OI 5м: {c.get('oi_5m_pct') if c.get('oi_5m_pct') is not None else 'нет данных'}%",
        f"Futures imbalance 5м: {c.get('futures_imbalance_pct') if c.get('futures_imbalance_pct') is not None else 'нет данных'}%",
        f"Funding: {c.get('funding_pct') if c.get('funding_pct') is not None else 'нет данных'}%",
        f"Цена 5м: {((c.get('price_state') or {}).get('change_5m_pct')) if (c.get('price_state') or {}).get('change_5m_pct') is not None else 'нет данных'}%",
        "Структура вверх: " + ("ДА" if c.get("structure_break") else "НЕТ"),
    ]
    if phase == "DUMP_EXHAUSTED":
        lines += ["✅ Цена перестала ускорять падение и начала отходить от локального дна.",
                  "⚠️ Это ещё не LONG: ждём, чтобы покупатели реально забрали инициативу."]
    elif phase == "BULLS_GAINING":
        lines += ["✅ Покупатели усиливаются: фьючерсный поток разворачивается вверх.",
                  "✅ Цена подтверждает покупки ростом, а не стоит против положительной дельты.",
                  "⏳ До LONG ещё ждём слом локальной нисходящей структуры и squeeze."]
    elif phase == "SQUEEZE_SETUP":
        lines += ["✅ Дамп выдохся, быки усиливаются, отрицательный funding даёт топливо для squeeze.",
                  "Это ещё наблюдение, не вход."]
    elif phase == "LONG_CONFIRMED":
        lines += ["Покупатели подтверждены. Не догоняем цену — ждём откат/ретест."]
    elif phase == "RETEST":
        lines += ["Цена откатила, но дно пока удержано. Ждём повторного включения покупателей."]
    else:
        lines += ["Модель видит рабочую LONG-зону после подтверждённого ретеста.",
                  "Бот ордер не выставляет. Стоп — ниже локального дна/ретеста."]
    return "\n".join(lines)


def deliver(sender, now=None, db_path=None):
    now = time.time() if now is None else float(now)
    path = DB_PATH if db_path is None else db_path
    with closing(sqlite3.connect(path, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        _setup(db)
        rows = db.execute("""SELECT * FROM post_dump_long_notify
            WHERE status='PENDING' ORDER BY audit_id LIMIT 3""").fetchall()
        sent = 0
        for row in rows:
            payload = json.loads(row["payload"])
            try:
                ok = sender(_message(payload), symbol=row["symbol"])
            except TypeError:
                ok = sender(_message(payload))
            if ok is True:
                db.execute("UPDATE post_dump_long_notify SET status='SENT',sent_at=? WHERE audit_id=?",
                           (now, row["audit_id"]))
                sent += 1
                print("[POST_DUMP_LONG_NOTIFY] symbol=", row["symbol"],
                      "phase=", row["phase"], "status=SENT", flush=True)
            else:
                break
        db.commit()
    return sent
