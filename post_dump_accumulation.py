"""Post-dump accumulation detector.

Research-only. Finds coins that have already fallen hard, then compress near the
lows while OI and taker buying begin to build before price acceleration.

Lifecycle:
FALLEN_COIN -> BASE_FORMING -> BUYER_ABSORPTION -> OI_BUILDUP
-> ENERGY_BUILDUP -> BULLS_ACCUMULATING -> BREAKOUT_READY -> LONG_TRIGGER

ENERGY_BUILDUP is direction-neutral. BULLS_ACCUMULATING is allowed only after
price breaks the frozen base ceiling with strong OI and futures buying.

No orders. No new service. Uses existing ticker/OI/flow caches only.
"""
import json
import math
import os
import sqlite3
import time
from contextlib import closing

from post_pump_short_evidence import take_cached_evidence
from ticker_metrics import approx_quote_volume_usdt, spread_bps
from long_forward_stats import register as register_forward, advance as advance_forward, maybe_log_report

DB_PATH=os.getenv("PUMP_WATCH_DB","/data/pump_watch.sqlite3")
MIN_DROP_FROM_HIGH_PCT=25.0
MIN_NOTIONAL=8_000_000.0
MAX_SPREAD_BPS=45.0
SAMPLE_RETENTION_SEC=90*60
BASE_EARLY_AGE_SEC=10*60
BASE_CONFIRM_AGE_SEC=30*60
BASE_MAX_RANGE_PCT=3.0
BASE_MAX_DISTANCE_FROM_LOW_PCT=4.0
BUY_IMB_MIN=8.0
SPOT_BLOCK=-20.0
OI_BUILD_MIN=0.10
OI_STRONG_MIN=0.30
BREAKOUT_PCT=0.35
BREAKOUT_HOLD_PCT=0.20
BULL_CONFIRM_HOLD_SEC=120
LONG_TRIGGER_HOLD_SEC=60
MAX_ACTIVE=12
EPISODE_TTL_SEC=24*3600


def _num(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError,OverflowError):
        return None


def _profile(t):
    if not isinstance(t,dict):
        return None
    inst=str(t.get("instId") or "")
    if not inst.endswith("-USDT-SWAP"):
        return None
    last=_num(t.get("last")); high=_num(t.get("high24h")); low=_num(t.get("low24h"))
    vol=approx_quote_volume_usdt(t); spr=spread_bps(t)
    if any(x is None or x<=0 for x in (last,high,vol)) or spr is None:
        return None
    drop=100*(1-last/high)
    return {
        "symbol":inst[:-10]+"USDT","last":last,"high":high,"low24h":low,
        "drop_from_high_pct":drop,"notional":vol,"spread":spr,
        "liquid":vol>=MIN_NOTIONAL and spr<=MAX_SPREAD_BPS,
    }


def _setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS post_dump_accumulation_state(
        symbol TEXT PRIMARY KEY,episode_at REAL NOT NULL,last_seen_at REAL NOT NULL,
        phase TEXT NOT NULL,phase_at REAL NOT NULL,episode_low REAL NOT NULL,
        base_ceiling REAL,last_price REAL NOT NULL,last_reason TEXT,
        last_evidence_json TEXT
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_dump_accumulation_quotes(
        symbol TEXT NOT NULL,ts REAL NOT NULL,price REAL NOT NULL,
        PRIMARY KEY(symbol,ts)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_dump_accumulation_audit(
        id INTEGER PRIMARY KEY,symbol TEXT NOT NULL,episode_at REAL NOT NULL,
        at REAL NOT NULL,phase TEXT NOT NULL,reason TEXT NOT NULL,
        price REAL NOT NULL,drop_from_high_pct REAL NOT NULL,
        evidence_json TEXT,paper_only INTEGER NOT NULL DEFAULT 1
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS post_dump_accumulation_notify(
        audit_id INTEGER PRIMARY KEY,symbol TEXT NOT NULL,phase TEXT NOT NULL,
        payload TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'PENDING',sent_at REAL
    )""")


def _oi(e):
    a=_num(e.get("agg_oi_5m_pct"))
    l=_num(e.get("local_oi_5m_pct"))
    return a if a is not None else l


def _base_stats(db,symbol,now,current):
    rows=db.execute("""SELECT ts,price FROM post_dump_accumulation_quotes
        WHERE symbol=? AND ts>=? AND ts<? ORDER BY ts""",
        (symbol,now-SAMPLE_RETENTION_SEC,now)).fetchall()
    pts=[(float(r["ts"]),float(r["price"])) for r in rows]
    if not pts:
        return {"ready":False}
    prices=[p for _,p in pts]+[current]
    age=now-pts[0][0]
    lo=min(prices); hi=max(prices)
    rng=100*(hi/lo-1) if lo>0 else None
    dist=100*(current/lo-1) if lo>0 else None
    return {
        "ready":age>=BASE_EARLY_AGE_SEC,
        "confirmed_age":age>=BASE_CONFIRM_AGE_SEC,
        "age_sec":age,"low":lo,"high":hi,
        "range_pct":rng,"distance_from_low_pct":dist,
        "base_forming":(
            age>=BASE_EARLY_AGE_SEC and rng is not None and rng<=BASE_MAX_RANGE_PCT
            and dist is not None and dist<=BASE_MAX_DISTANCE_FROM_LOW_PCT
        ),
        "mature_base":(
            age>=BASE_CONFIRM_AGE_SEC and rng is not None and rng<=BASE_MAX_RANGE_PCT
            and dist is not None and dist<=BASE_MAX_DISTANCE_FROM_LOW_PCT
        ),
    }


def _conditions(e,base,price):
    fut=_num(e.get("futures_taker_5m_imbalance_pct"))
    spot=_num(e.get("spot_taker_5m_imbalance_pct"))
    oi=_oi(e)
    spot_block=spot is not None and spot<=SPOT_BLOCK
    buyer_absorption=(base.get("mature_base") is True and fut is not None
                      and fut>=BUY_IMB_MIN and not spot_block)
    oi_build=oi is not None and oi>=OI_BUILD_MIN
    energy_building=buyer_absorption and oi_build
    strong_oi=oi is not None and oi>=OI_STRONG_MIN
    return {
        "futures_imbalance_pct":fut,"spot_imbalance_pct":spot,"oi_5m_pct":oi,
        "buyer_absorption":buyer_absorption,"oi_build":oi_build,
        "energy_building":energy_building,"strong_oi":strong_oi,
        "spot_block":spot_block,
    }


def _audit(db,st,p,now,phase,reason,snapshot):
    cur=db.execute("""INSERT INTO post_dump_accumulation_audit(
        symbol,episode_at,at,phase,reason,price,drop_from_high_pct,
        evidence_json,paper_only) VALUES(?,?,?,?,?,?,?,?,1)""",
        (p["symbol"],st["episode_at"],now,phase,reason,p["last"],
         p["drop_from_high_pct"],json.dumps(snapshot,sort_keys=True)))
    aid=int(cur.lastrowid)
    if phase in ("ENERGY_BUILDUP","BULLS_ACCUMULATING","BREAKOUT_READY","LONG_TRIGGER"):
        register_forward(db, "POST_DUMP_ACCUM", aid, p["symbol"], phase,
                         now, p["last"])
    if phase in ("BASE_FORMING","BUYER_ABSORPTION","ENERGY_BUILDUP",
                 "BULLS_ACCUMULATING","BREAKOUT_READY","LONG_TRIGGER"):
        payload={"audit_id":aid,"symbol":p["symbol"],"phase":phase,"at":now,
                 "price":p["last"],"drop_from_high_pct":p["drop_from_high_pct"],
                 "evidence":snapshot}
        db.execute("""INSERT OR IGNORE INTO post_dump_accumulation_notify
            (audit_id,symbol,phase,payload) VALUES(?,?,?,?)""",
            (aid,p["symbol"],phase,json.dumps(payload)))
    return aid


def _set_phase(db,st,p,now,phase,reason,snapshot,ceiling=None):
    db.execute("""UPDATE post_dump_accumulation_state SET
        phase=?,phase_at=?,last_seen_at=?,last_price=?,
        episode_low=?,base_ceiling=COALESCE(?,base_ceiling),
        last_reason=?,last_evidence_json=? WHERE symbol=?""",
        (phase,now,now,p["last"],min(float(st["episode_low"]),p["last"]),
         ceiling,reason,json.dumps(snapshot,sort_keys=True),p["symbol"]))
    _audit(db,st,p,now,phase,reason,snapshot)


def observe(tickers,now=None,db_path=None,evidence_reader=None):
    now=time.time() if now is None else float(now)
    path=db_path or DB_PATH
    evidence_reader=evidence_reader or take_cached_evidence
    profiles={p["symbol"]:p for p in map(_profile,tickers or ()) if p}
    with closing(sqlite3.connect(path,timeout=5)) as db:
        db.row_factory=sqlite3.Row
        _setup(db)
        db.execute("DELETE FROM post_dump_accumulation_state WHERE ?-last_seen_at>?",
                   (now,EPISODE_TTL_SEC))
        states={r["symbol"]:r for r in db.execute(
            "SELECT * FROM post_dump_accumulation_state").fetchall()}
        candidates=[p for p in profiles.values()
                    if p["liquid"] and p["drop_from_high_pct"]>=MIN_DROP_FROM_HIGH_PCT]
        candidates.sort(key=lambda p:-p["drop_from_high_pct"])
        for p in candidates:
            if p["symbol"] in states or len(states)>=MAX_ACTIVE:
                continue
            db.execute("""INSERT INTO post_dump_accumulation_state(
                symbol,episode_at,last_seen_at,phase,phase_at,episode_low,
                last_price,last_reason) VALUES(?,?,?,?,?,?,?,?)""",
                (p["symbol"],now,now,"FALLEN_COIN",now,p["last"],p["last"],
                 "STRONG_DROP_OBSERVED"))
            st=db.execute("SELECT * FROM post_dump_accumulation_state WHERE symbol=?",
                          (p["symbol"],)).fetchone()
            _audit(db,st,p,now,"FALLEN_COIN","STRONG_DROP_OBSERVED",{})
            states[p["symbol"]]=st

        for symbol,strow in list(states.items()):
            p=profiles.get(symbol)
            if p is None:
                continue
            st=dict(strow)
            base=_base_stats(db,symbol,now,p["last"])
            ev=evidence_reader(symbol,now) or {}
            cond=_conditions(ev,base,p["last"])
            snap={"base":base,"evidence":ev,"conditions":cond}
            phase=st["phase"]

            if p["last"] < float(st["episode_low"])*0.985:
                _set_phase(db,st,p,now,"FALLEN_COIN","NEW_LOW_RESET",snap)
            elif phase=="FALLEN_COIN" and base.get("base_forming"):
                _set_phase(db,st,p,now,"BASE_FORMING","PRICE_COMPRESSED_NEAR_LOW",
                           snap,ceiling=base.get("high"))
            elif phase=="BASE_FORMING" and cond["buyer_absorption"]:
                _set_phase(db,st,p,now,"BUYER_ABSORPTION",
                           "BUYING_APPEARS_WHILE_PRICE_STAYS_COMPRESSED",snap)
            elif phase=="BUYER_ABSORPTION" and cond["oi_build"]:
                _set_phase(db,st,p,now,"OI_BUILDUP",
                           "OI_EXPANDS_DURING_BASE",snap)
            elif phase=="OI_BUILDUP" and cond["energy_building"]:
                _set_phase(db,st,p,now,"ENERGY_BUILDUP",
                           "BUY_FLOW_AND_OI_BUILD_DIRECTION_NOT_CONFIRMED",snap)
            elif phase=="ENERGY_BUILDUP":
                ceiling=_num(st.get("base_ceiling")) or _num(base.get("high"))
                bullish_break=(
                    ceiling is not None
                    and p["last"]>=ceiling*(1+BREAKOUT_PCT/100)
                    and cond["strong_oi"]
                    and (_num(cond["futures_imbalance_pct"]) or -999)>=BUY_IMB_MIN
                    and not cond["spot_block"]
                )
                if bullish_break:
                    _set_phase(db,st,p,now,"BULLS_ACCUMULATING",
                               "BULLISH_BASE_BREAK_OI_AND_BUY_FLOW_CONFIRMED",snap)
                elif p["last"] < float(st["episode_low"])*0.995:
                    _set_phase(db,st,p,now,"FALLEN_COIN",
                               "ENERGY_BUILDUP_FAILED_NEW_LOW",snap)
            elif phase=="BULLS_ACCUMULATING":
                ceiling=_num(st.get("base_ceiling"))
                holds_break=(
                    ceiling is not None
                    and p["last"]>=ceiling*(1+BREAKOUT_HOLD_PCT/100)
                    and cond["oi_build"]
                    and (_num(cond["futures_imbalance_pct"]) or -999)>=BUY_IMB_MIN
                    and not cond["spot_block"]
                )
                if now-float(st["phase_at"])>=BULL_CONFIRM_HOLD_SEC and holds_break:
                    _set_phase(db,st,p,now,"BREAKOUT_READY",
                               "BULLISH_BREAK_HELD_WITH_OI_AND_BUY_FLOW",snap)
                elif ceiling is not None and p["last"]<ceiling:
                    _set_phase(db,st,p,now,"ENERGY_BUILDUP",
                               "BULLISH_BREAK_NOT_HELD_DIRECTION_UNCONFIRMED",snap)
            elif phase=="BREAKOUT_READY":
                ceiling=_num(st.get("base_ceiling"))
                long_confirmed=(
                    ceiling is not None
                    and p["last"]>=ceiling*(1+BREAKOUT_HOLD_PCT/100)
                    and cond["strong_oi"]
                    and (_num(cond["futures_imbalance_pct"]) or -999)>=BUY_IMB_MIN
                    and not cond["spot_block"]
                )
                if now-float(st["phase_at"])>=LONG_TRIGGER_HOLD_SEC and long_confirmed:
                    _set_phase(db,st,p,now,"LONG_TRIGGER",
                               "BULLISH_BREAK_HELD_AND_FLOW_RECONFIRMED",snap)
                elif ceiling is not None and p["last"]<ceiling:
                    _set_phase(db,st,p,now,"ENERGY_BUILDUP",
                               "BREAKOUT_FAILED_BACK_INSIDE_BASE",snap)

            db.execute("""INSERT OR REPLACE INTO post_dump_accumulation_quotes
                (symbol,ts,price) VALUES(?,?,?)""",(symbol,now,p["last"]))
            db.execute("""UPDATE post_dump_accumulation_state SET
                last_seen_at=?,last_price=?,episode_low=?,last_evidence_json=?
                WHERE symbol=?""",
                (now,p["last"],min(float(st["episode_low"]),p["last"]),
                 json.dumps(snap,sort_keys=True),symbol))
        db.execute("DELETE FROM post_dump_accumulation_quotes WHERE ts<?",
                   (now-SAMPLE_RETENTION_SEC,))
        quotes={symbol:p["last"] for symbol,p in profiles.items()}
        advance_forward(db,"POST_DUMP_ACCUM",quotes,now)
        maybe_log_report(db,"POST_DUMP_ACCUM",now,days=30)
        db.commit()


def _message(e):
    phase=e["phase"]; c=(e.get("evidence") or {}).get("conditions") or {}
    b=(e.get("evidence") or {}).get("base") or {}
    titles={
        "BASE_FORMING":"МОНЕТА ЛЕЖИТ ПОСЛЕ ДАМПА — ФОРМИРУЕТСЯ БАЗА",
        "BUYER_ABSORPTION":"ПОКУПАТЕЛИ ПОЯВЛЯЮТСЯ — ЦЕНА ЕЩЁ НЕ УШЛА",
        "ENERGY_BUILDUP":"СЖАТИЕ ПОСЛЕ ДАМПА — ЭНЕРГИЯ НАКАПЛИВАЕТСЯ",
        "BULLS_ACCUMULATING":"БЫЧЬЕ НАКОПЛЕНИЕ ПОДТВЕРЖДЕНО",
        "BREAKOUT_READY":"ПРОБОЙ БАЗЫ УДЕРЖИВАЕТСЯ",
        "LONG_TRIGGER":"LONG-СЦЕНАРИЙ ПОДТВЕРЖДЁН ПОСЛЕ УДЕРЖАНИЯ ПРОБОЯ",
    }
    priority={
        "BASE_FORMING":"⚪ ИНФО — НАБЛЮДАЕМ",
        "BUYER_ABSORPTION":"⚪ ИНФО — ПОКУПКИ ПОЯВИЛИСЬ",
        "ENERGY_BUILDUP":"🟡 НАБЛЮДАЕМ — НАПРАВЛЕНИЕ НЕ ПОДТВЕРЖДЕНО",
        "BULLS_ACCUMULATING":"🟡 ГОТОВЬСЯ — БЫЧЬЕ НАПРАВЛЕНИЕ ПОДТВЕРЖДЕНО",
        "BREAKOUT_READY":"🟡 ГОТОВЬСЯ — ПРОБОЙ УДЕРЖИВАЕТСЯ",
        "LONG_TRIGGER":"🟡 ТЕСТОВЫЙ LONG — НЕ ВХОДИТЬ АВТОМАТИЧЕСКИ",
    }
    lines=[priority[phase],titles[phase],f"<b>{e['symbol']}</b>",
           f"Цена: {e['price']:.8g}",
           f"Падение от 24ч максимума: -{e['drop_from_high_pct']:.1f}%",
           f"Диапазон базы: {b.get('range_pct') if b.get('range_pct') is not None else 'нет данных'}%",
           f"OI 5м: {c.get('oi_5m_pct') if c.get('oi_5m_pct') is not None else 'нет данных'}%",
           f"Futures imbalance 5м: {c.get('futures_imbalance_pct') if c.get('futures_imbalance_pct') is not None else 'нет данных'}%"]
    if phase=="BASE_FORMING":
        lines+=["База найдена рано. Для рабочих выводов ждём минимум 30 минут устойчивого сжатия."]
    elif phase=="BUYER_ABSORPTION":
        lines+=["База держится минимум 30 минут, покупки уже появились, но цена ещё сжата.",
                "Это наблюдение, не вход."]
    elif phase=="ENERGY_BUILDUP":
        lines+=["OI и покупки растут внутри базы, но цена ещё не доказала направление.",
                "Не входить. Ждём пробой зафиксированной верхней границы базы."]
    elif phase=="BULLS_ACCUMULATING":
        lines+=["Цена пробила верх базы, а OI и покупки подтверждают движение вверх.",
                "Это уже бычье подтверждение, но вход ещё не разрешён: ждём удержание пробоя."]
    elif phase=="BREAKOUT_READY":
        lines+=["Пробой удерживается минимум 2 минуты вместе с OI и покупками.",
                "Не догоняем цену. Ждём повторного подтверждения потока."]
    elif phase=="LONG_TRIGGER":
        lines+=["Пробой удержан и поток повторно подтверждён.",
                "Сигнал пока тестовый: статистика ещё недостаточна для автоматического зелёного входа.",
                "Бот ордер не выставляет."]
    return "\n".join(lines)


def deliver(sender,now=None,db_path=None):
    now=time.time() if now is None else float(now)
    path=db_path or DB_PATH
    with closing(sqlite3.connect(path,timeout=5)) as db:
        db.row_factory=sqlite3.Row; _setup(db)
        rows=db.execute("""SELECT * FROM post_dump_accumulation_notify
            WHERE status='PENDING' ORDER BY audit_id LIMIT 3""").fetchall()
        sent=0
        for row in rows:
            e=json.loads(row["payload"])
            try:
                ok=sender(_message(e),symbol=row["symbol"])
            except TypeError:
                ok=sender(_message(e))
            if ok is True:
                db.execute("""UPDATE post_dump_accumulation_notify
                    SET status='SENT',sent_at=? WHERE audit_id=?""",
                    (now,row["audit_id"]))
                sent+=1
                print("[POST_DUMP_ACCUM_NOTIFY]",row["symbol"],row["phase"],
                      "status=SENT",flush=True)
            else:
                break
        db.commit()
    return sent
