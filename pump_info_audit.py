"""Forward audit of ACTUALLY DELIVERED PumpDump price-watch notifications.

Observable quote markouts, not TP/SL first-touch, executable fills, or proven
trading edge. Reuses existing scanner ticker batches: NO extra market requests.
Each event is tracked by observed exchange and source time window (5/10/20/30m).
Previously sent events lacking entry prices cannot be reconstructed.
"""
import math
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

DB_PATH = os.getenv("PUMP_INFO_AUDIT_DB", "/data/pump_info_audit.sqlite3")
HORIZONS = (5, 10, 20, 30)
WINDOWS = ("5m", "10m", "20m", "30m")
COST_PCT = 0.22
MIN_DIRECTIONAL_MOVE_PCT = 0.50
MAX_QUOTE_LAG_SEC = 240
MAX_EVENTS = 7000
RETENTION_SEC = 35 * 86400
_last_report = 0


@contextmanager
def _db():
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path), timeout=5)
    try:
        db.execute("PRAGMA busy_timeout=5000")
        yield db
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def _setup(db):
    db.execute("""CREATE TABLE IF NOT EXISTS info_alerts(
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL, exchange TEXT NOT NULL,
        window TEXT NOT NULL, kind TEXT NOT NULL,
        reason TEXT NOT NULL, sent_at REAL NOT NULL,
        sent_minute INTEGER NOT NULL, reference_price REAL NOT NULL,
        trigger_pct REAL,
        UNIQUE(symbol,exchange,window,kind,reason,sent_minute)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS info_markouts(
        alert_id INTEGER NOT NULL, horizon_min INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING', verdict TEXT,
        sampled_at REAL, quote_lag_sec REAL, price REAL,
        directional_pct REAL, hypothetical_net_pct REAL,
        PRIMARY KEY(alert_id,horizon_min),
        FOREIGN KEY(alert_id) REFERENCES info_alerts(id)
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS info_watch_due
        ON info_alerts(exchange,sent_at)""")
    db.execute("""CREATE TABLE IF NOT EXISTS info_daily_reports(
        day TEXT PRIMARY KEY, sent_at REAL NOT NULL
    )""")


def _number(raw):
    try:
        n = float(raw)
    except (ValueError, TypeError, OverflowError):
        return None
    return n if math.isfinite(n) else None


def record_delivered(event, reason, now=None):
    """Record only after a positive Telegram acknowledgement; fail closed."""
    now = time.time() if now is None else float(now)
    if not isinstance(event, dict) or not math.isfinite(now) or now <= 0:
        return False
    symbol = event.get("symbol")
    kind = event.get("type")
    side = (event.get("decision") or {}).get("direction")
    exchange = event.get("exchange") or "OKX"  # 5m OKX scanner has no field
    window = str(event.get("window") or "~5m").lstrip("~")
    price = _number(event.get("price"))
    move = _number(event.get("change"))
    reason = str(reason)
    if (not isinstance(symbol,str) or not symbol.endswith("USDT")
            or len(symbol)>35 or not symbol[:-4].isalnum()
            or (kind,side) not in (("PUMP","UP"),("DUMP","DOWN"))
            or exchange not in ("OKX","Bybit") or window not in WINDOWS
            or price is None or price<=0
            or reason not in ("PRICE_THRESHOLD","PRICE_CONTINUATION")):
        return False
    with _db() as db:
        _setup(db)
        cursor=db.execute("""INSERT OR IGNORE INTO info_alerts
            (symbol,exchange,window,kind,reason,sent_at,sent_minute,
             reference_price,trigger_pct) VALUES (?,?,?,?,?,?,?,?,?)""",
            (symbol,exchange,window,kind,reason,now,int(now//60),price,move))
        if cursor.rowcount != 1:
            return False
        key=cursor.lastrowid
        db.executemany("""INSERT INTO info_markouts(alert_id,horizon_min)
            VALUES (?,?)""",((key,h) for h in HORIZONS))
        # Bounded retention; first delete children of expired/overflow alerts.
        old=now-RETENTION_SEC
        db.execute("""DELETE FROM info_markouts WHERE alert_id IN (
            SELECT id FROM info_alerts WHERE sent_at<?)""",(old,))
        db.execute("""DELETE FROM info_alerts WHERE sent_at<?""",(old,))
        db.execute("""DELETE FROM info_markouts WHERE alert_id IN (
            SELECT id FROM info_alerts ORDER BY sent_at DESC
            LIMIT -1 OFFSET ?)""",(MAX_EVENTS,))
        db.execute("""DELETE FROM info_alerts WHERE id NOT IN
            (SELECT DISTINCT alert_id FROM info_markouts)""")
    print(f"[PUMP_INFO_AUDIT_SAVED] symbol={symbol} exchange={exchange} "
          f"window={window} kind={kind} price={price:.9g} "
          f"reason={reason} observational_only=true",flush=True)
    return True


def _quotes(tickers,exchange):
    prices={}
    if exchange not in ("OKX","Bybit"):
        return prices
    for t in tickers or ():
        if not isinstance(t,dict):
            continue
        if exchange=="OKX":
            inst=t.get("instId")
            if not isinstance(inst,str) or not inst.endswith("-USDT-SWAP"):
                continue
            symbol=inst[:-10]+"USDT"
            price=_number(t.get("last"))
        else:
            symbol=t.get("symbol")
            price=_number(t.get("lastPrice"))
        if (isinstance(symbol,str) and symbol.endswith("USDT")
                and len(symbol)<=35 and symbol[:-4].isalnum()
                and price is not None and price>0):
            prices[symbol]=price
    return prices


def _verdict(kind,ref,price):
    signed=(1 if kind=="PUMP" else -1)*(price/ref-1)*100
    if signed>=MIN_DIRECTIONAL_MOVE_PCT:
        verdict="CONTINUED"
    elif signed<=-MIN_DIRECTIONAL_MOVE_PCT:
        verdict="REVERSED"
    else:
        verdict="FLAT"
    return verdict,signed,signed-COST_PCT


def observe_quotes(tickers,exchange,now=None):
    """One sampled quote per horizon, strictly after alert ACK, <=4m lag."""
    now=time.time() if now is None else float(now)
    if not math.isfinite(now) or exchange not in ("OKX","Bybit"):
        return 0
    prices=_quotes(tickers,exchange)
    with _db() as db:
        _setup(db)
        rows=db.execute("""SELECT o.alert_id,o.horizon_min,a.sent_at,
                   a.symbol,a.kind,a.window,a.reference_price
            FROM info_markouts o JOIN info_alerts a ON a.id=o.alert_id
            WHERE o.status='PENDING' AND a.exchange=? AND a.sent_at<=?
            ORDER BY a.sent_at,o.horizon_min""",(exchange,now)).fetchall()
        resolved=0
        for alert_id,h,ts,symbol,kind,window,ref in rows:
            due=ts+60*h
            lag=now-due
            if lag<0:
                continue
            if lag>MAX_QUOTE_LAG_SEC:
                db.execute("""UPDATE info_markouts SET status='MISSING'
                    WHERE alert_id=? AND horizon_min=? AND status='PENDING'""",
                    (alert_id,h))
                continue
            quote=prices.get(symbol)
            if quote is None:
                continue
            label,pct,net=_verdict(kind,ref,quote)
            cursor=db.execute("""UPDATE info_markouts SET
                status='DONE',verdict=?,sampled_at=?,quote_lag_sec=?,
                price=?,directional_pct=?,hypothetical_net_pct=?
                WHERE alert_id=? AND horizon_min=? AND status='PENDING'""",
                (label,now,lag,quote,pct,net,alert_id,h))
            if cursor.rowcount:
                resolved+=1
                print(f"[PUMP_INFO_AUDIT_RESULT] symbol={symbol} "
                      f"exchange={exchange} window={window} kind={kind} "
                      f"horizon={h}m verdict={label} move={pct:+.3f}% "
                      f"net_model={net:+.3f}% lag_sec={lag:.0f} "
                      "quote_markout_no_orders=true",flush=True)
    return resolved


def summary(day=None,now=None):
    """Source-window and horizon outcomes; never treat missing as a loss."""
    now=time.time() if now is None else float(now)
    criteria=""
    params=()
    if day is not None:
        local=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=ZoneInfo("Europe/Warsaw"))
        start=local.timestamp()
        end=(local+timedelta(days=1)).timestamp()
        criteria=" WHERE a.sent_at>=? AND a.sent_at<?"
        params=(start,end)
    with _db() as db:
        _setup(db)
        rows=db.execute("""SELECT a.window,a.kind,o.horizon_min,o.status,
                        o.verdict,o.directional_pct,o.hypothetical_net_pct
            FROM info_alerts a JOIN info_markouts o ON a.id=o.alert_id"""
            +criteria,params).fetchall()
    out={}
    for window,kind,h,status,verdict,direction_pct,net in rows:
        key=(window,kind,h)
        item=out.setdefault(key,{"total":0,"priced":0,"continued":0,
                                 "reversed":0,"flat":0,"missing":0,
                                 "pending":0,"sum_direction":0.0,"sum_net":0.0})
        item["total"]+=1
        if status=="MISSING":
            item["missing"]+=1
        elif status!="DONE":
            item["pending"]+=1
        else:
            item["priced"]+=1
            item[{"CONTINUED":"continued","REVERSED":"reversed",
                  "FLAT":"flat"}[verdict]]+=1
            item["sum_direction"]+=direction_pct
            item["sum_net"]+=net
    return out


def print_report(now=None):
    global _last_report
    now=time.time() if now is None else float(now)
    if now-_last_report < 3600:
        return False
    _last_report=now
    data=summary(now=now)
    total=sum(v["total"] for (w,k,h),v in data.items() if h==30)
    print(f"[PUMP_INFO_AUDIT_SUMMARY] alerts={total} "
          f"groups={len(data)} baseline_cost={COST_PCT:.2f}% "
          "source=telegram_ack observational_only=true",flush=True)
    for (window,kind,h),v in sorted(data.items()):
        n=v["priced"]
        average="NA" if not n else f"{v['sum_direction']/n:+.3f}%"
        print(f"[PUMP_INFO_AUDIT_GROUP] window={window} kind={kind} "
              f"h={h}m total={v['total']} observed={n} "
              f"continued={v['continued']} reversed={v['reversed']} "
              f"flat={v['flat']} missing={v['missing']} pending={v['pending']} "
              f"mean_directional={average} no_trades=true",flush=True)
    return True


def _daily_text(day,data):
    parts=["📊 PUMPDUMP — ПРОВЕРКА УВЕДОМЛЕНИЙ",
           f"Дата отправки: {day} (Польша)",
           "Реальные уведомления с Telegram ACK; только последующие котировки.",
           "30 минут после уведомления по каждой группе:"]
    for window in WINDOWS:
        for kind,icon in (("PUMP","🚀"),("DUMP","🔻")):
            v=data.get((window,kind,30))
            if not v:
                continue
            n=v["priced"]
            mean="нет данных" if not n else f"{v['sum_direction']/n:+.2f}%"
            parts.append(f"{icon} {window} {kind}: {n}/{v['total']} замеров, "
                         f"продолжилось {v['continued']}, "
                         f"развернулось {v['reversed']}, "
                         f"боковик {v['flat']}, "
                         f"неизвестно {v['missing']+v['pending']}; "
                         f"среднее {mean}")
    parts.extend((
        "Продолжение/разворот: изменение минимум ±0.5% в направлении импульса.",
        "⚠️ НЕ подтверждение торговой стратегии. Это цена по котировкам,",
        "не сделки, не TP/SL и не фактическая прибыль.",
        "Модельные расходы 0.22% только в журнале; проскальзывание не учтено.",
    ))
    return "\n".join(parts)


def daily_digest(send,now=None):
    """One summary of the previous complete Warsaw day, after 09:00 local."""
    now=time.time() if now is None else float(now)
    local=datetime.fromtimestamp(now,ZoneInfo("Europe/Warsaw"))
    if local.hour<9 or not callable(send):
        return False
    day=(local.date()-timedelta(days=1)).isoformat()
    data=summary(day,now=now)
    if not data:
        return False
    with _db() as db:
        _setup(db)
        if db.execute("SELECT 1 FROM info_daily_reports WHERE day=?",(day,)).fetchone():
            return False
    try:
        if send(_daily_text(day,data)) is not True:
            return False
        with _db() as db:
            _setup(db)
            db.execute("INSERT OR IGNORE INTO info_daily_reports VALUES (?,?)",
                       (day,now))
        print(f"[PUMP_INFO_AUDIT_DAILY_SENT] day={day} "
              "observational_only=true",flush=True)
        return True
    except Exception as exc:
        print("[PUMP_INFO_AUDIT_DAILY_ERROR]",type(exc).__name__,flush=True)
        return False
