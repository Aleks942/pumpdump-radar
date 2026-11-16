"""Telegram manual coin research for PumpDump Radar.

Commands accepted only from configured CHAT_ID:
  /check JCTUSDT   (or /check JCT, or plain JCTUSDT)
  /stop JCTUSDT
  /watchlist

/check starts a six-hour research watch, sends an immediate factual snapshot,
then refreshes every minute and only sends again when the compact verdict
changes or after 30 minutes. No orders and no trade approval.
"""
import math
import re
from difflib import get_close_matches
import threading
import time
from collections import deque

import requests

from aggregated_oi import watch_symbol, get_aggregated_oi
from bybit_taker_stream import watch_symbol as watch_bybit_flow_symbol, get_flow as get_bybit_flow
from spot_cvd_engine import get_spot_cvd
from liquidation_engine import get_liquidation_summary, watch_bybit_symbol

WATCH_TTL_SEC=6*3600
REFRESH_SEC=60
STATUS_REPEAT_SEC=30*60
_BYBIT_OI_HISTORY={}
_BYBIT_OI_LOCK=threading.RLock()
_PRICE_HISTORY={}
_PRICE_LOCK=threading.RLock()
_ALLOWED=re.compile(r"^[A-Z0-9]{1,20}$")


def normalize_symbol(text):
    raw=str(text or "").strip().upper()
    raw=raw.replace("-USDT-SWAP","USDT").replace("/","").replace("-","")
    if raw.startswith("$"):
        raw=raw[1:]
    if raw.endswith("USDT"):
        base=raw[:-4]
    else:
        base=raw
        raw=base+"USDT"
    if not _ALLOWED.fullmatch(base):
        return None
    return raw


def parse_command(text):
    raw=str(text or "").strip()
    if not raw:
        return None,None
    parts=raw.split()
    cmd=parts[0].lower().split("@",1)[0]
    if cmd in ("/check","/research","/coin") and len(parts)>=2:
        return "CHECK",normalize_symbol(parts[1])
    if cmd in ("/stop","/uncheck") and len(parts)>=2:
        return "STOP",normalize_symbol(parts[1])
    if cmd=="/watchlist":
        return "LIST",None
    if len(parts)==1 and not raw.startswith("/"):
        symbol=normalize_symbol(raw)
        return ("CHECK",symbol) if symbol else (None,None)
    return None,None


def _num(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError,OverflowError):
        return None


def _okx_inst(symbol):
    return symbol[:-4]+"-USDT-SWAP"


def _get_json(url,params=None,timeout=7):
    r=requests.get(url,params=params,timeout=timeout)
    r.raise_for_status()
    data=r.json()
    if not isinstance(data,dict) or data.get("code")!="0":
        raise RuntimeError("OKX_SCHEMA")
    return data


def _bybit_json(path,params=None,timeout=7):
    r=requests.get("https://api.bybit.com"+path,params=params,timeout=timeout)
    r.raise_for_status()
    data=r.json()
    if not isinstance(data,dict) or data.get("retCode")!=0:
        raise RuntimeError("BYBIT_SCHEMA")
    return data


def suggest_bybit_symbol(symbol):
    try:
        data=_bybit_json("/v5/market/instruments-info",
                         {"category":"linear","limit":1000})
        rows=((data.get("result") or {}).get("list") or [])
        symbols=[
            str(row.get("symbol") or "").upper()
            for row in rows
            if str(row.get("quoteCoin") or "").upper()=="USDT"
            and str(row.get("status") or "").lower()=="trading"
        ]
        return get_close_matches(str(symbol).upper(),symbols,n=3,cutoff=0.72)
    except Exception:
        return []


def fetch_ticker(symbol):
    data=_bybit_json("/v5/market/tickers",
                     {"category":"linear","symbol":symbol})
    rows=((data.get("result") or {}).get("list") or [])
    if len(rows)!=1:
        return None
    row=rows[0]
    last=_num(row.get("lastPrice")); high=_num(row.get("highPrice24h"))
    low=_num(row.get("lowPrice24h")); prev24=_num(row.get("prevPrice24h"))
    oi=_num(row.get("openInterest")); oi_value=_num(row.get("openInterestValue"))
    funding=_num(row.get("fundingRate"))
    if last is None or last<=0:
        return None
    return {
        "exchange":"Bybit",
        "last":last,"high24h":high,"low24h":low,"open24h":prev24,
        "change24h_pct":(100*(last/prev24-1)) if prev24 and prev24>0 else None,
        "drop_from_high_pct":(100*(1-last/high)) if high and high>0 else None,
        "open_interest":oi,
        "open_interest_value":oi_value,
        "funding_pct":funding*100 if funding is not None else None,
    }


def fetch_funding(symbol):
    ticker=fetch_ticker(symbol)
    return ticker.get("funding_pct") if ticker else None

def _flow5(symbol):
    try:
        row=get_bybit_flow(symbol,category="linear") or {}
        if not isinstance(row,dict):
            return {}
        # Manual research reports partial data while the 5m window warms.
        if row.get("ready") is True:
            row["quality"]="READY"
        elif row.get("quality") in ("PARTIAL","WARMING"):
            row["quality"]="WARMING"
        return row
    except Exception:
        return {}

def _bybit_oi5(symbol,current_oi,now=None):
    now=time.time() if now is None else float(now)
    current=_num(current_oi)
    if current is None or current<=0:
        return {"ready":False,"quality":"NO_CURRENT_OI"}
    with _BYBIT_OI_LOCK:
        q=_BYBIT_OI_HISTORY.setdefault(symbol,deque(maxlen=20))
        if not q or now-q[-1][0]>=20:
            q.append((now,current))
        while q and now-q[0][0]>15*60:
            q.popleft()
        target=now-300
        candidates=[row for row in q if row[0]<=now-240]
        if not candidates:
            return {"ready":False,"quality":"WARMING"}
        ts,previous=min(candidates,key=lambda row:abs(row[0]-target))
        if abs(ts-target)>90 or previous<=0:
            return {"ready":False,"quality":"WARMING"}
        return {
            "ready":True,
            "quality":"READY",
            "period_sec":now-ts,
            "change_pct":100*(current/previous-1),
        }


def _price5(symbol,current_price,now=None):
    now=time.time() if now is None else float(now)
    current=_num(current_price)
    if current is None or current<=0:
        return {"ready":False,"quality":"NO_CURRENT_PRICE"}
    with _PRICE_LOCK:
        q=_PRICE_HISTORY.setdefault(symbol,deque(maxlen=20))
        if not q or now-q[-1][0]>=20:
            q.append((now,current))
        while q and now-q[0][0]>15*60:
            q.popleft()
        target=now-300
        candidates=[row for row in q if row[0]<=now-240]
        if not candidates:
            return {"ready":False,"quality":"WARMING"}
        ts,previous=min(candidates,key=lambda row:abs(row[0]-target))
        if abs(ts-target)>90 or previous<=0:
            return {"ready":False,"quality":"WARMING"}
        return {
            "ready":True,
            "quality":"READY",
            "period_sec":now-ts,
            "change_pct":100*(current/previous-1),
        }


def _agg_oi5(symbol):
    try:
        data=get_aggregated_oi(symbol) or {}
        return ((data.get("windows") or {}).get("5m") or {})
    except Exception:
        return {}


def _liqs(symbol):
    # Manual mode is Bybit-first. Do not force an OKX liquidation REST call:
    # Bybit-only symbols such as EVAA would generate false OKX errors.
    try:
        watch_bybit_symbol(symbol)
    except Exception:
        pass
    try:
        return get_liquidation_summary(symbol) or {}
    except Exception:
        return {}


def _fmt(v,d=2,suffix="%"):
    return "нет данных" if v is None else f"{v:+.{d}f}{suffix}"


def _verdict(ticker,price5,oi_local,oi_agg,fut,spot,funding,liq):
    # Manual mode is Bybit-first: local Bybit OI is primary.
    p5=_num(price5.get("change_pct")) if price5.get("ready") is True else None
    oi=_num(oi_local.get("change_pct")) if oi_local.get("ready") is True else None
    if oi is None:
        oi=_num(oi_agg.get("change_pct")) if oi_agg.get("ready") is True else None
    fimb=_num(fut.get("imbalance_pct")) if fut.get("quality")=="READY" else None
    simb=_num(spot.get("cvd_percent")) if spot and spot.get("available") else None
    long_liq=_num((liq or {}).get("long_liq"))
    short_liq=_num((liq or {}).get("short_liq"))

    reasons=[]
    if p5 is not None:
        reasons.append("цена 5м растёт" if p5>=0.40 else
                       "цена 5м падает" if p5<=-0.40 else
                       "цена 5м держится")
    if oi is not None:
        reasons.append("OI растёт" if oi>=0.10 else
                       "OI снижается" if oi<=-0.10 else
                       "OI почти не меняется")
    if fimb is not None:
        if fimb>=8: reasons.append("фьючерсы покупают")
        elif fimb<=-8: reasons.append("фьючерсы продают")
    if simb is not None:
        if simb>=10: reasons.append("спот покупает")
        elif simb<=-10: reasons.append("спот продаёт")
    if funding is not None and funding<=-0.03:
        reasons.append("отрицательный фандинг даёт топливо для выноса шортов")

    # Specific market-structure labels come before the generic pressure score.
    if (p5 is not None and p5>=0.40 and oi is not None and oi<=-0.10
            and ((fimb is not None and fimb>=5)
                 or (short_liq is not None and short_liq>max(long_liq or 0,0)))):
        return ("🟡 ФОРМИРУЕТСЯ ШОРТ-СКВИЗ",
                ", ".join(reasons) or "цена растёт при сокращении OI")

    if (p5 is not None and p5<=-0.40 and oi is not None and oi>=0.10
            and fimb is not None and fimb<=-8):
        return ("🟠 НАБОР НОВЫХ ШОРТОВ",
                ", ".join(reasons) or "цена падает, OI растёт, фьючерсы продают")

    if (p5 is not None and p5>=0.40 and oi is not None and oi>=0.10
            and fimb is not None and fimb>=8):
        return ("🟡 НАБОР НОВЫХ ЛОНГОВ",
                ", ".join(reasons) or "цена и OI растут вместе с покупками")

    if (p5 is not None and p5<=-0.40 and oi is not None and oi<=-0.10):
        return ("🟠 ЛИКВИДАЦИЯ/ЗАКРЫТИЕ ЛОНГОВ",
                ", ".join(reasons) or "цена и OI снижаются одновременно")

    if (fimb is not None and fimb<=-8 and simb is not None and simb>=10
            and (p5 is None or p5>-0.40)):
        return ("🟡 ПОГЛОЩЕНИЕ ПРОДАЖ — СПОТ ВЫКУПАЕТ",
                ", ".join(reasons) or "фьючерсы продают, а спот покупает")

    score=0
    if oi is not None:
        if oi>=0.10: score+=1
        elif oi<=-0.10: score-=1
    if fimb is not None:
        if fimb>=8: score+=2
        elif fimb<=-8: score-=2
    if simb is not None:
        if simb>=10: score+=1
        elif simb<=-10: score-=1

    if score>=3:
        label="🟡 БЫЧЬЕ ДАВЛЕНИЕ — НУЖНО ПОДТВЕРЖДЕНИЕ ЦЕНОЙ"
    elif score<=-3:
        label="🟡 МЕДВЕЖЬЕ ДАВЛЕНИЕ — НУЖНО ПОДТВЕРЖДЕНИЕ ЦЕНОЙ"
    else:
        label="⚪ НАПРАВЛЕНИЕ ПОКА НЕ ПОДТВЕРЖДЕНО"

    missing=[]
    if p5 is None: missing.append("изменение цены за 5м")
    if oi is None: missing.append("OI за 5м")
    if fimb is None: missing.append("поток фьючерсов")
    if not reasons and missing:
        reasons.append("ещё накапливаются данные: "+", ".join(missing))
    return label,", ".join(reasons) if reasons else "данных недостаточно"


def _detail_lines(ticker,price5_pct,local_oi,agg_oi,fimb,simb,funding,
                  long_liq,short_liq,liq_available):
    lines=["", "🧠 <b>ЧТО ПРОИСХОДИТ СЕЙЧАС</b>"]
    missing=[]
    if price5_pct is None: missing.append("изменение цены за 5 минут")
    if local_oi is None and agg_oi is None: missing.append("динамика открытого интереса за 5 минут")
    if fimb is None: missing.append("поток фьючерсных сделок")
    if simb is None: missing.append("спотовый поток")

    if price5_pct is not None:
        if price5_pct>=0.40:
            lines.append(f"• Цена за 5м растёт: {price5_pct:+.2f}%. Покупатель пока двигает цену вверх.")
        elif price5_pct<=-0.40:
            lines.append(f"• Цена за 5м падает: {price5_pct:+.2f}%. Продавец пока двигает цену вниз.")
        else:
            lines.append(f"• Цена за 5м почти стоит: {price5_pct:+.2f}%. Идёт борьба без явного импульса.")

    oi=local_oi if local_oi is not None else agg_oi
    if oi is not None:
        if oi>=0.10:
            lines.append(f"• Открытый интерес растёт: {oi:+.2f}%. В рынок заходят новые позиции.")
        elif oi<=-0.10:
            lines.append(f"• Открытый интерес снижается: {oi:+.2f}%. Позиции закрываются или ликвидируются.")
        else:
            lines.append(f"• Открытый интерес почти не меняется: {oi:+.2f}%.")

    if fimb is not None:
        if fimb>=8:
            lines.append(f"• Фьючерсы покупают: дисбаланс {fimb:+.2f}%. Агрессивный поток сейчас в сторону покупок.")
        elif fimb<=-8:
            lines.append(f"• Фьючерсы продают: дисбаланс {fimb:+.2f}%. Агрессивный поток сейчас в сторону продаж.")
        else:
            lines.append(f"• На фьючерсах перевес слабый: {fimb:+.2f}%.")

    if simb is not None:
        if simb>=10:
            lines.append(f"• Спот покупает: {simb:+.2f}%. Это усиливает бычий сценарий.")
        elif simb<=-10:
            lines.append(f"• Спот продаёт: {simb:+.2f}%. Это усиливает медвежий сценарий.")
        else:
            lines.append(f"• На споте сильного перевеса нет: {simb:+.2f}%.")

    if funding is not None:
        if funding>0:
            lines.append(f"• Фандинг {funding:+.4f}%: лонги платят шортам. Сильный плюс может означать перегрев лонгов.")
        elif funding<0:
            lines.append(f"• Фандинг {funding:+.4f}%: шорты платят лонгам. При росте цены это может стать топливом для выноса шортов.")
        else:
            lines.append("• Фандинг около нуля: выраженного перекоса по стоимости удержания позиции нет.")

    if liq_available and long_liq is not None and short_liq is not None:
        if long_liq>short_liq and long_liq>0:
            lines.append(f"• За 5м больше ликвидировали лонгов: ${long_liq:,.0f} против ${short_liq:,.0f} шортов.")
        elif short_liq>long_liq and short_liq>0:
            lines.append(f"• За 5м больше ликвидировали шортов: ${short_liq:,.0f} против ${long_liq:,.0f} лонгов.")
        elif long_liq==0 and short_liq==0:
            lines.append("• В доступной выборке за 5м ликвидаций не зафиксировано. Это не означает, что их не было на всём рынке.")

    bull=0; bear=0
    if price5_pct is not None:
        if price5_pct>=0.40: bull+=1
        elif price5_pct<=-0.40: bear+=1
    if fimb is not None:
        if fimb>=8: bull+=2
        elif fimb<=-8: bear+=2
    if simb is not None:
        if simb>=10: bull+=1
        elif simb<=-10: bear+=1

    lines += ["", "⚖️ <b>КТО СИЛЬНЕЕ</b>"]
    if bull>=3 and bull>bear:
        lines.append("• Сейчас преимущество у покупателей, но вход подтверждаем только ценой и структурой.")
    elif bear>=3 and bear>bull:
        lines.append("• Сейчас преимущество у продавцов, но вход подтверждаем только ценой и структурой.")
    else:
        lines.append("• Явного победителя пока нет: рынок в переходном состоянии или данных ещё мало.")

    lines += ["", "📌 <b>ЧТО ПОДТВЕРДИТ ЛОНГ</b>",
              "• Цена продолжает расти и ломает локальный максимум.",
              "• Открытый интерес растёт вместе с ценой либо снижается на росте при явных ликвидациях шортов.",
              "• Фьючерсный поток остаётся положительным; желательно, чтобы спот тоже покупал."]
    lines += ["", "📌 <b>ЧТО ПОДТВЕРДИТ ШОРТ</b>",
              "• Цена ломает локальную поддержку и не возвращается выше неё.",
              "• При падении OI растёт — это усиливает сценарий набора новых шортов.",
              "• Фьючерсный поток остаётся отрицательным; продажи на споте усиливают сценарий."]

    if missing:
        lines += ["", "⏳ <b>ЧЕГО ПОКА НЕ ХВАТАЕТ</b>",
                  "• " + "; ".join(missing) + ".",
                  "• Первые несколько минут после запуска наблюдения бот накапливает 5-минутную историю. До её появления вывод предварительный."]

    lines += ["", "🎯 <b>ДЕЙСТВИЕ</b>"]
    if missing:
        lines.append("• Пока ждать. Не входить только по 24ч движению или одному фандингу.")
    elif bull>=3 and bull>bear:
        lines.append("• Готовиться к ЛОНГ-сценарию, но ждать подтверждения цены/структуры.")
    elif bear>=3 and bear>bull:
        lines.append("• Готовиться к ШОРТ-сценарию, но ждать подтверждения цены/структуры.")
    else:
        lines.append("• Наблюдать. Направление ещё не подтверждено.")
    return lines

def build_snapshot(symbol):
    ticker=fetch_ticker(symbol)
    if ticker is None:
        return None,None
    try:
        watch_symbol(symbol)
        watch_bybit_flow_symbol(symbol)
    except Exception:
        pass
    price5=_price5(symbol,ticker.get("last"))
    oi_local=_bybit_oi5(symbol,ticker.get("open_interest"))
    oi_agg=_agg_oi5(symbol)
    fut=_flow5(symbol)
    try:
        spot=get_spot_cvd(_okx_inst(symbol)) or {}
    except Exception:
        spot={}
    funding=ticker.get("funding_pct")
    liq=_liqs(symbol)
    verdict,reasons=_verdict(ticker,price5,oi_local,oi_agg,fut,spot,funding,liq)

    price5_pct=_num(price5.get("change_pct")) if price5.get("ready") else None
    local_oi=_num(oi_local.get("change_pct")) if oi_local.get("ready") else None
    agg_oi=_num(oi_agg.get("change_pct")) if oi_agg.get("ready") else None
    fimb=_num(fut.get("imbalance_pct")) if fut.get("quality")=="READY" else None
    fdelta=_num(fut.get("delta_quote")) if fut.get("quality")=="READY" else None
    simb=_num(spot.get("cvd_percent")) if spot.get("available") else None
    sdelta=_num(spot.get("delta")) if spot.get("available") else None

    long_liq=_num(liq.get("long_liq"))
    short_liq=_num(liq.get("short_liq"))
    liq_quality=str(liq.get("quality") or "UNAVAILABLE")
    liq_sources=str(liq.get("exchanges") or "нет данных")
    lines=[
        "🔬 <b>РУЧНОЕ ИССЛЕДОВАНИЕ</b>",
        f"<b>{symbol}</b> · Bybit",
        f"Цена: {ticker['last']:.8g}",
        f"24ч: {_fmt(ticker.get('change24h_pct'))}",
        f"От максимума 24ч: {_fmt(-(ticker.get('drop_from_high_pct') or 0)) if ticker.get('drop_from_high_pct') is not None else 'нет данных'}",
        "",
        f"Изменение цены 5м: {_fmt(price5_pct)}",
        f"Открытый интерес сейчас: {ticker.get('open_interest_value'):,.0f} USDT" if ticker.get("open_interest_value") is not None else "Открытый интерес сейчас: нет данных",
        f"Открытый интерес Bybit 5м: {_fmt(local_oi)}",
        f"Открытый интерес по биржам 5м: {_fmt(agg_oi)}",
        f"Дельта фьючерсов 5м: {fdelta:+,.2f} USDT" if fdelta is not None else "Дельта фьючерсов 5м: нет данных",
        f"Дисбаланс фьючерсов 5м: {_fmt(fimb)}",
        f"Дельта спота: {sdelta:+,.2f} USDT" if sdelta is not None else "Дельта спота: нет данных",
        f"Дисбаланс спота: {_fmt(simb)}",
        f"Фандинг Bybit: {_fmt(funding,4)}",
        f"Ликвидации 5м ЛОНГ/ШОРТ: {long_liq:,.0f} USD / {short_liq:,.0f} USD · {liq_sources}" if liq.get("available") and long_liq is not None and short_liq is not None else f"Ликвидации 5м: нет подтверждённых данных · {liq_quality}",
        "",
        verdict,
        f"Почему: {reasons}",
    ]
    lines.extend(_detail_lines(
        ticker,price5_pct,local_oi,agg_oi,fimb,simb,funding,
        long_liq,short_liq,liq.get("available") is True
    ))
    lines += ["", "⚠️ Это исследование, не автоматическое разрешение на вход."]
    return "\n".join(lines),verdict


class ManualResearchBot:
    def __init__(self,token,chat_id,sender):
        self.token=str(token or "")
        self.chat_id=str(chat_id or "")
        self.sender=sender
        self.offset=0
        self.watch={}
        self.lock=threading.RLock()

    def _authorized(self,message):
        chat=(message or {}).get("chat") or {}
        return str(chat.get("id"))==self.chat_id

    def _send(self,text,symbol="MANUAL"):
        try:
            return self.sender(text,symbol=symbol)
        except TypeError:
            return self.sender(text)

    def add(self,symbol,now=None):
        now=time.time() if now is None else float(now)
        with self.lock:
            self.watch[symbol]={"expires":now+WATCH_TTL_SEC,
                                "last_verdict":None,"last_sent":0.0,
                                "candidate_verdict":None,"candidate_count":0}
        try:
            watch_symbol(symbol)
            watch_bybit_symbol(symbol)
        except Exception:
            pass

    def remove(self,symbol):
        with self.lock:
            return self.watch.pop(symbol,None) is not None

    def list_symbols(self):
        with self.lock:
            return sorted(self.watch)

    def _handle(self,text):
        action,symbol=parse_command(text)
        if action=="CHECK" and symbol:
            try:
                report,verdict=build_snapshot(symbol)
            except Exception as exc:
                print("[MANUAL_RESEARCH_ERROR]",symbol,type(exc).__name__,flush=True)
                report=None; verdict=None
            if report:
                self.add(symbol)
                try:
                    watch_bybit_flow_symbol(symbol)
                except Exception:
                    pass
                with self.lock:
                    if symbol in self.watch:
                        self.watch[symbol]["last_verdict"]=verdict
                        self.watch[symbol]["last_sent"]=time.time()
                        self.watch[symbol]["candidate_verdict"]=None
                        self.watch[symbol]["candidate_count"]=0
                self._send(report,symbol)
            else:
                suggestions=suggest_bybit_symbol(symbol)
                if suggestions:
                    msg=(f"⚠️ {symbol}: такого USDT-фьючерса на Bybit не найдено. "
                         f"Возможно, ты имел в виду: {', '.join(suggestions)}")
                else:
                    msg=(f"⚠️ {symbol}: такого USDT-фьючерса на Bybit не найдено. "
                         "Проверь тикер и отправь его ещё раз.")
                self._send(msg,symbol)
            return True
        if action=="STOP" and symbol:
            removed=self.remove(symbol)
            self._send(("✅ Наблюдение остановлено: " if removed else "ℹ️ Монета не была в наблюдении: ")+symbol,symbol)
            return True
        if action=="LIST":
            symbols=self.list_symbols()
            self._send("👀 Ручное наблюдение: "+(", ".join(symbols) if symbols else "пусто"))
            return True
        return False

    def prime_offset(self):
        if not self.token:
            return 0
        try:
            r=requests.get(f"https://api.telegram.org/bot{self.token}/getUpdates",
                           params={"timeout":0,"limit":100,"allowed_updates":["message"]},
                           timeout=10)
            data=r.json()
            if r.status_code!=200 or not isinstance(data,dict) or data.get("ok") is not True:
                return 0
            rows=data.get("result") or []
            if rows:
                self.offset=max(int(x.get("update_id") or 0) for x in rows)+1
            return len(rows)
        except Exception:
            return 0

    def poll_once(self):
        if not self.token:
            return 0
        r=requests.get(f"https://api.telegram.org/bot{self.token}/getUpdates",
                       params={"offset":self.offset,"timeout":0,"allowed_updates":["message"]},
                       timeout=10)
        data=r.json()
        if r.status_code!=200 or not isinstance(data,dict) or data.get("ok") is not True:
            return 0
        count=0
        for upd in data.get("result") or []:
            uid=int(upd.get("update_id") or 0)
            self.offset=max(self.offset,uid+1)
            msg=upd.get("message") or {}
            if not self._authorized(msg):
                continue
            text=msg.get("text")
            if isinstance(text,str) and self._handle(text):
                count+=1
        return count

    def refresh(self,now=None):
        now=time.time() if now is None else float(now)
        with self.lock:
            symbols=list(self.watch)
        for symbol in symbols:
            with self.lock:
                state=self.watch.get(symbol)
            if not state:
                continue
            if now>=state["expires"]:
                self.remove(symbol)
                self._send(f"⌛ Ручное наблюдение {symbol} завершено через 6 часов.",symbol)
                continue
            try:
                report,verdict=build_snapshot(symbol)
            except Exception as exc:
                print("[MANUAL_RESEARCH_REFRESH_ERROR]",symbol,type(exc).__name__,flush=True)
                continue
            periodic=now-state.get("last_sent",0)>=STATUS_REPEAT_SEC
            confirmed_change=False
            with self.lock:
                current=self.watch.get(symbol)
                if current is None:
                    continue
                if verdict==current.get("last_verdict"):
                    current["candidate_verdict"]=None
                    current["candidate_count"]=0
                elif verdict==current.get("candidate_verdict"):
                    current["candidate_count"]=int(current.get("candidate_count",0))+1
                else:
                    current["candidate_verdict"]=verdict
                    current["candidate_count"]=1
                # Direction must survive two consecutive refreshes before
                # Telegram treats it as a real state change. This prevents
                # 1-minute bearish/bullish/neutral flapping.
                confirmed_change=current["candidate_count"]>=2
            if report and (confirmed_change or periodic):
                self._send(report,symbol)
                with self.lock:
                    if symbol in self.watch:
                        self.watch[symbol]["last_verdict"]=verdict
                        self.watch[symbol]["last_sent"]=now
                        self.watch[symbol]["candidate_verdict"]=None
                        self.watch[symbol]["candidate_count"]=0

    def run_forever(self):
        print("[MANUAL_RESEARCH_BOT_STARTED]",flush=True)
        last_refresh=0.0
        poll_errors=0
        while True:
            try:
                self.poll_once()
                poll_errors=0
            except Exception as exc:
                poll_errors+=1
                print("[MANUAL_RESEARCH_POLL_ERROR]",type(exc).__name__,
                      "streak=",poll_errors,flush=True)
            now=time.time()
            if now-last_refresh>=REFRESH_SEC:
                self.refresh(now)
                last_refresh=now
            # Telegram/network errors are transient. Back off instead of
            # hammering getUpdates every two seconds during an outage.
            delay=min(30,2**min(poll_errors,4)) if poll_errors else 2
            time.sleep(delay)


def start(token,chat_id,sender):
    bot=ManualResearchBot(token,chat_id,sender)
    skipped=bot.prime_offset()
    print("[MANUAL_RESEARCH_PRIMED] skipped_old_updates=",skipped,flush=True)
    thread=threading.Thread(target=bot.run_forever,
                            name="telegram-manual-research",daemon=True)
    thread.start()
    return bot
