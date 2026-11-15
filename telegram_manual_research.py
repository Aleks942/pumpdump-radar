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


def _verdict(ticker,oi_local,oi_agg,fut,spot,funding):
    # Manual mode is Bybit-first: local Bybit OI is primary.
    oi=_num(oi_local.get("change_pct")) if oi_local.get("ready") is True else None
    if oi is None:
        oi=_num(oi_agg.get("change_pct")) if oi_agg.get("ready") is True else None
    fimb=_num(fut.get("imbalance_pct")) if fut.get("quality")=="READY" else None
    simb=_num(spot.get("cvd_percent")) if spot and spot.get("available") else None
    score=0
    reasons=[]
    if oi is not None:
        if oi>=0.10: score+=1; reasons.append("OI растёт")
        elif oi<=-0.10: score-=1; reasons.append("OI снижается")
    if fimb is not None:
        if fimb>=8: score+=2; reasons.append("фьючерсные покупки")
        elif fimb<=-8: score-=2; reasons.append("фьючерсные продажи")
    if simb is not None:
        if simb>=10: score+=1; reasons.append("спот покупает")
        elif simb<=-10: score-=1; reasons.append("спот продаёт")
    if funding is not None and funding<=-0.03:
        reasons.append("отрицательный funding: возможное squeeze-топливо")
    if score>=3:
        label="🟡 БЫЧЬЕ ДАВЛЕНИЕ — НУЖНО ПОДТВЕРЖДЕНИЕ ЦЕНОЙ"
    elif score<=-3:
        label="🟡 МЕДВЕЖЬЕ ДАВЛЕНИЕ — НУЖНО ПОДТВЕРЖДЕНИЕ ЦЕНОЙ"
    else:
        label="⚪ НАПРАВЛЕНИЕ ПОКА НЕ ПОДТВЕРЖДЕНО"
    return label,", ".join(reasons) if reasons else "данных недостаточно"


def build_snapshot(symbol):
    ticker=fetch_ticker(symbol)
    if ticker is None:
        return None,None
    try:
        watch_symbol(symbol)
        watch_bybit_flow_symbol(symbol)
    except Exception:
        pass
    oi_local=_bybit_oi5(symbol,ticker.get("open_interest"))
    oi_agg=_agg_oi5(symbol)
    fut=_flow5(symbol)
    try:
        spot=get_spot_cvd(_okx_inst(symbol)) or {}
    except Exception:
        spot={}
    funding=ticker.get("funding_pct")
    liq=_liqs(symbol)
    verdict,reasons=_verdict(ticker,oi_local,oi_agg,fut,spot,funding)

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
        f"OI сейчас: {ticker.get('open_interest_value'):,.0f} USDT" if ticker.get("open_interest_value") is not None else "OI сейчас: нет данных",
        f"OI Bybit 5м: {_fmt(local_oi)}",
        f"OI агрегированный 5м: {_fmt(agg_oi)}",
        f"Futures delta 5м: {fdelta:+,.2f} USDT" if fdelta is not None else "Futures delta 5м: нет данных",
        f"Futures imbalance 5м: {_fmt(fimb)}",
        f"Spot delta: {sdelta:+,.2f} USDT" if sdelta is not None else "Spot delta: нет данных",
        f"Spot imbalance: {_fmt(simb)}",
        f"Funding Bybit: {_fmt(funding,4)}",
        f"Ликвидации 5м LONG/SHORT: {long_liq:,.0f} USD / {short_liq:,.0f} USD · {liq_sources}" if liq.get("available") and long_liq is not None and short_liq is not None else f"Ликвидации 5м: нет подтверждённых данных · {liq_quality}",
        "",
        verdict,
        f"Почему: {reasons}",
        "Это исследование, не автоматическое разрешение на вход.",
    ]
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
            self.add(symbol)
            try:
                watch_bybit_flow_symbol(symbol)
            except Exception:
                pass
            try:
                report,verdict=build_snapshot(symbol)
            except Exception as exc:
                print("[MANUAL_RESEARCH_ERROR]",symbol,type(exc).__name__,flush=True)
                report=None; verdict=None
            if report:
                with self.lock:
                    if symbol in self.watch:
                        self.watch[symbol]["last_verdict"]=verdict
                        self.watch[symbol]["last_sent"]=time.time()
                        self.watch[symbol]["candidate_verdict"]=None
                        self.watch[symbol]["candidate_count"]=0
                self._send(report,symbol)
            else:
                self._send(f"⚠️ {symbol}: не удалось получить Bybit USDT perpetual данные.",symbol)
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
