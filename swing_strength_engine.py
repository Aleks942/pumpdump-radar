"""Medium-term SWING strength engine for PumpDump Radar.

Primary horizon: 1 hour to 3 days. Extended context: up to 7 days.
5m/15m flow is timing/context only; it never defines the medium-term direction.

Free public Bybit data only. No orders.
"""
import math
import threading
import time

import requests

BYBIT_BASE="https://api.bybit.com"
CACHE_TTL_SEC=300
_lock=threading.RLock()
_cache={}


def _num(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError,OverflowError):
        return None


def _bybit(path,params,request_get=None):
    request_get=request_get or requests.get
    r=request_get(BYBIT_BASE+path,params=params,timeout=7)
    r.raise_for_status()
    data=r.json()
    if not isinstance(data,dict) or data.get("retCode")!=0:
        raise RuntimeError("BYBIT_API")
    result=data.get("result") or {}
    if not isinstance(result,dict):
        raise RuntimeError("BYBIT_SCHEMA")
    return result


def _interval_sec(interval):
    if interval=="D":
        return 86400
    return int(interval)*60


def _closed_klines(symbol,interval,limit=80,request_get=None,now=None):
    now=time.time() if now is None else float(now)
    result=_bybit("/v5/market/kline",{
        "category":"linear","symbol":symbol,"interval":str(interval),"limit":limit
    },request_get)
    raw=result.get("list") or []
    sec=_interval_sec(interval)
    rows=[]
    for row in raw:
        if not isinstance(row,list) or len(row)<6:
            continue
        ts=_num(row[0]); o=_num(row[1]); h=_num(row[2]); l=_num(row[3])
        c=_num(row[4]); v=_num(row[5])
        if ts is None or any(x is None or x<=0 for x in (o,h,l,c)) or v is None or v<0:
            continue
        if ts>100_000_000_000:
            ts/=1000.0
        if ts+sec>now+3:
            continue
        if not l<=min(o,c)<=max(o,c)<=h:
            continue
        rows.append({"ts":ts,"o":o,"h":h,"l":l,"c":c,"v":v})
    rows.sort(key=lambda x:x["ts"])
    return rows


def _ema(values,n):
    if len(values)<n:
        return None
    a=2.0/(n+1)
    out=sum(values[:n])/n
    for x in values[n:]:
        out=out+a*(x-out)
    return out


def _trend(candles):
    if len(candles)<25:
        return {"ready":False,"regime":"UNKNOWN"}
    closes=[x["c"] for x in candles]
    ema20=_ema(closes,20)
    ema20_prev=_ema(closes[:-3],20)
    if ema20 is None or ema20_prev is None:
        return {"ready":False,"regime":"UNKNOWN"}
    last=closes[-1]
    slope=(ema20/ema20_prev-1)*100 if ema20_prev>0 else 0.0
    recent=candles[-6:]
    prev=candles[-12:-6]
    recent_hi=max(x["h"] for x in recent)
    recent_lo=min(x["l"] for x in recent)
    prev_hi=max(x["h"] for x in prev)
    prev_lo=min(x["l"] for x in prev)
    hh_hl=recent_hi>prev_hi and recent_lo>prev_lo
    lh_ll=recent_hi<prev_hi and recent_lo<prev_lo
    if last>ema20 and slope>0.05 and hh_hl:
        regime="UP"
    elif last<ema20 and slope<-0.05 and lh_ll:
        regime="DOWN"
    else:
        regime="MIXED"
    change3=100*(closes[-1]/closes[-4]-1) if len(closes)>=4 else None
    return {
        "ready":True,"regime":regime,"last":last,"ema20":ema20,
        "ema_slope_pct":slope,"hh_hl":hh_hl,"lh_ll":lh_ll,
        "change_3bars_pct":change3,
    }


def _volume_state(candles):
    if len(candles)<12:
        return {"ready":False}
    recent=sum(x["v"] for x in candles[-3:])/3
    prior=sum(x["v"] for x in candles[-12:-3])/9
    ratio=(recent/prior) if prior>0 else None
    return {"ready":ratio is not None,"ratio":ratio}


def _atr(candles,n=14):
    if len(candles)<n+1:
        return None
    trs=[]
    prev=candles[-n-1]["c"]
    for row in candles[-n:]:
        trs.append(max(row["h"]-row["l"],abs(row["h"]-prev),abs(row["l"]-prev)))
        prev=row["c"]
    return sum(trs)/len(trs) if trs else None


def _entry_setup(candles,bias):
    """Closed-15m retest/reclaim trigger. It never creates SWING direction."""
    if len(candles)<25 or bias not in ("BULLISH","BEARISH"):
        return {"ready":False,"phase":"WAIT_RETEST","reason":"15м истории недостаточно"}

    closes=[x["c"] for x in candles]
    ema20=_ema(closes,20)
    atr=_atr(candles,14)
    if ema20 is None or atr is None or atr<=0:
        return {"ready":False,"phase":"WAIT_RETEST","reason":"15м EMA/ATR ещё не готовы"}

    last=candles[-1]
    prev=candles[-2]
    recent=candles[-5:-1]
    band=0.60*atr

    if bias=="BULLISH":
        touched=any(x["l"]<=ema20+band and x["h"]>=ema20-band for x in recent)
        reclaimed=last["c"]>ema20 and last["c"]>prev["h"] and last["c"]>last["o"]
        if touched and reclaimed:
            return {"ready":True,"phase":"READY_ENTRY",
                    "reason":"15м ретест завершён возвратом покупателей выше локального максимума"}
        if touched:
            return {"ready":False,"phase":"RETEST_FORMING",
                    "reason":"15м ретест есть, но возврат покупателей ещё не подтверждён"}
        return {"ready":False,"phase":"WAIT_RETEST",
                "reason":"ждать 15м откат к рабочей зоне; цену не догонять"}

    touched=any(x["h"]>=ema20-band and x["l"]<=ema20+band for x in recent)
    reclaimed=last["c"]<ema20 and last["c"]<prev["l"] and last["c"]<last["o"]
    if touched and reclaimed:
        return {"ready":True,"phase":"READY_ENTRY",
                "reason":"15м ретест завершён возвратом продавцов ниже локального минимума"}
    if touched:
        return {"ready":False,"phase":"RETEST_FORMING",
                "reason":"15м ретест есть, но возврат продавцов ещё не подтверждён"}
    return {"ready":False,"phase":"WAIT_RETEST",
            "reason":"ждать 15м откат к рабочей зоне; цену не догонять"}


def _trade_plan(candles,bias,entry_price):
    """Structure-based paper plan for a confirmed SWING entry."""
    entry=_num(entry_price)
    if entry is None or entry<=0 or len(candles)<16:
        return {"ready":False,"reason":"недостаточно 15м данных для SL/TP"}
    atr=_atr(candles,14)
    if atr is None or atr<=0:
        return {"ready":False,"reason":"15м ATR не готов"}
    recent=candles[-8:]
    buffer=0.20*atr

    if bias=="BULLISH":
        stop=min(x["l"] for x in recent)-buffer
        risk=entry-stop
        side="LONG"
    elif bias=="BEARISH":
        stop=max(x["h"] for x in recent)+buffer
        risk=stop-entry
        side="SHORT"
    else:
        return {"ready":False,"reason":"нет SWING-направления"}

    if risk<=0:
        return {"ready":False,"reason":"структурный стоп оказался по неверную сторону входа"}
    stop_pct=100.0*risk/entry
    if stop_pct<0.25 or stop_pct>6.0:
        return {"ready":False,"reason":f"структурный стоп {stop_pct:.2f}% вне рабочего диапазона"}

    sign=1.0 if side=="LONG" else -1.0
    return {
        "ready":True,"side":side,"entry":entry,"stop":stop,
        "stop_pct":stop_pct,
        "tp1":entry+sign*2.0*risk,
        "tp2":entry+sign*3.0*risk,
        "rr1":2.0,"rr2":3.0,
    }


def _breakout_state(candles,bias):
    """Require a closed 1H structural breakout before retest/entry."""
    side="LONG" if bias=="BULLISH" else "SHORT" if bias=="BEARISH" else None
    if side is None:
        return {"ready":False,"phase":"WAIT_BREAKOUT","side":None,
                "reason":"SWING-направление ещё не подтверждено"}
    if len(candles)<8:
        return {"ready":False,"phase":"WAIT_BREAKOUT","side":side,
                "reason":"недостаточно закрытых 1H свечей для проверки пробоя"}

    last=candles[-1]
    prior=candles[-7:-1]
    high=max(x["h"] for x in prior)
    low=min(x["l"] for x in prior)

    if side=="LONG":
        confirmed=last["c"]>high and last["c"]>last["o"]
        level=high
        reason=(
            f"1H закрылась выше сопротивления {level:.8g}"
            if confirmed else
            f"ждать закрытие 1H выше сопротивления {level:.8g}"
        )
    else:
        confirmed=last["c"]<low and last["c"]<last["o"]
        level=low
        reason=(
            f"1H закрылась ниже поддержки {level:.8g}"
            if confirmed else
            f"ждать закрытие 1H ниже поддержки {level:.8g}"
        )

    return {
        "ready":confirmed,
        "phase":"BREAKOUT_CONFIRMED" if confirmed else "WAIT_BREAKOUT",
        "side":side,
        "level":level,
        "closed_at":last["ts"],
        "reason":reason,
    }


def _oi_change(symbol,interval,lookback_points,request_get=None,now=None):
    now=time.time() if now is None else float(now)
    try:
        result=_bybit("/v5/market/open-interest",{
            "category":"linear","symbol":symbol,
            "intervalTime":interval,"limit":max(lookback_points+2,6)
        },request_get)
        raw=result.get("list") or []
        pts=[]
        for row in raw:
            if not isinstance(row,dict):
                continue
            ts=_num(row.get("timestamp")); oi=_num(row.get("openInterest"))
            if ts is None or oi is None or oi<=0:
                continue
            if ts>100_000_000_000:
                ts/=1000.0
            if 0<=now-ts<=14*86400:
                pts.append((ts,oi))
        pts.sort()
        if len(pts)<lookback_points+1:
            return {"ready":False,"quality":"WARMING"}
        period=pts[-1][0]-pts[-1-lookback_points][0]
        if not (lookback_points*3600-900<=period<=lookback_points*3600+900):
            return {"ready":False,"quality":"GAPPED_OI_HISTORY"}
        current=pts[-1][1]
        previous=pts[-1-lookback_points][1]
        return {
            "ready":True,"quality":"READY","current":current,
            "change_pct":100*(current/previous-1),
            "period_sec":pts[-1][0]-pts[-1-lookback_points][0],
        }
    except Exception as exc:
        return {"ready":False,"quality":"UNAVAILABLE","error":type(exc).__name__}


def _ticker(symbol,request_get=None):
    result=_bybit("/v5/market/tickers",{"category":"linear","symbol":symbol},request_get)
    rows=result.get("list") or []
    if len(rows)!=1:
        return {}
    row=rows[0]
    last=_num(row.get("lastPrice")); high=_num(row.get("highPrice24h"))
    low=_num(row.get("lowPrice24h")); funding=_num(row.get("fundingRate"))
    return {
        "last":last,"high24h":high,"low24h":low,
        "funding_pct":funding*100 if funding is not None else None,
    }


def _weekly_position(daily,current):
    if len(daily)<7 or current is None:
        return None
    rows=daily[-7:]
    lo=min(x["l"] for x in rows); hi=max(x["h"] for x in rows)
    if hi<=lo:
        return None
    raw=100*(current-lo)/(hi-lo)
    return max(0.0,min(100.0,raw))


def _score(t1,t4,td,oi1,oi4,weekly_pos,vol1,short_term=None):
    score=50
    reasons=[]
    risks=[]

    for name,t,up_pts,down_pts in (
        ("1H",t1,12,12),("4H",t4,18,18),("1D",td,15,15)
    ):
        reg=t.get("regime")
        if reg=="UP":
            score+=up_pts; reasons.append(f"{name}: структура вверх")
        elif reg=="DOWN":
            score-=down_pts; risks.append(f"{name}: структура вниз")
        else:
            reasons.append(f"{name}: смешанная структура")

    # HH/HL and LH/LL are already required by _trend() for UP/DOWN.
    # Scoring them again would double-count the same structural evidence.

    p1=_num(t1.get("change_3bars_pct"))
    oi1c=_num(oi1.get("change_pct")) if oi1.get("ready") else None
    oi4c=_num(oi4.get("change_pct")) if oi4.get("ready") else None
    if p1 is not None and oi1c is not None:
        if p1>0.3 and oi1c>0.15:
            score+=8; reasons.append("1H: цена и OI растут — новые позиции поддерживают движение")
        elif p1< -0.3 and oi1c>0.15:
            score-=8; risks.append("1H: цена падает при росте OI — вероятен набор шортов")
        elif p1>0.3 and oi1c< -0.15:
            reasons.append("1H: цена растёт при снижении OI — возможен вынос/закрытие шортов")
        elif p1< -0.3 and oi1c< -0.15:
            risks.append("1H: цена и OI снижаются — позиции закрываются, тренд может быть истощён")

    if oi4c is not None:
        if t4.get("regime")=="UP" and oi4c>0.2:
            score+=6; reasons.append("4H: OI поддерживает восходящую структуру")
        elif t4.get("regime")=="DOWN" and oi4c>0.2:
            score-=6; risks.append("4H: OI поддерживает нисходящую структуру")

    # 7d range position is context only. High is not automatically bullish
    # and low is not automatically bearish; scoring it encouraged late entries.

    vr=_num(vol1.get("ratio")) if vol1.get("ready") else None
    if vr is not None and vr>=1.4:
        if t1.get("regime")=="UP":
            score+=4; reasons.append("1H: объём усиливается вместе с ростом")
        elif t1.get("regime")=="DOWN":
            score-=4; risks.append("1H: объём усиливается вместе с падением")

    # 5m/15m evidence is timing/context only. It must never alter
    # the medium-term score or flip BULLISH/BEARISH direction.
    # short_term is intentionally accepted for API compatibility.

    score=max(0,min(100,int(round(score))))

    # 1D is already part of the weighted score. A second hard veto made
    # 1H-3D reversals detectable only after the daily chart had already flipped.

    if score>=70:
        bias="BULLISH"; label="🟢 БЫЧЬЯ СРЕДНЕСРОЧНАЯ СТРУКТУРА"
    elif score<=30:
        bias="BEARISH"; label="🔴 МЕДВЕЖЬЯ СРЕДНЕСРОЧНАЯ СТРУКТУРА"
    else:
        bias="NEUTRAL"; label="🟡 СРЕДНЕСРОЧНО БАЛАНС НЕ РЕШЁН"

    return {"score":score,"bias":bias,"label":label,
            "reasons":reasons[:8],"risks":risks[:6]}


def analyze(symbol,current_price=None,short_term=None,request_get=None,now=None,use_cache=True):
    now=time.time() if now is None else float(now)
    key=str(symbol).upper()
    if use_cache and request_get is None:
        with _lock:
            cached=_cache.get(key)
            if cached and 0<=now-cached["at"]<=CACHE_TTL_SEC:
                return dict(cached["value"])

    try:
        ticker=_ticker(key,request_get)
        price=current_price if current_price is not None and current_price>0 else ticker.get("last")
        c15=_closed_klines(key,"15",80,request_get,now)
        c60=_closed_klines(key,"60",80,request_get,now)
        c240=_closed_klines(key,"240",80,request_get,now)
        cd=_closed_klines(key,"D",40,request_get,now)
        t15=_trend(c15); t1=_trend(c60); t4=_trend(c240); td=_trend(cd)
        oi1=_oi_change(key,"1h",1,request_get,now)
        oi4=_oi_change(key,"1h",4,request_get,now)
        weekly=_weekly_position(cd,price)
        vol1=_volume_state(c60)
        scored=_score(t1,t4,td,oi1,oi4,weekly,vol1,short_term)
        entry_setup=_entry_setup(c15,scored.get("bias"))
        breakout=_breakout_state(c60,scored.get("bias"))
        trade_plan=_trade_plan(c15,scored.get("bias"),price)
        # Strong directional claims require complete higher-timeframe and OI evidence.
        evidence_ready=all(x.get("ready") for x in (t1,t4,td,oi1,oi4))
        if not evidence_ready:
            scored=dict(scored,score=50,bias="NEUTRAL",
                label="🟡 СРЕДНЕСРОЧНЫЕ ДАННЫЕ НЕПОЛНЫЕ",
                risks=["Нет полного подтверждения 1H/4H/1D и OI; направление не подтверждено"])
        result={
            "ready":evidence_ready,
            "symbol":key,"price":price,"ticker":ticker,
            "trend15":t15,"trend1h":t1,"trend4h":t4,"trend1d":td,
            "oi1h":oi1,"oi4h":oi4,"weekly_position_pct":weekly,
            "volume1h":vol1,"breakout":breakout,"entry_setup":entry_setup,
            "trade_plan":trade_plan,
            **scored,
        }
    except Exception as exc:
        result={"ready":False,"symbol":key,"score":50,"bias":"UNKNOWN",
                "label":"⚪ СРЕДНЕСРОЧНЫЕ ДАННЫЕ НЕДОСТУПНЫ",
                "reasons":[],"risks":[type(exc).__name__]}

    if use_cache and request_get is None:
        with _lock:
            _cache[key]={"at":now,"value":dict(result)}
    return result


def format_report(result):
    if not isinstance(result,dict):
        return ["⚪ Среднесрочный анализ недоступен."]
    lines=["", "🧭 <b>СРЕДНЕСРОК: 1 ЧАС — 3 ДНЯ</b>"]
    lines.append(result.get("label") or "⚪ Нет оценки")
    lines.append(f"Сила направления: {int(result.get('score',50))}/100")
    lines.append(
        "Структура: 15м {0} · 1ч {1} · 4ч {2} · 1д {3}".format(
            (result.get("trend15") or {}).get("regime","?"),
            (result.get("trend1h") or {}).get("regime","?"),
            (result.get("trend4h") or {}).get("regime","?"),
            (result.get("trend1d") or {}).get("regime","?"),
        )
    )
    oi1=(result.get("oi1h") or {}).get("change_pct")
    oi4=(result.get("oi4h") or {}).get("change_pct")
    lines.append("OI: 1ч {0} · 4ч {1}".format(
        f"{oi1:+.2f}%" if oi1 is not None else "нет данных",
        f"{oi4:+.2f}%" if oi4 is not None else "нет данных",
    ))
    wp=result.get("weekly_position_pct")
    if wp is not None:
        lines.append(f"Положение в диапазоне 7 дней: {wp:.0f}% от минимума к максимуму")

    reasons=result.get("reasons") or []
    risks=result.get("risks") or []
    bias=result.get("bias")
    if bias=="BEARISH":
        support=risks
        opposition=reasons
    else:
        support=reasons
        opposition=risks
    if support:
        lines += ["", "✅ <b>ЧТО ГОВОРИТ ЗА СЦЕНАРИЙ</b>"]
        lines += ["• "+x for x in support]
    if opposition:
        lines += ["", "⚠️ <b>ЧТО МЕШАЕТ / РИСК</b>"]
        lines += ["• "+x for x in opposition]

    lines += ["", "🎯 <b>СРЕДНЕСРОЧНЫЙ ВЫВОД</b>"]
    if bias=="BULLISH":
        lines.append("• Приоритет — искать ЛОНГ после отката/ретеста. 5м используется только для точного входа.")
        lines.append("• Бычий сценарий слабеет, если 1H и затем 4H структура разворачиваются вниз.")
    elif bias=="BEARISH":
        lines.append("• Приоритет — искать ШОРТ после отката/ретеста. 5м используется только для точного входа.")
        lines.append("• Медвежий сценарий слабеет, если 1H и затем 4H структура разворачиваются вверх.")
    else:
        lines.append("• Среднесрочного преимущества пока нет. Сделку лучше не форсировать.")
    lines.append("• Расширенный контекст: до 7 дней; это не прогноз с гарантией.")
    return lines


def entry_decision(result,short_verdict=None,live_ready=True):
    """Truthful entry state; never changes the medium-term SWING bias."""
    if not isinstance(result,dict):
        return {"state":"BLOCKED","side":None,"reason":"нет среднесрочных данных"}

    bias=str(result.get("bias") or "UNKNOWN")
    price=_num(result.get("price"))
    ticker=result.get("ticker") or {}
    high=_num(ticker.get("high24h"))
    low=_num(ticker.get("low24h"))
    t15=(result.get("trend15") or {}).get("regime")
    state=str(short_verdict or "")

    side="LONG" if bias=="BULLISH" else "SHORT" if bias=="BEARISH" else None
    if side is None:
        return {"state":"BLOCKED","side":None,"reason":"нет подтверждённого SWING-направления"}
    if not live_ready:
        return {"state":"WARMING","side":side,"reason":"5м данные ещё накапливаются; вход не разрешён"}

    drop_from_high=(100*(1-price/high)) if price and high and high>0 else None
    rebound_from_low=(100*(price/low-1)) if price and low and low>0 else None

    bearish_now=any(x in state for x in (
        "НАБОР НОВЫХ ШОРТОВ",
        "ЛИКВИДАЦИЯ/ЗАКРЫТИЕ ЛОНГОВ",
        "ПОГЛОЩЕНИЕ ПОКУПОК",
        "МЕДВЕЖЬЕ ДАВЛЕНИЕ",
    ))
    bullish_now=any(x in state for x in (
        "НАБОР НОВЫХ ЛОНГОВ",
        "ШОРТ-СКВИЗ",
        "ПОГЛОЩЕНИЕ ПРОДАЖ",
        "БЫЧЬЕ ДАВЛЕНИЕ",
    ))

    if bias=="BULLISH":
        if bearish_now:
            return {"state":"BLOCKED","side":"LONG","reason":"5м поток сейчас против LONG"}
        if drop_from_high is not None and drop_from_high>=8.0 and t15!="UP":
            return {"state":"BLOCKED","side":"LONG","reason":f"цена уже откатила {drop_from_high:.1f}% от 24ч максимума, 15м не подтверждает рост"}
    elif bias=="BEARISH":
        if bullish_now:
            return {"state":"BLOCKED","side":"SHORT","reason":"5м поток сейчас против SHORT"}
        if rebound_from_low is not None and rebound_from_low>=8.0 and t15!="DOWN":
            return {"state":"BLOCKED","side":"SHORT","reason":f"цена уже отскочила {rebound_from_low:.1f}% от 24ч минимума, 15м не подтверждает падение"}

    breakout=result.get("breakout") or {}
    breakout_phase=str(breakout.get("phase") or "WAIT_BREAKOUT")
    breakout_reason=str(breakout.get("reason") or "1H пробой ещё не подтверждён")
    if breakout_phase!="BREAKOUT_CONFIRMED":
        return {"state":"WAIT_BREAKOUT","side":side,"reason":breakout_reason}

    setup=result.get("entry_setup") or {}
    phase=str(setup.get("phase") or "WAIT_RETEST")
    setup_reason=str(setup.get("reason") or "15м ретест ещё не подтверждён")

    if phase=="RETEST_FORMING":
        return {"state":"RETEST","side":side,"reason":setup_reason}

    if phase=="READY_ENTRY":
        expected_15m="UP" if side=="LONG" else "DOWN"
        if t15!=expected_15m:
            return {"state":"RETEST","side":side,
                    "reason":"ретест есть, но 15м структура ещё не подтверждает SWING"}
        return {"state":"READY_ENTRY","side":side,"reason":setup_reason}

    return {"state":"WAIT_RETEST","side":side,
            "reason":"1H пробой подтверждён; "+setup_reason}


def format_compact_report(result,short_verdict=None,live_ready=True):
    """Compact professional trader view: decision first, evidence second."""
    if not isinstance(result,dict):
        return ["⚪ Среднесрочный анализ недоступен."]

    score=int(result.get("score",50))
    bias=str(result.get("bias") or "UNKNOWN")
    label=result.get("label") or "⚪ Нет оценки"
    t15=(result.get("trend15") or {}).get("regime","?")
    t1=(result.get("trend1h") or {}).get("regime","?")
    t4=(result.get("trend4h") or {}).get("regime","?")
    td=(result.get("trend1d") or {}).get("regime","?")
    oi1=(result.get("oi1h") or {}).get("change_pct")
    oi4=(result.get("oi4h") or {}).get("change_pct")
    reasons=result.get("reasons") or []
    risks=result.get("risks") or []

    arrows={"UP":"↑","DOWN":"↓","MIXED":"↔","UNKNOWN":"?"}
    structure=f"1H {arrows.get(t1,t1)} · 4H {arrows.get(t4,t4)} · 1D {arrows.get(td,td)}"
    oi_text="OI 1H {0} · 4H {1}".format(
        f"{oi1:+.2f}%" if oi1 is not None else "н/д",
        f"{oi4:+.2f}%" if oi4 is not None else "н/д",
    )

    entry=entry_decision(result,short_verdict,live_ready=live_ready)
    entry_state=entry.get("state")
    entry_side=entry.get("side")
    entry_reason=entry.get("reason")
    headline=label
    if entry_state in ("BLOCKED","WARMING") and entry_side=="LONG":
        headline="🟡 SWING LONG, НО ВХОД НА ПАУЗЕ"
    elif entry_state in ("BLOCKED","WARMING") and entry_side=="SHORT":
        headline="🟡 SWING SHORT, НО ВХОД НА ПАУЗЕ"

    lines=[
        f"{headline} · {score}/100",
        "Горизонт: 1ч–3д · контекст до 7д",
        f"Структура: {structure}",
        oi_text,
    ]
    if short_verdict:
        lines.append("Состояние 5м: "+str(short_verdict))
    if bias=="BEARISH":
        support=risks
        opposition=reasons
    else:
        support=reasons
        opposition=risks
    if support:
        lines.append("Сильная сторона: "+support[0])
    if opposition:
        lines.append("Главный риск: "+opposition[0])

    if entry_state in ("BLOCKED","WARMING"):
        lines.append("⛔ Вход: НЕ ВХОДИТЬ — "+str(entry_reason)+".")
    elif entry_state=="READY_ENTRY":
        lines.append("🟢 Этап: ENTRY · Вход: ГОТОВ — "+str(entry_reason)+".")
    elif entry_state=="RETEST":
        lines.append("🟠 Этап: RETEST · Вход: ЖДАТЬ подтверждение — "+str(entry_reason)+".")
    elif entry_state=="WAIT_RETEST":
        lines.append("🟡 Этап: BREAKOUT · Вход: ЖДАТЬ RETEST — "+str(entry_reason)+".")
    elif entry_state=="WAIT_BREAKOUT":
        lines.append("🟡 Этап: SWING WATCH · Вход: ЖДАТЬ BREAKOUT — "+str(entry_reason)+".")
    else:
        lines.append("⛔ Вход: НЕ ВХОДИТЬ — состояние входа не определено.")

    plan=result.get("trade_plan") or {}
    if entry_state=="READY_ENTRY" and plan.get("ready"):
        lines.append(
            "План: Entry {0:.8g} · SL {1:.8g} ({2:.2f}%) · "
            "TP1 {3:.8g} (2R) · TP2 {4:.8g} (3R)".format(
                plan["entry"],plan["stop"],plan["stop_pct"],
                plan["tp1"],plan["tp2"],
            )
        )

    if bias=="BULLISH":
        lines.append("Отмена SWING: слом 1H вниз; подтверждение отмены — 4H вниз.")
    elif bias=="BEARISH":
        lines.append("Отмена SWING: слом 1H вверх; подтверждение отмены — 4H вверх.")

    symbol=str(result.get("symbol") or "SYMBOL")
    lines.append(f"Подробнее: /detail {symbol}")
    return lines
