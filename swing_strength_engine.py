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

    if t1.get("hh_hl"):
        score+=7; reasons.append("1H: максимумы и минимумы повышаются")
    if t1.get("lh_ll"):
        score-=7; risks.append("1H: максимумы и минимумы снижаются")
    if t4.get("hh_hl"):
        score+=8; reasons.append("4H: повышающаяся структура")
    if t4.get("lh_ll"):
        score-=8; risks.append("4H: понижающаяся структура")

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

    if weekly_pos is not None:
        if weekly_pos>=70:
            score+=5; reasons.append(f"Цена в верхней части недельного диапазона ({weekly_pos:.0f}%)")
        elif weekly_pos<=30:
            score-=5; risks.append(f"Цена в нижней части недельного диапазона ({weekly_pos:.0f}%)")

    vr=_num(vol1.get("ratio")) if vol1.get("ready") else None
    if vr is not None and vr>=1.4:
        if t1.get("regime")=="UP":
            score+=4; reasons.append("1H: объём усиливается вместе с ростом")
        elif t1.get("regime")=="DOWN":
            score-=4; risks.append("1H: объём усиливается вместе с падением")

    st=short_term or {}
    fimb=_num(st.get("futures_imbalance_pct"))
    price5=_num(st.get("price5_pct"))
    if fimb is not None and fimb<=-8 and price5 is not None and price5>-0.25:
        score+=4
        reasons.insert(0,"Продавцы давят, но цена держится — возможное поглощение покупателем")
    if fimb is not None and fimb>=8 and price5 is not None and price5<0.25:
        score-=4
        risks.insert(0,"Покупают, но цена не растёт — возможное поглощение продавцом")

    score=max(0,min(100,int(round(score))))
    if score>=70:
        bias="BULLISH"; label="🟢 СИЛЬНАЯ МОНЕТА / ПОКУПАТЕЛЬ КОНТРОЛИРУЕТ"
    elif score<=30:
        bias="BEARISH"; label="🔴 СЛАБАЯ МОНЕТА / ПРОДАВЕЦ КОНТРОЛИРУЕТ"
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
        price=current_price or ticker.get("last")
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
        result={
            "ready":all(x.get("ready") for x in (t1,t4,td)),
            "symbol":key,"price":price,"ticker":ticker,
            "trend15":t15,"trend1h":t1,"trend4h":t4,"trend1d":td,
            "oi1h":oi1,"oi4h":oi4,"weekly_position_pct":weekly,
            "volume1h":vol1,
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
    if reasons:
        lines += ["", "✅ <b>ЧТО ГОВОРИТ ЗА СЦЕНАРИЙ</b>"]
        lines += ["• "+x for x in reasons]
    if risks:
        lines += ["", "⚠️ <b>ЧТО МЕШАЕТ / РИСК</b>"]
        lines += ["• "+x for x in risks]

    bias=result.get("bias")
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


def format_compact_report(result,short_verdict=None):
    """Compact professional trader view: decision first, evidence second."""
    if not isinstance(result,dict):
        return ["⚪ Среднесрочный анализ недоступен."]

    score=int(result.get("score",50))
    bias=str(result.get("bias") or "UNKNOWN")
    label=result.get("label") or "⚪ Нет оценки"
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

    lines=[
        f"{label} · {score}/100",
        "Горизонт: 1ч–3д · контекст до 7д",
        f"Структура: {structure}",
        oi_text,
    ]
    if reasons:
        lines.append("Сильная сторона: "+reasons[0])
    if risks:
        lines.append("Главный риск: "+risks[0])

    if bias=="BULLISH":
        lines.append("План: искать ЛОНГ только после отката/ретеста.")
        lines.append("Отмена: слом 1H вниз; подтверждение отмены — 4H вниз.")
    elif bias=="BEARISH":
        lines.append("План: искать ШОРТ только после отката/ретеста.")
        lines.append("Отмена: слом 1H вверх; подтверждение отмены — 4H вверх.")
    else:
        lines.append("План: вне сделки — среднесрочного преимущества нет.")

    if short_verdict:
        text=str(short_verdict)
        if bias=="BULLISH" and any(x in text for x in ("БЫЧЬЕ","НОВЫХ ЛОНГОВ","ПОГЛОЩЕНИЕ","ШОРТ-СКВИЗ")):
            lines.append("Вход 5м: 🟢 тайминг улучшается, ждать ретест.")
        elif bias=="BEARISH" and any(x in text for x in ("МЕДВЕЖЬЕ","НОВЫХ ШОРТОВ","ЛИКВИДАЦИЯ")):
            lines.append("Вход 5м: 🟢 тайминг улучшается, ждать ретест.")
        else:
            lines.append("Вход 5м: 🟡 пока нет подтверждения.")

    lines.append("Подробнее: /detail SYMBOL")
    return lines
