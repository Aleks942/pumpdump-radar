"""Historical pump-to-pullback study (NO lookahead in the live bot).

A peak is defined *retrospectively*: the hourly high exceeds highs from the
previous and next 6 closed hours. Count only peaks with >=6% rise from
PRECEDING 48h lows. A reversal is >=4% fall below the peak within 6 later
closed hourly candles. Report all candidates including non-reversals.
These are conditional descriptive statistics, NOT probability forecasts.
"""
import datetime as dt
import json
import math
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

SYMBOLS = ("BTC ETH SOL XRP BNB DOGE ADA SUI AVAX LINK TRX DOT LTC BCH XLM "
           "HBAR TON NEAR UNI AAVE ARB OP INJ SEI PEPE WIF BONK ENA ZEC "
           "APT FIL ATOM ICP TIA PUMP PENGU FARTCOIN").split()
URL = "https://www.okx.com/api/v5/market/history-candles"
BYBIT_URL = "https://api.bybit.com/v5/market/kline"
HOUR_MS = 3600000
BAR = "1H"
DAYS = 30
LOOKBACK_HOURS = 48
PUMP_MIN_PCT = 6
DROP_MIN_PCT = 4
FORWARD_HOURS = 6


def fetch_bybit(symbol, now=None, session=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    boundary = int(now.timestamp()*1000)//HOUR_MS*HOUR_MS
    earliest = boundary - (DAYS*24 + LOOKBACK_HOURS + 24)*HOUR_MS
    session = session or requests.Session()
    r = session.get(BYBIT_URL, params={"category":"linear",
        "symbol":symbol+"USDT", "interval":"60", "limit":"1000",
        "end":str(boundary-1)}, timeout=16)
    r.raise_for_status()
    body=r.json()
    if body.get("retCode")!=0:
        raise ValueError("BYBIT_"+str(body.get("retCode")))
    result={}
    for x in (body.get("result") or {}).get("list") or []:
        try:
            t=int(x[0])
            op,hi,lo,cl=map(float,x[1:5])
            if earliest<=t<boundary and all(math.isfinite(z)
                for z in (op,hi,lo,cl)) and 0<lo<=min(op,cl)<=max(op,cl)<=hi:
                result[t]=(op,hi,lo,cl)
        except (ValueError,IndexError,TypeError):
            pass
    return [(t,*result[t]) for t in sorted(result)]

def fetch_any(symbol):
    # Single 1,000-bar request to Bybit instead of many OKX pages.
    return fetch_bybit(symbol)

def fetch_history(symbol, now=None, session=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    # Exclude unfinished most recent hour and maintain full lookback.
    boundary = int(now.timestamp() * 1000) // HOUR_MS * HOUR_MS
    earliest = boundary - (DAYS*24 + LOOKBACK_HOURS + 24) * HOUR_MS
    cursor = None
    data = {}
    session = session or requests.Session()
    for page in range(11):
        params = {"instId": f"{symbol}-USDT-SWAP", "bar": BAR, "limit": "100"}
        if cursor is not None:
            params["after"] = str(cursor)
        response = session.get(URL, params=params, timeout=12)
        response.raise_for_status()
        body = response.json()
        if body.get("code") != "0":
            raise ValueError(str(body.get("msg") or body.get("code"))[:100])
        batch = body.get("data") or []
        if not batch:
            break
        oldest = min(int(x[0]) for x in batch)
        for item in batch:
            try:
                ts = int(item[0])
                op, hi, lo, cl = map(float, item[1:5])
                if (earliest <= ts < boundary and int(item[8]) == 1
                        and all(math.isfinite(v) for v in (op,hi,lo,cl))
                        and 0 < lo <= min(op,cl) <= max(op,cl) <= hi):
                    data[ts] = (op,hi,lo,cl)
            except (IndexError,ValueError,TypeError):
                continue
        if oldest <= earliest or (cursor is not None and oldest >= cursor):
            break
        cursor = oldest
        time.sleep(0.08)
    times = sorted(data)
    # Never interpolate across missing bars.
    result = [(ts,*data[ts]) for ts in times]
    return result


def extract_events(rows):
    results = []
    if len(rows) < LOOKBACK_HOURS + FORWARD_HOURS + 12:
        return results
    last_event = -999
    for i in range(LOOKBACK_HOURS, len(rows)-FORWARD_HOURS):
        if i-last_event < 12:
            continue
        ts, op, high, low, cl = rows[i]
        segment = rows[i-LOOKBACK_HOURS:i+FORWARD_HOURS+1]
        if any(segment[k+1][0] != segment[k][0]+HOUR_MS
               for k in range(len(segment)-1)):
            continue
        if high < max(r[2] for r in rows[i-6:i]):
            continue
        if high < max(r[2] for r in rows[i+1:i+1+FORWARD_HOURS]):
            continue
        rises = {}
        for hours in (6,24,48):
            preceding = min(r[3] for r in rows[i-hours:i])
            rises[hours] = (high / preceding-1)*100
        if rises[48] < PUMP_MIN_PCT:
            continue
        after_lows = [r[3] for r in rows[i+1:i+1+FORWARD_HOURS]]
        max_drop = (1-min(after_lows)/high)*100
        first_drop = next((j+1 for j, v in enumerate(after_lows)
                           if (1-v/high)*100 >= DROP_MIN_PCT), None)
        results.append({"time":ts, "rise6":rises[6], "rise24":rises[24],
                        "rise48":rises[48], "drop6h":max_drop,
                        "reversal":first_drop is not None,
                        "reversal_after_h":first_drop})
        last_event = i
    return results


def summary(values):
    if not values:
        return None
    v=sorted(values)
    return {"mean":round(statistics.mean(v),2),
            "median":round(statistics.median(v),2),
            "p75":round(v[int((len(v)-1)*0.75)],2),
            "p90":round(v[int((len(v)-1)*0.90)],2),
            "max":round(v[-1],2)}


def report(history):
    all_events=[]
    for symbol, rows in history.items():
        for event in extract_events(rows):
            all_events.append({"symbol":symbol, **event})
    reversals = [x for x in all_events if x["reversal"]]
    report = {
        "study": "OKX USDT swaps, hourly candles, recent 30 days, complete bars",
        "definition": "48h preceding low -> local 6h peak >=6%; 6h subsequent drawdown >=4%",
        "symbols_ok":len(history), "peaks":len(all_events),
        "reversed":len(reversals),
        "fraction_reversed_pct":round(len(reversals)/len(all_events)*100,1) if all_events else None,
        "rise48_all_peaks_pct":summary([x["rise48"] for x in all_events]),
        "rise48_before_reversal_pct":summary([x["rise48"] for x in reversals]),
        "rise24_before_reversal_pct":summary([x["rise24"] for x in reversals]),
        "rise6_before_reversal_pct":summary([x["rise6"] for x in reversals]),
        "top_10_reversal_peaks":sorted(reversals,key=lambda x:-x["rise48"])[:10],
        "per_symbol":{s:{"n":len([v for v in reversals if v["symbol"]==s]),
                         "median_rise48":summary([v["rise48"] for v in reversals if v["symbol"]==s])}
                      for s in sorted(history)},
    }
    return report


def main():
    data={}
    errors={}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures={pool.submit(fetch_any, symbol):symbol for symbol in SYMBOLS}
        for fut in as_completed(futures):
            symbol=futures[fut]
            try:
                rows=fut.result()
                if len(rows)<72:
                    errors[symbol]=f"only_{len(rows)}_bars"
                else:
                    data[symbol]=rows
            except Exception as exc:
                errors[symbol]=f"{type(exc).__name__}:{str(exc)[:80]}"
    print("PUMP_PEAK_STUDY_START")
    print(json.dumps(report(data),ensure_ascii=False,sort_keys=True))
    print("PUMP_PEAK_STUDY_ERRORS "+json.dumps(errors,sort_keys=True))
    print("PUMP_PEAK_STUDY_END")
    if len(data)<8:
        raise SystemExit("Insufficient public market data; do not infer thresholds.")


if __name__=="__main__":
    main()
