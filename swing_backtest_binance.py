"""Temporary research runner: no-lookahead SWING validation using Binance public history.

Uses Binance USD-M 1h candles plus 1h open-interest history, then reuses the
same build_signals/summarize logic as swing_backtest.py. This file is for
research only and does not affect the production scanner.
"""
import argparse
import json
import math
import time
import requests

from swing_backtest import build_signals, summarize, MODELED_ROUND_TRIP_COST_PCT

BINANCE_FAPI = "https://fapi.binance.com"


def _get(path, params):
    r = requests.get(BINANCE_FAPI + path, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data


def fetch_klines(symbol, start_ms, end_ms):
    out = {}
    cursor = int(start_ms)
    while cursor < end_ms:
        rows = _get("/fapi/v1/klines", {
            "symbol": symbol,
            "interval": "1h",
            "startTime": cursor,
            "endTime": int(end_ms),
            "limit": 1500,
        })
        if not isinstance(rows, list) or not rows:
            break
        last_ts = None
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            try:
                ts = int(row[0]); o=float(row[1]); h=float(row[2]); l=float(row[3]); c=float(row[4]); v=float(row[5])
            except (TypeError, ValueError, OverflowError):
                continue
            if not (start_ms <= ts <= end_ms):
                continue
            if min(o,h,l,c) <= 0 or v < 0 or not l <= min(o,c) <= max(o,c) <= h:
                continue
            out[ts] = {"ts": ts/1000.0, "o": o, "h": h, "l": l, "c": c, "v": v}
            last_ts = ts
        if last_ts is None:
            break
        nxt = last_ts + 3600_000
        if nxt <= cursor:
            break
        cursor = nxt
        time.sleep(0.05)
    return [out[k] for k in sorted(out)]


def fetch_oi_1h(symbol, start_ms, end_ms):
    out = {}
    cursor = int(start_ms)
    while cursor < end_ms:
        rows = _get("/futures/data/openInterestHist", {
            "symbol": symbol,
            "period": "1h",
            "startTime": cursor,
            "endTime": int(end_ms),
            "limit": 500,
        })
        if not isinstance(rows, list) or not rows:
            break
        last_ts = None
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                ts = int(row.get("timestamp"))
                value = float(row.get("sumOpenInterest"))
            except (TypeError, ValueError, OverflowError):
                continue
            if start_ms <= ts <= end_ms and value > 0 and math.isfinite(value):
                out[ts] = value
                last_ts = ts
        if last_ts is None:
            break
        nxt = last_ts + 3600_000
        if nxt <= cursor:
            break
        cursor = nxt
        time.sleep(0.05)
    return [(k/1000.0, out[k]) for k in sorted(out)]


def backtest_symbol(symbol, days=30, now=None):
    now = time.time() if now is None else float(now)
    end_ms = int(now * 1000)
    candle_start_ms = int((now - (days + 35) * 86400) * 1000)
    oi_start_ms = int((now - min(days, 29) * 86400) * 1000)

    c1 = fetch_klines(symbol, candle_start_ms, end_ms)
    c1 = [x for x in c1 if x["ts"] + 3600 <= now]
    oi = fetch_oi_1h(symbol, oi_start_ms, end_ms)

    events = build_signals(symbol, c1, oi)
    cutoff = now - days * 86400
    events = [e for e in events if e["ts"] >= cutoff]
    return {
        "symbol": symbol,
        "days": days,
        "hourly_candles": len(c1),
        "oi_points": len(oi),
        "event_count": len(events),
        "events": events,
        "summary": summarize(events),
        "modeled_round_trip_cost_pct": MODELED_ROUND_TRIP_COST_PCT,
    }


def main():
    p=argparse.ArgumentParser()
    p.add_argument("symbols", nargs="+")
    p.add_argument("--days", type=int, default=30)
    args=p.parse_args()

    reports=[]
    all_events=[]
    for raw in args.symbols:
        symbol=raw.upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        try:
            report=backtest_symbol(symbol,args.days)
            all_events.extend(report["events"])
            reports.append({k:v for k,v in report.items() if k!="events"})
        except Exception as exc:
            reports.append({"symbol":symbol,"days":args.days,"error":type(exc).__name__+":"+str(exc)[:200]})

    output={
        "provider":"Binance USD-M public history",
        "symbols":reports,
        "portfolio_event_count":len(all_events),
        "portfolio_summary":summarize(all_events),
        "note":"Historical 5m flow/spot CVD/liquidations excluded; score uses price structure, volume and 1h OI. Modeled round-trip cost 0.15%.",
    }
    print(json.dumps(output,ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
