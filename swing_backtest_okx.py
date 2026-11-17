"""Temporary research runner: no-lookahead SWING validation using OKX public history.

Uses OKX 1H historical candles and contract-level 1H open-interest history, then
reuses the same build_signals/summarize logic as swing_backtest.py. Research
only; it does not alter the production scanner.
"""
import argparse
import json
import math
import time
import requests

from swing_backtest import build_signals, summarize, MODELED_ROUND_TRIP_COST_PCT

OKX_BASE = "https://www.okx.com/api/v5"


def _get(path, params):
    r = requests.get(OKX_BASE + path, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or data.get("code") != "0":
        raise RuntimeError("OKX_API:" + str(data)[:200])
    rows = data.get("data")
    return rows if isinstance(rows, list) else []


def _inst_id(symbol):
    s = symbol.upper()
    if s.endswith("USDT"):
        s = s[:-4]
    return f"{s}-USDT-SWAP"


def fetch_klines(symbol, start_ms, end_ms):
    inst = _inst_id(symbol)
    out = {}
    after = None
    loops = 0
    while loops < 20:
        loops += 1
        params = {"instId": inst, "bar": "1H", "limit": "300"}
        if after is not None:
            params["after"] = str(after)
        rows = _get("/market/history-candles", params)
        if not rows:
            break
        oldest = None
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            try:
                ts=int(row[0]); o=float(row[1]); h=float(row[2]); l=float(row[3]); c=float(row[4]); v=float(row[5])
            except (TypeError, ValueError, OverflowError):
                continue
            if len(row) > 8 and str(row[8]) != "1":
                continue
            if ts > end_ms:
                continue
            if ts < start_ms:
                oldest = ts if oldest is None else min(oldest, ts)
                continue
            if min(o,h,l,c) <= 0 or v < 0 or not l <= min(o,c) <= max(o,c) <= h:
                continue
            out[ts] = {"ts": ts/1000.0, "o": o, "h": h, "l": l, "c": c, "v": v}
            oldest = ts if oldest is None else min(oldest, ts)
        if oldest is None or oldest <= start_ms:
            break
        if after == oldest:
            break
        after = oldest
        time.sleep(0.12)
    return [out[k] for k in sorted(out)]


def fetch_oi_1h(symbol, start_ms, end_ms):
    inst = _inst_id(symbol)
    out = {}
    end_cursor = None
    loops = 0
    while loops < 20:
        loops += 1
        params = {"instId": inst, "period": "1H", "limit": "100"}
        if end_cursor is not None:
            params["end"] = str(end_cursor)
        rows = _get("/rubik/stat/contracts/open-interest-history", params)
        if not rows:
            break
        oldest = None
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                continue
            try:
                ts = int(row[0])
                value = float(row[1])
            except (TypeError, ValueError, OverflowError):
                continue
            if ts > end_ms:
                continue
            if value > 0 and math.isfinite(value) and ts >= start_ms:
                out[ts] = value
            oldest = ts if oldest is None else min(oldest, ts)
        if oldest is None or oldest <= start_ms:
            break
        if end_cursor == oldest:
            break
        end_cursor = oldest
        time.sleep(0.22)
    return [(k/1000.0, out[k]) for k in sorted(out)]


def backtest_symbol(symbol, days=30, now=None):
    now = time.time() if now is None else float(now)
    end_ms = int(now * 1000)
    candle_start_ms = int((now - (days + 35) * 86400) * 1000)
    oi_start_ms = int((now - (days + 7) * 86400) * 1000)

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
        "summary": summarize(events),
        "_events": events,
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
            all_events.extend(report.pop("_events"))
            reports.append(report)
        except Exception as exc:
            reports.append({
                "symbol":symbol,
                "days":args.days,
                "error":type(exc).__name__+":"+str(exc)[:240],
            })

    output={
        "provider":"OKX public 1H candles + contract 1H OI history",
        "symbols":reports,
        "portfolio_event_count":len(all_events),
        "portfolio_summary":summarize(all_events),
        "note":"Historical 5m flow/spot CVD/liquidations excluded. Same SWING scoring core is used with 1H/4H/1D structure, volume and 1H/4H OI. Modeled round-trip cost 0.15%.",
    }
    print(json.dumps(output,ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
