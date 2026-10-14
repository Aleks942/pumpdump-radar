"""Public exchange positioning context for PumpDump Radar (research only).

On an already DELIVERED ordinary move notice, sample a bounded number of
official exchange endpoints in a daemon thread and store point-in-time
observations. Never alters Telegram, trade approvals, stops or order routing.

Binance top-trader endpoints require an optional X-MBX-APIKEY according to
the current USD-M official docs. Other endpoints here are public.
The data is NOT the proprietary CoinGlass Net Longs(New)/Net Shorts(New).
"""
import json
import math
import os
import re
import sqlite3
import threading
import time


DB_DEFAULT = "market_memory.db"
GLOBAL_COOLDOWN_SEC = 15 * 60
SYMBOL_COOLDOWN_SEC = 30 * 60
MAX_STORED = 3000
INTERVAL_SEC = 300
MAX_BAR_AGE_SEC = 30 * 60
_lock = threading.Lock()
_active = False
_last_attempt = -float("inf")
_symbol_attempts = {}

# Official documented USD-M, not COIN-M endpoints.
ENDPOINTS = (
    ("binance_global_account_ratio", "Binance", "https://fapi.binance.com",
     "/futures/data/globalLongShortAccountRatio", "binance_series", False),
    ("binance_taker_buy_sell", "Binance", "https://fapi.binance.com",
     "/futures/data/takerlongshortRatio", "binance_series", False),
    ("binance_oi_history", "Binance", "https://fapi.binance.com",
     "/futures/data/openInterestHist", "binance_series", False),
    ("binance_funding", "Binance", "https://fapi.binance.com",
     "/fapi/v1/premiumIndex", "binance_funding", False),
    ("bybit_account_ratio", "Bybit", "https://api.bybit.com",
     "/v5/market/account-ratio", "bybit_series", False),
    ("bybit_oi_history", "Bybit", "https://api.bybit.com",
     "/v5/market/open-interest", "bybit_series", False),
    ("binance_top_accounts", "Binance", "https://fapi.binance.com",
     "/futures/data/topLongShortAccountRatio", "binance_series", True),
    ("binance_top_positions", "Binance", "https://fapi.binance.com",
     "/futures/data/topLongShortPositionRatio", "binance_series", True),
)


def _finite(raw):
    try:
        x = float(raw)
    except (ValueError, TypeError, OverflowError):
        return None
    return x if math.isfinite(x) else None


def _time(raw):
    ts = _finite(raw)
    if ts is None or ts <= 0:
        return None
    return ts / 1000.0 if ts >= 1e11 else ts


def _valid_bar(raw, name, kind, symbol, at):
    if not isinstance(raw, dict):
        return None
    if raw.get("symbol") not in (symbol, None):
        return None
    stamp = raw.get("time") if kind == "binance_funding" else raw.get("timestamp")
    ts = _time(stamp)
    if ts is None or not 0 <= at - ts <= MAX_BAR_AGE_SEC:
        return None
    # Binance taker series timestamps mark bar starts; its open bar must
    # never be used as a closed 5-minute outcome.
    if name == "binance_taker_buy_sell" and ts + INTERVAL_SEC > at:
        return None
    return ts, raw


def _normalize_share_pair(raw, first, second):
    left = _finite(raw.get(first))
    right = _finite(raw.get(second))
    if left is None or right is None or not 0 <= left <= 1 or not 0 <= right <= 1:
        return None
    if abs(left + right - 1) > 0.03:
        return None
    return {"long_share": round(left, 6), "short_share": round(right, 6),
            "account_or_position_ratio": round(left/right, 6) if right else None}


def _select_metric(name, kind, data, symbol, at):
    """Use only attributable, chronological, finite exchange observations."""
    if kind.startswith("binance"):
        if kind == "binance_funding":
            items = [data] if isinstance(data, dict) else []
        else:
            items = data if isinstance(data, list) else []
    else:
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return None
        result = data.get("result")
        items = result.get("list", []) if isinstance(result, dict) else []
    if not isinstance(items, list):
        return None
    bars = [_valid_bar(row, name, kind, symbol, at) for row in items]
    bars = sorted((row for row in bars if row), key=lambda x: x[0])
    if not bars:
        return None
    timestamp, latest = bars[-1]
    values = {}
    if name.endswith("account_ratio") or name == "binance_top_accounts":
        if name == "bybit_account_ratio":
            shares = _normalize_share_pair(latest, "buyRatio", "sellRatio")
        else:
            shares = _normalize_share_pair(latest, "longAccount", "shortAccount")
        if shares is None:
            return None
        values.update(shares)
        values["metric_kind"] = "account_holder_share"
    elif name == "binance_top_positions":
        # USD-M endpoint documents longAccount/shortAccount field names even
        # though values refer to position size shares, NOT number of accounts.
        shares = _normalize_share_pair(latest, "longAccount", "shortAccount")
        if shares is None:
            return None
        values.update(shares)
        values["metric_kind"] = "top_trader_position_share"
    elif name == "binance_taker_buy_sell":
        buy = _finite(latest.get("buyVol"))
        sell = _finite(latest.get("sellVol"))
        if buy is None or sell is None or min(buy, sell) < 0 or buy+sell <= 0:
            return None
        values = {"buy_source_units": buy, "sell_source_units": sell,
                  "delta_ratio": round((buy-sell)/(buy+sell), 6)}
    elif name in ("binance_oi_history", "bybit_oi_history"):
        field = "sumOpenInterest" if name == "binance_oi_history" else "singleOpenInterest"
        current = _finite(latest.get(field))
        if name == "bybit_oi_history" and current is None:
            field = "openInterest"  # Documented sum-both-sides fallback; annotate.
            current = _finite(latest.get(field))
        if current is None or current <= 0:
            return None
        values = {"open_interest_source_units": current, "oi_field": field}
        if name == "binance_oi_history":
            value_usdt = _finite(latest.get("sumOpenInterestValue"))
            if value_usdt is not None and value_usdt > 0:
                values["open_interest_value_source_units"] = value_usdt
        if len(bars) >= 2:
            prev_ts, prev = bars[-2]
            old = _finite(prev.get(field))
            if old is not None and old > 0 and 0 < timestamp-prev_ts <= 420:
                values["oi_change_pct_5m"] = round(100*(current/old-1), 6)
    elif name == "binance_funding":
        funding = _finite(latest.get("lastFundingRate"))
        if funding is None or not -1 <= funding <= 1:
            return None
        values = {"last_funding_rate_fraction": funding}
        nft = _time(latest.get("nextFundingTime"))
        if nft is not None and nft >= at - 24*3600:
            values["next_funding_time"] = nft
    else:
        return None
    return {"source": "official_" + ("Binance_USDM" if kind.startswith("binance")
                                      else "Bybit_V5"),
            "symbol": symbol, "period": "5m" if kind != "binance_funding" else "latest",
            "sample_time": timestamp, "values": values}


def collect(symbol, observed_at=None, request_get=None, binance_key=None):
    """Capture source-attributed *available* data, never infer unavailable rows."""
    if not isinstance(symbol, str) or re.fullmatch(r"[A-Z0-9]{2,25}USDT", symbol) is None:
        return {"status": "INVALID_SYMBOL"}
    now = time.time() if observed_at is None else float(observed_at)
    if not math.isfinite(now):
        return {"status": "INVALID_TIME"}
    if request_get is None:
        import requests
        request_get = requests.get
    key = (os.getenv("BINANCE_MARKET_API_KEY", "")
           if binance_key is None else binance_key)
    results, unavailable = {}, {}
    blocked = set()
    for name, exchange, base, path, kind, key_required in ENDPOINTS:
        if key_required and (not isinstance(key, str) or not key.strip()):
            unavailable[name] = "BINANCE_MARKET_API_KEY_NOT_CONFIGURED"
            continue
        if exchange in blocked:
            unavailable[name] = "EXCHANGE_RATE_LIMITED"
            continue
        params = {"symbol": symbol}
        if kind == "binance_series":
            params.update(period="5m", limit=4)
        elif kind == "bybit_series":
            params.update(category="linear", limit=4)
            if name == "bybit_account_ratio":
                params["period"] = "5min"
            else:
                params["intervalTime"] = "5min"
        headers = {"X-MBX-APIKEY": key} if key_required else {}
        try:
            response = request_get(base+path, params=params,
                                   headers=headers, timeout=3)
            status = response.status_code
            if status != 200:
                unavailable[name] = "HTTP_" + str(status)
                if status in (418, 429):
                    blocked.add(exchange)
                continue
            raw = response.json()
            if exchange == "Binance" and isinstance(raw, dict) and (
                    raw.get("code") is not None and kind != "binance_funding"):
                unavailable[name] = "API_ERROR"
                continue
            metric = _select_metric(name, kind, raw, symbol, now)
            if metric is None:
                unavailable[name] = "NO_VALID_RECENT_SAMPLE"
            else:
                results[name] = metric
        except Exception:  # Per-source errors are unavailable, not trading signals.
            unavailable[name] = "HTTP_OR_SCHEMA_ERROR"
    status = "OK" if results and not unavailable else (
        "PARTIAL" if results else "UNAVAILABLE")
    return {"status": status, "symbol": symbol,
            "triggered_at": now, "retrieved_at": time.time(),
            "period": "5m",
            "metrics": results, "unavailable": unavailable,
            "not_coinglass_net_new_positions": True,
            "research_only": True, "trade_approved": False}


def _save(result):
    path = os.getenv("MARKET_MEMORY_DB", DB_DEFAULT)
    with sqlite3.connect(path, timeout=10) as db:
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("""CREATE TABLE IF NOT EXISTS public_derivatives_context (
            id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
            triggered_at REAL NOT NULL, retrieved_at REAL NOT NULL,
            payload_json TEXT NOT NULL)""")
        db.execute("""CREATE INDEX IF NOT EXISTS public_derivatives_symbol_ts
            ON public_derivatives_context(symbol, triggered_at)""")
        db.execute("""INSERT INTO public_derivatives_context
            (symbol,triggered_at,retrieved_at,payload_json)
            VALUES (?,?,?,?)""", (result["symbol"], result["triggered_at"],
                                  result["retrieved_at"],
                                  json.dumps(result, allow_nan=False)))
        db.execute("""DELETE FROM public_derivatives_context WHERE id IN (
            SELECT id FROM public_derivatives_context
            ORDER BY id DESC LIMIT -1 OFFSET ?)""", (MAX_STORED,))


def schedule(symbol, observed_at=None):
    """After confirmed Telegram delivery, enrich silently in background."""
    if not isinstance(symbol, str) or re.fullmatch(r"[A-Z0-9]{2,25}USDT", symbol) is None:
        return False
    now = time.time() if observed_at is None else float(observed_at)
    if not math.isfinite(now):
        return False
    global _active, _last_attempt
    with _lock:
        if (_active or now-_last_attempt < GLOBAL_COOLDOWN_SEC or
                now-_symbol_attempts.get(symbol, -float("inf")) <
                SYMBOL_COOLDOWN_SEC):
            return False
        _active = True
        _last_attempt = now
        _symbol_attempts[symbol] = now
        if len(_symbol_attempts) > 1000:
            for old in list(_symbol_attempts):
                if now-_symbol_attempts[old] > SYMBOL_COOLDOWN_SEC:
                    _symbol_attempts.pop(old, None)

    def worker():
        global _active
        try:
            result = collect(symbol, observed_at=now)
            if result["status"] in ("OK", "PARTIAL", "UNAVAILABLE"):
                _save(result)
                print("[PUBLIC_DERIVATIVES_CONTEXT]",
                      symbol, "status=", result["status"],
                      "available=", len(result["metrics"]),
                      "unavailable=", len(result["unavailable"]),
                      "research_only=true no_signal_gate=true", flush=True)
        except Exception as exc:
            print("[PUBLIC_DERIVATIVES_CONTEXT_ERROR]",
                  symbol, type(exc).__name__, flush=True)
        finally:
            with _lock:
                _active = False
    try:
        threading.Thread(target=worker, daemon=True).start()
        return True
    except Exception:
        with _lock:
            _active = False
        return False
