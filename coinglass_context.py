"""Optional, research-only CoinGlass v4 context for CONFIRMED price-move notices.

Never trades, changes filters, or sends Telegram. Without COINGLASS_API_KEY,
no requests are made. Separate CoinGlass time series/units are never labeled
as identical to exchange-computed fields or chart proprietary indicators.
"""
import json
import math
import os
import re
import sqlite3
import threading
import time

import requests

BASE = "https://open-api-v4.coinglass.com"
INTERVAL = "30m"   # Startup API plan supports >=30m; 15m requires higher plan.
MIN_GAP_SECONDS = 15 * 60
PER_COIN_GAP_SECONDS = 30 * 60
RETRY_SECONDS = 5 * 60
MAX_ROWS = 2500
_lock = threading.Lock()
_active = False
_last = -float("inf")
_per_coin = {}

# Only explicit official API endpoints. CoinGlass Net Position is NOT assumed
# identical to the proprietary Net Longs(New) / Net Shorts(New) chart plots.
ENDPOINTS = {
    "aggregated_oi": ("/api/futures/open-interest/aggregated-history", "coin"),
    "funding_oi_weighted": ("/api/futures/funding-rate/oi-weight-history", "coin"),
    "futures_cvd_binance": ("/api/futures/aggregated-cvd/history", "binance_coin"),
    "spot_cvd_binance": ("/api/spot/aggregated-cvd/history", "binance_coin"),
    "futures_taker_binance": (
        "/api/futures/aggregated-taker-buy-sell-volume/history", "binance_coin"),
    "spot_taker_binance": (
        "/api/spot/aggregated-taker-buy-sell-volume/history", "binance_coin"),
    "liquidations_binance": (
        "/api/futures/liquidation/aggregated-history", "binance_coin"),
    "net_position_v2_binance": (
        "/api/futures/v2/net-position/history", "binance_pair"),
}


def _finite(value):
    try:
        v = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return v if math.isfinite(v) else None


def _ts_seconds(raw):
    value = _finite(raw)
    if value is None or value <= 0:
        return None
    return value / 1000 if value > 10_000_000_000 else value


def _closed_rows(response, observed_at):
    """Require actual older candles, never mistake ongoing candle for closed."""
    if not isinstance(response, dict) or str(response.get("code")) != "0":
        return []
    data = response.get("data")
    if not isinstance(data, list):
        return []
    by_time = {}
    for bar in data:
        if not isinstance(bar, dict):
            continue
        bar_ts = _ts_seconds(bar.get("time"))
        # CoinGlass bars mark the interval start. Do not use unfinished candles.
        if (bar_ts is not None and observed_at - 6*3600 <= bar_ts
                and bar_ts + 1800 <= observed_at and bar_ts <= observed_at):
            by_time[bar_ts] = bar
    return [(ts, by_time[ts]) for ts in sorted(by_time)[-3:]]


def _metric(name, bars):
    if not bars:
        return None
    at, bar = bars[-1]
    out = {"bar_start_ts": at}
    def f(field):
        return _finite(bar.get(field))
    if name == "aggregated_oi":
        current = f("close")
        previous = _finite(bars[-2][1].get("close")) if len(bars) > 1 else None
        if current is None or current <= 0:
            return None
        out["oi_close_source_units"] = current
        if previous and previous > 0 and at-bars[-2][0] <= 3600:
            out["oi_change_pct_30m"] = round(100 * (current/previous-1), 5)
        return out
    if name == "funding_oi_weighted":
        value = f("close")
        if value is None:
            return None
        out["funding_oi_weight_close_source_units"] = value
        return out
    if name.endswith("cvd_binance"):
        cvd = f("cum_vol_delta")
        if cvd is None:
            return None
        out["cvd_cumulative_source_units"] = cvd
        if len(bars) > 1 and at-bars[-2][0] <= 3600:
            previous = _finite(bars[-2][1].get("cum_vol_delta"))
            if previous is not None:
                out["cvd_change_30m_source_units"] = round(cvd-previous, 4)
        return out
    if name.endswith("taker_binance"):
        buy = f("aggregated_buy_volume_usd")
        sell = f("aggregated_sell_volume_usd")
        if buy is None or sell is None or min(buy, sell) < 0:
            return None
        out.update(buy_usd=buy, sell_usd=sell)
        if buy+sell > 0:
            out["taker_delta_ratio"] = round((buy-sell)/(buy+sell), 5)
        return out
    if name == "liquidations_binance":
        long_usd = f("aggregated_long_liquidation_usd")
        short_usd = f("aggregated_short_liquidation_usd")
        if long_usd is None or short_usd is None or min(long_usd, short_usd) < 0:
            return None
        out.update(long_liquidation_usd=long_usd, short_liquidation_usd=short_usd)
        return out
    if name == "net_position_v2_binance":
        keys = ("net_long_change", "net_short_change",
                "net_long_change_cum", "net_short_change_cum")
        values = {key: f(key) for key in keys}
        if all(value is None for value in values.values()):
            return None
        out.update({key + "_source_units": value for key, value in values.items()
                    if value is not None})
        return out
    return None


def collect(symbol, observed_at=None, request_get=None, api_key=None):
    """Read 30m *closed* bars for a single observed coin; partial is explicit."""
    if not isinstance(symbol, str) or re.fullmatch(r"[A-Z0-9]{2,25}USDT", symbol) is None:
        return {"status": "INVALID_SYMBOL"}
    api_key = (os.getenv("COINGLASS_API_KEY") if api_key is None else api_key)
    if not isinstance(api_key, str) or not api_key.strip():
        return {"status": "NOT_CONFIGURED"}
    observed_at = time.time() if observed_at is None else float(observed_at)
    if not math.isfinite(observed_at):
        return {"status": "INVALID_TIME"}
    request_get = requests.get if request_get is None else request_get
    coin = symbol[:-4]
    header = {"CG-API-KEY": api_key, "accept": "application/json"}
    metrics = {}
    unavailable = {}
    # Max eight bounded requests on a triggered event; no market-wide polling.
    for name, (path, scope) in ENDPOINTS.items():
        params = {"interval": INTERVAL, "limit": 4}
        if scope == "coin":
            params["symbol"] = coin
        elif scope == "binance_pair":
            params.update(exchange="Binance", symbol=symbol)
        else:
            params.update(exchange_list="Binance", symbol=coin)
            if "cvd_" in name:
                params["unit"] = "usd"
            if "taker_" in name:
                params["unit"] = "usd"
        try:
            reply = request_get(BASE + path, headers=header, params=params,
                                timeout=3)
            if reply.status_code != 200:
                unavailable[name] = "HTTP_" + str(reply.status_code)
                continue
            bars = _closed_rows(reply.json(), observed_at)
            metric = _metric(name, bars)
            if metric is None:
                unavailable[name] = "NO_VALID_CLOSED_BAR"
            else:
                metrics[name] = metric
        except (requests.RequestException, ValueError, TypeError,
                KeyError, OverflowError):
            unavailable[name] = "REQUEST_OR_SCHEMA_ERROR"
    return {
        "status": "PARTIAL" if metrics and unavailable else (
            "OK" if metrics else "UNAVAILABLE"),
        "source": "CoinGlass_V4_OFFICIAL_API",
        "symbol": symbol,
        "exchange_scope": "Binance for spot/futures flow, net positions and liquidations; "
                          "aggregated across CoinGlass exchanges for OI/funding",
        "interval": INTERVAL,
        "observed_at": observed_at,
        "retrieved_at": time.time(),
        "metrics": metrics, "unavailable": unavailable,
        "not_trading_confirmation": True,
    }


def _save(payload):
    path = os.getenv("MARKET_MEMORY_DB", "market_memory.db")
    with sqlite3.connect(path, timeout=6) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS coinglass_context (
            id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
            observed_at REAL NOT NULL, retrieved_at REAL NOT NULL,
            payload_json TEXT NOT NULL
        )""")
        db.execute("CREATE INDEX IF NOT EXISTS cg_context_symbol_ts "
                   "ON coinglass_context(symbol, observed_at)")
        db.execute("""INSERT INTO coinglass_context
            (symbol,observed_at,retrieved_at,payload_json) VALUES (?,?,?,?)""",
            (payload["symbol"], payload["observed_at"],
             payload["retrieved_at"], json.dumps(payload, allow_nan=False)))
        db.execute("""DELETE FROM coinglass_context WHERE id IN (
            SELECT id FROM coinglass_context ORDER BY id DESC
            LIMIT -1 OFFSET ?)""", (MAX_ROWS,))


def schedule(symbol, observed_at=None):
    """Best-effort background enrichment AFTER a regular Telegram notice.

    Never used as an alert gate, never blocks the scanner or sends chat text.
    """
    key = os.getenv("COINGLASS_API_KEY", "").strip()
    if not key:
        return False
    observed_at = time.time() if observed_at is None else float(observed_at)
    if not math.isfinite(observed_at):
        return False
    if not isinstance(symbol, str) or re.fullmatch(r"[A-Z0-9]{2,25}USDT", symbol) is None:
        return False
    global _active, _last
    with _lock:
        if (_active or observed_at-_last < MIN_GAP_SECONDS
                or observed_at-_per_coin.get(symbol, -float("inf")) <
                PER_COIN_GAP_SECONDS):
            return False
        _active = True
        _last = observed_at
        _per_coin[symbol] = observed_at

    def worker():
        global _active
        try:
            result = collect(symbol, observed_at=observed_at, api_key=key)
            if result.get("status") not in ("NOT_CONFIGURED", "INVALID_SYMBOL",
                                            "INVALID_TIME"):
                _save(result)
                print("[COINGLASS_CONTEXT]", symbol,
                      "status=", result["status"],
                      "available=", len(result["metrics"]),
                      "unavailable=", len(result["unavailable"]),
                      "research_only=true no_trade_approval=true", flush=True)
        except Exception as exc:
            print("[COINGLASS_CONTEXT_ERROR]", symbol, type(exc).__name__,
                  flush=True)
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
