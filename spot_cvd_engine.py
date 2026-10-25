import math
import requests
import time
import threading

# An OKX 51001 invalid instrument is not a sell/buy signal.
# Cache only that explicit error to avoid repeated paid bandwidth/requests.
_INVALID_SPOT_UNTIL = {}
_INVALID_LOCK = threading.Lock()
_INVALID_TTL_SEC = 3600


OKX_SPOT_TRADES_URL = "https://www.okx.com/api/v5/market/trades"


def swap_to_spot_symbol(raw_symbol):
    """
    BTC-USDT-SWAP -> BTC-USDT
    """

    if not raw_symbol:
        return None

    if raw_symbol.endswith("-USDT-SWAP"):
        return raw_symbol.replace("-USDT-SWAP", "-USDT")

    return None


def classify_spot_cvd(cvd_percent):
    """
    Классификация давления на спотовом рынке.
    """

    if cvd_percent >= 30:
        return {
            "state": "STRONG_SPOT_BUY",
            "vote": "CONTINUE",
            "weight": 4,
            "text": "Спот активно покупают"
        }

    if cvd_percent >= 10:
        return {
            "state": "SPOT_BUY",
            "vote": "CONTINUE",
            "weight": 2,
            "text": "Спотовые покупки сильнее"
        }

    if cvd_percent <= -30:
        return {
            "state": "STRONG_SPOT_SELL",
            "vote": "EXHAUSTION",
            "weight": 4,
            "text": "На споте активно продают"
        }

    if cvd_percent <= -10:
        return {
            "state": "SPOT_SELL",
            "vote": "EXHAUSTION",
            "weight": 2,
            "text": "Спотовые продажи сильнее"
        }

    return {
        "state": "SPOT_BALANCE",
        "vote": "NEUTRAL",
        "weight": 1,
        "text": "На споте баланс"
    }


def _finite(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _compact_symbol(raw_symbol):
    raw = str(raw_symbol or "").strip().upper()
    if raw.endswith("-SWAP"):
        raw = raw[:-5]
    compact = raw.replace("-", "").replace("/", "")
    if not compact.endswith("USDT") or len(compact) <= 4:
        return None
    return compact


def _source_cache_key(source, symbol):
    return f"{source}:{symbol}"


def _source_blocked(source, symbol):
    with _INVALID_LOCK:
        until = _INVALID_SPOT_UNTIL.get(_source_cache_key(source, symbol), 0)
    return time.monotonic() < until


def _block_source(source, symbol):
    with _INVALID_LOCK:
        _INVALID_SPOT_UNTIL[_source_cache_key(source, symbol)] = (
            time.monotonic() + _INVALID_TTL_SEC
        )


def _build_result(spot_symbol, source, rows):
    buy_quote_volume = 0.0
    sell_quote_volume = 0.0
    valid_trades = 0

    for side, raw_price, raw_size in rows:
        price = _finite(raw_price)
        size = _finite(raw_size)
        side = str(side or "").upper()
        if price is None or size is None or price <= 0 or size <= 0:
            continue

        quote_volume = price * size
        if side == "BUY":
            buy_quote_volume += quote_volume
        elif side == "SELL":
            sell_quote_volume += quote_volume
        else:
            continue
        valid_trades += 1

    total_quote_volume = buy_quote_volume + sell_quote_volume
    if valid_trades <= 0 or total_quote_volume <= 0:
        return None

    delta = buy_quote_volume - sell_quote_volume
    cvd_percent = (delta / total_quote_volume) * 100
    classification = classify_spot_cvd(cvd_percent)

    result = {
        "available": True,
        "spot_symbol": spot_symbol,
        "source": source,
        "quality": "READY",
        "trade_count": valid_trades,
        "buy_quote_volume": buy_quote_volume,
        "sell_quote_volume": sell_quote_volume,
        "total_quote_volume": total_quote_volume,
        "delta": delta,
        "cvd_percent": cvd_percent,
        "state": classification["state"],
        "vote": classification["vote"],
        "weight": classification["weight"],
        "text": classification["text"],
    }

    print(
        "[SPOT_CVD]",
        spot_symbol,
        "source=", source,
        "trades=", valid_trades,
        "buy_usdt=", round(buy_quote_volume, 2),
        "sell_usdt=", round(sell_quote_volume, 2),
        "delta=", round(delta, 2),
        "cvd_percent=", round(cvd_percent, 2),
        "state=", classification["state"],
        flush=True,
    )
    return result


def _fetch_binance(compact_symbol, limit):
    source = "Binance"
    if _source_blocked(source, compact_symbol):
        return None, "UNSUPPORTED"

    try:
        response = requests.get(
            "https://api.binance.com/api/v3/trades",
            params={
                "symbol": compact_symbol,
                "limit": min(max(int(limit), 1), 1000),
            },
            timeout=6,
        )
        payload = response.json()

        if response.status_code != 200:
            if isinstance(payload, dict) and payload.get("code") == -1121:
                _block_source(source, compact_symbol)
                return None, "UNSUPPORTED"
            print(
                "[SPOT_CVD_SOURCE_ERROR]",
                compact_symbol, source, "HTTP", response.status_code,
                flush=True,
            )
            return None, "DATA_ERROR"

        if not isinstance(payload, list):
            return None, "DATA_ERROR"

        rows = []
        for trade in payload:
            if not isinstance(trade, dict):
                continue
            # isBuyerMaker=True => market taker was SELL.
            side = "SELL" if trade.get("isBuyerMaker") is True else "BUY"
            rows.append((side, trade.get("price"), trade.get("qty")))
        return rows, "READY" if rows else "NO_TRADES"

    except Exception as error:
        print(
            "[SPOT_CVD_SOURCE_ERROR]",
            compact_symbol, source, type(error).__name__,
            flush=True,
        )
        return None, "DATA_ERROR"


def _fetch_bybit(compact_symbol, limit):
    source = "Bybit"
    if _source_blocked(source, compact_symbol):
        return None, "UNSUPPORTED"

    try:
        response = requests.get(
            "https://api.bybit.com/v5/market/recent-trade",
            params={
                "category": "spot",
                "symbol": compact_symbol,
                "limit": min(max(int(limit), 1), 1000),
            },
            timeout=6,
        )
        payload = response.json()

        if response.status_code != 200:
            print(
                "[SPOT_CVD_SOURCE_ERROR]",
                compact_symbol, source, "HTTP", response.status_code,
                flush=True,
            )
            return None, "DATA_ERROR"

        if not isinstance(payload, dict) or payload.get("retCode") != 0:
            if isinstance(payload, dict) and payload.get("retCode") in (10001, 10029):
                _block_source(source, compact_symbol)
                return None, "UNSUPPORTED"
            return None, "DATA_ERROR"

        result = payload.get("result") or {}
        trades = result.get("list")
        if not isinstance(trades, list):
            return None, "DATA_ERROR"

        rows = [
            (trade.get("side"), trade.get("price"), trade.get("size"))
            for trade in trades
            if isinstance(trade, dict)
        ]
        return rows, "READY" if rows else "NO_TRADES"

    except Exception as error:
        print(
            "[SPOT_CVD_SOURCE_ERROR]",
            compact_symbol, source, type(error).__name__,
            flush=True,
        )
        return None, "DATA_ERROR"


def _fetch_okx(spot_symbol, limit):
    source = "OKX"
    if _source_blocked(source, spot_symbol):
        return None, "UNSUPPORTED"

    try:
        response = requests.get(
            OKX_SPOT_TRADES_URL,
            params={
                "instId": spot_symbol,
                "limit": str(min(max(int(limit), 1), 500)),
            },
            timeout=6,
        )
        payload = response.json()

        if response.status_code != 200:
            print(
                "[SPOT_CVD_SOURCE_ERROR]",
                spot_symbol, source, "HTTP", response.status_code,
                flush=True,
            )
            return None, "DATA_ERROR"

        if not isinstance(payload, dict):
            return None, "DATA_ERROR"

        if payload.get("code") == "51001":
            _block_source(source, spot_symbol)
            return None, "UNSUPPORTED"

        if payload.get("code") != "0":
            print("[SPOT_CVD_API_ERROR]", spot_symbol, payload, flush=True)
            return None, "DATA_ERROR"

        trades = payload.get("data")
        if not isinstance(trades, list):
            return None, "DATA_ERROR"

        rows = [
            (trade.get("side"), trade.get("px"), trade.get("sz"))
            for trade in trades
            if isinstance(trade, dict)
        ]
        return rows, "READY" if rows else "NO_TRADES"

    except Exception as error:
        print(
            "[SPOT_CVD_SOURCE_ERROR]",
            spot_symbol, source, type(error).__name__,
            flush=True,
        )
        return None, "DATA_ERROR"


def get_spot_cvd(raw_symbol, limit=100):
    """
    Immediate Spot CVD snapshot with exchange fallback.

    Priority:
      Binance Spot -> Bybit Spot -> OKX Spot

    The first source with real trades wins. Missing/blocked markets remain
    UNKNOWN and never become directional evidence.
    """
    compact_symbol = _compact_symbol(raw_symbol)
    spot_symbol = swap_to_spot_symbol(raw_symbol)

    if not compact_symbol or not spot_symbol:
        return {
            "available": False,
            "spot_symbol": None,
            "source": None,
            "quality": "UNAVAILABLE",
            "state": "NO_SPOT_SYMBOL",
            "vote": "UNKNOWN",
            "weight": 0,
            "text": "Спотовая пара не определена",
        }

    attempts = (
        ("Binance", compact_symbol, _fetch_binance),
        ("Bybit", compact_symbol, _fetch_bybit),
        ("OKX", spot_symbol, _fetch_okx),
    )
    statuses = []

    for source, symbol, fetcher in attempts:
        rows, status = fetcher(symbol, limit)
        statuses.append((source, status))

        if rows:
            result = _build_result(spot_symbol, source, rows)
            if result is not None:
                print(
                    "[SPOT_CVD_SOURCE]",
                    compact_symbol,
                    "selected=", source,
                    flush=True,
                )
                return result

    all_unsupported = all(status == "UNSUPPORTED" for _, status in statuses)
    print(
        "[SPOT_CVD_UNAVAILABLE]",
        compact_symbol,
        "sources=", statuses,
        flush=True,
    )
    return {
        "available": False,
        "spot_symbol": spot_symbol,
        "source": None,
        "quality": "UNAVAILABLE",
        "state": "SPOT_UNAVAILABLE" if all_unsupported else "SPOT_DATA_PARTIAL",
        "vote": "UNKNOWN",
        "weight": 0,
        "text": (
            "Спотовой пары нет на Binance/Bybit/OKX"
            if all_unsupported
            else "Нет надёжных Spot CVD данных"
        ),
    }
