# market_data_v3.py
# PumpDump Radar V3
#
# Задача этого модуля:
# ТОЛЬКО получать и приводить к единому виду рыночные данные.
#
# Здесь НЕТ:
# - LONG / SHORT
# - Smart Money score
# - торговых решений
# - Chief
# - market stages
#
# market_data_v3 отвечает только на вопрос:
# "Что сейчас происходит на рынке?"

import time
import requests
from collections import defaultdict, deque
from trade_flow_collector_v3 import get_spot_windows, get_futures_windows


OKX_BASE = "https://www.okx.com"

REQUEST_TIMEOUT = 12

# Храним timestamped OI snapshots.
# Это исправляет проблему V2, где OI сравнивался
# с первым элементом массива без гарантированного временного окна.
OI_HISTORY = defaultdict(lambda: deque(maxlen=300))


# ============================================================
# COMMON
# ============================================================

def okx_get(path, params=None):
    """
    Безопасный GET к публичному OKX API.

    Возвращает:
        list | None
    """

    try:
        response = requests.get(
            OKX_BASE + path,
            params=params or {},
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()

        payload = response.json()

        if payload.get("code") != "0":
            print(
                "[V3_OKX_API_ERROR]",
                path,
                payload.get("code"),
                payload.get("msg"),
                flush=True,
            )
            return None

        return payload.get("data", [])

    except Exception as exc:
        print(
            "[V3_OKX_ERROR]",
            path,
            exc,
            flush=True,
        )
        return None


def normalize_swap_symbol(symbol):
    """
    BTCUSDT -> BTC-USDT-SWAP
    BTC-USDT -> BTC-USDT-SWAP
    BTC-USDT-SWAP -> BTC-USDT-SWAP
    """

    if not symbol:
        return None

    symbol = symbol.upper().strip()

    if symbol.endswith("-USDT-SWAP"):
        return symbol

    if symbol.endswith("-USDT"):
        return symbol + "-SWAP"

    if symbol.endswith("USDT"):
        base = symbol[:-4]

        if base:
            return f"{base}-USDT-SWAP"

    return None


def swap_to_spot_symbol(symbol):
    """
    BTC-USDT-SWAP -> BTC-USDT
    """

    swap_symbol = normalize_swap_symbol(symbol)

    if not swap_symbol:
        return None

    return swap_symbol.replace("-USDT-SWAP", "-USDT")


def public_symbol(symbol):
    """
    BTC-USDT-SWAP -> BTCUSDT
    """

    swap_symbol = normalize_swap_symbol(symbol)

    if not swap_symbol:
        return None

    return swap_symbol.replace("-USDT-SWAP", "USDT")


# ============================================================
# PRICE / CANDLES
# ============================================================

def get_ticker(symbol):
    inst_id = normalize_swap_symbol(symbol)

    if not inst_id:
        return None

    data = okx_get(
        "/api/v5/market/ticker",
        {"instId": inst_id},
    )

    if not data:
        return None

    row = data[0]

    try:
        return {
            "symbol": public_symbol(inst_id),
            "inst_id": inst_id,
            "price": float(row.get("last") or 0),
            "bid": float(row.get("bidPx") or 0),
            "ask": float(row.get("askPx") or 0),
            "volume_24h_contracts": float(
                row.get("vol24h") or 0
            ),
            "volume_24h_currency": float(
                row.get("volCcy24h") or 0
            ),
            "ts": int(row.get("ts") or 0),
        }

    except (TypeError, ValueError):
        return None


def get_candles(symbol, bar="1m", limit=100):
    inst_id = normalize_swap_symbol(symbol)

    if not inst_id:
        return []

    data = okx_get(
        "/api/v5/market/candles",
        {
            "instId": inst_id,
            "bar": bar,
            "limit": str(limit),
        },
    )

    if not data:
        return []

    result = []

    for row in data:
        try:
            result.append({
                "ts": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "volume_currency": (
                    float(row[6])
                    if len(row) > 6
                    else None
                ),
                "volume_quote": (
                    float(row[7])
                    if len(row) > 7
                    else None
                ),
                "confirmed": (
                    row[8] == "1"
                    if len(row) > 8
                    else None
                ),
            })

        except (TypeError, ValueError, IndexError):
            continue

    # OKX возвращает новые свечи первыми.
    # В V3 везде используем:
    # oldest -> newest
    result.reverse()

    return result


def get_price_change(symbol, minutes=5):
    """
    Изменение цены за фиксированное временное окно.

    Используем 1m candles, а не "N последних вызовов".
    """

    minutes = max(1, int(minutes))

    candles = get_candles(
        symbol,
        bar="1m",
        limit=minutes + 2,
    )

    if len(candles) < minutes + 1:
        return None

    # Берём open свечи примерно N минут назад
    # и последний доступный close.
    start = candles[-(minutes + 1)]["open"]
    end = candles[-1]["close"]

    if start <= 0:
        return None

    return ((end - start) / start) * 100.0


# ============================================================
# FUNDING
# ============================================================

def get_funding(symbol):
    inst_id = normalize_swap_symbol(symbol)

    if not inst_id:
        return None

    data = okx_get(
        "/api/v5/public/funding-rate",
        {"instId": inst_id},
    )

    if not data:
        return None

    row = data[0]

    try:
        rate_raw = float(row.get("fundingRate") or 0)

        return {
            "rate_raw": rate_raw,
            "rate_pct": rate_raw * 100.0,
            "funding_time": int(
                row.get("fundingTime") or 0
            ),
            "next_funding_time": int(
                row.get("nextFundingTime") or 0
            ),
        }

    except (TypeError, ValueError):
        return None


# ============================================================
# OPEN INTEREST
# ============================================================

def get_open_interest(symbol, save_snapshot=True):
    inst_id = normalize_swap_symbol(symbol)

    if not inst_id:
        return None

    data = okx_get(
        "/api/v5/public/open-interest",
        {
            "instType": "SWAP",
            "instId": inst_id,
        },
    )

    if not data:
        return None

    row = data[0]

    try:
        oi = float(row.get("oi") or 0)

        # Некоторые ответы OKX также содержат
        # oiCcy / oiUsd. Не предполагаем их наличие.
        oi_ccy = row.get("oiCcy")
        oi_usd = row.get("oiUsd")

        result = {
            "oi": oi,
            "oi_ccy": (
                float(oi_ccy)
                if oi_ccy not in (None, "")
                else None
            ),
            "oi_usd": (
                float(oi_usd)
                if oi_usd not in (None, "")
                else None
            ),
            "exchange_ts": int(
                row.get("ts") or 0
            ),
            "local_ts": time.time(),
        }

        if save_snapshot and oi > 0:
            save_oi_snapshot(
                inst_id,
                oi,
                result["local_ts"],
            )

        return result

    except (TypeError, ValueError):
        return None


def save_oi_snapshot(symbol, oi, ts=None):
    inst_id = normalize_swap_symbol(symbol)

    if not inst_id:
        return

    if oi is None or oi <= 0:
        return

    now = float(ts or time.time())

    history = OI_HISTORY[inst_id]

    # Не сохраняем почти идентичные timestamps.
    if history:
        last_ts = history[-1]["ts"]

        if now - last_ts < 0.5:
            history[-1] = {
                "ts": now,
                "oi": float(oi),
            }
        else:
            history.append({
                "ts": now,
                "oi": float(oi),
            })
    else:
        history.append({
            "ts": now,
            "oi": float(oi),
        })

    # Нам сейчас достаточно 60 минут истории.
    cutoff = now - 3600

    while history and history[0]["ts"] < cutoff:
        history.popleft()


def get_oi_change(symbol, minutes=5):
    """
    Рассчитывает изменение OI по реальному timestamp.

    ВАЖНО:
    функция НЕ говорит LONG или SHORT.

    Она сообщает только:
        OI вырос / упал на X%.
    """

    inst_id = normalize_swap_symbol(symbol)

    if not inst_id:
        return None

    history = OI_HISTORY.get(inst_id)

    if not history or len(history) < 2:
        return None

    now_ts = history[-1]["ts"]
    current_oi = history[-1]["oi"]

    target_ts = now_ts - (minutes * 60)

    older = None

    # Ищем snapshot, ближайший к нужному времени.
    older = min(
        history,
        key=lambda x: abs(x["ts"] - target_ts),
    )

    # Не выдаём "5m change", если ближайшая точка
    # слишком далеко от настоящих 5 минут.
    tolerance = max(15, minutes * 60 * 0.10)

    actual_age = now_ts - older["ts"]

    if abs(actual_age - minutes * 60) > tolerance:
        return None

    old_oi = older["oi"]

    if old_oi <= 0:
        return None

    change_pct = (
        (current_oi - old_oi)
        / old_oi
    ) * 100.0

    return {
        "minutes": minutes,
        "current_oi": current_oi,
        "old_oi": old_oi,
        "change_pct": change_pct,
        "actual_seconds": actual_age,
    }


# ============================================================
# TRADE FLOW
# ============================================================

def get_trade_flow(symbol, market="swap", seconds=300, limit=500):
    """
    Получает текущий snapshot агрессивных сделок.

    market:
        "swap" -> futures/perpetual
        "spot" -> spot

    В отличие от V2:
    - считаем quote volume (price * size)
    - фильтруем сделки по timestamp
    - возвращаем числовые факты
    - НЕ присваиваем LONG / SHORT
    """

    if market == "spot":
        inst_id = swap_to_spot_symbol(symbol)
    else:
        inst_id = normalize_swap_symbol(symbol)

    if not inst_id:
        return None

    data = okx_get(
        "/api/v5/market/trades",
        {
            "instId": inst_id,
            "limit": str(min(int(limit), 500)),
        },
    )

    if not data:
        return None

    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - (int(seconds) * 1000)

    buy_quote = 0.0
    sell_quote = 0.0

    trade_count = 0
    oldest_ts = None
    newest_ts = None

    for trade in data:
        try:
            ts = int(trade.get("ts") or 0)

            if ts <= 0:
                continue

            if ts < cutoff_ms:
                continue

            price = float(trade.get("px") or 0)
            size = float(trade.get("sz") or 0)

            side = str(
                trade.get("side") or ""
            ).lower()

            if price <= 0 or size <= 0:
                continue

            quote = price * size

            if side == "buy":
                buy_quote += quote

            elif side == "sell":
                sell_quote += quote

            else:
                continue

            trade_count += 1

            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts

            if newest_ts is None or ts > newest_ts:
                newest_ts = ts

        except (TypeError, ValueError):
            continue

    total = buy_quote + sell_quote
    delta = buy_quote - sell_quote

    imbalance = (
        delta / total
        if total > 0
        else 0.0
    )

    coverage_seconds = 0.0

    if oldest_ts and newest_ts:
        coverage_seconds = (
            newest_ts - oldest_ts
        ) / 1000.0

    return {
        "market": market,
        "inst_id": inst_id,
        "requested_seconds": int(seconds),

        "trade_count": trade_count,

        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "total_quote": total,

        "delta_quote": delta,

        # -1.0 ... +1.0
        "imbalance": imbalance,

        # -100 ... +100
        "imbalance_pct": imbalance * 100.0,

        "coverage_seconds": coverage_seconds,

        # Если API limit закончился раньше нужного окна,
        # мы честно помечаем данные как неполные.
        "possibly_truncated": (
            len(data) >= min(int(limit), 500)
            and oldest_ts is not None
            and oldest_ts > cutoff_ms
        ),
    }


def get_spot_flow(symbol, seconds=300):
    return get_trade_flow(
        symbol,
        market="spot",
        seconds=seconds,
    )


def get_futures_flow(symbol, seconds=300):
    return get_trade_flow(
        symbol,
        market="swap",
        seconds=seconds,
    )


# ============================================================
# VOLUME
# ============================================================

def get_volume_stats(symbol, lookback=20):
    candles = get_candles(
        symbol,
        bar="1m",
        limit=max(lookback + 1, 5),
    )

    if len(candles) < 5:
        return None

    latest = candles[-1]

    history = candles[
        -min(len(candles), lookback + 1):-1
    ]

    volumes = [
        x["volume_quote"]
        if x["volume_quote"] is not None
        else x["volume"]
        for x in history
    ]

    latest_volume = (
        latest["volume_quote"]
        if latest["volume_quote"] is not None
        else latest["volume"]
    )

    if not volumes:
        return None

    avg = sum(volumes) / len(volumes)

    ratio = (
        latest_volume / avg
        if avg > 0
        else None
    )

    return {
        "latest": latest_volume,
        "average": avg,
        "ratio": ratio,
        "samples": len(volumes),
    }


# ============================================================
# COMPLETE RAW SNAPSHOT
# ============================================================

def build_market_snapshot(symbol):
    """
    Один стандартизированный объект для Smart Money Engine V3.

    Никаких торговых решений здесь нет.
    """

    inst_id = normalize_swap_symbol(symbol)

    if not inst_id:
        return None

    ticker = get_ticker(inst_id)

    if ticker is None:
        return None

    oi = get_open_interest(
        inst_id,
        save_snapshot=True,
    )

    snapshot = {
        "symbol": public_symbol(inst_id),
        "inst_id": inst_id,
        "timestamp": time.time(),

        "price": ticker["price"],

        "price_change": {
            "1m": get_price_change(inst_id, 1),
            "5m": get_price_change(inst_id, 5),
            "15m": get_price_change(inst_id, 15),
        },

        "funding": get_funding(inst_id),

        "open_interest": oi,

        "oi_change": {
            "1m": get_oi_change(inst_id, 1),
            "5m": get_oi_change(inst_id, 5),
            "15m": get_oi_change(inst_id, 15),
        },

        "spot_flow": {
            "1m": get_spot_flow(inst_id, 60),
            "5m": get_spot_flow(inst_id, 300),
        },

        "futures_flow": {
            "1m": get_futures_flow(inst_id, 60),
            "5m": get_futures_flow(inst_id, 300),
        },

        "volume": get_volume_stats(
            inst_id,
            lookback=20,
        ),
    }

    return snapshot
