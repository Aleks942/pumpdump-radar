# market_data_v3.py
# PumpDump Radar V3
#
# Единый слой рыночных данных V3.
#
# Задача:
# - получить данные рынка
# - привести их к единому формату
# - собрать один MarketSnapshot
#
# Здесь НЕТ:
# - LONG / SHORT
# - Smart Money score
# - Chief
# - торговых решений
# - market stages

import time
import requests

from collections import defaultdict, deque

from trade_flow_collector_v3 import (
    get_spot_windows,
    get_futures_windows,
)


# ============================================================
# SETTINGS
# ============================================================

OKX_BASE = "https://www.okx.com"

REQUEST_TIMEOUT = 12

# OI snapshots храним по timestamp.
# maxlen здесь является только дополнительной защитой памяти.
OI_HISTORY = defaultdict(
    lambda: deque(maxlen=300)
)


# ============================================================
# OKX REST
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
            repr(exc),
            flush=True,
        )
        return None


# ============================================================
# SYMBOLS
# ============================================================

def normalize_swap_symbol(symbol):
    """
    BTCUSDT
    BTC-USDT
    BTC-USDT-SWAP

    -> BTC-USDT-SWAP
    """

    if not symbol:
        return None

    symbol = str(symbol).upper().strip()

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

    swap_symbol = normalize_swap_symbol(
        symbol
    )

    if not swap_symbol:
        return None

    return swap_symbol.replace(
        "-USDT-SWAP",
        "-USDT",
    )


def public_symbol(symbol):
    """
    BTC-USDT-SWAP -> BTCUSDT
    """

    swap_symbol = normalize_swap_symbol(
        symbol
    )

    if not swap_symbol:
        return None

    return swap_symbol.replace(
        "-USDT-SWAP",
        "USDT",
    )


# ============================================================
# TICKER
# ============================================================

def get_ticker(symbol):
    inst_id = normalize_swap_symbol(
        symbol
    )

    if not inst_id:
        return None

    data = okx_get(
        "/api/v5/market/ticker",
        {
            "instId": inst_id,
        },
    )

    if not data:
        return None

    row = data[0]

    try:
        return {
            "symbol": public_symbol(
                inst_id
            ),
            "inst_id": inst_id,

            "price": float(
                row.get("last") or 0
            ),

            "bid": float(
                row.get("bidPx") or 0
            ),

            "ask": float(
                row.get("askPx") or 0
            ),

            "volume_24h_contracts": float(
                row.get("vol24h") or 0
            ),

            "volume_24h_currency": float(
                row.get("volCcy24h") or 0
            ),

            "ts": int(
                row.get("ts") or 0
            ),
        }

    except (TypeError, ValueError):
        return None


# ============================================================
# CANDLES
# ============================================================

def get_candles(
    symbol,
    bar="1m",
    limit=100,
):
    inst_id = normalize_swap_symbol(
        symbol
    )

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

        except (
            TypeError,
            ValueError,
            IndexError,
        ):
            continue

    # OKX -> newest first.
    # V3 -> oldest first.
    result.reverse()

    return result


# ============================================================
# PRICE CHANGE
# ============================================================

def _price_change_from_candles(
    candles,
    minutes,
):
    """
    Считает изменение цены из УЖЕ загруженных свечей.

    Новый REST-запрос здесь не выполняется.
    """

    minutes = max(
        1,
        int(minutes),
    )

    if len(candles) < minutes + 1:
        return None

    start = candles[
        -(minutes + 1)
    ]["open"]

    end = candles[-1]["close"]

    if start <= 0:
        return None

    return (
        (end - start)
        / start
    ) * 100.0


def get_price_change(
    symbol,
    minutes=5,
):
    """
    Совместимость для внешних вызовов.

    Для MarketSnapshot эта функция
    отдельно НЕ вызывается.
    """

    minutes = max(
        1,
        int(minutes),
    )

    candles = get_candles(
        symbol,
        bar="1m",
        limit=minutes + 2,
    )

    return _price_change_from_candles(
        candles,
        minutes,
    )


# ============================================================
# FUNDING
# ============================================================

def get_funding(symbol):
    inst_id = normalize_swap_symbol(
        symbol
    )

    if not inst_id:
        return None

    data = okx_get(
        "/api/v5/public/funding-rate",
        {
            "instId": inst_id,
        },
    )

    if not data:
        return None

    row = data[0]

    try:
        rate_raw = float(
            row.get("fundingRate") or 0
        )

        return {
            "rate_raw": rate_raw,

            "rate_pct": (
                rate_raw * 100.0
            ),

            "funding_time": int(
                row.get(
                    "fundingTime"
                ) or 0
            ),

            "next_funding_time": int(
                row.get(
                    "nextFundingTime"
                ) or 0
            ),
        }

    except (TypeError, ValueError):
        return None


# ============================================================
# OPEN INTEREST
# ============================================================

def get_open_interest(
    symbol,
    save_snapshot=True,
):
    inst_id = normalize_swap_symbol(
        symbol
    )

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
        oi = float(
            row.get("oi") or 0
        )

        oi_ccy = row.get(
            "oiCcy"
        )

        oi_usd = row.get(
            "oiUsd"
        )

        result = {
            "oi": oi,

            "oi_ccy": (
                float(oi_ccy)
                if oi_ccy
                not in (None, "")
                else None
            ),

            "oi_usd": (
                float(oi_usd)
                if oi_usd
                not in (None, "")
                else None
            ),

            "exchange_ts": int(
                row.get("ts") or 0
            ),

            "local_ts": time.time(),
        }

        if (
            save_snapshot
            and oi > 0
        ):
            save_oi_snapshot(
                inst_id,
                oi,
                result["local_ts"],
            )

        return result

    except (TypeError, ValueError):
        return None


def save_oi_snapshot(
    symbol,
    oi,
    ts=None,
):
    inst_id = normalize_swap_symbol(
        symbol
    )

    if not inst_id:
        return

    if oi is None or oi <= 0:
        return

    now = float(
        ts or time.time()
    )

    history = OI_HISTORY[
        inst_id
    ]

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

    cutoff = now - 3600

    while (
        history
        and history[0]["ts"] < cutoff
    ):
        history.popleft()


def get_oi_change(
    symbol,
    minutes=5,
):
    """
    OI change по реальному timestamp.

    Никакого LONG/SHORT здесь нет.
    """

    inst_id = normalize_swap_symbol(
        symbol
    )

    if not inst_id:
        return None

    history = OI_HISTORY.get(
        inst_id
    )

    if (
        not history
        or len(history) < 2
    ):
        return None

    now_ts = history[-1]["ts"]
    current_oi = history[-1]["oi"]

    minutes = max(
        1,
        int(minutes),
    )

    target_seconds = (
        minutes * 60
    )

    target_ts = (
        now_ts - target_seconds
    )

    older = min(
        history,
        key=lambda x: abs(
            x["ts"] - target_ts
        ),
    )

    # Максимум 10% отклонения окна.
    # Минимальный tolerance = 15 sec.
    tolerance = max(
        15,
        target_seconds * 0.10,
    )

    actual_age = (
        now_ts - older["ts"]
    )

    if abs(
        actual_age - target_seconds
    ) > tolerance:
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

        "actual_seconds": (
            actual_age
        ),
    }


# ============================================================
# VOLUME
# ============================================================

def _volume_stats_from_candles(
    candles,
    lookback=20,
):
    """
    Volume stats из уже загруженных candles.

    Новый REST-запрос НЕ выполняется.
    """

    if len(candles) < 5:
        return None

    lookback = max(
        1,
        int(lookback),
    )

    latest = candles[-1]

    history = candles[
        -min(
            len(candles),
            lookback + 1,
        ):-1
    ]

    volumes = []

    for candle in history:
        value = (
            candle["volume_quote"]
            if candle["volume_quote"]
            is not None
            else candle["volume"]
        )

        if value is not None:
            volumes.append(value)

    latest_volume = (
        latest["volume_quote"]
        if latest["volume_quote"]
        is not None
        else latest["volume"]
    )

    if (
        not volumes
        or latest_volume is None
    ):
        return None

    average = (
        sum(volumes)
        / len(volumes)
    )

    ratio = (
        latest_volume / average
        if average > 0
        else None
    )

    return {
        "latest": latest_volume,
        "average": average,
        "ratio": ratio,
        "samples": len(volumes),
    }


def get_volume_stats(
    symbol,
    lookback=20,
):
    """
    Совместимость для внешних вызовов.

    MarketSnapshot использует уже
    загруженные candles.
    """

    candles = get_candles(
        symbol,
        bar="1m",
        limit=max(
            lookback + 1,
            5,
        ),
    )

    return _volume_stats_from_candles(
        candles,
        lookback,
    )


# ============================================================
# DATA QUALITY
# ============================================================

def _flow_quality(flow):
    """
    Краткое качество одного Trade Flow блока.
    """

    if not flow:
        return "INVALID"

    states = []

    for window in (
        "1m",
        "5m",
        "15m",
    ):
        row = flow.get(window)

        if not row:
            states.append(
                "INVALID"
            )
            continue

        states.append(
            row.get(
                "quality",
                "INVALID",
            )
        )

    if "INVALID" in states:
        return "INVALID"

    if all(
        state == "READY"
        for state in states
    ):
        return "READY"

    return "WARMING"


def build_data_quality(
    ticker,
    candles,
    funding,
    oi,
    spot_flow,
    futures_flow,
):
    """
    Только качество данных.

    Это НЕ торговая оценка.
    """

    price_ok = bool(
        ticker
        and ticker.get("price", 0) > 0
    )

    candles_ok = (
        len(candles) >= 16
    )

    funding_ok = (
        funding is not None
    )

    oi_ok = bool(
        oi
        and oi.get("oi", 0) > 0
    )

    spot_quality = _flow_quality(
        spot_flow
    )

    futures_quality = _flow_quality(
        futures_flow
    )

    # Для базового snapshot не требуем,
    # чтобы 15m Trade Flow уже прогрелся.
    #
    # critical_ready означает:
    # REST-данные получены и 1m flow
    # уже пригоден к использованию.

    spot_1m_ready = bool(
        spot_flow
        and spot_flow.get("1m")
        and spot_flow["1m"].get(
            "window_ready"
        )
    )

    futures_1m_ready = bool(
        futures_flow
        and futures_flow.get("1m")
        and futures_flow["1m"].get(
            "window_ready"
        )
    )

    critical_ready = (
        price_ok
        and candles_ok
        and funding_ok
        and oi_ok
        and spot_1m_ready
        and futures_1m_ready
    )

    return {
        "price": (
            "READY"
            if price_ok
            else "INVALID"
        ),

        "candles": (
            "READY"
            if candles_ok
            else "INVALID"
        ),

        "funding": (
            "READY"
            if funding_ok
            else "INVALID"
        ),

        "open_interest": (
            "READY"
            if oi_ok
            else "INVALID"
        ),

        "spot_flow": (
            spot_quality
        ),

        "futures_flow": (
            futures_quality
        ),

        "critical_ready": (
            critical_ready
        ),
    }


# ============================================================
# COMPLETE MARKET SNAPSHOT
# ============================================================

def build_market_snapshot(symbol):
    """
    Единый MarketSnapshot V3.

    Один цикл выполняет:

        1 x ticker
        1 x candles
        1 x funding
        1 x open interest

    Trade Flow берётся из live WebSocket collector.

    Здесь НЕТ торгового решения.
    """

    inst_id = normalize_swap_symbol(
        symbol
    )

    if not inst_id:
        return None

    # --------------------------------------------------------
    # 1. TICKER
    # --------------------------------------------------------

    ticker = get_ticker(
        inst_id
    )

    if ticker is None:
        return None

    # --------------------------------------------------------
    # 2. CANDLES
    #
    # Одного запроса достаточно одновременно для:
    # - price change 1m
    # - price change 5m
    # - price change 15m
    # - volume stats
    # --------------------------------------------------------

    candles = get_candles(
        inst_id,
        bar="1m",
        limit=25,
    )

    # --------------------------------------------------------
    # 3. FUNDING
    # --------------------------------------------------------

    funding = get_funding(
        inst_id
    )

    # --------------------------------------------------------
    # 4. OPEN INTEREST
    # --------------------------------------------------------

    oi = get_open_interest(
        inst_id,
        save_snapshot=True,
    )

    # --------------------------------------------------------
    # LIVE TRADE FLOW
    # --------------------------------------------------------

    spot_flow = get_spot_windows(
        inst_id
    )

    futures_flow = (
        get_futures_windows(
            inst_id
        )
    )

    # --------------------------------------------------------
    # CALCULATIONS FROM EXISTING DATA
    # --------------------------------------------------------

    price_change = {
        "1m": (
            _price_change_from_candles(
                candles,
                1,
            )
        ),

        "5m": (
            _price_change_from_candles(
                candles,
                5,
            )
        ),

        "15m": (
            _price_change_from_candles(
                candles,
                15,
            )
        ),
    }

    oi_change = {
        "1m": get_oi_change(
            inst_id,
            1,
        ),

        "5m": get_oi_change(
            inst_id,
            5,
        ),

        "15m": get_oi_change(
            inst_id,
            15,
        ),
    }

    volume = (
        _volume_stats_from_candles(
            candles,
            lookback=20,
        )
    )

    data_quality = build_data_quality(
        ticker=ticker,
        candles=candles,
        funding=funding,
        oi=oi,
        spot_flow=spot_flow,
        futures_flow=futures_flow,
    )

    # --------------------------------------------------------
    # FINAL SNAPSHOT
    # --------------------------------------------------------

    snapshot = {
        "symbol": public_symbol(
            inst_id
        ),

        "inst_id": inst_id,

        "timestamp": time.time(),

        "price": ticker["price"],

        "ticker": ticker,

        "price_change": (
            price_change
        ),

        "funding": funding,

        "open_interest": oi,

        "oi_change": (
            oi_change
        ),

        "spot_flow": (
            spot_flow
        ),

        "futures_flow": (
            futures_flow
        ),

        "volume": volume,

        "data_quality": (
            data_quality
        ),
    }

    return snapshot
