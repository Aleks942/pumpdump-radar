# trade_flow_collector_v3.py
# PumpDump Radar V3
#
# Timestamped Trade Flow Collector.
#
# V3.2:
# - реальные окна 1m / 5m / 15m
# - SPOT и SWAP отдельно
# - контроль непрерывности данных
# - READY зависит от времени работы stream,
#   а не от наличия сделки в начале окна
# - data gap делает данные временно невалидными
#
# Здесь НЕТ:
# - LONG / SHORT
# - score
# - Chief
# - торговых решений

import time
import threading
from collections import defaultdict, deque


MAX_HISTORY_SECONDS = 15 * 60

# Если stream не подтверждал активность дольше этого времени,
# считаем, что соединение/данные потенциально потеряны.
STREAM_STALE_SECONDS = 30.0

TRADE_HISTORY = {
    "spot": defaultdict(deque),
    "swap": defaultdict(deque),
}

# Состояние потока отдельно для SPOT / SWAP.
STREAM_STATE = {
    "spot": {
        "connected": False,
        "started_at": None,
        "last_activity_at": None,
        "last_trade_at": None,
        "generation": 0,
    },
    "swap": {
        "connected": False,
        "started_at": None,
        "last_activity_at": None,
        "last_trade_at": None,
        "generation": 0,
    },
}

_LOCK = threading.RLock()


# ============================================================
# NORMALIZATION
# ============================================================

def normalize_symbol(symbol):
    if not symbol:
        return None

    symbol = str(symbol).upper().strip()

    symbol = symbol.replace("/", "")
    symbol = symbol.replace("-USDT-SWAP", "USDT")
    symbol = symbol.replace("-USDT", "USDT")

    if not symbol.endswith("USDT"):
        return None

    return symbol


def _normalize_market(market):
    market = str(market or "").lower()

    if market in (
        "future",
        "futures",
        "perp",
        "swap",
    ):
        return "swap"

    if market == "spot":
        return "spot"

    return None


# ============================================================
# STREAM STATE
# ============================================================

def mark_stream_connected(market):
    """
    Новый непрерывный период данных.

    При reconnect started_at начинается заново.
    Поэтому старое окно не сможет ошибочно получить READY.
    """

    market = _normalize_market(market)

    if not market:
        return False

    now = time.time()

    with _LOCK:
        state = STREAM_STATE[market]

        state["connected"] = True
        state["started_at"] = now
        state["last_activity_at"] = now
        state["last_trade_at"] = None
        state["generation"] += 1

    return True


def mark_stream_activity(market):
    """
    Подтверждает, что WebSocket продолжает получать сообщения.

    Это важно: отсутствие сделок само по себе
    не означает data gap.
    """

    market = _normalize_market(market)

    if not market:
        return False

    now = time.time()

    with _LOCK:
        state = STREAM_STATE[market]

        if not state["connected"]:
            state["connected"] = True
            state["started_at"] = now
            state["generation"] += 1

        state["last_activity_at"] = now

    return True


def mark_stream_disconnected(market):
    """
    После disconnect окна больше не считаются READY.
    """

    market = _normalize_market(market)

    if not market:
        return False

    with _LOCK:
        state = STREAM_STATE[market]

        state["connected"] = False
        state["started_at"] = None
        state["last_activity_at"] = None
        state["last_trade_at"] = None

    return True


def get_stream_state(market):
    market = _normalize_market(market)

    if not market:
        return None

    now = time.time()

    with _LOCK:
        state = dict(STREAM_STATE[market])

    started_at = state.get("started_at")
    last_activity = state.get("last_activity_at")

    continuous_seconds = 0.0

    if (
        state.get("connected")
        and started_at is not None
    ):
        continuous_seconds = max(
            0.0,
            now - started_at,
        )

    stale_seconds = None

    if last_activity is not None:
        stale_seconds = max(
            0.0,
            now - last_activity,
        )

    stream_healthy = (
        state.get("connected") is True
        and last_activity is not None
        and stale_seconds <= STREAM_STALE_SECONDS
    )

    state["continuous_seconds"] = continuous_seconds
    state["stale_seconds"] = stale_seconds
    state["stream_healthy"] = stream_healthy

    return state


# ============================================================
# CLEANUP
# ============================================================

def _cleanup(market, symbol, now=None):
    now = float(now or time.time())

    cutoff = now - MAX_HISTORY_SECONDS

    rows = TRADE_HISTORY[market].get(symbol)

    if not rows:
        return

    while rows and rows[0]["ts"] < cutoff:
        rows.popleft()

    if not rows:
        TRADE_HISTORY[market].pop(
            symbol,
            None,
        )


# ============================================================
# SAVE TRADE
# ============================================================

def save_trade(
    symbol,
    market,
    side,
    price,
    size,
    event_ts=None,
    quote_value=None,
):
    symbol = normalize_symbol(symbol)
    market = _normalize_market(market)

    if not symbol or not market:
        return False

    side = str(side or "").upper()

    if side not in ("BUY", "SELL"):
        return False

    try:
        price = float(price)
        size = float(size)
    except (TypeError, ValueError):
        return False

    if price <= 0 or size <= 0:
        return False

    ts = float(
        event_ts or time.time()
    )

    if ts > 10_000_000_000:
        ts /= 1000.0

    if quote_value is None:
        quote_value = price * size

    try:
        quote_value = float(
            quote_value
        )
    except (TypeError, ValueError):
        return False

    if quote_value <= 0:
        return False

    row = {
        "ts": ts,
        "side": side,
        "price": price,
        "size": size,
        "quote": quote_value,
    }

    now = time.time()

    with _LOCK:
        TRADE_HISTORY[
            market
        ][symbol].append(row)

        state = STREAM_STATE[market]

        # Защита на случай использования collector
        # без явного mark_stream_connected().
        if not state["connected"]:
            state["connected"] = True
            state["started_at"] = now
            state["generation"] += 1

        state["last_activity_at"] = now
        state["last_trade_at"] = ts

        _cleanup(
            market,
            symbol,
            now=now,
        )

    return True


# ============================================================
# FLOW
# ============================================================

def get_flow(
    symbol,
    market,
    seconds,
):
    symbol = normalize_symbol(symbol)
    market = _normalize_market(market)

    if not symbol or not market:
        return None

    seconds = max(
        1,
        int(seconds),
    )

    now = time.time()
    cutoff = now - seconds

    with _LOCK:
        _cleanup(
            market,
            symbol,
            now=now,
        )

        all_rows = TRADE_HISTORY[
            market
        ].get(
            symbol,
            deque(),
        )

        rows = [
            row
            for row in all_rows
            if row["ts"] >= cutoff
        ]

    buy_quote = 0.0
    sell_quote = 0.0

    buy_count = 0
    sell_count = 0

    for row in rows:
        quote = row["quote"]

        if row["side"] == "BUY":
            buy_quote += quote
            buy_count += 1

        elif row["side"] == "SELL":
            sell_quote += quote
            sell_count += 1

    total_quote = (
        buy_quote + sell_quote
    )

    delta_quote = (
        buy_quote - sell_quote
    )

    imbalance = (
        delta_quote / total_quote
        if total_quote > 0
        else 0.0
    )

    # --------------------------------------------------------
    # DATA QUALITY
    # --------------------------------------------------------

    state = get_stream_state(
        market
    )

    continuous_seconds = (
        state.get(
            "continuous_seconds",
            0.0,
        )
        if state
        else 0.0
    )

    stream_healthy = (
        state.get(
            "stream_healthy",
            False,
        )
        if state
        else False
    )

    # Теперь coverage означает:
    # сколько времени collector непрерывно наблюдал рынок.
    coverage_ratio = min(
        1.0,
        continuous_seconds / seconds,
    )

    coverage_seconds = min(
        continuous_seconds,
        float(seconds),
    )

    window_ready = (
        stream_healthy
        and continuous_seconds >= (
            seconds * 0.95
        )
    )

    if not stream_healthy:
        quality = "INVALID"

    elif window_ready:
        quality = "READY"

    else:
        quality = "WARMING"

    return {
        "symbol": symbol,
        "market": market,

        "window_seconds": seconds,

        "trade_count": len(rows),

        "buy_count": buy_count,
        "sell_count": sell_count,

        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "total_quote": total_quote,

        "delta_quote": delta_quote,

        "imbalance": imbalance,
        "imbalance_pct": (
            imbalance * 100.0
        ),

        "coverage_seconds": (
            coverage_seconds
        ),

        "coverage_pct": (
            coverage_ratio * 100.0
        ),

        "window_ready": (
            window_ready
        ),

        "quality": quality,

        "stream_healthy": (
            stream_healthy
        ),

        "stream_continuous_seconds": (
            continuous_seconds
        ),

        "stream_stale_seconds": (
            state.get("stale_seconds")
            if state
            else None
        ),

        "stream_generation": (
            state.get("generation")
            if state
            else None
        ),
    }


# ============================================================
# STANDARD WINDOWS
# ============================================================

def get_flow_windows(
    symbol,
    market,
):
    return {
        "1m": get_flow(
            symbol,
            market,
            60,
        ),

        "5m": get_flow(
            symbol,
            market,
            300,
        ),

        "15m": get_flow(
            symbol,
            market,
            900,
        ),
    }


def get_spot_windows(symbol):
    return get_flow_windows(
        symbol,
        "spot",
    )


def get_futures_windows(symbol):
    return get_flow_windows(
        symbol,
        "swap",
    )


# ============================================================
# DIAGNOSTICS
# ============================================================

def get_history_size(
    symbol,
    market,
):
    symbol = normalize_symbol(symbol)
    market = _normalize_market(market)

    if not symbol or not market:
        return 0

    with _LOCK:
        return len(
            TRADE_HISTORY[
                market
            ].get(
                symbol,
                [],
            )
        )
