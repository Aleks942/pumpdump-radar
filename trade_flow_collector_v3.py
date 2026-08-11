# trade_flow_collector_v3.py
# PumpDump Radar V3
#
# Накопительный Trade Flow Collector.
#
# Задача:
# - сохранять реальные сделки с timestamp
# - отдельно хранить SPOT и SWAP
# - считать настоящие окна 1m / 5m / 15m
# - контролировать полноту временного окна
#
# Здесь НЕТ:
# - LONG / SHORT
# - score
# - Chief
# - торговых решений

import time
import threading
from collections import defaultdict, deque


# ============================================================
# SETTINGS
# ============================================================

MAX_HISTORY_SECONDS = 15 * 60

# Отдельная история Spot и Futures.
TRADE_HISTORY = {
    "spot": defaultdict(deque),
    "swap": defaultdict(deque),
}

_LOCK = threading.RLock()


# ============================================================
# NORMALIZATION
# ============================================================

def normalize_symbol(symbol):
    """
    Приводит:
        BTC-USDT-SWAP
        BTC-USDT
        BTCUSDT

    к:
        BTCUSDT
    """

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
    """
    Приводит разные названия futures/perpetual к swap.
    """

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
# CLEANUP
# ============================================================

def _cleanup(market, symbol, now=None):
    """
    Удаляет сделки старше 15 минут.
    """

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
    """
    Сохраняет ОДНУ реальную сделку.

    side:
        BUY
        SELL

    event_ts:
        реальное время сделки.

    quote_value:
        готовый dollar/USDT notional сделки.

    Если quote_value отсутствует:
        временно используем price * size.

    ВАЖНО:
    для SWAP параметр size может означать
    количество КОНТРАКТОВ, а не количество монет.

    Поэтому при подключении OKX WebSocket
    SWAP quote_value будем рассчитывать
    с учётом ctVal конкретного инструмента.
    """

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

    # Если timestamp пришёл в milliseconds.
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

    with _LOCK:

        TRADE_HISTORY[
            market
        ][symbol].append(row)

        _cleanup(
            market,
            symbol,
            now=ts,
        )

    return True


# ============================================================
# FLOW CALCULATION
# ============================================================

def get_flow(
    symbol,
    market,
    seconds,
):
    """
    Считает поток сделок строго внутри
    указанного временного окна.

    Например:

        seconds=60
        -> реальные последние 60 секунд

        seconds=300
        -> реальные последние 5 минут

        seconds=900
        -> реальные последние 15 минут

    Возвращает ФАКТЫ.

    Никаких LONG / SHORT решений здесь нет.
    """

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

    # --------------------------------------------------------
    # BUY / SELL
    # --------------------------------------------------------

    buy_quote = 0.0
    sell_quote = 0.0

    buy_count = 0
    sell_count = 0

    oldest_ts = None
    newest_ts = None

    for row in rows:

        quote = row["quote"]

        if row["side"] == "BUY":

            buy_quote += quote
            buy_count += 1

        elif row["side"] == "SELL":

            sell_quote += quote
            sell_count += 1

        ts = row["ts"]

        if (
            oldest_ts is None
            or ts < oldest_ts
        ):
            oldest_ts = ts

        if (
            newest_ts is None
            or ts > newest_ts
        ):
            newest_ts = ts

    # --------------------------------------------------------
    # DELTA
    # --------------------------------------------------------

    total_quote = (
        buy_quote
        + sell_quote
    )

    delta_quote = (
        buy_quote
        - sell_quote
    )

    imbalance = (
        delta_quote / total_quote
        if total_quote > 0
        else 0.0
    )

    # --------------------------------------------------------
    # WINDOW COVERAGE
    # --------------------------------------------------------

    coverage_seconds = 0.0

    if oldest_ts is not None:

        # Важно:
        # смотрим возраст самой старой сделки.
        #
        # Это показывает, действительно ли
        # collector накопил нужное окно.
        coverage_seconds = max(
            0.0,
            now - oldest_ts,
        )

    coverage_ratio = min(
        1.0,
        coverage_seconds / seconds,
    )

    # Будущий Smart Money Engine сможет
    # использовать данные только после
    # заполнения минимум 95% окна.
    window_ready = (
        coverage_ratio >= 0.95
        and len(rows) > 0
    )

    # --------------------------------------------------------
    # RESULT
    # --------------------------------------------------------

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

        # -1 ... +1
        "imbalance": imbalance,

        # -100 ... +100
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
    }


# ============================================================
# STANDARD WINDOWS
# ============================================================

def get_flow_windows(
    symbol,
    market,
):
    """
    Стандартные окна PumpDump Radar V3.
    """

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
    """
    SPOT 1m / 5m / 15m.
    """

    return get_flow_windows(
        symbol,
        "spot",
    )


def get_futures_windows(symbol):
    """
    FUTURES 1m / 5m / 15m.
    """

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
    """
    Сколько сделок сейчас находится
    в памяти collector.
    """

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
