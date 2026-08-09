# liquidation_engine_v3.py
# PumpDump Radar V3
#
# Чистый слой liquidation data.
#
# Задачи:
# - принимать liquidation events
# - дедуплицировать события
# - хранить timestamp события
# - считать строгие окна 1m / 5m / 15m
#
# Здесь НЕТ LONG / SHORT решений.

import time
import threading
from collections import defaultdict, deque


MAX_HISTORY_SECONDS = 15 * 60

LIQ_EVENTS = defaultdict(lambda: deque())
SEEN_EVENTS = {}

_LOCK = threading.RLock()


def normalize_symbol(symbol):
    if not symbol:
        return None

    s = str(symbol).upper().strip()
    s = s.replace("/", "")
    s = s.replace("-USDT-SWAP", "USDT")
    s = s.replace("-USDT", "USDT")

    return s


def _cleanup(now=None):
    now = float(now or time.time())
    cutoff = now - MAX_HISTORY_SECONDS

    with _LOCK:

        # Удаляем старые liquidation events.
        for symbol in list(LIQ_EVENTS.keys()):
            rows = LIQ_EVENTS[symbol]

            while rows and rows[0]["ts"] < cutoff:
                rows.popleft()

            if not rows:
                del LIQ_EVENTS[symbol]

        # Удаляем старые dedup keys.
        for key, ts in list(SEEN_EVENTS.items()):
            if ts < cutoff:
                del SEEN_EVENTS[key]


def _make_event_key(
    symbol,
    exchange,
    side,
    price,
    qty,
    event_ts,
    event_id=None,
):
    """
    Если биржа дала настоящий event ID — используем его.

    Если ID нет, строим fingerprint из параметров события.
    """

    if event_id:
        return (
            str(exchange).upper(),
            str(event_id),
        )

    # Округление timestamp уменьшает вероятность,
    # что один REST event с теми же параметрами
    # попадёт в память повторно.
    ts_bucket = int(float(event_ts) * 1000)

    return (
        str(exchange).upper(),
        normalize_symbol(symbol),
        str(side).upper(),
        round(float(price), 12),
        round(float(qty), 12),
        ts_bucket,
    )


def save_liquidation(
    symbol,
    exchange,
    side,
    price,
    qty,
    event_ts=None,
    event_id=None,
):
    """
    Сохраняет ОДНО liquidation event.

    Возвращает:
        True  -> новое событие сохранено
        False -> дубль или некорректные данные
    """

    symbol = normalize_symbol(symbol)

    if not symbol:
        return False

    try:
        price = float(price)
        qty = float(qty)
    except (TypeError, ValueError):
        return False

    if price <= 0 or qty <= 0:
        return False

    side = str(side or "").upper()

    if side not in ("BUY", "SELL"):
        return False

    exchange = str(exchange or "UNKNOWN")

    ts = float(event_ts or time.time())

    # На некоторых API timestamp приходит в ms.
    if ts > 10_000_000_000:
        ts /= 1000.0

    usd_value = price * qty

    key = _make_event_key(
        symbol=symbol,
        exchange=exchange,
        side=side,
        price=price,
        qty=qty,
        event_ts=ts,
        event_id=event_id,
    )

    with _LOCK:
        if key in SEEN_EVENTS:
            return False

        SEEN_EVENTS[key] = ts

        LIQ_EVENTS[symbol].append({
            "ts": ts,
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "price": price,
            "qty": qty,
            "usd": usd_value,
            "event_id": event_id,
        })

    _cleanup()

    return True


def get_liquidations(symbol, seconds=300):
    """
    Возвращает liquidation facts за строгое временное окно.

    SELL liquidation:
        обычно ликвидируется LONG.

    BUY liquidation:
        обычно ликвидируется SHORT.

    ВАЖНО:
    Это НЕ торговый сигнал.
    """

    symbol = normalize_symbol(symbol)

    if not symbol:
        return None

    seconds = max(1, int(seconds))

    now = time.time()
    cutoff = now - seconds

    _cleanup(now)

    with _LOCK:
        rows = [
            x
            for x in LIQ_EVENTS.get(symbol, [])
            if x["ts"] >= cutoff
        ]

    long_liq = 0.0
    short_liq = 0.0

    long_count = 0
    short_count = 0

    exchanges = set()

    for row in rows:

        exchanges.add(row["exchange"])

        if row["side"] == "SELL":
            long_liq += row["usd"]
            long_count += 1

        elif row["side"] == "BUY":
            short_liq += row["usd"]
            short_count += 1

    total = long_liq + short_liq

    imbalance = 0.0

    if total > 0:
        imbalance = (
            short_liq - long_liq
        ) / total

    return {
        "symbol": symbol,
        "seconds": seconds,

        "long_liq_usd": long_liq,
        "short_liq_usd": short_liq,
        "total_liq_usd": total,

        "long_liq_count": long_count,
        "short_liq_count": short_count,
        "event_count": len(rows),

        # -1 = только LONG liquidations
        # +1 = только SHORT liquidations
        #
        # Это просто характеристика события,
        # НЕ LONG/SHORT score.
        "imbalance": imbalance,
        "imbalance_pct": imbalance * 100.0,

        "exchanges": sorted(exchanges),
    }


def get_liquidation_windows(symbol):
    """
    Стандартные окна V3.
    """

    return {
        "1m": get_liquidations(symbol, 60),
        "5m": get_liquidations(symbol, 300),
        "15m": get_liquidations(symbol, 900),
    }


def get_recent_events(symbol, seconds=300):
    """
    Для диагностики.
    Позволяет посмотреть реальные события,
    из которых был рассчитан summary.
    """

    symbol = normalize_symbol(symbol)

    if not symbol:
        return []

    now = time.time()
    cutoff = now - int(seconds)

    _cleanup(now)

    with _LOCK:
        return [
            dict(x)
            for x in LIQ_EVENTS.get(symbol, [])
            if x["ts"] >= cutoff
        ]
