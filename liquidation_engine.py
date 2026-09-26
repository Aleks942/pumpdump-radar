import json
import math
import threading
import time

import requests
import websocket


VERSION = "2026-09-26-event-time-v1"

WINDOW = 300
SOURCE_TTL = 60

OKX_REST = "https://www.okx.com/api/v5/public/liquidation-orders"
BINANCE_WS = "wss://fstream.binance.com/market/ws/!forceOrder@arr"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"

BYBIT_SYMBOLS = (
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
    "BNBUSDT", "ADAUSDT", "LINKUSDT", "TONUSDT", "AVAXUSDT",
)

LIQ_MEMORY = {}
_SOURCE = {}
_CONTRACTS = {}

_LOCK = threading.RLock()
_STARTED = False


def norm_symbol(symbol):
    return (
        str(symbol or "")
        .strip()
        .upper()
        .replace("-USDT-SWAP", "USDT")
        .replace("/", "")
        .replace("-", "")
    )


def _positive(value):
    value = float(value)

    if not math.isfinite(value) or value <= 0:
        raise ValueError("Invalid positive number")

    return value


def _timestamp(value):
    ts = _positive(value)

    if ts > 100_000_000_000:
        return ts / 1000

    return ts


def _prune(now):
    for symbol in list(LIQ_MEMORY):
        LIQ_MEMORY[symbol] = {
            key: row
            for key, row in LIQ_MEMORY[symbol].items()
            if 0 <= now - row["ts"] <= WINDOW
        }

        if not LIQ_MEMORY[symbol]:
            del LIQ_MEMORY[symbol]


def _status(exchange, symbol, ok, reason=""):
    with _LOCK:
        _SOURCE[(exchange, symbol)] = {
            "ok": ok,
            "at": time.time(),
            "reason": reason,
        }


def save_liq(
    symbol,
    exchange,
    side,
    price,
    qty,
    event_ts=None,
    quote_value=None,
    event_id=None,
):
    """
    side — сторона закрывающего ордера:
    SELL = ликвидация LONG.
    BUY = ликвидация SHORT.

    События без времени биржи не принимаются.
    """

    try:
        symbol = norm_symbol(symbol)
        side = str(side).upper()

        price = _positive(price)
        qty = _positive(qty)
        ts = _timestamp(event_ts)

        now = time.time()

        if not symbol.endswith("USDT"):
            return False

        if side not in ("BUY", "SELL"):
            return False

        if not 0 <= now - ts <= WINDOW:
            return False

        if exchange == "OKX" and quote_value is None:
            return False

        value = _positive(
            quote_value
            if quote_value is not None
            else price * qty
        )

        identity = (
            str(event_id)
            if event_id is not None
            else (ts, side, price, qty)
        )

        key = (exchange, identity)

        with _LOCK:
            _prune(now)

            rows = LIQ_MEMORY.setdefault(symbol, {})

            if key in rows:
                return False

            rows[key] = {
                "ts": ts,
                "exchange": exchange,
                "side": side,
                "usd": value,
            }

        return True

    except (TypeError, ValueError, OverflowError):
        return False


def get_liquidation_summary(symbol):
    symbol = norm_symbol(symbol)
    now = time.time()

    with _LOCK:
        _prune(now)
        rows = list(LIQ_MEMORY.get(symbol, {}).values())
        states = dict(_SOURCE)

    sources = {}
    active = set()

    for exchange in ("OKX", "Binance", "Bybit"):
        state = (
            states.get((exchange, symbol))
            if exchange == "OKX"
            else states.get((exchange, "*"))
        )

        if exchange == "Bybit" and symbol not in BYBIT_SYMBOLS:
            sources[exchange] = "NOT_SUBSCRIBED"

        elif not state:
            sources[exchange] = "UNAVAILABLE"

        elif not state["ok"]:
            sources[exchange] = "ERROR"

        elif not 0 <= now - state["at"] <= SOURCE_TTL:
            sources[exchange] = "STALE"

        else:
            sources[exchange] = "OBSERVED_PARTIAL"
            active.add(exchange)

    rows = [
        row for row in rows
        if row["exchange"] in active
    ]

    long_liq = sum(
        row["usd"]
        for row in rows
        if row["side"] == "SELL"
    )

    short_liq = sum(
        row["usd"]
        for row in rows
        if row["side"] == "BUY"
    )

    available = bool(active)

    quality = (
        "OBSERVED_PARTIAL"
        if available
        else "UNAVAILABLE"
    )

    if rows:
        power = "Наблюдаемые ликвидации"
    elif available:
        power = "Свежих событий в доступной выборке нет"
    else:
        power = "Нет данных"

    # Нули сохраняют совместимость с текущим detector:
    # при отсутствии данных оба сравнения 0 > 0 ложны.
    # Telegram учитывает available и пишет «нет данных».
    result = {
        "long_liq": round(long_liq, 2),
        "short_liq": round(short_liq, 2),
        "total_liq": round(long_liq + short_liq, 2),
        "available": available,
        "quality": quality,
        "complete": False,
        "window_seconds": WINDOW,
        "exchanges": (
            ", ".join(sorted(active))
            if active
            else "нет данных"
        ),
        "sources": sources,
        "event_count": len(rows),
        "power": power,
    }

    print(
        "[LIQ_5M]",
        symbol,
        "quality=", quality,
        "events=", len(rows),
        "long=", result["long_liq"],
        "short=", result["short_liq"],
        "sources=", sources,
        flush=True,
    )

    return result


def _okx_json(url, params):
    response = requests.get(
        url,
        params=params,
        timeout=10,
    )

    response.raise_for_status()
    payload = response.json()

    if (
        not isinstance(payload, dict)
        or payload.get("code") != "0"
    ):
        print(
            "[OKX_LIQ_API_REJECTED]",
            "code=",
            (
                payload.get("code")
                if isinstance(payload, dict)
                else "INVALID_JSON"
            ),
            flush=True,
        )

        raise ValueError("OKX API rejected request")

    if not isinstance(payload.get("data"), list):
        raise ValueError("Invalid OKX schema")

    return payload["data"]


def _contract(inst_id):
    now = time.time()

    with _LOCK:
        cached = _CONTRACTS.get(inst_id)

    if cached and 0 <= now - cached[0] <= 3600:
        return cached[1]

    rows = _okx_json(
        "https://www.okx.com/api/v5/public/instruments",
        {
            "instType": "SWAP",
            "instId": inst_id,
        },
    )

    info = next(
        (
            row for row in rows
            if row.get("instId") == inst_id
        ),
        None,
    )

    base_currency = inst_id.removesuffix("-USDT-SWAP")

    if (
        not info
        or info.get("ctType") != "linear"
        or info.get("settleCcy") != "USDT"
        or info.get("ctValCcy") != base_currency
    ):
        raise ValueError("Unsupported or unknown contract units")

    ct_val = _positive(info.get("ctVal"))

    with _LOCK:
        _CONTRACTS[inst_id] = (now, ct_val)

    return ct_val


def fetch_okx_liquidations(inst_id):
    inst_id = str(inst_id or "").strip().upper()

    if "-" not in inst_id and inst_id.endswith("USDT"):
        inst_id = inst_id[:-4] + "-USDT-SWAP"

    symbol = norm_symbol(inst_id)

    try:
        if not inst_id.endswith("-USDT-SWAP"):
            raise ValueError("Only USDT SWAP supported")

        blocks = _okx_json(
            OKX_REST,
            {
                "instType": "SWAP",
                "uly": inst_id.removesuffix("-SWAP"),
                "state": "filled",
                "limit": "100",
            },
        )

        pending = []
        now = time.time()

        for block in blocks:
            if (
                not isinstance(block, dict)
                or not block.get("instId")
            ):
                raise ValueError("Missing instrument identity")

            if block["instId"] != inst_id:
                continue

            if not isinstance(block.get("details"), list):
                raise ValueError("Missing liquidation details")

            for row in block["details"]:
                ts = _timestamp(row.get("ts"))

                if ts > now:
                    raise ValueError("Future event timestamp")

                if now - ts > WINDOW:
                    continue

                price = _positive(row.get("bkPx"))
                qty = _positive(row.get("sz"))

                position = str(row.get("posSide", "")).lower()
                side = str(row.get("side", "")).upper()

                if position in ("long", "short"):
                    expected = (
                        "SELL"
                        if position == "long"
                        else "BUY"
                    )

                    if side and side != expected:
                        raise ValueError("Conflicting liquidation side")

                    side = expected

                if side not in ("BUY", "SELL"):
                    raise ValueError("Unknown liquidation side")

                pending.append((side, price, qty, ts))

        ct_val = _contract(inst_id) if pending else None

        for side, price, qty, ts in pending:
            save_liq(
                inst_id,
                "OKX",
                side,
                price,
                qty,
                event_ts=ts,
                quote_value=price * qty * ct_val,
            )

        _status("OKX", symbol, True)
        return True

    except Exception as exc:
        _status("OKX", symbol, False, type(exc).__name__)

        print(
            "[OKX_LIQ_ERROR]",
            inst_id,
            type(exc).__name__,
            flush=True,
        )

        return False


def binance_on_message(ws, message):
    try:
        payload = json.loads(message)

        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]

        events = (
            payload
            if isinstance(payload, list)
            else [payload]
        )

        for event in events:
            if event.get("e") != "forceOrder":
                continue

            row = event.get("o", {})

            if str(row.get("st", event.get("st", "1"))) != "1":
                continue

            # Только финальный снимок исполненного ордера.
            if row.get("X") != "FILLED":
                continue

            save_liq(
                row.get("s"),
                "Binance",
                row.get("S"),
                row.get("ap"),
                row.get("z"),
                event_ts=row.get("T"),
            )

        if ws is not None:
            ws.liq_verified = True

        _status("Binance", "*", True)

    except Exception as exc:
        _status("Binance", "*", False, type(exc).__name__)

        print(
            "[BINANCE_LIQ_ERROR]",
            type(exc).__name__,
            flush=True,
        )


def bybit_on_open(ws):
    ws.send(
        json.dumps(
            {
                "op": "subscribe",
                "args": [
                    "allLiquidation." + symbol
                    for symbol in BYBIT_SYMBOLS
                ],
            }
        )
    )


def bybit_on_message(ws, message):
    try:
        payload = json.loads(message)

        if payload.get("success") is False:
            raise ValueError("Bybit subscription rejected")

        if (
            payload.get("op") == "subscribe"
            and payload.get("success") is True
        ):
            if ws is not None:
                ws.liq_verified = True

            _status("Bybit", "*", True)
            return

        topic = str(payload.get("topic", ""))

        if not topic.startswith("allLiquidation."):
            return

        symbol = topic.split(".", 1)[1]

        if symbol not in BYBIT_SYMBOLS:
            return

        rows = payload.get("data", [])

        if not isinstance(rows, list):
            raise ValueError("Invalid Bybit schema")

        for row in rows:
            if row.get("s") != symbol:
                raise ValueError("Bybit symbol mismatch")

            # Bybit передаёт сторону ликвидированной позиции.
            # Buy = LONG, закрывающий ордер SELL.
            side = {
                "BUY": "SELL",
                "SELL": "BUY",
            }.get(str(row.get("S")).upper())

            save_liq(
                symbol,
                "Bybit",
                side,
                row.get("p"),
                row.get("v"),
                event_ts=row.get("T"),
            )

        if ws is not None:
            ws.liq_verified = True

        _status("Bybit", "*", True)

    except Exception as exc:
        _status("Bybit", "*", False, type(exc).__name__)

        print(
            "[BYBIT_LIQ_ERROR]",
            type(exc).__name__,
            flush=True,
        )


def _run_ws(exchange, url, handler, opener=None):
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_message=handler,
                on_open=opener,
                on_pong=lambda ws, msg: (
                    _status(exchange, "*", True)
                    if getattr(ws, "liq_verified", False)
                    else None
                ),
                on_error=lambda ws, err: _status(
                    exchange, "*", False
                ),
                on_close=lambda ws, code, msg: _status(
                    exchange, "*", False
                ),
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:
            print(
                "[LIQ_WS_ERROR]",
                exchange,
                type(exc).__name__,
                flush=True,
            )

        finally:
            _status(exchange, "*", False)

        time.sleep(5)


def start_binance_liq_ws():
    _run_ws(
        "Binance",
        BINANCE_WS,
        binance_on_message,
    )


def start_bybit_liq_ws():
    _run_ws(
        "Bybit",
        BYBIT_WS,
        bybit_on_message,
        bybit_on_open,
    )


def start_liquidation_streams():
    global _STARTED

    with _LOCK:
        if _STARTED:
            return

        _STARTED = True

    threading.Thread(
        target=start_binance_liq_ws,
        daemon=True,
    ).start()

    threading.Thread(
        target=start_bybit_liq_ws,
        daemon=True,
    ).start()

    print(
        "[LIQ_ENGINE]",
        VERSION,
        "event-time; dedup; partial public data",
        flush=True,
    )
