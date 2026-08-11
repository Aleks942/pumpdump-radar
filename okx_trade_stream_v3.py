# okx_trade_stream_v3.py
# PumpDump Radar V3
#
# Живой поток сделок OKX:
# - SPOT trades
# - SWAP trades
# - реальные timestamps
# - правильный notional для SWAP через ctVal
#
# Здесь НЕТ:
# - LONG / SHORT
# - score
# - Chief
# - торговых решений

import json
import time
import threading
import requests
import websocket

from trade_flow_collector_v3 import save_trade, mark_stream_connected, mark_stream_activity, mark_stream_disconnected


OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_REST_URL = "https://www.okx.com"

REQUEST_TIMEOUT = 10

# Пока тестируем на BTC.
SPOT_SYMBOL = "BTC-USDT"
SWAP_SYMBOL = "BTC-USDT-SWAP"


# ============================================================
# INSTRUMENT CACHE
# ============================================================

CONTRACT_CACHE = {}
CACHE_LOCK = threading.RLock()


def get_swap_contract_info(inst_id):
    """
    Получает параметры SWAP инструмента.

    Для USDT linear SWAP:
    notional ~= price * size_contracts * ctVal

    ctVal берём из публичного instruments API.
    """

    with CACHE_LOCK:
        cached = CONTRACT_CACHE.get(inst_id)

    if cached:
        return cached

    try:
        response = requests.get(
            OKX_REST_URL + "/api/v5/public/instruments",
            params={
                "instType": "SWAP",
                "instId": inst_id,
            },
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()

        payload = response.json()

        if payload.get("code") != "0":
            print(
                "[V3_CONTRACT_API_ERROR]",
                payload.get("code"),
                payload.get("msg"),
                flush=True,
            )
            return None

        data = payload.get("data", [])

        if not data:
            print(
                "[V3_CONTRACT_NOT_FOUND]",
                inst_id,
                flush=True,
            )
            return None

        row = data[0]

        ct_val = float(
            row.get("ctVal") or 0
        )

        ct_val_ccy = row.get(
            "ctValCcy"
        )

        settle_ccy = row.get(
            "settleCcy"
        )

        info = {
            "inst_id": inst_id,
            "ct_val": ct_val,
            "ct_val_ccy": ct_val_ccy,
            "settle_ccy": settle_ccy,
            "ct_type": row.get("ctType"),
        }

        with CACHE_LOCK:
            CONTRACT_CACHE[
                inst_id
            ] = info

        print(
            "[V3_CONTRACT]",
            inst_id,
            info,
            flush=True,
        )

        return info

    except Exception as exc:
        print(
            "[V3_CONTRACT_ERROR]",
            inst_id,
            repr(exc),
            flush=True,
        )
        return None


def calc_swap_quote_value(
    inst_id,
    price,
    size_contracts,
):
    """
    Рассчитывает USDT notional SWAP сделки.

    Для linear USDT swap:
        quote ≈ price * contracts * ctVal

    Если параметры инструмента неизвестны,
    возвращаем None, а не придумываем объём.
    """

    info = get_swap_contract_info(
        inst_id
    )

    if not info:
        return None

    try:
        price = float(price)
        size_contracts = float(
            size_contracts
        )
        ct_val = float(
            info.get("ct_val") or 0
        )

    except (TypeError, ValueError):
        return None

    if (
        price <= 0
        or size_contracts <= 0
        or ct_val <= 0
    ):
        return None

    return (
        price
        * size_contracts
        * ct_val
    )


# ============================================================
# MESSAGE HANDLER
# ============================================================

def handle_trade(
    inst_id,
    trade,
):
    """
    Обрабатывает одну сделку из OKX trades channel.
    """

    try:
        side = str(
            trade.get("side") or ""
        ).upper()

        price = float(
            trade.get("px") or 0
        )

        size = float(
            trade.get("sz") or 0
        )

        ts = float(
            trade.get("ts") or 0
        )

    except (TypeError, ValueError):
        return

    if (
        side not in ("BUY", "SELL")
        or price <= 0
        or size <= 0
        or ts <= 0
    ):
        return

    # SPOT
    if inst_id == SPOT_SYMBOL:

        quote_value = (
            price * size
        )

        save_trade(
            symbol=inst_id,
            market="spot",
            side=side,
            price=price,
            size=size,
            event_ts=ts,
            quote_value=quote_value,
        )

        return

    # SWAP
    if inst_id == SWAP_SYMBOL:

        quote_value = (
            calc_swap_quote_value(
                inst_id,
                price,
                size,
            )
        )

        if quote_value is None:
            return

        save_trade(
            symbol=inst_id,
            market="swap",
            side=side,
            price=price,
            size=size,
            event_ts=ts,
            quote_value=quote_value,
        )


def on_message(ws, message):

    mark_stream_activity("spot")
    mark_stream_activity("swap")
    
    try:
        payload = json.loads(
            message
        )

    except Exception:
        return

    # subscribe confirmation / service events
    if "event" in payload:
        print(
            "[V3_WS_EVENT]",
            payload,
            flush=True,
        )
        return

    arg = payload.get(
        "arg",
        {},
    )

    if arg.get("channel") != "trades":
        return

    inst_id = arg.get(
        "instId"
    )

    if not inst_id:
        return

    data = payload.get(
        "data",
        [],
    )

    for trade in data:
        handle_trade(
            inst_id,
            trade,
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def on_open(ws):
    print(
        "[V3_WS_OPEN]",
        flush=True,
    )

    subscription = {
        "op": "subscribe",
        "args": [
            {
                "channel": "trades",
                "instId": SPOT_SYMBOL,
            },
            {
                "channel": "trades",
                "instId": SWAP_SYMBOL,
            },
        ],
    }

    ws.send(
        json.dumps(
            subscription
        )
    )

    print(
        "[V3_WS_SUBSCRIBE]",
        SPOT_SYMBOL,
        SWAP_SYMBOL,
        flush=True,
    )


def on_error(ws, error):
    print(
        "[V3_WS_ERROR]",
        repr(error),
        flush=True,
    )


def on_close(
    ws,
    close_status_code,
    close_msg,
):
    print(
        "[V3_WS_CLOSED]",
        close_status_code,
        close_msg,
        flush=True,
    )


# ============================================================
# RUNNER
# ============================================================

def run_stream_forever():
    """
    Перезапускает WebSocket после обрыва.
    """

    while True:

        try:
            print(
                "[V3_WS_CONNECTING]",
                OKX_WS_URL,
                flush=True,
            )

            ws = websocket.WebSocketApp(
                OKX_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except KeyboardInterrupt:
            print(
                "[V3_WS_STOPPED]",
                flush=True,
            )
            break

        except Exception as exc:
            print(
                "[V3_WS_FATAL]",
                repr(exc),
                flush=True,
            )

        print(
            "[V3_WS_RECONNECT_IN_5S]",
            flush=True,
        )

        time.sleep(5)


if __name__ == "__main__":
    run_stream_forever()
