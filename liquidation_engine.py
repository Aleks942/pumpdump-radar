# liquidation_engine.py
import time
import json
import threading
import websocket
import requests

LIQ_MEMORY = {}

BINANCE_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
OKX_REST = "https://www.okx.com/api/v5/public/liquidation-orders"


def norm_symbol(symbol):
    return symbol.replace("-USDT-SWAP", "USDT").replace("/", "").upper()


def save_liq(symbol, exchange, side, price, qty):
    symbol = norm_symbol(symbol)

    usd_value = float(price) * float(qty)

    now = time.time()

    if symbol not in LIQ_MEMORY:
        LIQ_MEMORY[symbol] = []

    LIQ_MEMORY[symbol].append({
        "ts": now,
        "exchange": exchange,
        "side": side,
        "usd": usd_value
    })

    LIQ_MEMORY[symbol] = [
        x for x in LIQ_MEMORY[symbol]
        if now - x["ts"] <= 300
    ]


def get_liquidation_summary(symbol):
    symbol = norm_symbol(symbol)
    now = time.time()

    rows = [
        x for x in LIQ_MEMORY.get(symbol, [])
        if now - x["ts"] <= 300
    ]

    long_liq = 0
    short_liq = 0

    exchanges = set()

    for x in rows:
        exchanges.add(x["exchange"])

        # SELL liquidation обычно означает вынос LONG
        if x["side"] == "SELL":
            long_liq += x["usd"]

        # BUY liquidation обычно означает вынос SHORT
        elif x["side"] == "BUY":
            short_liq += x["usd"]

    total = long_liq + short_liq

    if total >= 1_000_000:
        power = "Очень сильные ликвидации"
    elif total >= 300_000:
        power = "Сильные ликвидации"
    elif total >= 50_000:
        power = "Средние ликвидации"
    elif total > 0:
        power = "Небольшие ликвидации"
    else:
        power = "Ликвидаций не видно"

    return {
        "long_liq": round(long_liq, 2),
        "short_liq": round(short_liq, 2),
        "total_liq": round(total, 2),
        "power": power,
        "exchanges": ", ".join(sorted(exchanges)) if exchanges else "нет данных"
    }


def binance_on_message(ws, message):
    try:
        data = json.loads(message)

        if isinstance(data, list):
            events = data
        else:
            events = [data]

        for e in events:
            o = e.get("o", {})
            symbol = o.get("s")
            side = o.get("S")
            price = float(o.get("ap") or o.get("p") or 0)
            qty = float(o.get("q") or 0)

            if symbol and price > 0 and qty > 0:
                save_liq(symbol, "Binance", side, price, qty)

    except Exception as e:
        print("[BINANCE_LIQ_ERROR]", e)


def start_binance_liq_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS,
                on_message=binance_on_message
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("[BINANCE_WS_RESTART]", e)
            time.sleep(5)


def bybit_on_open(ws):
    symbols = [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
        "BNBUSDT", "ADAUSDT", "LINKUSDT", "TONUSDT", "AVAXUSDT"
    ]

    args = [f"allLiquidation.{s}" for s in symbols]

    ws.send(json.dumps({
        "op": "subscribe",
        "args": args
    }))


def bybit_on_message(ws, message):
    try:
        data = json.loads(message)

        topic = data.get("topic", "")

        if not topic.startswith("allLiquidation."):
            return

        symbol = topic.replace("allLiquidation.", "")

        rows = data.get("data", [])

        if isinstance(rows, dict):
            rows = [rows]

        for x in rows:
            side = x.get("S") or x.get("side")
            price = float(x.get("p") or x.get("price") or 0)
            qty = float(x.get("v") or x.get("qty") or x.get("size") or 0)

            if symbol and price > 0 and qty > 0:
                save_liq(symbol, "Bybit", side, price, qty)

    except Exception as e:
        print("[BYBIT_LIQ_ERROR]", e)


def start_bybit_liq_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(
                BYBIT_WS,
                on_open=bybit_on_open,
                on_message=bybit_on_message
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("[BYBIT_WS_RESTART]", e)
            time.sleep(5)


def fetch_okx_liquidations(inst_id):
    try:
        params = {
            "instType": "SWAP",
            "uly": inst_id.replace("-SWAP", ""),
            "state": "filled",
            "limit": "20"
        }

        r = requests.get(OKX_REST, params=params, timeout=10)
        data = r.json()

        if data.get("code") != "0":
            return

        rows = data.get("data", [])

        for block in rows:
            details = block.get("details", [])

            for x in details:
                side = x.get("side", "").upper()
                price = float(x.get("bkPx") or x.get("price") or 0)
                qty = float(x.get("sz") or 0)

                if price > 0 and qty > 0:
                    save_liq(inst_id, "OKX", side, price, qty)

    except Exception as e:
        print("[OKX_LIQ_ERROR]", inst_id, e)


def start_liquidation_streams():
    threading.Thread(target=start_binance_liq_ws, daemon=True).start()
    threading.Thread(target=start_bybit_liq_ws, daemon=True).start()
    print("[LIQ_ENGINE] Binance + Bybit liquidation streams started")
