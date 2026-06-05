# stats_engine.py
import json
import os
import time

STATS_FILE = "signals_stats.json"


def load_stats():
    if not os.path.exists(STATS_FILE):
        return []

    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_stats(data):
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def register_signal(signal):
    data = load_stats()

    item = {
        "ts": time.time(),
        "symbol": signal.get("symbol"),
        "type": signal.get("type"),
        "window": signal.get("window"),
        "change": signal.get("change"),
        "price": signal.get("price"),
        "setup": signal.get("setup", "НЕТ"),
        "oi_change": signal.get("oi_change"),
        "result_15m": None,
        "result_30m": None
    }

    data.append(item)

    # храним только последние 300 сигналов
    data = data[-300:]

    save_stats(data)

def update_signal_result(symbol, signal_time, current_price):
    data = load_stats()

    changed = False

    for item in data:

        if item["symbol"] != symbol:
            continue

        if item["result_15m"] is not None:
            continue

        age = time.time() - item["ts"]

        if age < 900:
            continue

        entry_price = item["price"]

        if item["type"] == "PUMP":
            result = (
                (current_price - entry_price)
                / entry_price
            ) * 100

        else:
            result = (
                (entry_price - current_price)
                / entry_price
            ) * 100

        item["result_15m"] = round(result, 2)

        changed = True

    if changed:
        save_stats(data)
