import os
import time
import requests
from datetime import datetime

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PUMP_THRESHOLD_5M = 5
DUMP_THRESHOLD_5M = -5

PUMP_THRESHOLD_20M = 7
DUMP_THRESHOLD_20M = -7

PUMP_THRESHOLD_30M = 7
DUMP_THRESHOLD_30M = -7

MIN_VOLUME_24H = 10000000

ALERT_COOLDOWN = 7200

TIME_WINDOWS = {
    "5m": {
        "bar": "5m",
        "candles": 2,
        "pump": PUMP_THRESHOLD_5M,
        "dump": DUMP_THRESHOLD_5M
    },

    "20m": {
        "bar": "5m",
        "candles": 5,
        "pump": PUMP_THRESHOLD_20M,
        "dump": DUMP_THRESHOLD_20M
    },

    "30m": {
        "bar": "5m",
        "candles": 7,
        "pump": PUMP_THRESHOLD_30M,
        "dump": DUMP_THRESHOLD_30M
    }
}

symbol_states = {}
signal_first_seen = {}
signal_24h_count = {}


def send_telegram(text):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }

    try:

        r = requests.post(
            url,
            json=payload,
            timeout=10
        )

        print("[TG STATUS]", r.status_code)

    except Exception as e:

        print("[TG ERROR]", e)


def get_market_tickers():

    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"

    try:

        r = requests.get(url, timeout=20)

        print("[OKX STATUS]", r.status_code)

        data = r.json()

        if data.get("code") != "0":

            print("[OKX ERROR]", data)

            return []

        return data["data"]

    except Exception as e:

        print("[OKX EXCEPTION]", e)

        return []

def get_funding_rate(raw_symbol):

    url = "https://www.okx.com/api/v5/public/funding-rate"

    params = {
        "instId": raw_symbol
    }

    try:

        r = requests.get(
            url,
            params=params,
            timeout=15
        )

        data = r.json()

        if data.get("code") != "0":
            print("[FUNDING ERROR]", raw_symbol, data)
            return None

        rows = data.get("data", [])

        if not rows:
            return None

        funding = float(rows[0].get("fundingRate", 0)) * 100

        return funding

    except Exception as e:

        print("[FUNDING EXCEPTION]", raw_symbol, e)

        return None


def get_window_move(symbol, bar, candles_count):

    url = "https://www.okx.com/api/v5/market/candles"

    params = {
        "instId": symbol,
        "bar": bar,
        "limit": str(candles_count)
    }

    try:

        r = requests.get(
            url,
            params=params,
            timeout=20
        )

        data = r.json()

        if data.get("code") != "0":

            print("[CANDLES ERROR]", symbol)

            return None

        candles = data.get("data", [])

        if len(candles) < candles_count:
            return None

        newest = candles[0]

        oldest = candles[-1]

        start_price = float(oldest[1])

        end_price = float(newest[4])

        if start_price == 0:
            return None

        change = (
            (end_price - start_price)
            / start_price
        ) * 100

        return {
            "start_price": start_price,
            "end_price": end_price,
            "change": change
        }

    except Exception as e:

        print("[CANDLES EXCEPTION]", symbol, e)

        return None


def clean_old_signal_counts():

    now = time.time()

    for symbol in list(signal_24h_count.keys()):

        signal_24h_count[symbol] = [
            t for t in signal_24h_count[symbol]
            if now - t < 86400
        ]

        if not signal_24h_count[symbol]:

            del signal_24h_count[symbol]


def add_signal_count(symbol):

    now = time.time()

    if symbol not in signal_24h_count:

        signal_24h_count[symbol] = []

    signal_24h_count[symbol].append(now)

    clean_old_signal_counts()

    return len(signal_24h_count[symbol])


def can_send(symbol, move_type, window, change):

    now = time.time()

    key = f"{symbol}_{move_type}_{window}"

    state = symbol_states.get(key)

    if state is None:

        symbol_states[key] = {
            "last_alert": now,
            "max_change": change
        }

        signal_first_seen[key] = now

        return True

    last_alert = state.get("last_alert", 0)

    old_change = state.get("max_change", change)

    if now - last_alert < ALERT_COOLDOWN:

        if move_type == "PUMP":

            if change < old_change + 3:
                return False

        if move_type == "DUMP":

            if change > old_change - 3:
                return False

    symbol_states[key] = {
        "last_alert": now,
        "max_change": change
    }

    return True


def analyze(ticker):

    try:

        raw_symbol = ticker["instId"]

        symbol = raw_symbol.replace(
            "-USDT-SWAP",
            "USDT"
        )

        if "USDT" not in symbol:
            return None

        price = float(ticker["last"])

        if price < 0.01:
            return None

        volume_24h = float(
            ticker["volCcy24h"]
        )

        if volume_24h < MIN_VOLUME_24H:
            return None

    funding = get_funding_rate(raw_symbol)

    except Exception as e:

        print("[ANALYZE ERROR]", e)

        return None

    best_signal = None

    for window_name, cfg in TIME_WINDOWS.items():

        move = get_window_move(
            raw_symbol,
            cfg["bar"],
            cfg["candles"]
        )

        if move is None:
            continue

        change = move["change"]

        move_type = None

        if change >= cfg["pump"]:

            move_type = "PUMP"

        elif change <= cfg["dump"]:

            move_type = "DUMP"

        else:
            continue

        if not can_send(
            symbol,
            move_type,
            window_name,
            change
        ):
            continue

        signal_count = add_signal_count(symbol)

        best_signal = {
            "symbol": symbol,
            "type": move_type,
            "window": window_name,
            "change": change,
            "start_price": move["start_price"],
            "end_price": move["end_price"],
            "price": price,
            "volume": volume_24h,
            "funding": funding,
            "signal_24h": signal_count
        }

        break

    return best_signal


def build_message(signal):

    emoji = "🚀" if signal["type"] == "PUMP" else "🔻"

    return f"""
{emoji} PumpDump Radar

Биржа:
OKX

Период:
{signal["window"]}

Монета:
{signal["symbol"]}

Тип:
{signal["type"]}

Движение:
{signal["change"]:.2f}%

Цена:
{signal["start_price"]} → {signal["end_price"]}

Объём 24ч:
{signal["volume"]:,.0f}


Funding:
{signal["funding"]}

Signal 24h:
{signal["signal_24h"]}

Время:
{datetime.utcnow().strftime("%H:%M UTC")}
"""


print("🚀 PumpDump Radar started")

send_telegram(
    "🚀 PumpDump Radar ONLINE"
)

while True:

    print("[SCAN] scanning market...")

    tickers = get_market_tickers()

    print(f"[TICKERS] {len(tickers)}")

    for ticker in tickers:

        signal = analyze(ticker)

        if not signal:
            continue

        msg = build_message(signal)

        send_telegram(msg)

        print(
            "[SIGNAL]",
            signal["window"],
            signal["symbol"],
            signal["type"],
            signal["change"]
        )

    time.sleep(60)
