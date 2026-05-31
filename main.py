import os
import time
import requests

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PUMP_THRESHOLD_5M = 3
DUMP_THRESHOLD_5M = -3

MIN_VOLUME_24H = 10000000
ALERT_COOLDOWN = 7200

symbol_states = {}
signal_first_seen = {}


def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        print("[TG STATUS]", r.status_code)
    except Exception as e:
        print("[TG ERROR]", e)


def get_market_tickers():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"

    try:
        r = requests.get(url, timeout=20)
        print("[OKX TICKERS STATUS]", r.status_code)

        data = r.json()

        if data.get("code") != "0":
            print("[OKX TICKERS ERROR]", data)
            return []

        return data["data"]

    except Exception as e:
        print("[OKX TICKERS EXCEPTION]", e)
        return []


def get_5m_change(symbol):
    url = "https://www.okx.com/api/v5/market/candles"

    params = {
        "instId": symbol,
        "bar": "5m",
        "limit": "2"
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        if data.get("code") != "0":
            print("[OKX CANDLES ERROR]", symbol, data)
            return None

        candles = data.get("data", [])

        if len(candles) < 2:
            return None

        last_closed = candles[1]

        open_price = float(last_closed[1])
        close_price = float(last_closed[4])

        if open_price == 0:
            return None

        change_5m = ((close_price - open_price) / open_price) * 100

        return change_5m

    except Exception as e:
        print("[OKX CANDLES EXCEPTION]", symbol, e)
        return None


def can_alert(symbol):
    now = time.time()
    last_time = signal_first_seen.get(symbol)

    if last_time is None:
        signal_first_seen[symbol] = now

    state = symbol_states.get(symbol)

    if state is None:
        return True

    last_alert = state.get("last_alert", 0)

    if now - last_alert > ALERT_COOLDOWN:
        return True

    return False


def analyze(ticker):
    try:
        symbol = ticker["instId"]

        if "USDT" not in symbol:
            return None

        price = float(ticker["last"])

        if price < 0.01:
            return None

        volume_24h = float(ticker["volCcy24h"])

        if volume_24h < MIN_VOLUME_24H:
            return None

    except Exception as e:
        print("[ANALYZE TICKER ERROR]", e)
        return None

    change_5m = get_5m_change(symbol)

    if change_5m is None:
        return None

    move_type = None

    if change_5m >= PUMP_THRESHOLD_5M:
        move_type = "PUMP"

    elif change_5m <= DUMP_THRESHOLD_5M:
        move_type = "DUMP"

    else:
        return None

    state = symbol_states.get(symbol)

    if state:
        old_change = state.get("max_change", change_5m)

        if move_type == "PUMP" and change_5m < old_change + 2:
            return None

        if move_type == "DUMP" and change_5m > old_change - 2:
            return None

    now = time.time()

    symbol_states[symbol] = {
        "type": move_type,
        "max_change": change_5m,
        "last_alert": now
    }

    if symbol not in signal_first_seen:
        signal_first_seen[symbol] = now

    return {
        "symbol": symbol,
        "type": move_type,
        "change_5m": change_5m,
        "price": price,
        "volume": volume_24h
    }


def build_message(signal):
    emoji = "🚀" if signal["type"] == "PUMP" else "🔻"

    first_seen = signal_first_seen.get(signal["symbol"], time.time())
    active_minutes = int((time.time() - first_seen) / 60)

    return f"""
{emoji} PumpDump Radar V2

Монета:
{signal["symbol"]}

Тип:
{signal["type"]}

Движение за 5 минут:
{signal["change_5m"]:.2f}%

Цена:
{signal["price"]}

Объём 24ч:
{signal["volume"]:,.0f}

⏱ Импульс активен:
{active_minutes} мин

Логика:
бот считает НЕ от утра, а по последней закрытой 5m свече
"""


print("🚀 PumpDump Radar V2 started")

send_telegram("🚀 PumpDump Radar V2 ONLINE")

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

        print("[SIGNAL]", signal["symbol"], signal["type"], signal["change_5m"])

    time.sleep(60)
