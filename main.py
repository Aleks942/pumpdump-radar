import os
import time
import requests

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PUMP_THRESHOLD = 5
DUMP_THRESHOLD = -5

MIN_VOLUME = 10000000

ALERT_COOLDOWN = 7200

last_alerts = {}

signal_first_seen = {}

symbol_states = {}


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

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:

        r = requests.get(
            url,
            headers=headers,
            timeout=20
        )

        print("[OKX STATUS]", r.status_code)

        data = r.json()

        if data.get("code") != "0":

            print("[OKX ERROR]", data)

            return []

        return data["data"]

    except Exception as e:

        print("[OKX EXCEPTION]", e)

        return []


def can_alert(symbol):

    now = time.time()

    if symbol not in signal_first_seen:
        signal_first_seen[symbol] = now

    last = last_alerts.get(symbol, 0)

    if now - last > ALERT_COOLDOWN:

        last_alerts[symbol] = now

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

        open_price = float(ticker["sodUtc0"])

        if open_price == 0:
            return None

        change = ((price - open_price) / open_price) * 100

        volume = float(ticker["volCcy24h"])

    except Exception as e:

        print("[ANALYZE ERROR]", e)

        return None

    if volume < MIN_VOLUME:
        return None

    move_type = None

    if change >= PUMP_THRESHOLD:
        move_type = "PUMP"

    elif change <= DUMP_THRESHOLD:
        move_type = "DUMP"

    else:

        if symbol in symbol_states:
            del symbol_states[symbol]

        return None

    existing = symbol_states.get(symbol)

    if existing:

        old_change = existing["max_change"]

        if move_type == "PUMP":

            if change < old_change + 3:
                return None

        if move_type == "DUMP":

            if change > old_change - 3:
                return None

    else:

        signal_first_seen[symbol] = time.time()

    symbol_states[symbol] = {
        "type": move_type,
        "max_change": change
    }

    return {
        "symbol": symbol,
        "type": move_type,
        "change": change,
        "price": price,
        "volume": volume
    }


def build_message(signal):

    emoji = "🚀" if signal["type"] == "PUMP" else "🔻"

    first_seen = signal_first_seen.get(signal["symbol"], time.time())

    active_minutes = int((time.time() - first_seen) / 60)

    return f"""
{emoji} PumpDump Radar

Монета:
{signal["symbol"]}

Тип:
{signal["type"]}

Изменение:
{signal["change"]:.2f}%

Цена:
{signal["price"]}

Объём:
{signal["volume"]:,.0f}

⏱ Активен:
{active_minutes} мин
"""


print("🚀 PumpDump Radar started")

send_telegram("🚀 PumpDump Radar ONLINE")


while True:

    print("[SCAN] scanning market...")

    tickers = get_market_tickers()

    print(f"[TICKERS] {len(tickers)}")

    for ticker in tickers:

        signal = analyze(ticker)

        if not signal:
            continue

        symbol = signal["symbol"]

        if not can_alert(symbol):
            continue

        msg = build_message(signal)

        send_telegram(msg)

        print("[SIGNAL]", symbol)

    time.sleep(60)
