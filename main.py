import os
import time
import requests

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PUMP_THRESHOLD = 0.3
DUMP_THRESHOLD = -0.3

MIN_VOLUME = 500000

ALERT_COOLDOWN = 1800

last_alerts = {}


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

        change = float(ticker["change24h"]) * 100

        volume = float(ticker["volCcy24h"])

        price = float(ticker["last"])

    except Exception as e:

        print("[ANALYZE ERROR]", e)

        return None

    if volume < MIN_VOLUME:
        return None

    if change >= PUMP_THRESHOLD:

        return {
            "symbol": symbol,
            "type": "PUMP",
            "change": change,
            "price": price,
            "volume": volume
        }

    if change <= DUMP_THRESHOLD:

        return {
            "symbol": symbol,
            "type": "DUMP",
            "change": change,
            "price": price,
            "volume": volume
        }

    return None


def build_message(signal):

    emoji = "🚀" if signal["type"] == "PUMP" else "🔻"

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
