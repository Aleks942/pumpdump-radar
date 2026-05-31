import os
import time
import requests

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")


def send_telegram(text):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }

    try:
        requests.post(url, json=payload, timeout=10)
        print("[TG] message sent")

    except Exception as e:
        print("[TG ERROR]", e)


print("🚀 PumpDump Radar started")

send_telegram("🚀 PumpDump Radar успешно запущен!")


while True:

    print("working...")

    time.sleep(60)
