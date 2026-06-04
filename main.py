import os
import time
import requests
import random
from datetime import datetime, UTC
from money_flow_engine import analyze_new_money
from liquidation_engine import (
    start_liquidation_streams,
    fetch_okx_liquidations,
    get_liquidation_summary
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PUMP_THRESHOLD_5M = float(os.getenv("PUMP_THRESHOLD_5M", 5))
DUMP_THRESHOLD_5M = float(os.getenv("DUMP_THRESHOLD_5M", -5))

PUMP_THRESHOLD_20M = float(os.getenv("PUMP_THRESHOLD_20M", 7))
DUMP_THRESHOLD_20M = float(os.getenv("DUMP_THRESHOLD_20M", -7))

PUMP_THRESHOLD_30M = float(os.getenv("PUMP_THRESHOLD_30M", 7))
DUMP_THRESHOLD_30M = float(os.getenv("DUMP_THRESHOLD_30M", -7))

MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", 10000000))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", 1800))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 120))
SCAN_SLEEP = int(os.getenv("SCAN_SLEEP", 60))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", 1000))

symbol_states = {}
signal_24h_count = {}
oi_memory = {}
rotation_index = 0

TIME_WINDOWS = {
    "5m": {"bar": "5m", "candles": 2, "pump": PUMP_THRESHOLD_5M, "dump": DUMP_THRESHOLD_5M},
    "20m": {"bar": "5m", "candles": 5, "pump": PUMP_THRESHOLD_20M, "dump": DUMP_THRESHOLD_20M},
    "30m": {"bar": "5m", "candles": 7, "pump": PUMP_THRESHOLD_30M, "dump": DUMP_THRESHOLD_30M},
}


def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
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
        print("[OKX STATUS]", r.status_code)

        data = r.json()

        if data.get("code") != "0":
            print("[OKX ERROR]", data)
            return []

        tickers = data.get("data", [])

        tickers = [
            t for t in tickers
            if "USDT-SWAP" in t.get("instId", "")
        ]

        random.shuffle(tickers)

        return tickers[:MAX_SYMBOLS]

    except Exception as e:
        print("[OKX EXCEPTION]", e)
        return []


def get_rotation_chunk(tickers):
    global rotation_index

    total = len(tickers)

    if total == 0:
        return []

    start = rotation_index * CHUNK_SIZE
    end = start + CHUNK_SIZE

    current_chunk = tickers[start:end]

    if not current_chunk:
        rotation_index = 0
        start = 0
        end = CHUNK_SIZE
        current_chunk = tickers[start:end]

    if end >= total:
        rotation_index = 0
    else:
        rotation_index += 1

    print("[ROTATION]", start, "-", min(end, total), "of", total)

    return current_chunk


def get_funding_rate(raw_symbol):
    url = "https://www.okx.com/api/v5/public/funding-rate"

    params = {
        "instId": raw_symbol
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()

        if data.get("code") != "0":
            return None

        rows = data.get("data", [])

        if not rows:
            return None

        return float(rows[0].get("fundingRate", 0)) * 100

    except Exception as e:
        print("[FUNDING EXCEPTION]", raw_symbol, e)
        return None


def get_open_interest(raw_symbol):
    url = "https://www.okx.com/api/v5/public/open-interest"

    params = {
        "instId": raw_symbol
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()

        if data.get("code") != "0":
            return None

        rows = data.get("data", [])

        if not rows:
            return None

        return float(rows[0].get("oi", 0))

    except Exception as e:
        print("[OI EXCEPTION]", raw_symbol, e)
        return None


def get_window_move(raw_symbol, bar, candles_count):
    url = "https://www.okx.com/api/v5/market/candles"

    params = {
        "instId": raw_symbol,
        "bar": bar,
        "limit": str(candles_count)
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        if data.get("code") != "0":
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

        change = ((end_price - start_price) / start_price) * 100

        return {
            "start_price": start_price,
            "end_price": end_price,
            "change": change
        }

    except Exception as e:
        print("[CANDLES EXCEPTION]", raw_symbol, e)
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

    return len(signal_24h_count.get(symbol, []))


def can_send(symbol, move_type, window, change):
    now = time.time()
    key = f"{symbol}_{move_type}_{window}"

    state = symbol_states.get(key)

    if state is None:
        symbol_states[key] = {
            "last_alert": now,
            "max_change": change
        }
        return True

    last_alert = state.get("last_alert", 0)
    old_change = state.get("max_change", change)

    if now - last_alert < ALERT_COOLDOWN:
        if move_type == "PUMP" and change < old_change + 0.5:
            return False

        if move_type == "DUMP" and change > old_change - 0.5:
            return False

    symbol_states[key] = {
        "last_alert": now,
        "max_change": change
    }

    return True


def classify_flow(move_type, funding, oi_change):
    if oi_change is None:
        return "OI пока нет данных"

    if move_type == "PUMP":
        if oi_change > 2:
            return "Цена растёт + OI растёт: новые деньги заходят в рост"
        if oi_change < -2:
            return "Цена растёт + OI падает: возможный short squeeze"

    if move_type == "DUMP":
        if oi_change > 2:
            return "Цена падает + OI растёт: новые шорты давят цену"
        if oi_change < -2:
            return "Цена падает + OI падает: позиции закрываются, возможная капитуляция"

    if funding is not None:
        if move_type == "PUMP" and funding < -0.01:
            return "Памп против отрицательного funding: шортистов могут выносить"
        if move_type == "DUMP" and funding > 0.01:
            return "Дамп против положительного funding: лонгистов могут выносить"

    return "Движение есть, но сильного OI/funding подтверждения пока нет"


def analyze(ticker):
    try:
        raw_symbol = ticker["instId"]
        symbol = raw_symbol.replace("-USDT-SWAP", "USDT")

        price = float(ticker["last"])

        if price < 0.01:
            return None

        volume_24h = float(ticker["volCcy24h"])

        if volume_24h < MIN_VOLUME_24H:
            return None

    except Exception as e:
        print("[ANALYZE TICKER ERROR]", e)
        return None

    best_signal = None

    for window_name, cfg in TIME_WINDOWS.items():
        move = get_window_move(raw_symbol, cfg["bar"], cfg["candles"])

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

        if not can_send(symbol, move_type, window_name, change):
            continue

        funding = get_funding_rate(raw_symbol)
        oi = get_open_interest(raw_symbol)

        oi_change = None
        old_oi = oi_memory.get(symbol)

        if oi is not None and old_oi is not None and old_oi > 0:
            oi_change = ((oi - old_oi) / old_oi) * 100

        if oi is not None:
            oi_memory[symbol] = oi

        signal_count = add_signal_count(symbol)
        flow_comment = classify_flow(move_type, funding, oi_change)
        
        money = analyze_new_money(raw_symbol)
        fetch_okx_liquidations(raw_symbol)
        liquidations = get_liquidation_summary(raw_symbol)
        
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
            "oi": oi,
            "oi_change": oi_change,
            "flow_comment": flow_comment,
            "signal_24h": signal_count,

            "money": money,
            "liquidations": liquidations,
        }

        break

    return best_signal

STATE_MAP = {
    "STRONG_NEW_MONEY": "Заходят крупные деньги",
    "BUILDING_MONEY": "Деньги постепенно заходят",
    "WEAK_FLOW": "Слабый приток денег",
    "NO_CLEAR_MONEY": "Притока денег не видно"
}

PRESSURE_MAP = {
    "STRONG_BUY_PRESSURE": "Покупатели очень активны",
    "BUY_PRESSURE": "Покупатели сильнее",
    "BALANCED": "Покупатели и продавцы равны",
    "SELL_PRESSURE": "Продавцы сильнее",
    "STRONG_SELL_PRESSURE": "Продавцы очень активны"
}

def liquidation_strength(long_liq, short_liq):
    total = long_liq + short_liq

    if total >= 1000000:
        return "🔴 Очень сильные"

    elif total >= 250000:
        return "🟠 Сильные"

    elif total >= 50000:
        return "🟡 Средние"

    else:
        return "⚪ Слабые"


def signal_quality(money, long_liq, short_liq):
    score = 0

    total = long_liq + short_liq

    if total >= 50000:
        score += 3

    if total >= 250000:
        score += 2

    if money:
        money_score = money.get("money_score", 0)

        if money_score >= 4:
            score += 3

        elif money_score >= 2:
            score += 1

    return min(score, 10)
def build_message(signal):

    emoji = "🚀" if signal["type"] == "PUMP" else "🔻"
    side_text = "ПАМП" if signal["type"] == "PUMP" else "ДАМП"

    money = signal.get("money")
    liquidations = signal.get("liquidations")

    money_state = "Нет данных"
    pressure_state = "Нет данных"

    setup = "НЕТ"

    if money:

        money_state = STATE_MAP.get(
            money.get("money_state"),
            money.get("money_state")
        )

        pressure_state = PRESSURE_MAP.get(
            money.get("pressure"),
            money.get("pressure")
        )

        pressure = money.get("pressure", "")
        score = money.get("money_score", 0)

        if (
            signal["type"] == "DUMP"
            and "BUY" in pressure
            and score >= 3
        ):
            setup = "ЛОНГ ⭐⭐⭐⭐"

        elif (
            signal["type"] == "PUMP"
            and "SELL" in pressure
            and score >= 2
        ):
            setup = "ШОРТ ⭐⭐⭐⭐"

    long_liq = 0
    short_liq = 0

    if liquidations:
        long_liq = liquidations.get("long_liq", 0)
        short_liq = liquidations.get("short_liq", 0)

    liq_strength = liquidation_strength(long_liq, short_liq)
    quality = signal_quality(money, long_liq, short_liq)

    return f"""
{emoji} <b>{signal["symbol"]}</b> | <b>{side_text}</b>

⏱ Период: {signal["window"]}

📈 Движение: {signal["change"]:.2f}%

🎯 СЕТАП: {setup}

⭐ Качество: {quality}/10

💰 Деньги:
{money_state}

⚖️ Давление:
{pressure_state}

💥 Ликвидации:
{liq_strength}
L: ${long_liq:,.0f} | S: ${short_liq:,.0f}

🕒 {datetime.now(UTC).strftime("%H:%M")}
"""

print("🚀 PumpDump Radar V2 started")
send_telegram("🚀 PumpDump Radar V2 ONLINE")

start_liquidation_streams()

while True:
    print("[SCAN] scanning market...")

    tickers = get_market_tickers()
    print(f"[TICKERS] {len(tickers)}")

    current_chunk = get_rotation_chunk(tickers)
    print(f"[CHUNK] {len(current_chunk)}")

    for ticker in current_chunk:
        signal = analyze(ticker)

        if not signal:
            continue

        send_telegram(build_message(signal))

        print(
            "[SIGNAL]",
            signal["window"],
            signal["symbol"],
            signal["type"],
            round(signal["change"], 2)
        )

    time.sleep(SCAN_SLEEP)
