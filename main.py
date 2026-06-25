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

from stats_engine import (
    register_signal,
    update_signal_result
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")


PUMP_THRESHOLD_20M = float(os.getenv("PUMP_THRESHOLD_20M", 6))
DUMP_THRESHOLD_20M = float(os.getenv("DUMP_THRESHOLD_20M", -6))

PUMP_THRESHOLD_40M = float(os.getenv("PUMP_THRESHOLD_40M", 8))
DUMP_THRESHOLD_40M = float(os.getenv("DUMP_THRESHOLD_40M", -8))



MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", 10000000))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", 3600))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 347))
SCAN_SLEEP = int(os.getenv("SCAN_SLEEP", 60))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", 1000))

symbol_states = {}
signal_24h_count = {}
OI_HISTORY = {}
OI_CHANGE_HISTORY = {}
print("[BOOT] OI_HISTORY CREATED")
rotation_index = 0

TIME_WINDOWS = {

    "5m": {
        "bar": "1m",
        "candles": 5,
        "pump": float(os.getenv("PUMP_THRESHOLD_5M", 5)),
        "dump": -float(os.getenv("DUMP_THRESHOLD_5M", 5)),
    },

    "10m": {
        "bar": "1m",
        "candles": 10,
        "pump": float(os.getenv("PUMP_THRESHOLD_10M", 6)),
        "dump": -float(os.getenv("DUMP_THRESHOLD_10M", 6)),
    },

    "20m": {
        "bar": "1m",
        "candles": 20,
        "pump": float(os.getenv("PUMP_THRESHOLD_20M", 7)),
        "dump": -float(os.getenv("DUMP_THRESHOLD_20M", 7)),
    },

    "30m": {
        "bar": "1m",
        "candles": 30,
        "pump": float(os.getenv("PUMP_THRESHOLD_30M", 8)),
        "dump": -float(os.getenv("DUMP_THRESHOLD_30M", 8)),
    }
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
        
def test_binance():
    try:

        print("[BINANCE TEST START]")

        r = requests.get(
            "https://fapi.binance.com/fapi/v1/ticker/24hr",
            timeout=20
        )

        print("[BINANCE STATUS]", r.status_code)

    except Exception as e:
        print("[BINANCE ERROR]", e)

def test_bybit():
    try:

        print("[BYBIT TEST START]")

        r = requests.get(
            "https://api.bybit.com/v5/market/tickers?category=linear",
            timeout=20
        )

        print("[BYBIT STATUS]", r.status_code)

    except Exception as e:
        print("[BYBIT ERROR]", e)

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

        # random.shuffle(tickers)

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
    key = f"{symbol}_{move_type}"

    state = symbol_states.get(key)

    print(
        "[COOLDOWN]",
        key,
        state
    )

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

def classify_oi_flow(move_type, oi_change):

    if oi_change is None:
        return "Нет данных"

    if move_type == "PUMP":

        if oi_change >= 5:
            return "🔥 НОВЫЕ ЛОНГИ: в рынок заходят реальные деньги"
    
        if oi_change >= 2:
            return "🟡 Умеренный приток новых денег"
    
        if oi_change <= -5:
            return "🔥 SHORT SQUEEZE: шортистов массово выносит"
    
        if oi_change <= -2:
            return "🟡 Возможный short squeeze"

    if move_type == "DUMP":

        if oi_change >= 5:
            return "🔥 НОВЫЕ ШОРТЫ: продавцы активно давят цену"
    
        if oi_change >= 2:
            return "🟡 Умеренный набор шортов"
    
        if oi_change <= -5:
            return "🔥 КАПИТУЛЯЦИЯ ЛОНГОВ"
    
        if oi_change <= -2:
            return "🟡 Возможная капитуляция"


def update_oi_change_history(symbol, oi_change):
    if oi_change is None:
        return []
    
    if symbol not in OI_CHANGE_HISTORY:
        OI_CHANGE_HISTORY[symbol] = []
    
    OI_CHANGE_HISTORY[symbol].append(oi_change)
    
    if len(OI_CHANGE_HISTORY[symbol]) > 5:
        OI_CHANGE_HISTORY[symbol].pop(0)
    
    return OI_CHANGE_HISTORY[symbol]  

def detect_exhaustion(move_type, change, oi_change_history):

    if not oi_change_history:
        return None

    if len(oi_change_history) < 2:
        return None

    try:
        prev = float(oi_change_history[-2])
        last = float(oi_change_history[-1])
    except Exception:
        return None

    acceleration = last < prev

    slowing = last > prev

    # ==========================
    # PUMP
    # ==========================

    if move_type == "PUMP":

        # Деньги выходят всё быстрее
        if last <= -2 and acceleration:

            return {
                "type": "LONGS_EXHAUSTING",
                "strength": 9,
                "side_hint": "SHORT",
                "history": [prev, last]
            }

        # Деньги ещё выходят, но уже слабее
        if last <= -2 and slowing:

            return {
                "type": "LONGS_COOLING",
                "strength": 5,
                "side_hint": "WAIT",
                "history": [prev, last]
            }

    # ==========================
    # DUMP
    # ==========================

    if move_type == "DUMP":

        if last <= -2 and acceleration:

            return {
                "type": "SHORTS_EXHAUSTING",
                "strength": 9,
                "side_hint": "LONG",
                "history": [prev, last]
            }

        if last <= -2 and slowing:

            return {
                "type": "SHORTS_COOLING",
                "strength": 5,
                "side_hint": "WAIT",
                "history": [prev, last]
            }

    return None

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

    funding = get_funding_rate(raw_symbol)
    oi = get_open_interest(raw_symbol)

    oi_change = None

    if oi is not None:

        if symbol not in OI_HISTORY:
            OI_HISTORY[symbol] = []

        OI_HISTORY[symbol].append(oi)

        print(
            "[OI_LEN]",
            symbol,
            len(OI_HISTORY[symbol])
        )

        if len(OI_HISTORY[symbol]) > 60:
            OI_HISTORY[symbol].pop(0)

        if len(OI_HISTORY[symbol]) >= 2:

            old_oi = OI_HISTORY[symbol][0]

            print(
                "[OLD_OI]",
                symbol,
                old_oi,
                oi
            )

            if old_oi > 0:

                oi_change = (
                    (oi - old_oi) / old_oi
                ) * 100

                print(
                    "[OI_DEBUG]",
                    symbol,
                    len(OI_HISTORY[symbol]),
                    round(oi_change, 2)
                )

                print(
                    "[OI_CHANGE]",
                    symbol,
                    round(oi_change, 4)
                )

                if oi_change >= 5:
                    print(
                        "[SMART_OI] NEW MONEY",
                        symbol,
                        round(oi_change, 2)
                    )

                if oi_change <= -5:
                    print(
                        "[SMART_OI] EXIT MONEY",
                        symbol,
                        round(oi_change, 2)
                    )

    oi_change_history = update_oi_change_history(
        symbol,
        oi_change
    )

    print(
        "[OI_CHANGE_HISTORY]",
        symbol,
        [round(x, 2) for x in oi_change_history]
    )
    exhaustion = None
    for window_name, cfg in TIME_WINDOWS.items():

        move = get_window_move(
            raw_symbol,
            cfg["bar"],
            cfg["candles"]
        )

        if move is None:
            continue

        change = move["change"]

        if abs(change) >= 3:
            print(
                "[TRIGGER_CANDIDATE]",
                symbol,
                window_name,
                round(change, 2)
            )

        print(
            "[RAW_CHANGE]",
            symbol,
            change
        )

        move_type = None

        if change >= cfg["pump"]:
            move_type = "PUMP"

        elif change <= cfg["dump"]:
            move_type = "DUMP"

        else:
            continue

        print(
            "[MOVE]",
            symbol,
            move_type,
            "CHANGE=",
            round(change, 2),
            "OI=",
            round(oi_change, 2) if oi_change is not None else None
        )

        exhaustion = detect_exhaustion(
            move_type,
            change,
            oi_change_history
        )
        
        if exhaustion:
            print(
                "[EXHAUSTION]",
                symbol,
                exhaustion["type"],
                exhaustion["history"]
            )
        
        # ===================================
        # OI WARNING — не режем сигнал, а помечаем
        # ===================================

        oi_warning = None

        if (
            move_type == "PUMP"
            and oi_change is not None
            and oi_change > 3
        ):
            oi_warning = "⚠️ PUMP + OI ↑: новые деньги заходят, шорт опаснее"

            print(
                "[OI_WARNING] PUMP WITH NEW MONEY",
                symbol,
                round(oi_change, 2)
            )

        if (
            move_type == "DUMP"
            and oi_change is not None
            and oi_change > 3
        ):
            oi_warning = "⚠️ DUMP + OI ↑: новые шорты набиваются, лонг опаснее"

            print(
                "[OI_WARNING] DUMP WITH NEW SHORTS",
                symbol,
                round(oi_change, 2)
            )

        if not can_send(symbol, move_type, window_name, change):
            continue

        signal_count = add_signal_count(symbol)

        flow_comment = classify_flow(
            move_type,
            funding,
            oi_change
        )

        oi_flow = classify_oi_flow(
            move_type,
            oi_change
        )

        print(
            "[FLOW]",
            symbol,
            flow_comment
        )

        print(
            "[OI_FLOW]",
            symbol,
            oi_flow
        )

        money = analyze_new_money(raw_symbol)

        fetch_okx_liquidations(raw_symbol)
        liquidations = get_liquidation_summary(raw_symbol)

        # ==========================
        # MOVE STATUS ENGINE
        # ==========================
        
        temp_signal = {
            "symbol": symbol,
            "type": move_type,
            "change": change,
            "oi_change": oi_change,
            "money": money,
            "liquidations": liquidations,
        }
        
        temp_signal["trend_strength"] = analyze_trend_strength(temp_signal)
        
        move_status = classify_move_status(temp_signal)
        decision = chief_trader(temp_signal)

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
            "oi_warning": oi_warning,
            "flow_comment": flow_comment,
            "oi_flow": oi_flow,
            "signal_24h": signal_count,
            "oi_change_history": oi_change_history,
            "money": money,
            "liquidations": liquidations,
            "trend_strength": temp_signal["trend_strength"],
            "move_status": move_status,
            "decision": decision,
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



def classify_market_state(
    move_type,
    oi_change,
    long_liq,
    short_liq,
    pressure
):

    if oi_change is None:
        return "НЕ ХВАТАЕТ ДАННЫХ"

    # НОВЫЕ ЛОНГИ

    if (
        move_type == "PUMP"
        and oi_change >= 3
        and short_liq < long_liq * 2
    ):
        return "🚀 НОВЫЕ ЛОНГИ — ПАМП ПРОДОЛЖАЕТСЯ"

    # НОВЫЕ ШОРТЫ

    if (
        move_type == "DUMP"
        and oi_change >= 3
        and long_liq < short_liq * 2
    ):
        return "🔻 НОВЫЕ ШОРТЫ — ДАМП ПРОДОЛЖАЕТСЯ"

    # КАПИТУЛЯЦИЯ ШОРТОВ

    if (
        move_type == "PUMP"
        and oi_change <= -5
        and short_liq > long_liq
    ):
        return "🔥 КАПИТУЛЯЦИЯ ШОРТОВ — ВЫДОХ ПАМПА"

    # КАПИТУЛЯЦИЯ ЛОНГОВ

    if (
        move_type == "DUMP"
        and oi_change <= -5
        and long_liq > short_liq
    ):
        return "🔥 КАПИТУЛЯЦИЯ ЛОНГОВ — ВЫДОХ ДАМПА"

    return "⚪ ПЕРЕХОДНАЯ ФАЗА — НУЖНО НАБЛЮДАТЬ"
    

def classify_move_status(signal):

    try:

        move_type = signal.get("type")
        oi = signal.get("oi_change")
        trend = signal.get("trend_strength", {})
        trend_score = trend.get("score", 0)

        money = signal.get("money")
        pressure = ""

        if money:
            pressure = money.get("pressure", "")

        liquidations = signal.get("liquidations")

        long_liq = 0
        short_liq = 0

        if liquidations:
            long_liq = liquidations.get("long_liq", 0)
            short_liq = liquidations.get("short_liq", 0)

        score = 0
        reasons = []

        # ======================
        # OI
        # ======================

        if oi is not None:

            if oi >= 3:
                score += 2
                reasons.append("NEW_MONEY")

            elif oi <= -3:
                score -= 2
                reasons.append("MONEY_EXIT")

        # ======================
        # TREND
        # ======================

        if trend_score >= 7:
            score += 2
            reasons.append("TREND_STRONG")

        elif trend_score <= 3:
            score -= 2
            reasons.append("TREND_WEAK")

        # ======================
        # PRESSURE
        # ======================

        if move_type == "PUMP":

            if "BUY" in pressure:
                score += 1
                reasons.append("BUY_PRESSURE")

            elif "SELL" in pressure:
                score -= 1
                reasons.append("SELL_PRESSURE")

        if move_type == "DUMP":

            if "SELL" in pressure:
                score += 1
                reasons.append("SELL_PRESSURE")

            elif "BUY" in pressure:
                score -= 1
                reasons.append("BUY_PRESSURE")

        # ======================
        # LIQUIDATIONS
        # ======================

        if move_type == "PUMP":

            if short_liq > long_liq * 2 and short_liq > 100000:
                score -= 1
                reasons.append("SHORTS_FLUSHED")

        if move_type == "DUMP":

            if long_liq > short_liq * 2 and long_liq > 100000:
                score -= 1
                reasons.append("LONGS_FLUSHED")

        # ======================
        # FINAL
        # ======================

        if score >= 3:

            status = "🟢 ПРОДОЛЖАЕТСЯ"

        elif score <= -2:

            status = "🔴 ВЫДЫХАЕТСЯ"

        else:

            status = "🟡 НЕЯСНО"

        print(
            "[MOVE_STATUS]",
            signal.get("symbol"),
            status,
            score,
            reasons,
            flush=True
        )

        return {
            "status": status,
            "score": score,
            "reasons": reasons
        }

    except Exception as e:

        print(
            "[MOVE_STATUS_ERROR]",
            e,
            flush=True
        )

        return {
            "status": "🟡 НЕЯСНО",
            "score": 0,
            "reasons": []
        }

def oi_vote(signal):

    oi = signal.get("oi_change")

    if oi is None:

        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "OI_NO_DATA",
            "text": "OI нет данных"
        }

    abs_oi = abs(oi)

    if abs_oi >= 10:
        weight = 6

    elif abs_oi >= 7:
        weight = 5

    elif abs_oi >= 5:
        weight = 4

    elif abs_oi >= 3:
        weight = 3

    else:
        weight = 1

    if oi >= 3:

        return {
            "vote": "CONTINUE",
            "weight": weight,
            "reason": "OI_UP_NEW_MONEY",
            "text": f"OI растёт (+{oi:.2f}%)"
        }

    if oi <= -3:

        return {
            "vote": "EXHAUSTION",
            "weight": weight,
            "reason": "OI_DOWN_EXIT",
            "text": f"OI падает ({oi:.2f}%)"
        }

    return {
        "vote": "NEUTRAL",
        "weight": 1,
        "reason": "OI_NEUTRAL",
        "text": f"OI почти без изменений ({oi:.2f}%)"
    }

def trend_vote(signal):

    trend = signal.get("trend_strength", {})
    score = trend.get("score", 0)

    if score >= 8:

        return {
            "vote": "CONTINUE",
            "weight": 4,
            "reason": "TREND_CONTINUE"
        }

    if score >= 6:

        return {
            "vote": "CONTINUE",
            "weight": 3,
            "reason": "TREND_CONTINUE"
        }

    if score <= 2:

        return {
            "vote": "EXHAUSTION",
            "weight": 3,
            "reason": "TREND_EXHAUSTION"
        }

    return {
        "vote": "NEUTRAL",
        "weight": 1,
        "reason": "TREND_NEUTRAL"
    }


def money_vote(signal):

    money = signal.get("money")

    if not money:

        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "NO_MONEY_DATA",
            "text": "Нет данных по деньгам"
        }

    score = money.get("money_score", 0)

    state = money.get("money_state", "")

    # ==========================
    # VERY STRONG MONEY
    # ==========================

    if score >= 5:

        return {
            "vote": "CONTINUE",
            "weight": 4,
            "reason": "STRONG_NEW_MONEY",
            "text": "Заходят новые деньги"
        }

    # ==========================
    # BUILDING MONEY
    # ==========================

    if score >= 3:

        return {
            "vote": "CONTINUE",
            "weight": 2,
            "reason": "BUILDING_MONEY",
            "text": "Деньги постепенно заходят"
        }

    # ==========================
    # WEAK FLOW
    # ==========================

    if (
        "WEAK" in state
        or
        "NO_CLEAR" in state
    ):

        return {
            "vote": "EXHAUSTION",
            "weight": 2,
            "reason": "WEAK_MONEY_FLOW",
            "text": "Деньги выходят из рынка"
        }

    return {

        "vote": "NEUTRAL",

        "weight": 1,

        "reason": "MONEY_NEUTRAL",

        "text": "Поток денег нейтральный"

    }
def pressure_vote(signal):

    money = signal.get("money")

    if not money:
        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "PRESSURE_NO_DATA"
        }

    move = signal.get("type")
    pressure = money.get("pressure", "")

    # =========================
    # PUMP
    # =========================

    if move == "PUMP":

        if pressure == "STRONG_BUY_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 3,
                "reason": "PRESSURE_CONTINUE"
            }

        if pressure == "BUY_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 2,
                "reason": "PRESSURE_CONTINUE"
            }

        if pressure == "STRONG_SELL_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 3,
                "reason": "PRESSURE_EXHAUSTION"
            }

        if pressure == "SELL_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 2,
                "reason": "PRESSURE_EXHAUSTION"
            }

    # =========================
    # DUMP
    # =========================

    if move == "DUMP":

        if pressure == "STRONG_SELL_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 3,
                "reason": "PRESSURE_CONTINUE"
            }

        if pressure == "SELL_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 2,
                "reason": "PRESSURE_CONTINUE"
            }

        if pressure == "STRONG_BUY_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 3,
                "reason": "PRESSURE_EXHAUSTION"
            }

        if pressure == "BUY_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 2,
                "reason": "PRESSURE_EXHAUSTION"
            }

    return {
        "vote": "NEUTRAL",
        "weight": 1,
        "reason": "PRESSURE_NEUTRAL"
    }

def liquidation_vote(signal):

    liq = signal.get("liquidations")

    if not liq:
        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "NO_LIQUIDATIONS"
        }

    long_liq = liq.get("long_liq", 0)
    short_liq = liq.get("short_liq", 0)

    move = signal.get("type")

    # =====================================
    # PUMP
    # =====================================

    if move == "PUMP":

        if short_liq >= 100000 and short_liq > long_liq * 2:
            return {
                "vote": "EXHAUSTION",
                "weight": 4,
                "reason": "SHORT_SQUEEZE"
            }

        return {
            "vote": "NEUTRAL",
            "weight": 1,
            "reason": "NO_SQUEEZE"
        }

    # =====================================
    # DUMP
    # =====================================

    if move == "DUMP":

        if long_liq >= 100000 and long_liq > short_liq * 2:
            return {
                "vote": "EXHAUSTION",
                "weight": 4,
                "reason": "LONG_CAPITULATION"
            }

        return {
            "vote": "NEUTRAL",
            "weight": 1,
            "reason": "NO_CAPITULATION"
        }

    return {
        "vote": "UNKNOWN",
        "weight": 0,
        "reason": "UNKNOWN"
    }

def chief_trader(signal):

    votes = []

    votes.append(oi_vote(signal))
    votes.append(trend_vote(signal))
    votes.append(money_vote(signal))
    votes.append(pressure_vote(signal))
    votes.append(liquidation_vote(signal))

    continue_score = 0
    exhaustion_score = 0

    reasons = []

    for v in votes:

        reasons.append(v["reason"])

        if v["vote"] == "CONTINUE":
            continue_score += v["weight"]

        elif v["vote"] == "EXHAUSTION":
            exhaustion_score += v["weight"]

    # =====================================
    # CHIEF TRADER PRIORITY RULES
    # =====================================

    votes_map = {
        v["reason"]: v
        for v in votes
    }

    # =====================================
    # RULE 1
    # OI EXIT + CAPITULATION
    # Самый сильный сигнал
    # =====================================

    if (
        votes_map.get("OI_DOWN_EXIT", {}).get("vote") == "EXHAUSTION"
        and
        votes_map.get("LONG_CAPITULATION", {}).get("vote") == "EXHAUSTION"
    ):

        return {

            "stage": "EXHAUSTION",

            "action": "LOOK_REVERSAL",

            "confidence": 95,

            "continue_score": continue_score,

            "exhaustion_score": exhaustion_score,

            "reasons": reasons

        }

    # =====================================
    # RULE 2
    # TREND + NEW MONEY
    # =====================================

    if (
        votes_map.get("TREND", {}).get("vote") == "CONTINUE"
        and
        votes_map.get("OI_UP_NEW_MONEY", {}).get("vote") == "CONTINUE"
    ):

        return {

            "stage": "EARLY",

            "action": "IGNORE_REVERSAL",

            "confidence": 95,

            "continue_score": continue_score,

            "exhaustion_score": exhaustion_score,

            "reasons": reasons

        }

    # =====================================
    # RULE 3
    # Everything is getting weaker
    # =====================================

    weak = 0

    if votes_map.get("WEAK_TREND"):
        weak += 1

    if votes_map.get("WEAK_MONEY_FLOW"):
        weak += 1

    pressure_vote_data = votes_map.get("PRESSURE_EXHAUSTION")

    if pressure_vote_data:
        weak += 1

    if weak >= 2:

        return {

            "stage": "LATE",

            "action": "WATCH",

            "confidence": 80,

            "continue_score": continue_score,

            "exhaustion_score": exhaustion_score,

            "reasons": reasons

        }

    # ===========================
    # STAGE CLASSIFIER
    # ===========================
    
    score = continue_score - exhaustion_score
    
    if score >= 6:
    
        stage = "EARLY"
        action = "IGNORE_REVERSAL"
    
    elif score >= 3:
    
        stage = "BUILDING"
        action = "WAIT"
    
    elif score > -3:
    
        stage = "LATE"
        action = "WATCH"
    
    else:
    
        stage = "EXHAUSTION"
        action = "LOOK_REVERSAL"
    
    confidence = min(
        100,
        55 + abs(score) * 7
    )
    
    return {
    
        "stage": stage,
    
        "action": action,
    
        "confidence": confidence,
    
        "continue_score": continue_score,
    
        "exhaustion_score": exhaustion_score,
    
        "reasons": reasons
    
    }


def analyze_trend_strength(signal):
    try:
        score = 0
        reasons = []

        move_type = signal.get("type")
        change = signal.get("change", 0)
        oi_change = signal.get("oi_change")
        funding = signal.get("funding")
        money = signal.get("money")
        liquidations = signal.get("liquidations")

        # ==========================
        # PRICE STRENGTH
        # ==========================

        if abs(change) >= 10:
            score += 2
            reasons.append("PRICE_IMPULSE")

        elif abs(change) >= 5:
            score += 1
            reasons.append("PRICE_STRONG")
       
        money_score = 0
        pressure = ""

        if money:
            money_score = money.get("money_score", 0)
            pressure = money.get("pressure", "")

        long_liq = 0
        short_liq = 0

        if liquidations:
            long_liq = liquidations.get("long_liq", 0)
            short_liq = liquidations.get("short_liq", 0)

        # PRICE
        
        if move_type == "PUMP" and change > 0:
            score += 1
            reasons.append("PRICE_UP")
        
        if move_type == "DUMP" and change < 0:
            score += 1
            reasons.append("PRICE_DOWN")

        # OI

        if oi_change is not None:
        
            if move_type == "PUMP" and oi_change >= 3:
                score += 3
                reasons.append("OI_SUPPORTS_PUMP")
        
            elif move_type == "DUMP" and oi_change >= 3:
                score += 3
                reasons.append("OI_SUPPORTS_DUMP")
        
            elif oi_change <= -3:
                score -= 2
                reasons.append("OI_EXITING")

        # NO OI

        if oi_change is None:
        
            score = min(score, 3)
        
            reasons.append("NO_OI_DATA")

        # MONEY FLOW
        if money_score >= 4:
            score += 2
            reasons.append("STRONG_MONEY_FLOW")

        elif money_score >= 2:
            score += 1
            reasons.append("MODERATE_MONEY_FLOW")

        # PRESSURE
        if move_type == "PUMP" and "BUY" in pressure:
            score += 1
            reasons.append("BUY_PRESSURE")

        if move_type == "DUMP" and "SELL" in pressure:
            score += 1
            reasons.append("SELL_PRESSURE")

        # FUNDING
        if funding is not None:
            if move_type == "PUMP" and funding < 0:
                score += 1
                reasons.append("NEGATIVE_FUNDING_SUPPORTS_PUMP")

            if move_type == "DUMP" and funding > 0:
                score += 1
                reasons.append("POSITIVE_FUNDING_SUPPORTS_DUMP")

        # LIQUIDATIONS
        if move_type == "PUMP" and short_liq >= 50000 and short_liq > long_liq:
            score += 1
            reasons.append("SHORTS_LIQUIDATED")

        if move_type == "DUMP" and long_liq >= 50000 and long_liq > short_liq:
            score += 1
            reasons.append("LONGS_LIQUIDATED")

        score = max(0, min(score, 10))

        print(
            "[SMART_TREND]",
            signal.get("symbol"),
            move_type,
            "score=",
            score,
            "change=",
            round(change, 2),
            "oi=",
            round(oi_change, 2) if oi_change is not None else None,
            "funding=",
            round(funding, 4) if funding is not None else None,
            "reasons=",
            reasons,
            flush=True
        )

        return {
            "score": score,
            "reasons": reasons
        }

    except Exception as e:
        print("[SMART_TREND_ERROR]", e, flush=True)
        return {
            "score": 0,
            "reasons": []
        }

    
def build_message(signal):

    emoji = "🚀" if signal["type"] == "PUMP" else "🔻"
    side_text = "ПАМП" if signal["type"] == "PUMP" else "ДАМП"

    money = signal.get("money")
    liquidations = signal.get("liquidations")

    long_liq = 0
    short_liq = 0
    
    if liquidations:
        long_liq = liquidations.get("long_liq", 0)
        short_liq = liquidations.get("short_liq", 0)

    oi_change = signal.get("oi_change")
    oi_flow = classify_oi_flow(
        signal["type"],
        oi_change
    )

    oi_text = "нет данных"
    
    if oi_change is not None:
    
        if oi_change >= 5:
            oi_text = f"🟢 +{oi_change:.2f}% (заходят деньги)"
    
        elif oi_change >= 2:
            oi_text = f"🟡 +{oi_change:.2f}%"
    
        elif oi_change <= -5:
            oi_text = f"🔴 {oi_change:.2f}% (деньги выходят)"
    
        elif oi_change <= -2:
            oi_text = f"🟠 {oi_change:.2f}%"
    
        else:
            oi_text = f"{oi_change:.2f}%"

  

    money_state = "Нет данных"
    pressure_state = "Нет данных"
    
    setup = "НЕТ"
    risk_note = ""
    
    
    if money:
    
        money_state = STATE_MAP.get(
            money.get("money_state"),
            money.get("money_state")
        )

        # ===================================
        # OI HAS PRIORITY
        # ===================================
        
        if (
            oi_change is not None
            and oi_change <= -5
        ):
            money_state = "Деньги активно выходят"
        
        elif (
            oi_change is not None
            and oi_change >= 5
        ):
            money_state = "Заходят реальные деньги"
            
        pressure_state = PRESSURE_MAP.get(
            money.get("pressure"),
            money.get("pressure")
        )
    
        pressure = money.get("pressure", "")
        score = money.get("money_score", 0)

        new_longs = (
            signal["type"] == "PUMP"
            and oi_change is not None
            and oi_change >= 3
        )
        
        new_shorts = (
            signal["type"] == "DUMP"
            and oi_change is not None
            and oi_change >= 3
        )
            

        # Ранний сигнал на выдох

        if (
            signal["type"] == "DUMP"
            and abs(signal["change"]) >= 5
            and (
                oi_change is None
                or oi_change < 3
            )
        ):
            setup = "ЛОНГ ⭐⭐⭐"
        
        elif (
            signal["type"] == "PUMP"
            and abs(signal["change"]) >= 5
            and (
                oi_change is None
                or oi_change < 3
            )
        ):
            setup = "ШОРТ ⭐⭐⭐"
        
        
        if (
            signal["type"] == "DUMP"
            and "BUY" in pressure
            and score >= 2
        ):
            setup = "ЛОНГ ⭐⭐⭐⭐"

       
        
        elif (
            signal["type"] == "PUMP"
            and "SELL" in pressure
            and score >= 2
        ):
            setup = "ШОРТ ⭐⭐⭐⭐"

        

        # ===================================
        # LIQUIDATION EXHAUSTION
        # ===================================
        
        if (
            signal["type"] == "DUMP"
            and abs(signal["change"]) >= 4
            and long_liq >= 100000
            and long_liq > short_liq * 3
        ):
            setup = "ЛОНГ ⭐⭐⭐⭐"
        
        if (
            signal["type"] == "PUMP"
            and abs(signal["change"]) >= 4
            and short_liq >= 100000
            and short_liq > long_liq * 3
        ):
            setup = "ШОРТ ⭐⭐⭐⭐"
    
        # Сильный LONG после выноса лонгов

        if (
            signal["type"] == "DUMP"
            and abs(signal["change"]) >= 5
            and long_liq >= 200000
            and long_liq > short_liq * 2
            and oi_change is not None
            and oi_change <= -3
        ):
            setup = "ЛОНГ ⭐⭐⭐⭐⭐"
        
        
        # Сильный SHORT после выноса шортов
        
        if (
            signal["type"] == "PUMP"
            and abs(signal["change"]) >= 5
            and short_liq >= 200000
            and short_liq > long_liq * 2
            and oi_change is not None
            and oi_change <= -3
        ):
            setup = "ШОРТ ⭐⭐⭐⭐⭐"

        # =========================
        # FINAL SAFETY OVERRIDE
        # =========================
        
        if new_longs:
            setup = "ШОРТ ⭐⭐"
        
            setup_reason = (
                "Новые лонги продолжают заходить. "
                "Памп ещё не выдохся."
            )
        
        if new_shorts:
            setup = "ЛОНГ ⭐⭐"
        
            setup_reason = (
                "Новые шорты продолжают заходить. "
                "Дамп ещё не выдохся."
            )
                
  
    liq_strength = liquidation_strength(long_liq, short_liq)
    quality = signal_quality(money, long_liq, short_liq)
    new_listing_warning = ""

    # =========================
    # NO OI = LOW CONFIDENCE
    # =========================
    
    if oi_change is None:
    
        quality = min(quality, 2)
    
        if "ШОРТ" in setup:
            setup = "ШОРТ ⭐⭐"
    
        elif "ЛОНГ" in setup:
            setup = "ЛОНГ ⭐⭐"
    
        risk_note = (
            "Нет данных OI. "
            "Уверенность снижена."
        )

    if (
        oi_change is not None
        and abs(oi_change) >= 80
    ):
        quality = max(0, quality - 3)
    
        new_listing_warning = (
            "⚠️ Возможна новая или низколиквидная монета. "
            "Движение может быть манипулятивным."
        )

    market_state = classify_market_state(
        signal["type"],
        oi_change,
        long_liq,
        short_liq,
        pressure_state
    )

    trend = signal.get("trend_strength", {})
    trend_score = trend.get("score", 0)
    move = signal.get("move_status", {})

    move_status = move.get(
        "status",
        "🟡 НЕЯСНО"
    )

    decision = signal.get("decision", {})

    decision_text = decision.get(
        "text",
        "🟡 ЖДАТЬ"
    )
    if trend_score >= 9:
        trend_text = "🟢 ОЧЕНЬ СИЛЬНЫЙ ТРЕНД — против движения опасно"
    elif trend_score >= 7:
        trend_text = "🟡 СИЛЬНЫЙ ТРЕНД — лучше ждать подтверждения отката"
    elif trend_score >= 4:
        trend_text = "🟠 СРЕДНИЙ ТРЕНД — возможен откат"
    else:
        trend_text = "🔴 СЛАБЫЙ ТРЕНД — движение может выдыхаться"

    # =========================
    # DECISION ENGINE V1
    # =========================

    if trend_score >= 8:

        if signal["type"] == "PUMP":
            setup = "🚫 ШОРТ ОТМЕНЁН"
            setup_reason = (
                "Импульс ещё очень сильный. "
                "Деньги продолжают поддерживать рост."
            )

        else:
            setup = "🚫 ЛОНГ ОТМЕНЁН"
            setup_reason = (
                "Импульс ещё очень сильный. "
                "Давление продавцов сохраняется."
            )

    elif trend_score >= 6:

        if signal["type"] == "PUMP":
            setup = "⏳ ЖДАТЬ ШОРТ"
            setup_reason = (
                "Есть признаки силы. "
                "Нужен выдох импульса."
            )

        else:
            setup = "⏳ ЖДАТЬ ЛОНГ"
            setup_reason = (
                "Есть признаки силы. "
                "Нужен выдох дампа."
            )

 
    # =========================
    # CHIEF TRADER RESULT
    # =========================

    decision = signal.get("decision", {})

    stage = decision.get("stage", "UNKNOWN")

    stage_map = {
        "EARLY": "🟢 РАННИЙ ИМПУЛЬС",
        "BUILDING": "🟡 ИМПУЛЬС РАЗВИВАЕТСЯ",
        "LATE": "🟠 ПОЗДНЯЯ СТАДИЯ",
        "EXHAUSTION": "🔴 ИМПУЛЬС ВЫДЫХАЕТСЯ",
    }

    move_status = stage_map.get(
        stage,
        "⚪ НЕТ ДАННЫХ"
    )

    action = decision.get("action", "WAIT")

    action_map = {
        "IGNORE_REVERSAL": "🚫 НЕ ЛЕЗТЬ",
        "WAIT": "🟡 ЖДАТЬ",
        "WATCH": "👀 НАБЛЮДАТЬ",
        "LOOK_REVERSAL": "ИСКАТЬ КОРРЕКЦИЮ",
    }

    decision_text = action_map.get(
        action,
        "🟡 ЖДАТЬ"
    )
    
    # =========================
    # CHIEF TRADER REASONS
    # =========================
    
    reason_map = {
    
        "NO_OI": "• OI отсутствует",
    
        "OI_EXIT": "• Деньги выходят из OI",
    
        "OI_NEW": "• Заходят новые деньги",
    
        "TREND_VERY_STRONG": "• Очень сильный тренд",
    
        "TREND_STRONG": "• Сильный тренд",
    
        "TREND_WEAK": "• Тренд выдыхается",
    
        "STRONG_NEW_MONEY": "• Заходят крупные деньги",
    
        "BUILDING_MONEY": "• Деньги постепенно заходят",
    
        "WEAK_MONEY_FLOW": "• Слабый поток денег",
    
        "STRONG_BUY": "• Сильное давление покупателей",
    
        "BUY": "• Давление покупателей",
    
        "STRONG_SELL": "• Сильное давление продавцов",
    
        "SELL": "• Давление продавцов",
    
        "SHORT_SQUEEZE": "• Ликвидация шортов",
    
        "LONG_CAPITULATION": "• Капитуляция лонгов",
    }
    
    reasons = []
    
    for r in decision.get("reasons", []):
    
        if r in reason_map:
    
            reasons.append(reason_map[r])
    
    if not reasons:
    
        reasons.append("• Нет сильных подтверждений")
    
    reasons_text = "\n".join(reasons[:3])
    
    return f"""
{emoji} <b>{signal["symbol"]}</b>

📈 {signal["change"]:.2f}% за {signal["window"]}

📊 <b>{move_status}</b>

🎯 <b>{decision_text}</b>

──────────────

📦 <b>OI</b>
{oi_text}

💰 <b>Деньги</b>
{money_state}

⚖️ <b>Давление</b>
{pressure_state}

💥 <b>Ликвидации</b>
L ${long_liq:,.0f} | S ${short_liq:,.0f}

──────────────

🧠 <b>Почему</b>

{reasons_text}

🕒 {datetime.now(UTC).strftime("%H:%M")}
"""

print("🚀 PumpDump Radar V2 started")


test_binance()
test_bybit()

send_telegram("🚀 PumpDump Radar V2 ONLINE")

start_liquidation_streams()

while True:
    print("[SCAN] scanning market...")

    tickers = get_market_tickers()
    print(f"[TICKERS] {len(tickers)}")

    current_chunk = get_rotation_chunk(tickers)
    print(f"[CHUNK] {len(current_chunk)}")

    checked = 0
    signals = 0
    no_signal = 0

    for ticker in current_chunk:
        checked += 1

        signal = analyze(ticker)

        if not signal:
            no_signal += 1
            continue

        signals += 1

        send_telegram(build_message(signal))
        register_signal(signal)

        print(
            "[SIGNAL]",
            signal["window"],
            signal["symbol"],
            signal["type"],
            round(signal["change"], 2)
        )

    print(
        "[SCAN_STATS]",
        "checked=", checked,
        "signals=", signals,
        "no_signal=", no_signal
    )

    time.sleep(SCAN_SLEEP)
