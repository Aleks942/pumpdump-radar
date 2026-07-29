import os
import csv
import time
import random
import requests
from datetime import datetime, UTC

from money_flow_engine import analyze_new_money
from spot_cvd_engine import get_spot_cvd
from money_scenarios import detect_money_scenario
from smart_score_engine import calculate_smart_score
from chief_trader_v7 import chief_trader_v7
from telegram_builder import build_short_message

from liquidation_engine import (
    start_liquidation_streams,
    fetch_okx_liquidations,
    get_liquidation_summary
)

from stats_engine import (
    register_signal,
    update_signal_result,
)

from scenario_stats_engine import (
    register_scenario_signal,
    update_scenario_results,
)

from market_memory import (
    initialize_market_memory,
    market_memory_healthcheck,
    save_market_signal,
    update_market_memory,
    save_oi_snapshot,
    load_recent_oi_history,
    get_reversal_statistics,
    get_similar_reversal_statistics,
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
signal_memory = {}
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

    history = OI_CHANGE_HISTORY[symbol]

    history.append(float(oi_change))

    if len(history) > 12:
        history.pop(0)

    # ===========================
    # SMART OI ANALYSIS
    # ===========================

    if len(history) >= 5:

        avg = sum(history) / len(history)

        first_half = history[:len(history)//2]
        second_half = history[len(history)//2:]

        avg_first = sum(first_half) / len(first_half)
        avg_second = sum(second_half) / len(second_half)

        acceleration = avg_second - avg_first

        print(
            "[SMART_OI]",
            symbol,
            "AVG=",
            round(avg, 3),
            "ACC=",
            round(acceleration, 3),
            flush=True
        )

        if avg > 0.25 and acceleration > 0.15:

            print(
                "[SMART_ACCUMULATION]",
                symbol,
                flush=True
            )

        elif avg < -0.25 and acceleration < -0.15:

            print(
                "[SMART_DISTRIBUTION]",
                symbol,
                flush=True
            )

    return history

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

def get_oi_slope(symbol):

    history = OI_HISTORY.get(symbol, [])

    if len(history) < 5:
        return None

    first = history[0]
    last = history[-1]

    if first <= 0:
        return None

    total_change = (
        (last - first)
        / first
    ) * 100

    diffs = []

    for i in range(1, len(history)):

        prev = history[i - 1]
        cur = history[i]

        if prev <= 0:
            continue

        diff = (
            (cur - prev)
            / prev
        ) * 100

        diffs.append(diff)

    acceleration = 0.0

    if len(diffs) >= 2:
        acceleration = diffs[-1] - diffs[0]

    return {
        "history": len(history),
        "total_change": total_change,
        "acceleration": acceleration
    }


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

    try:
        save_oi_snapshot(symbol, oi)
    except Exception as e:
        print(
            "[SAVE_OI_SNAPSHOT_ERROR]",
            symbol,
            e,
            flush=True
        )

    oi_change = None

    if oi is not None:

        if symbol not in OI_HISTORY:
            OI_HISTORY[symbol] = []

        OI_HISTORY[symbol].append(oi)

        print(
            "[OI_KEYS]",
            len(OI_HISTORY),
            flush=True
        )

        
        if symbol == "DOGEUSDT":
            print(
                "[DOGE_HISTORY]",
                len(OI_HISTORY[symbol]),
                OI_HISTORY[symbol],
                flush=True
            )

        if len(OI_HISTORY[symbol]) > 60:
            OI_HISTORY[symbol].pop(0)

        if len(OI_HISTORY[symbol]) >= 2:

            old_oi = OI_HISTORY[symbol][0]

            if old_oi > 0:

                oi_change = (
                    (oi - old_oi) / old_oi
                ) * 100

                

                

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
    
   

    oi_slope = get_oi_slope(symbol)

    if oi_slope:
    
        print(
            "[OI_SLOPE]",
            symbol,
            "history=",
            oi_slope["history"],
            "total=",
            round(oi_slope["total_change"], 2),
            "acc=",
            round(oi_slope["acceleration"], 2),
            flush=True
        )


    smart_money_state = None
    
    if oi_slope:
    
        if (
            oi_slope["total_change"] >= 5
            and oi_slope["acceleration"] > 0
        ):
    
            smart_money_state = "SMART_ACCUMULATION"
    
            print(
                "[SMART_ACCUMULATION]",
                symbol,
                round(oi_slope["total_change"], 2),
                flush=True
            )
    
        elif (
            oi_slope["total_change"] <= -5
            and oi_slope["acceleration"] < 0
        ):
    
            smart_money_state = "SMART_DISTRIBUTION"
    
            print(
                "[SMART_DISTRIBUTION]",
                symbol,
                round(oi_slope["total_change"], 2),
                flush=True
            )
    # ====================================
    # BEST SIGNAL SELECTOR
    # ====================================
    
    best_signal = None
    best_quality = -1
    
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

       
        move_type = None

        if change >= cfg["pump"]:
            move_type = "PUMP"
        
        elif change <= cfg["dump"]:
            move_type = "DUMP"
        
        else:
            continue

        print(
            "[FILTERED]",
            symbol,
            window_name,
            "change=", round(change, 2),
            "need pump=", cfg["pump"],
            "need dump=", cfg["dump"],
            flush=True
        )

        
        print(
            "[MOVE FOUND]",
            symbol,
            move_type,
            round(change, 2),
            window_name,
            flush=True
        )

        print(
            "[MOVE FOUND]",
            symbol,
            move_type,
            change,
            window_name,
            flush=True
        )

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

        spot_cvd = get_spot_cvd(raw_symbol)
        
        fetch_okx_liquidations(raw_symbol)
        liquidations = get_liquidation_summary(raw_symbol)

        # ====================================
        # SMART ACCUMULATION
        # ====================================
        
        accumulation = detect_accumulation(
            {
                "change": change,
                "oi_change": oi_change,
                "oi_slope": oi_slope,
                "money": money,
            }
        )

        # ==========================
        # MOVE STATUS ENGINE
        # ==========================
        
        temp_signal = {
            "symbol": symbol,
            "type": move_type,
            "change": change,
        
            "funding": funding,
        
            "oi": oi,
            "oi_change": oi_change,
            "oi_slope": oi_slope,
            "oi_change_history": oi_change_history,
        
            "money": money,
            "spot_cvd": spot_cvd,
            "liquidations": liquidations,
        
            "flow_comment": flow_comment,
            "oi_flow": oi_flow,
        
            "accumulation": accumulation,
            "smart_money_state": smart_money_state,
        }
        
        temp_signal["trend_strength"] = analyze_trend_strength(temp_signal)

        

        money_scenario = detect_money_scenario(temp_signal)
        
        temp_signal["money_scenario"] = money_scenario
        
        print(
            "[MONEY_SCENARIO]",
            symbol,
            money_scenario.get("name"),
            "title=",
            money_scenario.get("title"),
            "bias=",
            money_scenario.get("bias"),
            "strength=",
            money_scenario.get("strength"),
            "text=",
            money_scenario.get("text"),
            flush=True
        )
        
        # ====================================
        # CHIEF TRADER V7
        # ====================================
        
        decision = chief_trader_v7(
            temp_signal
        )
        
        # ------------------------------------
        # Совместимость со старым кодом
        # ------------------------------------
        
        decision["quality"] = decision.get(
            "quality_score",
            0
        )
        
        decision["action"] = decision.get(
            "trade_state",
            "IGNORE"
        )
        
        decision["continue_score"] = decision.get(
            "buyers_power",
            50
        )
        
        decision["exhaustion_score"] = decision.get(
            "sellers_power",
            50
        )
        
        decision["market_text"] = decision.get(
            "market_summary",
            ""
        )
        
        decision["control_text"] = decision.get(
            "control_summary",
            ""
        )
        
        decision["action_text"] = decision.get(
            "action_summary",
            ""
        )
        
        decision["forecast_text"] = decision.get(
            "next_move_summary",
            ""
        )

        decision["stronger_summary"] = decision.get(
            "control_summary",
            ""
        )
        
        decision["decision_text"] = decision.get(
            "action_summary",
            ""
        )
        
        decision["move_status"] = decision.get(
            "market_summary",
            ""
        )
        
        decision["market_phase"] = decision.get(
            "stage",
            ""
        )
        
        decision["reasons"] = decision.get(
            "confirmations",
            []
        )
        
        decision["explanation"] = decision.get(
            "confirmations",
            []
        )
        
        temp_signal["decision"] = decision

        # ====================================
        # SMART SCORE ENGINE
        # ====================================
        
        smart_score = calculate_smart_score(
            temp_signal
        )
        
        temp_signal["smart_score"] = smart_score
        
        print(
            "[SMART_SCORE]",
            symbol,
            smart_score.get("score"),
            smart_score.get("rating"),
            flush=True
        )
        
        print(
            "[CHIEF_V7]",
            symbol,
            decision["trade_state"],
            decision["stage"],
            decision["quality"],
            flush=True
        )
        
        # ====================================
        # SIGNAL QUALITY FILTER
        # ====================================

        quality = decision.get("quality", 0)
        
        print(
            "[QUALITY]",
            symbol,
            "window=", window_name,
            "quality=", quality,
            "stage=", decision.get("stage"),
            "action=", decision.get("action"),
            "continue=", decision.get("continue_score"),
            "exhaustion=", decision.get("exhaustion_score"),
            flush=True
        )
        print(
            "[QUALITY FILTER]",
            symbol,
            window_name,
            "quality=", quality,
            flush=True
        )
        if quality < 3:
            continue
        
        if quality > best_quality:
        
            best_quality = quality
        
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
                "oi_slope": oi_slope,
                "accumulation": accumulation,
                "smart_money_state": smart_money_state,
                "flow_comment": flow_comment,
                "oi_flow": oi_flow,
                "signal_24h": signal_count,
                "oi_change_history": oi_change_history,
                "money": money,
                "spot_cvd": spot_cvd,
                "liquidations": liquidations,
                "money_scenario": money_scenario,
                "trend_strength": temp_signal["trend_strength"],
                "decision": decision,
                "smart_score": smart_score,
            }

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
    


def analyze_trend_strength(signal):
    try:
        score = 0
        reasons = []

        move_type = signal.get("type")
        change = signal.get("change", 0)

        # ==========================
        # PRICE STRENGTH
        # ==========================

        if abs(change) >= 10:
            score += 2
            reasons.append("PRICE_IMPULSE")

        elif abs(change) >= 5:
            score += 1
            reasons.append("PRICE_STRONG")

        # ==========================
        # PRICE DIRECTION
        # ==========================

        if move_type == "PUMP" and change > 0:
            score += 1
            reasons.append("PRICE_UP")

        elif move_type == "DUMP" and change < 0:
            score += 1
            reasons.append("PRICE_DOWN")

        score = max(0, min(score, 10))

        print(
            "[SMART_TREND]",
            signal.get("symbol"),
            move_type,
            "score=",
            score,
            "change=",
            round(change, 2),
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


def calculate_signal_quality(signal):

    score = 0

    # -------------------------
    # OI
    # -------------------------

    oi = signal.get("oi_change")

    if oi is not None:

        if oi >= 8:
            score += 20

        elif oi >= 5:
            score += 15

        elif oi >= 2:
            score += 8

    # -------------------------
    # Давление
    # -------------------------

    money = signal.get("money")

    if money:

        pressure = money.get("pressure")

        if pressure == "BUYERS_DOMINATE":
            score += 15

        elif pressure == "SELLERS_DOMINATE":
            score += 15

    # -------------------------
    # Ликвидации
    # -------------------------

    liq = signal.get("liquidations")

    if liq:

        total = (
            liq.get("long_liq", 0)
            +
            liq.get("short_liq", 0)
        )

        if total > 2_000_000:
            score += 20

        elif total > 500_000:
            score += 10

    # -------------------------
    # Скорость цены
    # -------------------------

    move = abs(signal.get("change", 0))

    if move >= 8:
        score += 20

    elif move >= 5:
        score += 15

    elif move >= 3:
        score += 10

    # -------------------------
    # Ограничение
    # -------------------------

    score = min(score, 100)

    return score

def analyze_oi_price_divergence(signal):

    """
    Сравнивает:
    - движение цены;
    - изменение OI;
    - ускорение OI;
    - CVD спота;
    - давление покупателей и продавцов.

    Возвращает состояние и понятное объяснение.
    """

    move_type = signal.get("type")
    price_change = float(signal.get("change") or 0)

    oi_change = signal.get("oi_change")
    oi_slope = signal.get("oi_slope") or {}

    money = signal.get("money") or {}
    pressure = money.get("pressure")

    spot_cvd = signal.get("spot_cvd") or {}

    try:
        spot_cvd_percent = float(
            spot_cvd.get("cvd_percent")
            or spot_cvd.get("percent")
            or 0
        )
    except (TypeError, ValueError):
        spot_cvd_percent = 0.0

    oi_acceleration = float(
        oi_slope.get("acceleration") or 0
    )

    if oi_change is None:
        return {
            "state": "NO_DATA",
            "title": "⚪ Недостаточно данных",
            "text": "Нет надёжных данных OI для сравнения с ценой.",
            "risk": 0,
        }

    oi_change = float(oi_change)

    # =====================================
    # PUMP — движение вверх
    # =====================================

    if move_type == "PUMP":

        # Цена и OI растут, покупатели сохраняют контроль
        if (
            price_change >= 5
            and oi_change >= 3
            and pressure in (
                "BUY_PRESSURE",
                "STRONG_BUY_PRESSURE",
                "BUYERS_DOMINATE",
            )
            and spot_cvd_percent >= 0
        ):
            return {
                "state": "HEALTHY_PUMP",
                "title": "🟢 Рост подтверждён деньгами",
                "text": (
                    "Цена и OI растут одновременно. "
                    "Покупатели пока контролируют движение."
                ),
                "risk": 20,
            }

        # OI растёт намного быстрее цены — позиции набиваются,
        # но цена уже почти не реагирует
        if (
            oi_change >= 5
            and price_change <= 3
            and oi_acceleration > 0
        ):
            return {
                "state": "OI_PRICE_STALL",
                "title": "🟠 OI растёт, но цена тормозит",
                "text": (
                    "Новые позиции открываются, однако цена почти не растёт. "
                    "Возможны поглощение покупок и подготовка отката."
                ),
                "risk": 65,
            }

        # OI растёт, но поток ордеров уже становится продающим
        if (
            oi_change >= 3
            and (
                pressure in (
                    "SELL_PRESSURE",
                    "STRONG_SELL_PRESSURE",
                    "SELLERS_DOMINATE",
                )
                or spot_cvd_percent < 0
            )
        ):
            return {
                "state": "PUMP_TRAP",
                "title": "🔴 Возможная ловушка покупателей",
                "text": (
                    "OI продолжает расти, но CVD или давление уже указывает "
                    "на продажи. Вероятность резкого отката повышена."
                ),
                "risk": 85,
            }

    # =====================================
    # DUMP — движение вниз
    # =====================================

    if move_type == "DUMP":

        # Цена падает и OI растёт — открываются новые шорты
        if (
            price_change <= -5
            and oi_change >= 3
            and pressure in (
                "SELL_PRESSURE",
                "STRONG_SELL_PRESSURE",
                "SELLERS_DOMINATE",
            )
            and spot_cvd_percent <= 0
        ):
            return {
                "state": "HEALTHY_DUMP",
                "title": "🔴 Падение подтверждено деньгами",
                "text": (
                    "Цена падает, OI растёт, продавцы контролируют движение. "
                    "Лонг против импульса опасен."
                ),
                "risk": 20,
            }

        # Новые шорты набиваются, но цена перестала падать
        if (
            oi_change >= 5
            and abs(price_change) <= 3
            and oi_acceleration > 0
        ):
            return {
                "state": "SHORT_TRAP",
                "title": "🟠 Шорты набиваются, цена держится",
                "text": (
                    "OI быстро растёт, но цена перестала снижаться. "
                    "Возможен вынос шортистов и отскок вверх."
                ),
                "risk": 70,
            }

        # OI растёт, но покупки уже усиливаются
        if (
            oi_change >= 3
            and (
                pressure in (
                    "BUY_PRESSURE",
                    "STRONG_BUY_PRESSURE",
                    "BUYERS_DOMINATE",
                )
                or spot_cvd_percent > 0
            )
        ):
            return {
                "state": "DUMP_REVERSAL",
                "title": "🟢 Возможен разворот вверх",
                "text": (
                    "OI остаётся высоким, но покупатели начинают поглощать "
                    "продажи. Вероятность отскока повышена."
                ),
                "risk": 75,
            }

    return {
        "state": "NEUTRAL",
        "title": "⚪ Явного расхождения нет",
        "text": "Цена, OI и поток ордеров пока не дают сильного преимущества.",
        "risk": 30,
    }

def detect_accumulation(signal):

    """
    Ищет признаки накопления до импульса.
    """

    oi_change = signal.get("oi_change")
    oi_slope = signal.get("oi_slope") or {}

    money = signal.get("money") or {}

    pressure = money.get("pressure")

    change = float(signal.get("change") or 0)

    if oi_change is None:
        return None

    accel = oi_slope.get("acceleration", 0)

    # ==========================
    # SMART ACCUMULATION
    # ==========================

    if (
        abs(change) <= 2
        and oi_change >= 3
        and accel > 0
        and pressure in (
            "BUY_PRESSURE",
            "STRONG_BUY_PRESSURE",
            "BUYERS_DOMINATE",
        )
    ):

        return {
            "state": "SMART_ACCUMULATION",
            "title": "🟢 Накопление",
            "text": (
                "Цена почти стоит на месте, "
                "но открытый интерес растёт. "
                "Появляются признаки набора позиции."
            )
        }

    # ==========================
    # SMART DISTRIBUTION
    # ==========================

    if (
        abs(change) <= 2
        and oi_change >= 3
        and accel > 0
        and pressure in (
            "SELL_PRESSURE",
            "STRONG_SELL_PRESSURE",
            "SELLERS_DOMINATE",
        )
    ):

        return {
            "state": "SMART_DISTRIBUTION",
            "title": "🔴 Распределение",
            "text": (
                "Цена почти не меняется, "
                "но продавцы постепенно усиливаются."
            )
        }

    return None

def classify_move_type(signal):

    oi = signal.get("oi_change")

    money = signal.get("money") or {}
    pressure = money.get("pressure")

    liq = signal.get("liquidations") or {}

    long_liq = liq.get("long_liq", 0)
    short_liq = liq.get("short_liq", 0)

    # -----------------------------
    # Недостаточно данных
    # -----------------------------

    if oi is None:

        return (
            "⚪ Недостаточно данных",
            "Не хватает информации для анализа."
        )

    # -----------------------------
    # Агрессивный набор позиции
    # -----------------------------

    if (
        oi >= 8
        and pressure == "BUYERS_DOMINATE"
    ):

        return (
            "🔥 Агрессивный набор позиции",
            "Крупные деньги активно покупают."
        )

    # -----------------------------
    # Сильные покупки
    # -----------------------------

    if (
        oi >= 5
        and pressure == "BUYERS_DOMINATE"
    ):

        return (
            "🟢 Сильные покупки",
            "Высока вероятность продолжения роста."
        )

    # -----------------------------
    # Закрытие шортов
    # -----------------------------

    if (
        oi <= 0
        and short_liq > long_liq
    ):

        return (
            "🟡 Закрытие шортов",
            "Рост может быстро закончиться."
        )

    # -----------------------------
    # Фиксация прибыли
    # -----------------------------

    if (
        oi <= -5
        and long_liq > short_liq
    ):

        return (
            "🔴 Фиксация прибыли",
            "Крупные игроки выходят из позиции."
        )

    # -----------------------------
    # Ловушка
    # -----------------------------

    if (
        abs(oi) < 2
        and abs(signal.get("change", 0)) > 5
    ):

        return (
            "⚠️ Подозрительное движение",
            "Цена выросла, но новые деньги почти не пришли."
        )

    return (
        "⚪ Обычное движение",
        "Явных признаков преимущества пока нет."
    )

def predict_reversal(signal):

    score = 0

    # ==========================
    # Статистика прошлых сигналов
    # ==========================
    
    stats = get_reversal_statistics()

    similar_stats = get_similar_reversal_statistics(signal)

    oi = signal.get("oi_change")
    change = abs(signal.get("change", 0))

    liq = signal.get("liquidations") or {}

    long_liq = liq.get("long_liq", 0)
    short_liq = liq.get("short_liq", 0)

    money = signal.get("money") or {}
    pressure = money.get("pressure")

    # --------------------------
    # OI
    # --------------------------

    if oi is not None:

        if oi <= -5:
            score += 35

        elif oi <= -2:
            score += 20

        elif oi >= 5:
            score -= 20

    # --------------------------
    # Размер движения
    # --------------------------

    if change >= 10:
        score += 30

    elif change >= 7:
        score += 20

    elif change >= 5:
        score += 10

    # --------------------------
    # Давление
    # --------------------------

    if pressure == "BUYERS_DOMINATE":

        if signal["type"] == "PUMP":
            score -= 10

    elif pressure == "SELLERS_DOMINATE":

        if signal["type"] == "DUMP":
            score -= 10

    # --------------------------
    # Ликвидации
    # --------------------------

    if signal["type"] == "PUMP":

        if short_liq > long_liq * 2:
            score += 15

    else:

        if long_liq > short_liq * 2:
            score += 15

    # --------------------------
    # Ограничение
    # --------------------------
    
    score = max(0, min(score, 100))
    
    # ==========================
    # Коррекция по собственной статистике
    # ==========================
    
    if stats:
    
        probability = stats["probability"]
    
        if probability >= 80:
            score += 20
    
        elif probability >= 70:
            score += 15
    
        elif probability >= 60:
            score += 10
    
        elif probability <= 30:
            score -= 10
    
    score = max(0, min(score, 100))

    # =====================================
    # Похожие исторические ситуации
    # =====================================
    
    if similar_stats:
    
        probability = similar_stats["probability"]
    
        if probability >= 85:
            score += 25
    
        elif probability >= 75:
            score += 18
    
        elif probability >= 65:
            score += 12
    
        elif probability <= 25:
            score -= 15
    
    # --------------------------
    # Финальная оценка
    # --------------------------
    
    if score >= 70:
    
        return (
            score,
            "🔴 Очень высокая",
            "Откат вероятен в ближайшее время."
        )
    
    if score >= 50:
    
        return (
            score,
            "🟠 Высокая",
            "Лучше дождаться коррекции."
        )
    
    if score >= 30:
    
        return (
            score,
            "🟡 Средняя",
            "Следить за развитием движения."
        )
    
    return (
        score,
        "🟢 Низкая",
        "Пока движение выглядит здоровым."
    )

def build_clear_trade_summary(signal, reversal_score):

    move_type = signal.get("type")
    oi_change = signal.get("oi_change")

    money = signal.get("money") or {}
    pressure = money.get("pressure")

    # =========================
    # КТО СИЛЬНЕЕ
    # =========================

    if pressure in (
        "BUY_PRESSURE",
        "STRONG_BUY_PRESSURE",
        "BUYERS_DOMINATE",
    ):
        stronger_text = "🟢 Покупатели сильнее"

    elif pressure in (
        "SELL_PRESSURE",
        "STRONG_SELL_PRESSURE",
        "SELLERS_DOMINATE",
    ):
        stronger_text = "🔴 Продавцы сильнее"

    else:
        stronger_text = "⚪ Явного преимущества нет"

    # =========================
    # ПАМП
    # =========================

    if move_type == "PUMP":

        if oi_change is not None and oi_change >= 3:
            market_text = (
                "🟢 Рост поддерживается новыми позициями"
            )

        elif oi_change is not None and oi_change <= -3:
            market_text = (
                "🟠 Цена растёт, но открытые позиции закрываются"
            )

        else:
            market_text = "🟡 Рост продолжается без ясного подтверждения OI"

        if reversal_score >= 70:

            action_text = (
                "🔴 Не покупать. Искать ШОРТ только после подтверждения разворота"
            )

            next_move_text = "🔴 Вероятна коррекция вниз"

        elif reversal_score >= 50:

            action_text = (
                "👀 Не покупать по текущей цене. Ждать откат"
            )

            next_move_text = "🟠 Возможна коррекция вниз"

        elif reversal_score >= 30:

            action_text = (
                "⚠️ Новый вход рискован. Наблюдать за ослаблением роста"
            )

            next_move_text = "🟡 Рост может замедлиться"

        else:

            action_text = (
                "🟢 Не шортить. Искать вход только после небольшого отката"
            )

            next_move_text = "🟢 Вероятнее продолжение роста"

    # =========================
    # ДАМП
    # =========================

    else:

        if oi_change is not None and oi_change >= 3:
            market_text = (
                "🔴 Падение поддерживается новыми позициями"
            )

        elif oi_change is not None and oi_change <= -3:
            market_text = (
                "🟠 Цена падает, но открытые позиции закрываются"
            )

        else:
            market_text = "🟡 Падение продолжается без ясного подтверждения OI"

        if reversal_score >= 70:

            action_text = (
                "🟢 Не шортить. Искать ЛОНГ только после подтверждения разворота"
            )

            next_move_text = "🟢 Вероятен сильный отскок вверх"

        elif reversal_score >= 50:

            action_text = (
                "👀 Не открывать новый шорт. Ждать отскок"
            )

            next_move_text = "🟢 Возможен отскок вверх"

        elif reversal_score >= 30:

            action_text = (
                "⚠️ Новый шорт рискован. Наблюдать за ослаблением падения"
            )

            next_move_text = "🟡 Падение может замедлиться"

        else:

            action_text = (
                "🔴 Пока не покупать. Падение может продолжиться"
            )

            next_move_text = "🔴 Вероятнее продолжение падения"

    return {
        "market": market_text,
        "stronger": stronger_text,
        "action": action_text,
        "next_move": next_move_text,
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

        # =========================
        # НЕТ OI → не делаем выводов
        # =========================
        
        if oi_change is None:
            money_state = "Недостаточно данных OI"
            
        pressure_state = PRESSURE_MAP.get(
            money.get("pressure"),
            money.get("pressure")
        )
    
    
    # =========================
    # CHIEF TRADER
    # Главный аналитик
    # =========================
    
    chief = signal.get("decision", {})
    
    # =========================
    # Единственный источник решения
    # =========================
    
    decision = chief
    
    smart = signal.get("smart_score", {})
    
    smart_score = smart.get("score", 0)
    
    smart_rating = smart.get("rating", "-")
    
    smart_stars = smart.get("stars", "")
    
    smart_quality = smart.get("quality", "")
    
    smart_risk = smart.get("risk", "")

    reversal_score = decision.get("reversal_score", 0)

    reversal_level = decision.get(
        "reversal_level",
        "⚪ Нет данных"
    )
    
    reversal_text = decision.get(
        "reversal_text",
        "Недостаточно данных."
    )
        
    confidence = decision.get("confidence", 50)
    
    stage = decision.get("stage", "UNKNOWN")
    action = decision.get("action", "WAIT")

    decision_text = decision.get(
        "decision_text",
        "🟡 Подождать"
    )

    move_status = decision.get(
        "move_status",
        "⚪ Недостаточно данных"
    )

    # ============================================================
    # OLD MARKET PHASE ENGINE
    # Перенесён в Chief Trader V3.
    # Пока оставлен для проверки. Не удалять до завершения рефакторинга.
    # ============================================================
    
    """
    # ==========================
    # MARKET PHASE ENGINE
    # ==========================
    
    market_phase = "⚪ Рынок пока непонятен"
    trade_advice = "👀 Просто наблюдаем"
    
    oi_slope = signal.get("oi_slope")
    
    if oi_slope:
    
        total = oi_slope.get("total_change", 0)
        accel = oi_slope.get("acceleration", 0)
    
        # Идёт набор крупных позиций
        if total > 8 and accel > 0:
    
            market_phase = "🟢 Идёт накопление"
            trade_advice = "🟢 Можно искать ранний вход"
    
        # Начинается движение
        elif total > 3:
    
            market_phase = "🟡 Начало движения"
            trade_advice = "🟡 Готовимся ко входу"
    
        # Импульс развивается
        elif total > 0:
    
            market_phase = "🚀 Сильный импульс"
            trade_advice = "🚀 Можно удерживать позицию"
    
        # Импульс начинает затухать
        elif accel < -1:
    
            market_phase = "🟠 Импульс ослабевает"
            trade_advice = "⚠️ Возможен откат"
    
        # Деньги выходят
        elif total < -5:
    
            market_phase = "🔴 Идёт распродажа"
            trade_advice = "📉 Можно искать шорт"
    """

    
    # =========================
    # Понятные торговые решения
    # =========================

    if action == "IGNORE_REVERSAL":

        if signal["type"] == "PUMP":
            decision_text = "⛔ НЕ ШОРТИТЬ"
        else:
            decision_text = "⛔ НЕ ПОКУПАТЬ"

    elif action == "WATCH":

        decision_text = "👀 НАБЛЮДАТЬ"

    elif action == "LOOK_REVERSAL":

        decision_text = "🎯 ИСКАТЬ КОРРЕКЦИЮ"

    elif action == "WAIT":

        decision_text = "🟡 ЖДАТЬ"

    else:

        decision_text = "🟡 ЖДАТЬ"

    # =========================
    # Тип движения
    # =========================
    
    move_title, move_description = classify_move_type(signal)

    

    market_summary = decision["market_summary"]
    stronger_summary = decision.get(
        "control_summary",
        ""
    )
    action_summary = decision["action_summary"]
    next_move_summary = decision["next_move_summary"]
        
    

    # =========================
    # Убираем причины про OI,
    # если OI отсутствует
    # =========================
    
    if oi_change is None:
    
        filtered = []
    
        for r in decision.get("explanation", []):
    
            txt = r.lower()
    
            if (
                "oi" in txt
                or "деньг" in txt
                or "новые деньги" in txt
                or "заходят" in txt
                or "выходят" in txt
            ):
                continue
    
            filtered.append(r)
    
        if filtered:
            decision["reasons"] = filtered
    # =========================
    # CHIEF EXPLAINER
    # =========================

    
    if "Продавцы очень активны" in pressure_state:
        pressure_state = "🔴 SELL+++"
    
    elif "Продавцы сильнее" in pressure_state:
        pressure_state = "🔴 SELL"
    
    elif "Покупатели очень активны" in pressure_state:
        pressure_state = "🟢 BUY+++"
    
    elif "Покупатели сильнее" in pressure_state:
        pressure_state = "🟢 BUY"
    
    elif pressure_state and "равны" in pressure_state.lower():
        pressure_state = "⚪ BALANCE"

    
    reasons = []

    used_money = False
    
    for r in decision.get("reasons", ["Нет сильных подтверждений"]):
    
        # OI растёт — не допускаем вывод про выход денег
        if oi_change is not None and oi_change >= 2:
    
            if (
                "выход" in r.lower()
                or "oi падает" in r.lower()
            ):
                continue
    
        # OI падает — не допускаем вывод про вход денег
        if oi_change is not None and oi_change <= -2:
    
            if (
                "заход" in r.lower()
                or "oi растёт" in r.lower()
            ):
                continue
    
        # Если OI отсутствует — убираем выводы про OI и деньги
        if oi_change is None:
    
            if (
                "oi" in r.lower()
                or "деньг" in r.lower()
            ):
                continue
    
        # Не повторяем OI/деньги два раза
        if (
            "деньг" in r.lower()
            or "oi" in r.lower()
        ):
    
            if used_money:
                continue
    
            used_money = True
    
        reasons.append("• " + r.replace("• ", "").strip())
    
    reasons_text = "\n".join(reasons)
    if not reasons_text.strip():
        reasons_text = "• Нет сильных подтверждений"
    return f"""
    {emoji} <b>{signal["symbol"]}</b>   {signal["change"]:.2f}%   |   {signal["window"]}
    
    ━━━━━━━━━━━━━━
    
    📍 <b>Что происходит</b>
    
    {market_summary}
    
    ━━━━━━━━━━━━━━
    
    👑 <b>Кто сильнее</b>
    
    {stronger_summary}
    
    ━━━━━━━━━━━━━━
    
    🎯 <b>Что делать сейчас</b>
    
    {action_summary}
    
    ━━━━━━━━━━━━━━
    
    📈 <b>Что вероятнее дальше</b>
    
    {next_move_summary}
    
    🎯 Решение Chief: <b>{decision.get("trade_state", "WATCH")}</b>
    📍 Стадия: <b>{decision.get("market_stage_v6", "UNCERTAIN")}</b>
    🤖 Качество данных: <b>{confidence}%</b>
        
    ━━━━━━━━━━━━━━

    ⭐ <b>SMART SCORE</b>
    
    {smart_stars}
    
    🎯 <b>{smart_score}/100</b>
    
    🏆 Рейтинг: <b>{smart_rating}</b>
    
    📈 Качество: <b>{smart_quality}</b>
    
    ⚠️ Риск: <b>{smart_risk}</b>
    
    ━━━━━━━━━━━━━━
    
    📊 <b>Что видит бот</b>
    
    📦 OI        {oi_text}
    ⚖️ Давление  {pressure_state}
    💥 Ликвидации  L ${long_liq:,.0f} | S ${short_liq:,.0f}
    
    ━━━━━━━━━━━━━━
    
    🧠 <b>Почему принято такое решение</b>
    
    {reasons_text}
    
    🕒 {datetime.now(UTC).strftime("%H:%M")}
    """
    
def should_send_signal(signal):
    symbol = signal["symbol"]
    decision = signal.get("decision", {})
    action = decision.get("action")
    stage = decision.get("stage")
    change = abs(signal.get("change", 0))

    now = time.time()

    state = signal_memory.get(symbol)

    if state is None:
        signal_memory[symbol] = {
            "last_action": action,
            "last_stage": stage,
            "last_change": change,
            "last_time": now,
        }
        return True

    old_action = state.get("last_action")
    old_stage = state.get("last_stage")
    old_change = state.get("last_change", 0)
    old_time = state.get("last_time", 0)

    # если решение или стадия изменились — шлём
    if action != old_action or stage != old_stage:
        signal_memory[symbol] = {
            "last_action": action,
            "last_stage": stage,
            "last_change": change,
            "last_time": now,
        }
        return True

    # если движение стало намного сильнее — шлём обновление
    if change >= old_change + 3:
        signal_memory[symbol] = {
            "last_action": action,
            "last_stage": stage,
            "last_change": change,
            "last_time": now,
        }
        return True

    # через 30 минут можно напомнить
    if now - old_time >= 1800:
        signal_memory[symbol] = {
            "last_action": action,
            "last_stage": stage,
            "last_change": change,
            "last_time": now,
        }
        return True

    return False

print("🚀 PumpDump Radar V2 started")

initialize_market_memory()
print("========== INIT DONE ==========", flush=True)

market_memory_healthcheck()
print("========== HEALTH DONE ==========", flush=True)

restored_oi_history = load_recent_oi_history(limit=60)

if restored_oi_history:
    OI_HISTORY.clear()
    OI_HISTORY.update(restored_oi_history)

print(
    "[OI_HISTORY_READY]",
    "symbols=",
    len(OI_HISTORY),
    "points=",
    sum(len(values) for values in OI_HISTORY.values()),
    flush=True
)

test_binance()
test_bybit()

print("🚀 PumpDump Radar V2 ONLINE", flush=True)
send_telegram("🚀 PumpDump Radar V2 ONLINE")

start_liquidation_streams()

while True:
    print("[SCAN] scanning market...")

    tickers = get_market_tickers()
    print(f"[TICKERS] {len(tickers)}")
    
    current_chunk = get_rotation_chunk(tickers)
    print(f"[CHUNK] {len(current_chunk)}")
    
    current_prices = {}
    
    for t in current_chunk:
    
        symbol = (
            t.get("symbol")
            or str(t.get("instId") or "")
            .replace("-USDT-SWAP", "USDT")
            .replace("-", "")
        )
    
        try:
            price = float(
                t.get("lastPrice")
                or t.get("last")
                or 0
            )
    
            if symbol and price > 0:
                current_prices[symbol] = price
    
        except (TypeError, ValueError):
            continue
    
    checked = 0
    signals = 0
    no_signal = 0
    
    for ticker in current_chunk:
        checked += 1
    
        signal = analyze(ticker)
    
        if not signal:
            no_signal += 1
            continue
    
        print(
            "[SIGNAL READY]",
            signal["symbol"],
            signal["window"],
            signal["change"],
            flush=True
        )
    
        update_signal_result(
            signal["symbol"],
            signal["price"]
        )
    
        signals += 1
    
        if not should_send_signal(signal):
            print(
                "[SKIP DUPLICATE]",
                signal["symbol"],
                flush=True
            )
            continue
    
        send_telegram(build_short_message(signal))
        register_signal(signal)
    
        try:
            register_scenario_signal(signal)
        except Exception as e:
            print(
                "[REGISTER_SCENARIO_ERROR]",
                e,
                flush=True
            )
        
        try:
            record_id = save_market_signal(signal)
        
            print(
                "[SAVE_RESULT]",
                signal["symbol"],
                record_id,
                flush=True
            )

        except Exception as e:
            print(
                "[MARKET_MEMORY_CALL_ERROR]",
                e,
                flush=True
            )
            
        print(
            "[SIGNAL]",
            signal["window"],
            signal["symbol"],
            signal["type"],
            round(signal["change"], 2),
            flush=True
        )
    
    print(
        "[SCAN_STATS]",
        "checked=", checked,
        "signals=", signals,
        "no_signal=", no_signal,
        flush=True
    )

   
    try:
        update_market_memory(current_prices)
    except Exception as e:
        print(
            "[MARKET_MEMORY_UPDATE_LOOP_ERROR]",
            e,
            flush=True
        )

    print(
        "[OI_MEMORY]",
        "symbols=",
        len(OI_HISTORY),
        flush=True
    )
    
    time.sleep(SCAN_SLEEP)
