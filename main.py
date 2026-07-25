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

        print(
            "[OI_LEN]",
            symbol,
            len(OI_HISTORY[symbol])
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

        print(
            "[WINDOW]",
            symbol,
            window_name,
            "change=", round(change, 2),
            "pump=", cfg["pump"],
            "dump=", cfg["dump"],
            flush=True
        )
        print(
            "[CHECK]",
            symbol,
            window_name,
            "change=", change,
            "pump=", cfg["pump"],
            "dump=", cfg["dump"],
            "pump_ok=", change >= cfg["pump"],
            "dump_ok=", change <= cfg["dump"],
            flush=True
        )
        move_type = None

        if change >= cfg["pump"]:
            move_type = "PUMP"
        
        elif change <= cfg["dump"]:
            move_type = "DUMP"
        
        else:
            continue

        
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

        print(
            "[TEST CHIEF]",
            symbol,
            window_name,
            round(change, 2),
            flush=True
        )
        
        test = chief_trader_v7(temp_signal)
        
        print(
            "[TEST RESULT]",
            test.get("trade_state"),
            test.get("stage"),
            test.get("quality_score"),
            flush=True
        )

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
        
        temp_signal["decision"] = decision
        
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
    

def oi_vote(signal):

    oi = signal.get("oi_change")

    move_type = signal.get("type")

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

    # =====================================
    # PUMP
    # =====================================

    if move_type == "PUMP":

        if oi >= 3:

            return {
                "vote": "CONTINUE",
                "weight": weight,
                "reason": "LONGS_OPENING",
                "text": f"Во время роста OI увеличивается (+{oi:.2f}%). Покупатели продолжают открывать новые позиции."
            }

        if oi <= -3:

            return {
                "vote": "EXHAUSTION",
                "weight": weight,
                "reason": "LONGS_CLOSING",
                "text": f"Во время роста OI уменьшается ({oi:.2f}%). Покупатели закрывают позиции, импульс начинает слабеть."
            }

    # =====================================
    # DUMP
    # =====================================

    if move_type == "DUMP":

        if oi >= 3:

            return {
                "vote": "CONTINUE",
                "weight": weight,
                "reason": "SHORTS_OPENING",
                "text": f"Во время падения OI увеличивается (+{oi:.2f}%). Продавцы продолжают открывать новые шорты."
            }

        if oi <= -3:

            return {
                "vote": "EXHAUSTION",
                "weight": weight,
                "reason": "SHORTS_CLOSING",
                "text": f"Во время падения OI уменьшается ({oi:.2f}%). Продавцы закрывают шорты, давление ослабевает."
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

    # =========================
    # Очень сильный импульс
    # =========================

    if score >= 3:

        return {
            "vote": "CONTINUE",
            "weight": 3,
            "reason": "TREND_STRONG",
            "text": "Цена движется очень уверенно"
        }

    # =========================
    # Нормальный импульс
    # =========================

    if score >= 2:

        return {
            "vote": "CONTINUE",
            "weight": 2,
            "reason": "TREND_GOOD",
            "text": "Цена сохраняет направление"
        }

    # =========================
    # Слабый импульс
    # =========================

    if score <= 1:

        return {
            "vote": "EXHAUSTION",
            "weight": 2,
            "reason": "TREND_WEAK",
            "text": "Цена теряет импульс"
        }

    return {

        "vote": "NEUTRAL",

        "weight": 1,

        "reason": "TREND_NEUTRAL",

        "text": "Импульс неоднозначный"

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
            "text": "Заходят деньги"
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
            "text": "Деньги выходят"
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
            "reason": "PRESSURE_NO_DATA",
            "text": "Нет данных о давлении"
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
                "reason": "PRESSURE_CONTINUE",
                "text": "Покупатели полностью контролируют движение"
            }

        if pressure == "BUY_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 2,
                "reason": "PRESSURE_CONTINUE",
                "text": "Покупатели сильнее продавцов"
            }

        if pressure == "STRONG_SELL_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 3,
                "reason": "PRESSURE_EXHAUSTION",
                "text": "Продавцы усиливаются"
            }

        if pressure == "SELL_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 2,
                "reason": "PRESSURE_EXHAUSTION",
                "text": "Продавцы усиливаются"
            }

    # =========================
    # DUMP
    # =========================

    if move == "DUMP":

        if pressure == "STRONG_SELL_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 3,
                "reason": "PRESSURE_CONTINUE",
                "text": "Продавцы полностью контролируют движение"
            }

        if pressure == "SELL_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 2,
                "reason": "PRESSURE_CONTINUE",
                "text": "Продавцы сильнее покупателей"
            }

        if pressure == "STRONG_BUY_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 3,
                "reason": "PRESSURE_EXHAUSTION",
                "text": "Покупатели усиливаются"
            }

        if pressure == "BUY_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 2,
                "reason": "PRESSURE_EXHAUSTION",
                "text": "Покупатели усиливаются"
            }

    return {
        "vote": "NEUTRAL",
        "weight": 1,
        "reason": "PRESSURE_NEUTRAL",
        "text": "Давление покупателей и продавцов почти одинаковое"
    }

def liquidation_vote(signal):

    liq = signal.get("liquidations")

    if not liq:
        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "NO_LIQUIDATIONS",
            "text": "Нет данных по ликвидациям"
        }

    long_liq = liq.get("long_liq", 0)
    short_liq = liq.get("short_liq", 0)

    move = signal.get("type")

    # =========================
    # PUMP
    # =========================

    if move == "PUMP":

        if short_liq >= 100000 and short_liq > long_liq * 2:

            return {
                "vote": "EXHAUSTION",
                "weight": 4,
                "reason": "SHORT_SQUEEZE",
                "text": "Вынос шортов"
            }

        return {
            "vote": "NEUTRAL",
            "weight": 1,
            "reason": "NO_SQUEEZE",
            "text": "Массового шорт-сквиза нет"
        }

    # =========================
    # DUMP
    # =========================

    if move == "DUMP":

        if long_liq >= 100000 and long_liq > short_liq * 2:

            return {
                "vote": "EXHAUSTION",
                "weight": 4,
                "reason": "LONG_CAPITULATION",
                "text": "Вынос лонгов"
            }

        return {
            "vote": "NEUTRAL",
            "weight": 1,
            "reason": "NO_CAPITULATION",
            "text": "Массовой капитуляции нет"
        }

    return {
        "vote": "UNKNOWN",
        "weight": 0,
        "reason": "UNKNOWN",
        "text": ""
    }

def spot_vote(signal):

    spot = signal.get("spot_cvd")

    if not spot or not spot.get("available"):

        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "SPOT_NO_DATA",
            "text": "Нет данных Spot"
        }

    return {

        # Пока Spot только наблюдает
        "vote": "NEUTRAL",

        # На решение не влияет
        "weight": 0,

        # Чтобы Chief Trader видел состояние
        "reason": spot.get("state", "SPOT_UNKNOWN"),

        # Будет выводиться в разделе "Почему"
        "text": spot.get("text", "")

    }


def scenario_vote(signal):

    scenario = signal.get("money_scenario")

    if not scenario:

        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "NO_SCENARIO",
            "text": "Сценарий не определён"
        }

    bias = scenario.get("bias", "WAIT")
    name = scenario.get("name", "UNKNOWN")
    title = scenario.get("title", "")

    # ===========================
    # Продолжение движения
    # ===========================

    if bias == "CONTINUE":

        return {
            "vote": "CONTINUE",
            "weight": 3,
            "reason": name,
            "text": f"Сценарий: {title}"
        }

    # ===========================
    # Вероятна коррекция
    # ===========================

    if bias == "CORRECTION":

        return {
            "vote": "EXHAUSTION",
            "weight": 3,
            "reason": name,
            "text": f"Сценарий: {title}"
        }

    # ===========================
    # Нет явного сценария
    # ===========================

    return {

        "vote": "NEUTRAL",

        "weight": 1,

        "reason": name,

        "text": f"Сценарий: {title}"

    }
def analyze_quality(signal):

    """
    Оценивает качество текущего движения.

    Возвращает:
    score: 0–100
    level: текстовая оценка
    reasons: причины оценки

    Эта функция НЕ решает:
    - входить или нет;
    - поздний ли вход;
    - будет ли разворот.

    Она оценивает только подтверждение движения.
    """

    move_type = str(
        signal.get("type")
        or ""
    ).upper()

    score = 0
    reasons = []

    # =====================================
    # 1. СИЛА ЦЕНОВОГО ДВИЖЕНИЯ
    # =====================================

    try:
        change = abs(
            float(signal.get("change") or 0)
        )
    except (TypeError, ValueError):
        change = 0

    if change >= 10:
        score += 20
        reasons.append("Цена показывает очень сильное движение")

    elif change >= 7:
        score += 17
        reasons.append("Цена показывает сильное движение")

    elif change >= 5:
        score += 14
        reasons.append("Цена уверенно движется")

    elif change >= 3:
        score += 10
        reasons.append("Минимальный импульс подтверждён")

    # =====================================
    # 2. OPEN INTEREST
    # =====================================

    oi_change = signal.get("oi_change")

    if oi_change is not None:

        try:
            oi_change = float(oi_change)
        except (TypeError, ValueError):
            oi_change = None

    if oi_change is not None:

        abs_oi = abs(oi_change)

        if abs_oi >= 10:
            oi_points = 25

        elif abs_oi >= 7:
            oi_points = 21

        elif abs_oi >= 5:
            oi_points = 18

        elif abs_oi >= 3:
            oi_points = 14

        elif abs_oi >= 1:
            oi_points = 7

        else:
            oi_points = 2

        # Рост OI подтверждает и PUMP, и DUMP:
        # при PUMP открываются новые позиции вверх;
        # при DUMP могут открываться новые шорты.
        if oi_change >= 3:

            score += oi_points

            if move_type == "PUMP":
                reasons.append(
                    "Рост поддерживается увеличением OI"
                )

            elif move_type == "DUMP":
                reasons.append(
                    "Падение поддерживается увеличением OI"
                )

        # Снижение OI не подтверждает качество движения.
        elif oi_change <= -3:

            score += max(
                0,
                oi_points // 3
            )

            reasons.append(
                "OI снижается — часть движения идёт через закрытие позиций"
            )

        else:

            score += oi_points

    # =====================================
    # 3. ДАВЛЕНИЕ
    # =====================================

    money = signal.get("money") or {}

    pressure = str(
        money.get("pressure")
        or ""
    ).upper()

    if move_type == "PUMP":

        if pressure == "STRONG_BUY_PRESSURE":
            score += 20
            reasons.append(
                "Покупатели полностью контролируют давление"
            )

        elif pressure == "BUY_PRESSURE":
            score += 14
            reasons.append(
                "Покупатели сохраняют преимущество"
            )

        elif pressure == "SELL_PRESSURE":
            score += 4
            reasons.append(
                "Продавцы мешают продолжению роста"
            )

        elif pressure == "STRONG_SELL_PRESSURE":
            reasons.append(
                "Сильное давление продавцов против роста"
            )

    elif move_type == "DUMP":

        if pressure == "STRONG_SELL_PRESSURE":
            score += 20
            reasons.append(
                "Продавцы полностью контролируют давление"
            )

        elif pressure == "SELL_PRESSURE":
            score += 14
            reasons.append(
                "Продавцы сохраняют преимущество"
            )

        elif pressure == "BUY_PRESSURE":
            score += 4
            reasons.append(
                "Покупатели мешают продолжению падения"
            )

        elif pressure == "STRONG_BUY_PRESSURE":
            reasons.append(
                "Сильное давление покупателей против падения"
            )

    # =====================================
    # 4. СИЛА ТРЕНДА
    # =====================================

    trend = signal.get("trend_strength") or {}

    try:
        trend_score = float(
            trend.get("score")
            or 0
        )
    except (TypeError, ValueError):
        trend_score = 0

    if trend_score >= 6:
        score += 15
        reasons.append(
            "Тренд подтверждает направление"
        )

    elif trend_score >= 3:
        score += 10
        reasons.append(
            "Тренд умеренно поддерживает движение"
        )

    elif trend_score > 0:
        score += 5

    # =====================================
    # 5. ДЕНЕЖНЫЙ СЦЕНАРИЙ
    # =====================================

    scenario = signal.get("money_scenario") or {}

    scenario_name = str(
        scenario.get("name")
        or ""
    ).upper()

    continuation_scenarios = {
        "HEALTHY_PUMP",
        "HEALTHY_DUMP",
        "SPOT_SUPPORTED_RISE",
        "SPOT_SUPPORTED_FALL",
        "NEW_LONGS",
        "NEW_SHORTS",
    }

    exhaustion_scenarios = {
        "OVERHEATED_PUMP",
        "OVERHEATED_DUMP",
        "WEAK_MONEY_FLOW",
        "MIXED_MONEY_FLOW",
    }

    if scenario_name in continuation_scenarios:
        score += 12
        reasons.append(
            "Денежный сценарий поддерживает движение"
        )

    elif scenario_name in exhaustion_scenarios:
        score += 3
        reasons.append(
            "Денежный сценарий даёт слабое подтверждение"
        )

    # =====================================
    # 6. ФИНАЛЬНАЯ ОЦЕНКА
    # =====================================

    score = max(
        0,
        min(100, round(score))
    )

    if score >= 85:
        level = "🔥 Очень сильное движение"

    elif score >= 70:
        level = "🟢 Сильное движение"

    elif score >= 55:
        level = "🟡 Умеренно подтверждённое движение"

    elif score >= 40:
        level = "🟠 Слабое подтверждение"

    else:
        level = "🔴 Движение плохо подтверждено"

    return {
        "score": score,
        "level": level,
        "reasons": reasons[:3],
    }

def _chief_safe_float(value, default=0.0):

    try:
        return float(value)

    except (TypeError, ValueError):
        return default


def _chief_collect_votes(signal):

    """
    Собирает независимые оценки аналитических модулей.

    Каждый модуль должен вернуть:

    {
        "vote": "CONTINUE" | "EXHAUSTION" | "NEUTRAL" | "UNKNOWN",
        "weight": число,
        "reason": техническая причина,
        "text": понятное объяснение
    }
    """

    vote_functions = [

        ("OI", oi_vote),

        ("TREND", trend_vote),

        ("MONEY", money_vote),

        ("PRESSURE", pressure_vote),

        ("LIQUIDATIONS", liquidation_vote),

        ("SPOT", spot_vote),

        ("SCENARIO", scenario_vote),

    ]

    votes = []

    for module_name, vote_function in vote_functions:

        try:

            result = vote_function(signal)

            if not isinstance(result, dict):

                raise TypeError(
                    f"{module_name} returned non-dict"
                )

        except Exception as error:

            print(
                "[CHIEF_V6_VOTE_ERROR]",
                signal.get("symbol"),
                module_name,
                error,
                flush=True
            )

            result = {

                "vote": "UNKNOWN",

                "weight": 0,

                "reason": f"{module_name}_ERROR",

                "text": (
                    f"{module_name}: данные получить не удалось"
                ),
            }

        vote = str(
            result.get("vote")
            or "UNKNOWN"
        ).upper()

        if vote not in {

            "CONTINUE",

            "EXHAUSTION",

            "NEUTRAL",

            "UNKNOWN",

        }:

            vote = "UNKNOWN"

        weight = _chief_safe_float(
            result.get("weight"),
            0
        )

        weight = max(
            0.0,
            min(10.0, weight)
        )

        votes.append({

            "module": module_name,

            "vote": vote,

            "weight": weight,

            "reason": str(
                result.get("reason")
                or f"{module_name}_UNKNOWN"
            ),

            "text": str(
                result.get("text")
                or ""
            ).strip(),

        })

    return votes


def _chief_build_context(signal, votes):

    """
    Создаёт единый Decision Context.

    Здесь нет торгового решения.
    Только факты, голоса и качество данных.
    """

    continue_votes = 0
    exhaustion_votes = 0
    neutral_votes = 0
    unknown_votes = 0

    continue_weight = 0.0
    exhaustion_weight = 0.0

    for vote_data in votes:

        vote = vote_data["vote"]

        weight = vote_data["weight"]

        if vote == "CONTINUE":

            continue_votes += 1

            continue_weight += weight

        elif vote == "EXHAUSTION":

            exhaustion_votes += 1

            exhaustion_weight += weight

        elif vote == "NEUTRAL":

            neutral_votes += 1

        else:

            unknown_votes += 1

    active_votes = (
        continue_votes
        + exhaustion_votes
    )

    active_weight = (
        continue_weight
        + exhaustion_weight
    )

    if active_votes > 0:

        continue_vote_share = (
            continue_votes
            / active_votes
        )

        exhaustion_vote_share = (
            exhaustion_votes
            / active_votes
        )

        consensus = max(
            continue_vote_share,
            exhaustion_vote_share
        )

    else:

        continue_vote_share = 0.5

        exhaustion_vote_share = 0.5

        consensus = 0.0

    if active_weight > 0:

        continue_weight_share = (
            continue_weight
            / active_weight
        )

        exhaustion_weight_share = (
            exhaustion_weight
            / active_weight
        )

    else:

        continue_weight_share = 0.5

        exhaustion_weight_share = 0.5

    # Сила сценария:
    # 60% — вес голосов;
    # 40% — количество независимых подтверждений.

    continue_strength = (
        continue_weight_share * 0.60
        + continue_vote_share * 0.40
    )

    exhaustion_strength = (
        exhaustion_weight_share * 0.60
        + exhaustion_vote_share * 0.40
    )

    if continue_strength > exhaustion_strength:

        dominant_side = "CONTINUE"

        dominant_strength = continue_strength

        dominant_votes = continue_votes

    elif exhaustion_strength > continue_strength:

        dominant_side = "EXHAUSTION"

        dominant_strength = exhaustion_strength

        dominant_votes = exhaustion_votes

    else:

        dominant_side = "UNCERTAIN"

        dominant_strength = 0.5

        dominant_votes = 0

    money = signal.get("money") or {}

    pressure = str(
        money.get("pressure")
        or ""
    ).upper()

    spot = signal.get("spot_cvd") or {}

    spot_state = str(
        spot.get("state")
        or ""
    ).upper()

    spot_cvd_percent = _chief_safe_float(
        spot.get("cvd_percent"),
        0
    )

    trend = signal.get("trend_strength") or {}

    trend_score = _chief_safe_float(
        trend.get("score"),
        0
    )

    scenario = signal.get("money_scenario") or {}

    scenario_name = str(
        scenario.get("name")
        or ""
    ).upper()

    liquidations = (
        signal.get("liquidations")
        or {}
    )

    long_liq = _chief_safe_float(
        liquidations.get("long_liq"),
        0
    )

    short_liq = _chief_safe_float(
        liquidations.get("short_liq"),
        0
    )

    oi_change = signal.get("oi_change")

    if oi_change is not None:

        oi_change = _chief_safe_float(
            oi_change,
            None
        )

    oi_slope = signal.get("oi_slope") or {}

    oi_total = _chief_safe_float(
        oi_slope.get("total_change"),
        0
    )

    oi_acceleration = _chief_safe_float(
        oi_slope.get("acceleration"),
        0
    )

    # =====================================
    # ПОКРЫТИЕ ДАННЫХ
    # =====================================

    available_sources = 0

    core_sources = 0

    if oi_change is not None:

        available_sources += 1

        core_sources += 1

    if pressure:

        available_sources += 1

        core_sources += 1

    if spot_state or abs(spot_cvd_percent) > 0:

        available_sources += 1

        core_sources += 1

    if trend:

        available_sources += 1

    if scenario_name:

        available_sources += 1

    if long_liq > 0 or short_liq > 0:

        available_sources += 1

    if money:

        available_sources += 1

    total_sources = 7

    data_coverage = round(
        available_sources
        / total_sources
        * 100
    )

    return {

        "votes": votes,

        "continue_votes": continue_votes,

        "exhaustion_votes": exhaustion_votes,

        "neutral_votes": neutral_votes,

        "unknown_votes": unknown_votes,

        "continue_weight": round(
            continue_weight,
            2
        ),

        "exhaustion_weight": round(
            exhaustion_weight,
            2
        ),

        "active_votes": active_votes,

        "consensus": round(
            consensus * 100
        ),

        "continue_strength": round(
            continue_strength * 100
        ),

        "exhaustion_strength": round(
            exhaustion_strength * 100
        ),

        "dominant_side": dominant_side,

        "dominant_strength": round(
            dominant_strength * 100
        ),

        "dominant_votes": dominant_votes,

        "available_sources": available_sources,

        "core_sources": core_sources,

        "data_coverage": data_coverage,

        "oi_change": oi_change,

        "oi_total": oi_total,

        "oi_acceleration": oi_acceleration,

        "pressure": pressure,

        "spot_state": spot_state,

        "spot_cvd_percent": spot_cvd_percent,

        "trend_score": trend_score,

        "scenario_name": scenario_name,

        "long_liq": long_liq,

        "short_liq": short_liq,

    }


def _chief_classify_stage(signal, context):

    """
    Определяет стадию текущего движения.

    START       — движение только началось.
    EXPANSION   — импульс развивается.
    CLIMAX      — движение уже сильно растянуто.
    EXHAUSTION  — движение теряет поддержку.
    UNCERTAIN   — данных недостаточно.
    """

    change = abs(
        _chief_safe_float(
            signal.get("change"),
            0
        )
    )

    window = str(
        signal.get("window")
        or ""
    ).lower()

    dominant_side = context["dominant_side"]

    dominant_strength = context["dominant_strength"]

    consensus = context["consensus"]

    oi_change = context["oi_change"]

    oi_acceleration = context["oi_acceleration"]

    # Явное ослабление движения.

    if (
        dominant_side == "EXHAUSTION"
        and dominant_strength >= 65
        and consensus >= 60
    ):

        return "EXHAUSTION"

    # Слишком большое движение уже может быть поздним.

    if change >= 10:

        return "CLIMAX"

    if change >= 7 and window in {

        "20m",

        "30m",

    }:

        return "CLIMAX"

    # Раннее движение.

    if (
        change <= 4.5
        and window in {
            "5m",
            "10m",
            "20m",
        }
        and dominant_side == "CONTINUE"
        and dominant_strength >= 65
    ):

        return "START"

    # Развивающийся импульс.

    if (
        dominant_side == "CONTINUE"
        and dominant_strength >= 60
        and change < 8
    ):

        return "EXPANSION"

    # OI начал резко замедляться.

    if (
        oi_change is not None
        and oi_change < 0
        and oi_acceleration < -1
    ):

        return "EXHAUSTION"

    return "UNCERTAIN"


def _chief_validate_market(signal, context):

    """
    Первый фильтр V6.

    Решает, достоин ли сигнал дальнейшего анализа.
    """

    problems = []

    if context["available_sources"] < 3:

        problems.append(
            "Недостаточно независимых источников данных"
        )

    if context["core_sources"] < 1:

        problems.append(
            "Нет данных OI, давления или Spot CVD"
        )

    if context["active_votes"] < 2:

        problems.append(
            "Слишком мало активных голосов"
        )

    if context["data_coverage"] < 35:

        problems.append(
            "Низкое покрытие рыночных данных"
        )

    is_valid = len(problems) == 0

    return {

        "valid": is_valid,

        "problems": problems,

    }


def _chief_make_decision(
    signal,
    context,
    quality_engine,
    market_stage,
    validation
):

    """
    Главное торговое решение V6.

    IGNORE — сигнал не заслуживает внимания.
    WATCH  — наблюдать.
    SETUP  — идея формируется.
    ENTRY  — можно искать подтверждённый вход.
    EXIT   — текущее движение заканчивается.
    """

    move_type = str(
        signal.get("type")
        or ""
    ).upper()

    change = abs(
        _chief_safe_float(
            signal.get("change"),
            0
        )
    )

    window = str(
        signal.get("window")
        or ""
    ).lower()

    quality_score = round(
        _chief_safe_float(
            quality_engine.get("score"),
            0
        )
    )

    dominant_side = context["dominant_side"]

    dominant_strength = context["dominant_strength"]

    consensus = context["consensus"]

    active_votes = context["active_votes"]

    data_coverage = context["data_coverage"]

    core_sources = context["core_sources"]

    pressure = context["pressure"]

    # =====================================
    # 1. IGNORE
    # =====================================

    if not validation["valid"]:

        return {

            "trade_state": "IGNORE",

            "market_stage": "UNCERTAIN",

            "direction": "NONE",

            "title": "🚫 Пропустить сигнал",

            "action": (
                "Недостаточно данных для безопасного решения"
            ),

            "reason": (
                validation["problems"][0]
                if validation["problems"]
                else "Данных недостаточно"
            ),

        }

    # =====================================
    # 2. РАЗВОРОТ / ЗАВЕРШЕНИЕ ДВИЖЕНИЯ
    # =====================================

    if dominant_side == "EXHAUSTION":

        if (
            dominant_strength >= 78
            and consensus >= 70
            and active_votes >= 3
            and data_coverage >= 55
        ):

            if move_type == "PUMP":

                direction = "SHORT"

                action = (
                    "Ждать подтверждения разворота цены "
                    "и только потом искать шорт"
                )

            else:

                direction = "LONG"

                action = (
                    "Ждать подтверждения разворота цены "
                    "и только потом искать лонг"
                )

            return {

                "trade_state": "SETUP",

                "market_stage": "EXHAUSTION",

                "direction": direction,

                "title": "🎯 Формируется разворотный сетап",

                "action": action,

                "reason": (
                    "Несколько независимых модулей "
                    "подтверждают ослабление движения"
                ),

            }

        return {

            "trade_state": "EXIT",

            "market_stage": "EXHAUSTION",

            "direction": "NONE",

            "title": "🟠 Движение начинает выдыхаться",

            "action": (
                "Не входить против движения без "
                "подтверждения разворота"
            ),

            "reason": (
                "Есть признаки ослабления, "
                "но разворот ещё не подтверждён"
            ),

        }

    # =====================================
    # 3. ПРОДОЛЖЕНИЕ ДВИЖЕНИЯ
    # =====================================

    if dominant_side == "CONTINUE":

        # ENTRY разрешаем только для раннего движения,
        # хорошего качества и нескольких подтверждений.

        if (
            market_stage == "START"
            and dominant_strength >= 82
            and consensus >= 75
            and active_votes >= 4
            and quality_score >= 75
            and core_sources >= 2
            and data_coverage >= 55
            and change <= 5.5
            and window in {
                "5m",
                "10m",
                "20m",
            }
        ):

            if move_type == "PUMP":

                direction = "LONG"

                action = (
                    "Можно искать вход в лонг "
                    "после небольшого отката или ретеста"
                )

            else:

                direction = "SHORT"

                action = (
                    "Можно искать вход в шорт "
                    "после небольшого отката или ретеста"
                )

            return {

                "trade_state": "ENTRY",

                "market_stage": "START",

                "direction": direction,

                "title": "✅ Есть ранняя возможность входа",

                "action": action,

                "reason": (
                    "Движение раннее и подтверждено "
                    "несколькими независимыми источниками"
                ),

            }

        # SETUP — хороший сценарий,
        # но вход ещё нужно дождаться.

        if (
            dominant_strength >= 68
            and consensus >= 60
            and active_votes >= 3
            and quality_score >= 55
            and data_coverage >= 45
            and market_stage in {
                "START",
                "EXPANSION",
            }
        ):

            if move_type == "PUMP":

                direction = "LONG"

                action = (
                    "Наблюдать за откатом и искать "
                    "подтверждение продолжения роста"
                )

            else:

                direction = "SHORT"

                action = (
                    "Наблюдать за откатом и искать "
                    "подтверждение продолжения падения"
                )

            return {

                "trade_state": "SETUP",

                "market_stage": market_stage,

                "direction": direction,

                "title": "🎯 Формируется торговый сетап",

                "action": action,

                "reason": (
                    "Направление подтверждено, "
                    "но точка входа ещё не сформирована"
                ),

            }

        # Сильное, но позднее движение.

        if market_stage == "CLIMAX":

            return {

                "trade_state": "WATCH",

                "market_stage": "CLIMAX",

                "direction": "NONE",

                "title": "🔥 Сильное, но позднее движение",

                "action": (
                    "Не догонять цену. "
                    "Ждать откат или новый сетап"
                ),

                "reason": (
                    "Импульс подтверждён, "
                    "но цена уже прошла слишком много"
                ),

            }

    # =====================================
    # 4. WATCH
    # =====================================

    return {

        "trade_state": "WATCH",

        "market_stage": market_stage,

        "direction": "NONE",

        "title": "👀 Пока только наблюдать",

        "action": (
            "Пропустить вход и дождаться "
            "более ясного подтверждения"
        ),

        "reason": (
            "Эксперты пока не дают достаточно "
            "сильного и согласованного сигнала"
        ),

    }


def _chief_build_explanation(
    votes,
    dominant_side
):

    """
    Разделяет подтверждения и препятствия.
    """

    confirmations = []

    obstacles = []

    sorted_votes = sorted(

        votes,

        key=lambda item: item.get(
            "weight",
            0
        ),

        reverse=True
    )

    for vote_data in sorted_votes:

        vote = vote_data["vote"]

        text = vote_data.get("text", "")

        if not text:
            continue

        text = text.replace(
            "• ",
            ""
        ).strip()

        if vote == dominant_side:

            confirmations.append(text)

        elif vote in {

            "CONTINUE",

            "EXHAUSTION",

        }:

            obstacles.append(text)

    return {

        "confirmations": confirmations[:3],

        "obstacles": obstacles[:2],

    }


def chief_trader(signal):

    """
    Chief Trader V6.

    Главная задача:
    не угадывать точный процент движения,
    а определить наличие практической сделки.

    Возвращает:

    IGNORE
    WATCH
    SETUP
    ENTRY
    EXIT
    """

    symbol = signal.get(
        "symbol",
        "UNKNOWN"
    )

    move_type = str(
        signal.get("type")
        or ""
    ).upper()

    # =====================================
    # 1. QUALITY ENGINE
    # =====================================

    try:

        quality_engine = analyze_quality(
            signal
        )

        if not isinstance(
            quality_engine,
            dict
        ):

            raise TypeError(
                "Quality Engine returned non-dict"
            )

    except Exception as error:

        print(
            "[CHIEF_V6_QUALITY_ERROR]",
            symbol,
            error,
            flush=True
        )

        quality_engine = {

            "score": 0,

            "level": "⚪ Нет данных",

            "reasons": [],

        }

    # =====================================
    # 2. НЕЗАВИСИМЫЕ ГОЛОСА
    # =====================================

    votes = _chief_collect_votes(
        signal
    )

    # =====================================
    # 3. DECISION CONTEXT
    # =====================================

    context = _chief_build_context(
        signal,
        votes
    )

    # =====================================
    # 4. ПРОВЕРКА ДАННЫХ
    # =====================================

    validation = _chief_validate_market(
        signal,
        context
    )

    # =====================================
    # 5. СТАДИЯ ДВИЖЕНИЯ
    # =====================================

    market_stage = _chief_classify_stage(
        signal,
        context
    )

    # =====================================
    # 6. ТОРГОВОЕ РЕШЕНИЕ
    # =====================================

    trade_decision = _chief_make_decision(

        signal,

        context,

        quality_engine,

        market_stage,

        validation,

    )

    trade_state = trade_decision[
        "trade_state"
    ]

    market_stage = trade_decision[
        "market_stage"
    ]

    direction = trade_decision[
        "direction"
    ]

    # =====================================
    # 7. ОБЪЯСНЕНИЕ
    # =====================================

    explanation_data = (
        _chief_build_explanation(
            votes,
            context["dominant_side"]
        )
    )

    confirmations = explanation_data[
        "confirmations"
    ]

    obstacles = explanation_data[
        "obstacles"
    ]

    explanation = []

    explanation.extend(
        confirmations
    )

    if not explanation:

        explanation.append(
            trade_decision["reason"]
        )

    # =====================================
    # 8. ПОНЯТНЫЕ ТЕКСТЫ
    # =====================================

    market_summary = (
        trade_decision["title"]
    )

    action_summary = (
        trade_decision["action"]
    )

    if direction == "LONG":

        stronger_summary = (
            "🟢 Приоритет покупателей"
        )

    elif direction == "SHORT":

        stronger_summary = (
            "🔴 Приоритет продавцов"
        )

    elif context["dominant_side"] == "CONTINUE":

        if move_type == "PUMP":

            stronger_summary = (
                "🟢 Текущее преимущество у покупателей"
            )

        else:

            stronger_summary = (
                "🔴 Текущее преимущество у продавцов"
            )

    elif context["dominant_side"] == "EXHAUSTION":

        if move_type == "PUMP":

            stronger_summary = (
                "🟠 Продавцы начинают мешать росту"
            )

        else:

            stronger_summary = (
                "🟠 Покупатели начинают мешать падению"
            )

    else:

        stronger_summary = (
            "⚪ Явного преимущества нет"
        )

    if trade_state == "ENTRY":

        next_move_summary = (
            "✅ Рынок сформировал раннюю "
            "торговую возможность"
        )

    elif trade_state == "SETUP":

        next_move_summary = (
            "🎯 Сценарий развивается, "
            "но требуется подтверждение входа"
        )

    elif trade_state == "EXIT":

        next_move_summary = (
            "🟠 Текущее движение может "
            "подходить к завершению"
        )

    elif trade_state == "IGNORE":

        next_move_summary = (
            "🚫 Этот сигнал лучше пропустить"
        )

    else:

        next_move_summary = (
            "👀 Сценарий пока не готов для сделки"
        )

    # =====================================
    # 9. НАДЁЖНОСТЬ ДАННЫХ
    #
    # Это не вероятность рынка.
    # Только качество информации.
    # =====================================

    confidence = round(

        context["data_coverage"] * 0.45

        + context["consensus"] * 0.35

        + min(
            100,
            context["active_votes"] * 18
        ) * 0.20

    )

    if signal.get(
        "oi_change"
    ) is None:

        confidence = min(
            confidence,
            65
        )

    if not validation["valid"]:

        confidence = min(
            confidence,
            55
        )

    confidence = max(
        40,
        min(95, confidence)
    )

    # =====================================
    # 10. СОВМЕСТИМОСТЬ СО СТАРЫМ КОДОМ
    # =====================================

    action_map = {

        "IGNORE": "WAIT",

        "WATCH": "WATCH",

        "SETUP": "WAIT",

        "ENTRY": "ENTRY",

        "EXIT": "WATCH",

    }

    old_action = action_map.get(
        trade_state,
        "WAIT"
    )

    stage_map = {

        "START": "EARLY",

        "EXPANSION": "BUILDING",

        "CLIMAX": "LATE",

        "EXHAUSTION": "EXHAUSTION",

        "UNCERTAIN": "UNKNOWN",

    }

    old_stage = stage_map.get(
        market_stage,
        "UNKNOWN"
    )

    quality_score = round(
        _chief_safe_float(
            quality_engine.get("score"),
            0
        )
    )

    decision = {

        # Новая V6-логика

        "trade_state": trade_state,

        "market_stage_v6": market_stage,

        "direction": direction,

        "quality_engine": quality_engine,

        "decision_context": context,

        "confirmations": confirmations,

        "obstacles": obstacles,

        "data_valid": validation["valid"],

        "data_problems": validation["problems"],

        # Совместимость

        "final_state": trade_state,

        "stage": old_stage,

        "action": old_action,

        "confidence": confidence,

        "quality": quality_score,

        "market_summary": market_summary,

        "stronger_summary": stronger_summary,

        "action_summary": action_summary,

        "next_move_summary": next_move_summary,

        "market_phase": market_stage,

        "move_status": market_summary,

        "decision_text": action_summary,

        "reasons": explanation,

        "explanation": explanation,

        # Проценты больше не используются.
        # Оставляем 0 только для совместимости,
        # пока build_message не переделан.

        "scenario_probability": 0,

        "reversal_score": 0,

        "reversal_level": market_stage,

        "reversal_text": next_move_summary,

    }

    print(
        "[CHIEF_V6]",
        symbol,
        f"type={move_type}",
        f"trade={trade_state}",
        f"stage={market_stage}",
        f"direction={direction}",
        f"dominant={context['dominant_side']}",
        f"strength={context['dominant_strength']}",
        f"votes={context['active_votes']}",
        f"consensus={context['consensus']}%",
        f"coverage={context['data_coverage']}%",
        f"quality={quality_score}",
        f"confidence={confidence}%",
        flush=True
    )

    return {

        "decision": decision,

        # Новые поля

        "trade_state": trade_state,

        "market_stage_v6": market_stage,

        "direction": direction,

        "quality_engine": quality_engine,

        "decision_context": context,

        "confirmations": confirmations,

        "obstacles": obstacles,

        # Старые поля для совместимости

        "final_state": trade_state,

        "stage": old_stage,

        "action": old_action,

        "confidence": confidence,

        "quality": quality_score,

        "score": round(
            context["continue_weight"]
            - context["exhaustion_weight"],
            2
        ),

        "continue_score":
            context["continue_weight"],

        "exhaustion_score":
            context["exhaustion_weight"],

        "continue_base_score":
            context["continue_weight"],

        "exhaustion_base_score":
            context["exhaustion_weight"],

        "continue_bonus": 0,

        "exhaustion_bonus": 0,

        "applied_rules": [],

        "reasons": explanation,

        "explanation": explanation,

        "market_summary": market_summary,

        "stronger_summary": stronger_summary,

        "action_summary": action_summary,

        "next_move_summary": next_move_summary,

        "market_phase": market_stage,

        "move_status": market_summary,

        "decision_text": action_summary,

        "scenario_probability": 0,

        "reversal_score": 0,

        "reversal_level": market_stage,

        "reversal_text": next_move_summary,

    }

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
    
    chief = chief_trader(signal)
    
    # =========================
    # Единственный источник решения
    # =========================
    
    decision = chief["decision"]
    
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
    stronger_summary = decision["stronger_summary"]
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
    
        send_telegram(build_message(signal))
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
