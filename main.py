import os
import csv
import time
import random
import threading
import requests
from datetime import datetime, UTC
from spot_cvd_engine import get_spot_cvd
from telegram_builder import build_short_message
from trade_flow_collector_v3 import get_futures_windows
from okx_trade_stream_v3 import run_stream_forever
from pattern_detector import detect_pattern

from liquidation_engine import (
    start_liquidation_streams,
    fetch_okx_liquidations,
    get_liquidation_summary
)

from stats_engine import (
    register_signal,
    update_signal_result,
)

from market_memory import (
    initialize_market_memory,
    market_memory_healthcheck,
    save_market_signal,
    update_market_memory,
    save_oi_snapshot,
    load_recent_oi_history,
   
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
OI_HISTORY = {}
OI_TIME_HISTORY = {}
signal_memory = {}
ENTRY_TRACKER = {}
ENTRY_TRACKER_TEST = False
print("[BOOT] OI_HISTORY CREATED")
rotation_index = 0

TIME_WINDOWS = {

    "5m": {
        "bar": "1m",
        "candles": 5,
        "pump": float(os.getenv("PUMP_THRESHOLD_5M", 1.5)),
        "dump": -float(os.getenv("DUMP_THRESHOLD_5M", 1.5)),
    },

    "10m": {
        "bar": "1m",
        "candles": 10,
        "pump": float(os.getenv("PUMP_THRESHOLD_10M", 2.0)),
        "dump": -float(os.getenv("DUMP_THRESHOLD_10M", 2.0)),
    },

    "20m": {
        "bar": "1m",
        "candles": 20,
        "pump": float(os.getenv("PUMP_THRESHOLD_20M", 2.5)),
        "dump": -float(os.getenv("DUMP_THRESHOLD_20M", 2.5)),
    },

    "30m": {
        "bar": "1m",
        "candles": 30,
        "pump": float(os.getenv("PUMP_THRESHOLD_30M", 3)),
        "dump": -float(os.getenv("DUMP_THRESHOLD_30M", 3)),
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


def update_entry_tracker(symbol, current_price):

    item = ENTRY_TRACKER.get(symbol)

    print(
        "[TRACKER_DEBUG]",
        symbol,
        "found=",
        bool(item),
        "price=",
        current_price,
        flush=True
    )

    if not item:
        return

    entry_price = item.get("entry_price")
    direction = item.get("direction")
    entry_time = item.get("entry_time")
    checked = item.get("checked", set())

    if not entry_price or not entry_time:
        return

    elapsed = (datetime.now(UTC) - entry_time).total_seconds()

    print(
        "[TRACKER_TIME]",
        symbol,
        "elapsed_sec=",
        round(elapsed, 1),
        "checked=",
        checked,
        flush=True
    )

    for seconds, label in ENTRY_CHECKPOINTS.items():

        if elapsed >= seconds and label not in checked:

            price_change = (
                (current_price - entry_price) / entry_price
            ) * 100

            if direction == "SHORT":
                result = -price_change
            else:
                result = price_change

            print(
                "[ENTRY_RESULT]",
                symbol,
                "checkpoint=",
                label,
                "direction=",
                direction,
                "entry=",
                entry_price,
                "current=",
                current_price,
                "result=",
                round(result, 2),
                "%",
                "elapsed_min=",
                round(elapsed / 60, 1),
                flush=True
            )

            checked.add(label)

    item["checked"] = checked

    if "60m" in checked:
        del ENTRY_TRACKER[symbol]


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

    oi_trend_change = None
    oi_short_change = None
    
    if oi is not None:

        if symbol not in OI_HISTORY:
            OI_HISTORY[symbol] = []

        OI_HISTORY[symbol].append(oi)
       
        
        import time

        if symbol not in OI_TIME_HISTORY:
            OI_TIME_HISTORY[symbol] = []
        
        OI_TIME_HISTORY[symbol].append(time.time())
        
        if len(OI_TIME_HISTORY[symbol]) > 60:
            OI_TIME_HISTORY[symbol].pop(0)
        
        if len(OI_TIME_HISTORY[symbol]) >= 2:
            oi_window_minutes = (
                OI_TIME_HISTORY[symbol][-1]
                - OI_TIME_HISTORY[symbol][0]
            ) / 60
        
            print(
                "[OI_WINDOW]",
                symbol,
                "points=",
                len(OI_TIME_HISTORY[symbol]),
                "minutes=",
                round(oi_window_minutes, 1),
                flush=True,
            )

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

        # ====================================
        # SHORT OI — свежий поток денег
        # примерно последние 15–30 минут
        # ====================================
        
        oi_short_change = None
        
        if len(OI_HISTORY[symbol]) >= 3:
        
            short_old_oi = OI_HISTORY[symbol][-3]
        
            if short_old_oi > 0:
                oi_short_change = (
                    (oi - short_old_oi) / short_old_oi
                ) * 100
        
                print(
                    "[OI_SHORT]",
                    symbol,
                    "change=",
                    round(oi_short_change, 2),
                    "%",
                    flush=True
                )

        if len(OI_HISTORY[symbol]) >= 2:

            old_oi = OI_HISTORY[symbol][0]

            if old_oi > 0:

                oi_trend_change = (
                    (oi - old_oi) / old_oi
                ) * 100
    
   

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
    
           
    # ====================================
    # BEST SIGNAL SELECTOR
    # ====================================
    
    best_signal = None
   
    pattern_called = False
    
    for window_name, cfg in TIME_WINDOWS.items():

        move = get_window_move(
            raw_symbol,
            cfg["bar"],
            cfg["candles"]
        )

        if move is None:
            continue

        change = move["change"]

        if abs(change) >= 1:
            print(
                "[WINDOW_CHECK]",
                symbol,
                window_name,
                "change=", round(change, 2),
                "pump_need=", cfg["pump"],
                "dump_need=", cfg["dump"],
                flush=True
            )

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

        # ====================================
        # ONE PATTERN CHECK PER SYMBOL PER SCAN
        # ====================================

        if pattern_called:
            print(
                "[PATTERN_SKIP_EXTRA_WINDOW]",
                symbol,
                window_name,
                round(change, 2),
                flush=True
            )
            continue

        pattern_called = True

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
            round(oi_short_change, 2) if oi_short_change is not None else None
        )

       
        futures_windows = get_futures_windows(symbol)

        futures_5m = futures_windows.get("5m", {})
        
        futures_delta = futures_5m.get("delta_quote", 0)
        
        futures_imbalance = futures_5m.get("imbalance_pct", 0)
        
        futures_ready = futures_5m.get("window_ready", False)

        print("[FUTURES_FLOW]", symbol, "delta=", round(futures_delta, 2), "imbalance=",
        round(futures_imbalance, 2), "ready=", futures_ready)

        spot_cvd = get_spot_cvd(raw_symbol)
        
        fetch_okx_liquidations(raw_symbol)
        liquidations = get_liquidation_summary(raw_symbol)

       
        
        
        # ====================================
        # PATTERN DETECTOR
        # ====================================
        
        
        spot_cvd_value = spot_cvd.get("cvd_percent")
        
        long_liq = liquidations.get("long_liq", 0)
        
        short_liq = liquidations.get("short_liq", 0)

        print(
            "[PATTERN_INPUT]",
            symbol,
            "price=", change,
            "oi=", oi_short_change,
            "futures=", futures_imbalance,
            "spot=", spot_cvd_value,
            "long_liq=", long_liq,
            "short_liq=", short_liq,
            flush=True,
        )
        
        pattern_result = detect_pattern(
            price_change=change,
            oi_change=oi_short_change,
            futures_cvd=futures_imbalance,
            spot_cvd=spot_cvd_value,
            delta=futures_delta,
            long_liquidations=long_liq,
            short_liquidations=short_liq,
        )
        
        print(
            "[PATTERN]",
            symbol,
            pattern_result.get("pattern"),
            pattern_result.get("direction"),
            pattern_result.get("reason"),
            flush=True,
        )
        
        decision = {
            "pattern": pattern_result.get("pattern", "NONE"),
            "direction": pattern_result.get("direction", "NONE"),
            "reason": pattern_result.get("reason", ""),
        }
        
        
        

        if decision.get("pattern") == "NONE":
            continue 
    
        best_signal = {
            "symbol": symbol,
            "type": move_type,
            "window": window_name,
            "change": change,
            "start_price": move["start_price"],
            "end_price": move["end_price"],
            "price": price,
            "volume": volume_24h,
            "oi": oi,
            "oi_change": oi_short_change,
            "oi_trend_change": oi_trend_change,
            "oi_slope": oi_slope,
            "spot_cvd": spot_cvd,
            "liquidations": liquidations,
            "decision": decision,
            
        }

        return best_signal


    


def should_send_signal(signal):
    symbol = signal["symbol"]
    decision = signal.get("decision", {})

    pattern = decision.get("pattern", "NONE")
    direction = decision.get("direction", "NONE")
    change = abs(signal.get("change", 0))

    now = time.time()

    state = signal_memory.get(symbol)

    if state is None:
        signal_memory[symbol] = {
            "last_pattern": pattern,
            "last_direction": direction,
            "last_change": change,
            "last_time": now,
        }
        return True

    old_pattern = state.get("last_pattern")
    old_direction = state.get("last_direction")
    old_change = state.get("last_change", 0)
    old_time = state.get("last_time", 0)

    # новый паттерн или сменилось направление
    if pattern != old_pattern or direction != old_direction:
        signal_memory[symbol] = {
            "last_pattern": pattern,
            "last_direction": direction,
            "last_change": change,
            "last_time": now,
        }
        return True

    # движение усилилось минимум на 3%
    if change >= old_change + 3:
        signal_memory[symbol] = {
            "last_pattern": pattern,
            "last_direction": direction,
            "last_change": change,
            "last_time": now,
        }
        return True

    # повтор через 30 минут
    if now - old_time >= 1800:
        signal_memory[symbol] = {
            "last_pattern": pattern,
            "last_direction": direction,
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

initial_tickers = get_market_tickers()

swap_symbols = [t.get("instId") for t in initial_tickers if t.get("instId")]

threading.Thread(
    target=run_stream_forever,
    args=(swap_symbols,),
    daemon=True,
).start()

print("[V3_WS_THREAD_STARTED]", "symbols=", len(swap_symbols))

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

    

    for tracked_symbol in list(ENTRY_TRACKER.keys()):
        tracked_price = current_prices.get(tracked_symbol)

        if tracked_price:
            update_entry_tracker(
                tracked_symbol,
                tracked_price
            )

    for ticker in current_chunk:
        checked += 1

        symbol = ticker.get("instId", "").replace("-USDT-SWAP", "USDT")
        current_price = float(ticker.get("last") or 0)

        if symbol and current_price > 0:

            if ENTRY_TRACKER_TEST and not ENTRY_TRACKER:
                ENTRY_TRACKER[symbol] = {
                    "direction": "LONG",
                    "entry_price": current_price,
                    "entry_time": datetime.now(UTC),
                    "checked": set(),
                    "test": True,
                }

                print(
                    "[TEST_ENTRY_CREATED]",
                    symbol,
                    "price=",
                    current_price,
                    flush=True
                )

            if symbol in ENTRY_TRACKER:
                update_entry_tracker(symbol, current_price)

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
