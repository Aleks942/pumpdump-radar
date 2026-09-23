import os
import csv
import time
import random
import threading
import math
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
    save_entry_result,
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
ENTRY_TRACKER_LOCK = threading.Lock()
ENTRY_TRACKER_INTERVAL = 15
ENTRY_TRACKER_TEST = False

ENTRY_CHECKPOINTS = {
    300: "5m",
    600: "10m",
    1200: "20m",
    1800: "30m",
}

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

def send_telegram(text, symbol="SYSTEM"):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    started = time.monotonic()

    try:
        response = requests.post(url, json=payload, timeout=10)
        data = response.json()

        if not isinstance(data, dict):
            raise ValueError("Invalid Telegram response")

        if response.status_code == 200 and data.get("ok") is True:
            message = data.get("result") or {}

            print(
                "[TG_SENT]",
                symbol,
                "message_id=", message.get("message_id"),
                "telegram_ts=", message.get("date"),
                "confirmed_at=", datetime.now(UTC).isoformat(),
                "request_sec=", round(time.monotonic() - started, 3),
                flush=True,
            )
            return True

        if data.get("ok") is False:
            print(
                "[TG_REJECTED]",
                symbol,
                "http_status=", response.status_code,
                "error_code=", data.get("error_code"),
                "retry_after=",
                (data.get("parameters") or {}).get("retry_after"),
                flush=True,
            )
            return False

        print(
            "[TG_UNKNOWN]",
            symbol,
            "http_status=", response.status_code,
            flush=True,
        )
        return None

    except Exception as error:
        # Не выводим URL: он содержит токен бота.
        print(
            "[TG_UNKNOWN]",
            symbol,
            "error_type=", type(error).__name__,
            flush=True,
        )
        return None
        
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


def create_entry_tracker(
    symbol, pattern, direction, price,
    test=False, entry_time=None
):
    with ENTRY_TRACKER_LOCK:
        if symbol in ENTRY_TRACKER or (test and ENTRY_TRACKER):
            return False

        ENTRY_TRACKER[symbol] = {
            "pattern": pattern,
            "direction": direction,
            "entry_price": price,
            "entry_time": (
                entry_time
                if entry_time is not None
                else datetime.now(UTC)
            ),
            "checked": set(),
            "test": test,
        }

    print(
        "[TEST_ENTRY_CREATED]" if test else "[ENTRY_CREATED]",
        symbol,
        "pattern=", pattern,
        "direction=", direction,
        "price=", price,
        flush=True,
    )
    return True


def update_entry_tracker(symbol, current_price, observed_ts, expected_item):
    # Only the tracker worker calls this function. Network and DB operations
    # stay outside the dictionary lock so scanning can create new entries.
    with ENTRY_TRACKER_LOCK:
        item = ENTRY_TRACKER.get(symbol)
        if item is not expected_item:
            return
        entry_price = item["entry_price"]
        entry_time = item["entry_time"]
        direction = item["direction"]
        pattern = item["pattern"]
        checked = set(item["checked"])
        is_test = item.get("test", False)

    if not math.isfinite(current_price) or current_price <= 0 or entry_price <= 0:
        return
    elapsed = observed_ts - entry_time.timestamp()
    result = (current_price - entry_price) / entry_price * 100
    if direction == "SHORT":
        result = -result

    for seconds, label in ENTRY_CHECKPOINTS.items():
        if elapsed < seconds or label in checked:
            continue
        if not is_test:
            saved = save_entry_result(
                symbol=symbol, pattern=pattern, direction=direction,
                entry_price=entry_price, entry_ts=entry_time.timestamp(),
                checkpoint_seconds=seconds, current_price=current_price,
                observed_ts=observed_ts, result_pct=result,
            )
            if not saved:
                break

        with ENTRY_TRACKER_LOCK:
            if ENTRY_TRACKER.get(symbol) is not item:
                return
            item["checked"].add(label)
            checked.add(label)
        print(
            "[TEST_ENTRY_RESULT]" if is_test else "[ENTRY_SAVED]",
            symbol, "pattern=", pattern, "checkpoint=", label,
            "direction=", direction, "entry=", entry_price,
            "current=", current_price, "result=", round(result, 2),
            "elapsed_sec=", round(elapsed, 1),
            "delay_sec=", round(max(0, elapsed - seconds), 1),
            flush=True,
        )

    with ENTRY_TRACKER_LOCK:
        if (ENTRY_TRACKER.get(symbol) is item
                and all(label in item["checked"]
                        for label in ENTRY_CHECKPOINTS.values())):
            del ENTRY_TRACKER[symbol]


def poll_entry_tracker():
    # Snapshot before fetching: a new entry cannot inherit an older quote.
    with ENTRY_TRACKER_LOCK:
        tracked = dict(ENTRY_TRACKER)
    if not tracked:
        return

    response = requests.get(
        "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    observed_ts = time.time()
    if data.get("code") != "0":
        raise ValueError("OKX ticker response code: " + str(data.get("code")))

    updated = set()
    for ticker in data.get("data", []):
        symbol = str(ticker.get("instId") or "").replace("-USDT-SWAP", "USDT")
        if symbol not in tracked or symbol in updated:
            continue
        try:
            price = float(ticker.get("last") or 0)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(price) or price <= 0:
            continue
        try:
            update_entry_tracker(symbol, price, observed_ts, tracked[symbol])
            updated.add(symbol)
        except Exception as error:
            print("[ENTRY_TRACKER_SYMBOL_ERROR]", symbol, str(error), flush=True)
    missing = set(tracked) - updated
    if missing:
        print("[ENTRY_TRACKER_MISSING_PRICE]", ",".join(sorted(missing)), flush=True)


def run_entry_tracker():
    print("[ENTRY_TRACKER_STARTED]", "interval_sec=", ENTRY_TRACKER_INTERVAL, flush=True)
    while True:
        started = time.monotonic()
        try:
            poll_entry_tracker()
        except Exception as error:
            print("[ENTRY_TRACKER_ERROR]", str(error), flush=True)
        duration = time.monotonic() - started
        time.sleep(max(1.0, ENTRY_TRACKER_INTERVAL - duration))

def refresh_signal_price(signal):
    import math

    try:
        response = requests.get(
            "https://www.okx.com/api/v5/market/tickers",
            params={"instType": "SWAP"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        observed_at = datetime.now(UTC)

        if data.get("code") != "0":
            raise ValueError("OKX returned an error")

        target = signal["symbol"]
        fresh_price = None

        for ticker in data.get("data", []):
            instrument = str(ticker.get("instId") or "")

            if not instrument.endswith("-USDT-SWAP"):
                continue

            symbol = instrument.replace("-USDT-SWAP", "USDT")

            if symbol == target:
                fresh_price = float(ticker.get("last") or 0)
                break

        if (
            fresh_price is None
            or not math.isfinite(fresh_price)
            or fresh_price <= 0
        ):
            raise ValueError("No valid fresh price")

        old_price = signal["price"]
        signal["price"] = fresh_price
        signal["entry_observed_at"] = observed_at

        print(
            "[ENTRY_PRICE_REFRESH]",
            target,
            "scan_price=", old_price,
            "fresh_price=", fresh_price,
            "observed_at=", observed_at.isoformat(),
            flush=True,
        )
        return True

    except Exception as error:
        print(
            "[ENTRY_PRICE_REFRESH_ERROR]",
            signal.get("symbol"),
            str(error),
            flush=True,
        )
        return False

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

        print(
            "[FUTURES_FLOW]",
            symbol,
            "delta=", round(futures_delta, 2),
            "imbalance=", round(futures_imbalance, 2),
            "ready=", futures_ready,
            "quality=", futures_5m.get("quality"),
            "continuous_sec=", futures_5m.get("stream_continuous_seconds"),
            "stale_sec=", futures_5m.get("stream_stale_seconds"),
            "generation=", futures_5m.get("stream_generation"),
            flush=True,
        )

        if not futures_ready:
            print(
                "[PATTERN_SKIP_NOT_READY]",
                symbol,
                "Futures 5m window is not ready",
                flush=True,
            )
            continue

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
            "futures_flow": dict(futures_5m),
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

threading.Thread(
    target=run_entry_tracker,
    name="entry-tracker",
    daemon=True,
).start()

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

        symbol = ticker.get("instId", "").replace("-USDT-SWAP", "USDT")
        current_price = float(ticker.get("last") or 0)

        if ENTRY_TRACKER_TEST and symbol and current_price > 0:
            create_entry_tracker(symbol, "TEST", "LONG", current_price, test=True)

        signal = analyze(ticker)
    
        if not signal:
            no_signal += 1
            continue

        if not refresh_signal_price(signal):
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

        pattern = signal.get("decision", {}).get("pattern")
        pattern_direction = signal.get("decision", {}).get("direction")

        if pattern_direction == "UP":
            tracker_direction = "LONG"
        elif pattern_direction == "DOWN":
            tracker_direction = "SHORT"
        else:
            tracker_direction = None

        if tracker_direction:
            create_entry_tracker(
                signal["symbol"],
                pattern,
                tracker_direction,
                signal["price"],
                entry_time=signal["entry_observed_at"],
            )

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
