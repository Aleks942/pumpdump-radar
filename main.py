import os
import csv
import time
import random
import threading
import math
import requests
from ticker_metrics import approx_quote_volume_usdt, spread_bps
from mover_discovery import (observe as observe_movers, inspect_price_quotes,
                             select_continuing_moves, select_threshold_moves)
from mover_forward_audit import observe as observe_mover_forward, report as report_mover_forward
from bybit_mover_watch import fetch as fetch_bybit_movers, sample as sample_bybit_movers
from multi_window_watch import (sample_prices as sample_multi_window_prices,
                                select_events as select_multi_window_events,
                                mark_sent as mark_multi_window_sent)
from pump_info_audit import (
    record_delivered as record_pump_info_event,
    observe_quotes as observe_pump_info_quotes,
    print_report as report_pump_info_audit,
    daily_digest as send_pump_info_daily_digest,
)
from pump_lifecycle import observe as observe_pump_lifecycle
from pump_edge_report import print_candle_edge_report
from pump_exit_research import print_exit_research
from pump_factor_report import print_factor_edge
from pump_forward_oi import register_forward_oi, report_forward_oi
from datetime import datetime, UTC
from spot_cvd_engine import get_spot_cvd
from telegram_builder import build_short_message, build_market_watch_message
from coinglass_context import schedule as schedule_coinglass_context
from public_exchange_context import schedule as schedule_public_exchange_context
from move_verifier import schedule as schedule_move_verification
from pump_peak_watch import observe as observe_pump_peak, acknowledge as ack_pump_peak, message as pump_peak_message
from pump_30_alerts import observe as observe_30_pumps, acknowledge as ack_30_pump, message as pump_30_message
from post_pump_notifications import deliver as deliver_post_pump_notifications
from pump_cycle_research import (observe as observe_pump_cycle,
                                 observe_outcomes as observe_cycle_outcomes,
                                 report_outcomes as report_cycle_outcomes)
from post_pump_short_research import (
    observe as observe_post_pump_short,
    report as report_post_pump_short,
)
from post_pump_short_strategy import observe as observe_complete_post_pump_short
from pump_watch_forward import observe as observe_watch_forward, hourly_report as report_watch_forward
from pump_retrace_stats import (register_event as register_retrace_event,
                                observe_tickers as observe_retrace_tickers,
                                hourly_report as report_retrace_stats,
                                pending_daily_digest as pending_retrace_daily_digest,
                                acknowledge_daily_digest as ack_retrace_daily_digest)
from trend_probability_gate import consider as confirm_trend_edge, advance as advance_trend_study, hourly_report as report_trend_study
from horizon_audit import record as record_horizon_case, advance as advance_horizon_audit, hourly_report as report_horizon_audit
from flow_bridge import (enqueue_candidate as enqueue_flow_candidate,
                         deliver_candidate as deliver_flow_candidate,
                         probe_bridge)
from pump_flow_confirmed import dispatch as dispatch_confirmed_flow
from trade_flow_collector_v3 import get_futures_windows
from okx_trade_stream_v3 import run_stream_forever
from pattern_detector import detect_pattern
from oi_collector import get_oi_5m
from aggregated_oi import watch_symbol, get_aggregated_oi

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
# Strict-by-default: no unverified market commentary in Telegram.
# LEGACY is an explicit manual rollback setting, not the default.
TELEGRAM_POLICY = os.getenv("TELEGRAM_POLICY", "CONFIRMED_ONLY").strip().upper()
if TELEGRAM_POLICY not in ("CONFIRMED_ONLY", "LEGACY"):
    TELEGRAM_POLICY = "CONFIRMED_ONLY"
print(f"[TELEGRAM_POLICY] {TELEGRAM_POLICY} "
      "strict_first_touch_probability_gate=true", flush=True)


PUMP_THRESHOLD_20M = float(os.getenv("PUMP_THRESHOLD_20M", 6))
DUMP_THRESHOLD_20M = float(os.getenv("DUMP_THRESHOLD_20M", -6))

PUMP_THRESHOLD_40M = float(os.getenv("PUMP_THRESHOLD_40M", 8))
DUMP_THRESHOLD_40M = float(os.getenv("DUMP_THRESHOLD_40M", -8))



MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", 10000000))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", 3600))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 347))
SCAN_SLEEP = int(os.getenv("SCAN_SLEEP", 60))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", 1000))

ANTI_LATE_MAX_5M = float(os.getenv("ANTI_LATE_MAX_5M", 5.5))
ANTI_LATE_EXHAUSTION_MIN_MOVE = float(os.getenv("ANTI_LATE_EXHAUSTION_MIN_MOVE", 2.5))
ANTI_LATE_RETRACE_SHARE = float(os.getenv("ANTI_LATE_RETRACE_SHARE", 25.0))
ANTI_LATE_OPPOSITE_1M = float(os.getenv("ANTI_LATE_OPPOSITE_1M", 0.25))

symbol_states = {}
OI_HISTORY = {}
OI_TIME_HISTORY = {}
signal_memory = {}

# Bounded, information-only price-threshold notifications. They do not
# influence trade approval, signal_memory or entry trackers.
_MARKET_WATCH_SPACING_SEC = 0  # No global suppression between different symbols
_MARKET_WATCH_SYMBOL_SEC = 10 * 60  # Suppress duplicate sustained moves per symbol
_MARKET_WATCH_DAILY_MAX = None  # No daily cap on distinct market moves
_market_watch_delivery = {
    "day": None, "count": 0, "last_sent": None,
    "symbols": {}, "retry_after": 0.0,
}


def maybe_send_market_watch(signal, reason, now=None):
    """Notify about a market move, never about a proven trading opportunity.

    Only called on a real, chronologically sampled 5m market move.
    Reserve frequency budget AFTER Telegram confirms successful delivery.
    """
    now = time.time() if now is None else float(now)
    if not math.isfinite(now):
        return False
    symbol = signal.get("symbol")
    side = (signal.get("decision") or {}).get("direction")
    kind = signal.get("type")
    if (not isinstance(symbol, str) or not symbol.endswith("USDT")
            or (kind, side) not in (("PUMP", "UP"), ("DUMP", "DOWN"))):
        return False

    # CONFIRMED_ONLY applies to trade-direction alerts, not to factual market
    # movement notices. A threshold crossing may be shown as a yellow
    # informational card; LONG/SHORT still requires the strict confirmation
    # pipeline below. Existing per-symbol/per-window dedup prevents spam.

    day = int(now // 86400)
    state = _market_watch_delivery
    if day != state["day"]:
        state["day"], state["count"] = day, 0
    if now < state["retry_after"]:
        return False
    previous = state["last_sent"]
    if previous is not None and now - previous < _MARKET_WATCH_SPACING_SEC:
        return False
    # Independent thresholds are distinct informational events. Keep the
    # 10-minute suppression WITHIN a period while allowing 10/20/30m alerts
    # alongside a 5m alert for the same coin and direction.
    period = str(signal.get("window") or "~5m").lstrip("~")
    key = (symbol, side, period)
    if now - state["symbols"].get(key, -float("inf")) < _MARKET_WATCH_SYMBOL_SEC:
        return False
    try:
        message = build_market_watch_message(signal, reason)
        accepted = send_telegram(message, symbol=symbol)
    except Exception as exc:
        print("[PUMP_INFO_ERROR]", symbol, type(exc).__name__, flush=True)
        accepted = False
    if accepted is not True:
        state["retry_after"] = now + 300
        print("[PUMP_INFO_SEND_FAILED]", symbol, flush=True)
        return False
    state["last_sent"] = now
    state["symbols"][key] = now
    state["count"] += 1
    print("[PUMP_INFO_SENT]", symbol, "type=", kind,
          "count_today=", state["count"],
          "trade_approved=false informational_only=true", flush=True)
    # Observational journal must NEVER influence Telegram ACK or dedup logic.
    try:
        if not record_pump_info_event(signal, reason, now=now):
            print("[PUMP_INFO_AUDIT_NOT_RECORDED]", symbol,
                  "reason=invalid_or_duplicate", flush=True)
    except Exception as audit_error:
        print("[PUMP_INFO_AUDIT_SAVE_ERROR]", symbol,
              type(audit_error).__name__, flush=True)
    # Optional CoinGlass study remains behind successful Telegram ACK.
    try:
        schedule_coinglass_context(symbol, observed_at=now)
    except Exception as cg_exc:
        print("[COINGLASS_CONTEXT_SCHEDULE_ERROR]",
              symbol, type(cg_exc).__name__, flush=True)

    # Every delivered move starts a strict verifier on the exchange that
    # detected it. Detection source must not decide whether confirmation exists.
    # OKX uses its existing swap WebSocket + aggregated OI + Spot fallback;
    # Bybit keeps its dedicated post-trigger live stream. Missing evidence
    # remains UNKNOWN and can never become a confirmed LONG/SHORT.
    try:
        scheduled = schedule_move_verification(
            dict(signal),
            send_telegram,
            now=now,
        )
        print(
            "[MOVE_VERIFY_ROUTE]",
            symbol,
            "exchange=", str(signal.get("exchange") or "UNKNOWN").upper(),
            "scheduled=", scheduled is True,
            flush=True,
        )
    except Exception as verify_exc:
        print(
            "[MOVE_VERIFY_SCHEDULE_ERROR]",
            symbol,
            type(verify_exc).__name__,
            flush=True,
        )
    return True


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

        highs = []
        lows = []

        for candle in candles:
            try:
                highs.append(float(candle[2]))
                lows.append(float(candle[3]))
            except (TypeError, ValueError, IndexError):
                pass

        newest_open = float(newest[1])
        newest_close = float(newest[4])

        last_1m_change = 0.0
        if newest_open > 0:
            last_1m_change = (
                (newest_close - newest_open)
                / newest_open
                * 100
            )

        window_high = max(highs) if highs else end_price
        window_low = min(lows) if lows else end_price

        return {
            "start_price": start_price,
            "end_price": end_price,
            "change": change,
            "last_1m_change": last_1m_change,
            "window_high": window_high,
            "window_low": window_low,
        }

    except Exception as e:
        print("[CANDLES EXCEPTION]", raw_symbol, e)
        return None




def evaluate_anti_late(move_type, move):
    """
    Conservative anti-late / exhaustion filter for the 5m trigger.

    It blocks only two cases:
    1) price is already too extended for an early-entry signal;
    2) a strong move has materially retraced from its extreme while the
       newest 1m candle is already moving against the trigger direction.

    This filter does not change OI/CVD/pattern logic. It only decides whether
    the already-confirmed price trigger is too late to alert.
    """

    change = float(move.get("change") or 0.0)
    magnitude = abs(change)
    last_1m = float(move.get("last_1m_change") or 0.0)
    end_price = float(move.get("end_price") or 0.0)
    window_high = float(move.get("window_high") or end_price)
    window_low = float(move.get("window_low") or end_price)

    retrace_pct = 0.0

    if move_type == "PUMP" and window_high > 0:
        retrace_pct = max(
            0.0,
            (window_high - end_price) / window_high * 100,
        )
        opposite_1m = last_1m <= -ANTI_LATE_OPPOSITE_1M

    elif move_type == "DUMP" and window_low > 0:
        retrace_pct = max(
            0.0,
            (end_price - window_low) / window_low * 100,
        )
        opposite_1m = last_1m >= ANTI_LATE_OPPOSITE_1M

    else:
        opposite_1m = False

    retrace_share = (
        retrace_pct / magnitude * 100
        if magnitude > 0
        else 0.0
    )

    if magnitude >= ANTI_LATE_MAX_5M:
        return {
            "allow": False,
            "reason": "EXTENDED_MOVE",
            "move_pct": change,
            "last_1m_pct": last_1m,
            "retrace_pct": retrace_pct,
            "retrace_share": retrace_share,
        }

    if (
        magnitude >= ANTI_LATE_EXHAUSTION_MIN_MOVE
        and retrace_share >= ANTI_LATE_RETRACE_SHARE
        and opposite_1m
    ):
        return {
            "allow": False,
            "reason": "EXHAUSTION_REVERSAL",
            "move_pct": change,
            "last_1m_pct": last_1m,
            "retrace_pct": retrace_pct,
            "retrace_share": retrace_share,
        }

    return {
        "allow": True,
        "reason": "EARLY_ENOUGH",
        "move_pct": change,
        "last_1m_pct": last_1m,
        "retrace_pct": retrace_pct,
        "retrace_share": retrace_share,
    }


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
    test=False, entry_time=None, context=None
):
    entry_at = (
        entry_time
        if entry_time is not None
        else datetime.now(UTC)
    )

    with ENTRY_TRACKER_LOCK:
        if symbol in ENTRY_TRACKER or (test and ENTRY_TRACKER):
            return False

        item = {
            "pattern": pattern,
            "direction": direction,
            "entry_price": price,
            "entry_time": entry_at,
            "checked": set(),
            "test": test,
            "context_id": None,
        }

        ENTRY_TRACKER[symbol] = item

    context_id = None

    if not test:
        try:
            from market_memory import save_entry_context

            context_id = save_entry_context(
                symbol=symbol,
                pattern=pattern,
                direction=direction,
                entry_price=price,
                entry_ts=entry_at.timestamp(),
                signal=context,
            )

        except Exception as error:
            print(
                "[ENTRY_CONTEXT_ERROR]",
                symbol,
                type(error).__name__,
                str(error),
                flush=True,
            )

        with ENTRY_TRACKER_LOCK:
            if ENTRY_TRACKER.get(symbol) is item:
                item["context_id"] = context_id

        if context_id is None:
            print(
                "[ENTRY_CONTEXT_MISSING]",
                symbol,
                "entry_ts=", entry_at.timestamp(),
                flush=True,
            )

    print(
        "[TEST_ENTRY_CREATED]" if test else "[ENTRY_CREATED]",
        symbol,
        "pattern=", pattern,
        "direction=", direction,
        "price=", price,
        "context_id=", context_id,
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
    print(
        "[ENTRY_TRACKER_STARTED]",
        "interval_sec=", ENTRY_TRACKER_INTERVAL,
        flush=True,
    )

    try:
        from candle_path import start_candle_path
        start_candle_path()
    except Exception as error:
        print(
            "[CANDLE_PATH_START_ERROR]",
            type(error).__name__,
            str(error),
            flush=True,
        )

    while True:
        started = time.monotonic()

        try:
            poll_entry_tracker()
        except Exception as error:
            print(
                "[ENTRY_TRACKER_ERROR]",
                str(error),
                flush=True,
            )

        duration = time.monotonic() - started

        time.sleep(
            max(1.0, ENTRY_TRACKER_INTERVAL - duration)
        )


def refresh_signal_price(signal):
    import math

    try:
        # Fetch just the selected swap, rather than downloading every
        # market ticker again for each signal. The fresh quote is still
        # mandatory, and any invalid response fails closed.
        target = signal["symbol"]
        if (not isinstance(target, str) or not target.endswith("USDT")
                or len(target) <= 4):
            raise ValueError("Invalid target symbol")
        instrument = f"{target[:-4]}-USDT-SWAP"
        response = requests.get(
            "https://www.okx.com/api/v5/market/ticker",
            params={"instId": instrument},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        observed_at = datetime.now(UTC)

        if not isinstance(data, dict) or data.get("code") != "0":
            raise ValueError("OKX returned an error")

        tickers = data.get("data")
        if not isinstance(tickers, list) or len(tickers) != 1:
            raise ValueError("No unique fresh ticker")
        ticker = tickers[0]
        if not isinstance(ticker, dict) or ticker.get("instId") != instrument:
            raise ValueError("Ticker instrument mismatch")
        fresh_price = float(ticker.get("last") or 0)

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
        raw_symbol = ticker.get("instId") if isinstance(ticker, dict) else None
        if (
            not isinstance(raw_symbol, str)
            or not raw_symbol.endswith("-USDT-SWAP")
        ):
            return None
        symbol = raw_symbol.replace("-USDT-SWAP", "USDT")

        raw_price = ticker.get("last")
        if raw_price in (None, ""):
            return None
        price = float(raw_price)

        if not math.isfinite(price) or price <= 0:
            return None

        # Tiny-priced contracts are allowed only with a healthy live spread.
        # They previously failed a hard price floor even when highly liquid.
        if price < 0.01:
            spread = spread_bps(ticker)
            if spread is None or spread > 40.0:
                return None

        # OKX swap volCcy24h is BASE currency, not USDT turnover.
        # Use a conservative approx USDT notional for liquidity screening.
        volume_24h = approx_quote_volume_usdt(ticker)

        if volume_24h is None or volume_24h < MIN_VOLUME_24H:
            return None

    except (KeyError, TypeError, ValueError, OverflowError):
        # Empty/invalid exchange rows are expected occasionally; skip them
        # without flooding production logs.
        return None
    except Exception as e:
        print("[ANALYZE TICKER ERROR]", type(e).__name__, str(e)[:120], flush=True)
        return None

    best_signal = None


    watch_symbol(symbol)
    oi_data = get_oi_5m(symbol)

    print(
        "[OI_5M]",
        symbol,
        "quality=", oi_data["quality"],
        "period_sec=", (
            round(oi_data["period_sec"], 1)
            if oi_data["period_sec"] is not None
            else None
        ),
        "age_sec=", (
            round(oi_data["age_sec"], 1)
            if oi_data["age_sec"] is not None
            else None
        ),
        "change_pct=", (
            round(oi_data["change_pct"], 3)
            if oi_data["change_pct"] is not None
            else None
        ),
        flush=True,
    )

    if not oi_data["ready"]:
        return None

    oi = oi_data["oi"]
    oi_short_change = oi_data["change_pct"]

    try:
        save_oi_snapshot(symbol, oi)
    except Exception as e:
        print(
            "[SAVE_OI_SNAPSHOT_ERROR]",
            symbol,
            e,
            flush=True,
        )

    
    # ====================================
    # BEST SIGNAL SELECTOR
    # ====================================
    
    best_signal = None
   
    pattern_called = False
    
    for window_name, cfg in (("5m", TIME_WINDOWS["5m"]),):

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

        pattern_name = decision.get("pattern")

        if pattern_name == "NONE":
            continue

        # Aggregated OI is a mandatory confirmation layer.
        # Accept HIGH 3/3, or MEDIUM 2/2 only when the third exchange is
        # confirmed UNSUPPORTED. Errors, stale data and OI direction
        # disagreement stay blocked.
        aggregated_oi = get_aggregated_oi(symbol)
        agg_5m = (
            (aggregated_oi.get("windows") or {}).get("5m")
            or {}
        )
        agg_5m_ready = agg_5m.get("ready") is True
        agg_5m_change = agg_5m.get("change_pct")

        if not agg_5m_ready or agg_5m_change is None:
            print(
                "[PATTERN_SKIP_AGG_OI_NOT_READY]",
                symbol,
                "pattern=", pattern_name,
                "quality=", agg_5m.get("quality"),
                "ready_sources=", agg_5m.get("ready_sources"),
                "supported_sources=", agg_5m.get("supported_sources"),
                "confidence=", agg_5m.get("confidence"),
                "mode=", agg_5m.get("consensus_mode"),
                flush=True,
            )
            continue

        requires_oi_expansion = pattern_name in (
            "NEW_LONG_BUILDUP",
            "NEW_SHORT_BUILDUP",
        )
        requires_oi_contraction = pattern_name in (
            "SHORT_SQUEEZE",
            "LONG_LIQUIDATION",
        )

        aggregated_oi_confirms = (
            (requires_oi_expansion and agg_5m_change > 0)
            or
            (requires_oi_contraction and agg_5m_change < 0)
        )

        if not aggregated_oi_confirms:
            print(
                "[PATTERN_SKIP_AGG_OI_MISMATCH]",
                symbol,
                "pattern=", pattern_name,
                "local_oi_5m=", round(oi_short_change, 3),
                "aggregated_oi_5m=", round(agg_5m_change, 3),
                flush=True,
            )
            continue

        print(
            "[PATTERN_AGG_OI_CONFIRMED]",
            symbol,
            "pattern=", pattern_name,
            "local_oi_5m=", round(oi_short_change, 3),
            "aggregated_oi_5m=", round(agg_5m_change, 3),
            "confidence=", agg_5m.get("confidence"),
            "mode=", agg_5m.get("consensus_mode"),
            "direction_consensus=", agg_5m.get("direction_consensus"),
            flush=True,
        )

        anti_late = evaluate_anti_late(
            move_type=move_type,
            move=move,
        )

        print(
            "[ANTI_LATE]",
            symbol,
            "allow=", anti_late["allow"],
            "reason=", anti_late["reason"],
            "move_pct=", round(anti_late["move_pct"], 3),
            "last_1m_pct=", round(anti_late["last_1m_pct"], 3),
            "retrace_pct=", round(anti_late["retrace_pct"], 3),
            "retrace_share=", round(anti_late["retrace_share"], 1),
            flush=True,
        )

        if not anti_late["allow"]:
            print(
                "[ANTI_LATE_SKIP]",
                symbol,
                "pattern=", pattern_name,
                "reason=", anti_late["reason"],
                flush=True,
            )
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
            "aggregated_oi": aggregated_oi,
            "futures_flow": dict(futures_5m),
            "spot_cvd": spot_cvd,
            "liquidations": liquidations,
            "decision": decision,
            "anti_late": anti_late,
            
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

# Read-only verification of credentials and network path. No trading signals.
threading.Thread(target=probe_bridge, name="pump-flow-probe", daemon=True).start()

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
startup_sent = send_telegram("🚀 PumpDump Radar V2 ONLINE", symbol="SYSTEM")
print("[STARTUP_TELEGRAM] sent=", startup_sent is True, flush=True)

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

register_forward_oi()
_last_edge_report_mono = 0.0

while True:
    if time.monotonic() - _last_edge_report_mono >= 3600:
        print_candle_edge_report()
        print_exit_research()
        print_factor_edge()
        report_forward_oi()
        report_retrace_stats()
        if TELEGRAM_POLICY == "CONFIRMED_ONLY":
            report_trend_study()
            report_horizon_audit()
        _last_edge_report_mono = time.monotonic()

    print("[SCAN] scanning market...")

    tickers = get_market_tickers()
    print(f"[TICKERS] {len(tickers)}")
    try:
        observe_retrace_tickers(tickers, "OKX")
        daily_retrace = pending_retrace_daily_digest()
        if daily_retrace:
            daily_sent = send_telegram(
                daily_retrace["text"], symbol="PUMP_STATS"
            )
            ack_retrace_daily_digest(
                daily_retrace["session_key"], daily_sent
            )
            print(
                "[PUMP_RETRACE_DAILY]",
                "session=", daily_retrace["session_key"],
                "sent=", daily_sent is True,
                flush=True,
            )
    except Exception as retrace_exc:
        print("[PUMP_RETRACE_OKX_ERROR]", type(retrace_exc).__name__, flush=True)
    # Quote markouts use the same OKX market snapshot. No new REST calls.
    try:
        observe_pump_info_quotes(tickers, "OKX")
    except Exception as audit_error:
        print("[PUMP_INFO_AUDIT_OKX_ERROR]",
              type(audit_error).__name__, flush=True)

    # Read the existing market-wide ticker history with zero extra HTTP calls.
    mover_rows = []
    try:
        mover_rows = observe_movers(tickers)
        # Identical ticker batch: 5/15/30m quote markouts; NO extra API calls.
        observe_mover_forward(mover_rows)
        report_mover_forward()
    except Exception as mover_exc:
        print("[MOVER_RESEARCH_ERROR]", type(mover_exc).__name__, flush=True)

    # Early informational notices now require price follow-through AFTER
    # crossing the user-controlled 5m threshold. No OI/CVD requests, no
    # changes to the independent strict trade entry gate.
    early_continuations = []
    try:
        quote_rows = inspect_price_quotes(tickers)
        # Research-only lifecycle based on existing quotes. No extra HTTP,
        # Telegram or effect on the strict trading decision.
        observe_pump_lifecycle(quote_rows, TIME_WINDOWS["5m"]["pump"])
        # Collect at the first observed threshold crossing, even if subsequent
        # continuation fails or Telegram is rate-limited. The collector's own
        # 15m global / 30m per-symbol throttles bound external requests.
        first_moves = select_threshold_moves(
            quote_rows, TIME_WINDOWS["5m"]["pump"],
            -TIME_WINDOWS["5m"]["dump"])
        if first_moves:
            schedule_public_exchange_context(first_moves[0]["symbol"])
        early_continuations = select_continuing_moves(
            quote_rows,
            TIME_WINDOWS["5m"]["pump"],
            -TIME_WINDOWS["5m"]["dump"],
        )
        # Ordinary informational PUMP/DUMP notifications follow the user's
        # observed 5m percentage thresholds. The stronger continuation study
        # remains separate and never gates these notices.
        for market_move in first_moves:
            market_move["exchange"] = "OKX"
            try:
                register_retrace_event(market_move)
            except Exception as retrace_exc:
                print("[PUMP_RETRACE_REGISTER_ERROR]",
                      market_move.get("symbol"),
                      type(retrace_exc).__name__, flush=True)
            maybe_send_market_watch(market_move, "PRICE_THRESHOLD")
    except Exception as info_exc:
        print("[PUMP_CONTINUATION_ERROR]", type(info_exc).__name__, flush=True)

    # The longer timeframes use separately sampled, real 10/20/30m quotes.
    # This watch never affects the existing OKX/Bybit 5m notification path or
    # strict OI/CVD/price entry logic.
    try:
        extended_okx_quotes = sample_multi_window_prices(tickers, "OKX")
    except Exception as extended_exc:
        print("[MULTI_WINDOW_OKX_ERROR]", type(extended_exc).__name__, flush=True)
        extended_okx_quotes = []

    extended_bybit_quotes = []
    # Separate Bybit linear watch: does not alter OKX trade scanning.
    # Same sender deduplicates cross-exchange symbol+direction observations.
    try:
        bybit_tickers = fetch_bybit_movers()
        if bybit_tickers is not None:
            try:
                observe_retrace_tickers(bybit_tickers, "Bybit")
            except Exception as retrace_exc:
                print("[PUMP_RETRACE_BYBIT_ERROR]",
                      type(retrace_exc).__name__, flush=True)
            try:
                observe_pump_info_quotes(bybit_tickers, "Bybit")
            except Exception as audit_error:
                print("[PUMP_INFO_AUDIT_BYBIT_ERROR]",
                      type(audit_error).__name__, flush=True)
            bybit_quotes = sample_bybit_movers(bybit_tickers)
            extended_bybit_quotes = sample_multi_window_prices(
                bybit_tickers, "Bybit")
            bybit_moves = select_threshold_moves(
                bybit_quotes, TIME_WINDOWS["5m"]["pump"],
                -TIME_WINDOWS["5m"]["dump"])
            print("[BYBIT_WATCH] tickers=", len(bybit_tickers),
                  "threshold_moves=", len(bybit_moves), flush=True)
            for market_move in bybit_moves:
                market_move["exchange"] = "Bybit"
                try:
                    register_retrace_event(market_move)
                except Exception as retrace_exc:
                    print("[PUMP_RETRACE_REGISTER_ERROR]",
                          market_move.get("symbol"),
                          type(retrace_exc).__name__, flush=True)
                maybe_send_market_watch(market_move, "PRICE_THRESHOLD")
    except Exception as bybit_exc:
        print("[BYBIT_WATCH_ERROR]", type(bybit_exc).__name__, flush=True)

    # Each longer period can notify independently even if 5m is quiet.
    try:
        extended_events = select_multi_window_events(
            extended_okx_quotes + extended_bybit_quotes, TIME_WINDOWS)
        print("[MULTI_WINDOW_WATCH] 10m_20m_30m_events=",
              len(extended_events), "okx_quotes=", len(extended_okx_quotes),
              "bybit_quotes=", len(extended_bybit_quotes), flush=True)
        for event in extended_events:
            try:
                register_retrace_event(event)
            except Exception as retrace_exc:
                print("[PUMP_RETRACE_REGISTER_ERROR]",
                      event.get("symbol"),
                      type(retrace_exc).__name__, flush=True)
            if maybe_send_market_watch(event, "PRICE_THRESHOLD"):
                mark_multi_window_sent(event)
    except Exception as extended_exc:
        print("[MULTI_WINDOW_WATCH_ERROR]", type(extended_exc).__name__,
              flush=True)

    # Summaries are explicitly non-trading observations, not entry alerts.
    try:
        report_pump_info_audit()
        send_pump_info_daily_digest(
            lambda msg: send_telegram(msg, symbol="PUMP_AUDIT"))
    except Exception as audit_error:
        print("[PUMP_INFO_AUDIT_REPORT_ERROR]",
              type(audit_error).__name__, flush=True)

    # Parallel research-only watchlist. Does NOT gate or alter the original
    # price/OI/CVD/long-liquidation signal conditions below.
    try:
        watch_events = observe_pump_peak(tickers)
        for watch_event in watch_events:
            oi_watch = None
            futures_imbalance = None
            if watch_event["level"] >= 2 and TELEGRAM_POLICY == "LEGACY":
                try:
                    oi_watch = get_oi_5m(watch_event["symbol"])
                    flow_watch = get_futures_windows(watch_event["symbol"]).get("5m") or {}
                    if flow_watch.get("window_ready"):
                        futures_imbalance = flow_watch.get("imbalance_pct")
                except Exception as watch_context_exc:
                    print("[PUMP_WATCH_CONTEXT_ERROR]",
                          type(watch_context_exc).__name__, flush=True)
            if TELEGRAM_POLICY == "CONFIRMED_ONLY":
                # Silent samples remain observable in SQLite. Do not turn a
                # possible dump into a trading confirmation.
                ack_pump_peak(watch_event, False, research_only=True)
                print("[PUMP_WATCH_RESEARCH_ONLY]", watch_event["symbol"],
                      "level=", watch_event["level"], flush=True)
            else:
                watch_sent = send_telegram(
                    pump_peak_message(watch_event, oi_watch, futures_imbalance),
                    symbol=watch_event["symbol"],
                )
                ack_pump_peak(watch_event, watch_sent)
                print("[PUMP_WATCH_NOTIFY]", watch_event["symbol"],
                      "level=", watch_event["level"],
                      "sent=", watch_sent is True, flush=True)
    except Exception as watch_exc:
        print("[PUMP_WATCH_LOOP_ERROR]", type(watch_exc).__name__, flush=True)

    # Opt-in informational +30% watch. Independent of Order Flow and the
    # CONFIRMED_ONLY gate for actual trading signals. No new market requests.
    if (os.getenv("PUMP_30_INFO_ALERTS", "0") == "1"
            and os.getenv("POST_PUMP_TELEGRAM", "1") != "1"):
        try:
            for pump30_event in observe_30_pumps(tickers):
                delivered = send_telegram(
                    pump_30_message(pump30_event),
                    symbol=pump30_event["symbol"],
                )
                ack_30_pump(pump30_event, delivered)
                print("[PUMP_30_INFO]", pump30_event["symbol"],
                      "stage=", pump30_event["stage"],
                      "sent=", delivered is True, flush=True)
        except Exception as pump30_error:
            print("[PUMP_30_INFO_ERROR]",
                  type(pump30_error).__name__, flush=True)

    # Research-only pump lifecycle: correlates observed chronological pivots
    # with authenticated Bybit Flow sell pressure, never sends Telegram.
    # Works from the same ticker batch and existing Pump Watch SQLite volume.
    try:
        observe_pump_cycle(tickers, seeded_pumps=early_continuations)
        observe_cycle_outcomes(tickers)
        report_cycle_outcomes()
    except Exception as cycle_exc:
        print("[PUMP_CYCLE_ERROR]", type(cycle_exc).__name__, flush=True)

    # Complete independent POST-PUMP SHORT strategy. Discovers +30% pumps
    # directly from the same OKX ticker list, including late pullbacks
    # excluded by earlier Pump Watch. Max three candidates checked per scan,
    # 5m/15m/H1 closed candles, verified 5m Spot + cached OI/futures.
    # PAPER ONLY: never opens positions or overrides trading gates.
    try:
        observe_complete_post_pump_short(tickers)
    except Exception as complete_short_exc:
        print("[COMPLETE_POST_PUMP_SHORT_LOOP_ERROR]",
              type(complete_short_exc).__name__, flush=True)

    # User-enabled complete lifecycle. Preserves every strategy evidence gate;
    # messages explicitly label READY as a paper plan, never a live order.
    if os.getenv("POST_PUMP_TELEGRAM", "1") == "1":
        try:
            deliver_post_pump_notifications(send_telegram, tickers)
        except Exception as notify_error:
            print("[POST_PUMP_NOTIFY_ERROR]", type(notify_error).__name__, flush=True)

    # Retain the prior Pump Cycle research cohorts as a separate experiment.
    try:
        observe_post_pump_short(tickers)
        report_post_pump_short()
    except Exception as post_short_exc:
        print("[POST_PUMP_SHORT_LOOP_ERROR]",
              type(post_short_exc).__name__, flush=True)

    # Evaluate forward checkpoints from the same already-fetched ticker list.
    # Research-only: NEVER modifies original signal decisions or orders.
    try:
        observe_watch_forward(tickers)
        report_watch_forward()
        if TELEGRAM_POLICY == "CONFIRMED_ONLY":
            advance_trend_study()
            advance_horizon_audit()
    except Exception as forward_exc:
        print("[PUMP_WATCH_FORWARD_LOOP_ERROR]",
              type(forward_exc).__name__, flush=True)

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
    
        try:
            enqueue_flow_candidate(signal)
            if os.getenv("PUMP_FLOW_MODE", "OFF").upper() == "RESEARCH" and os.getenv("PUMP_FLOW_URL") and os.getenv("PUMP_FLOW_SHARED_TOKEN"):
                threading.Thread(target=deliver_flow_candidate, args=(dict(signal),), daemon=True).start()
        except Exception as exc:
            print("[FLOW_BRIDGE_ERROR]", type(exc).__name__, flush=True)

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

        if TELEGRAM_POLICY == "CONFIRMED_ONLY":
            verified, reason = confirm_trend_edge(signal)
            if reason in ("EDGE_UNPROVEN", "CONFIRMED"):
                record_horizon_case(signal)
            if not verified:
                # No separate message here: price-only market watch runs
                # above, preventing duplicates and preserving this strict gate.
                print("[TRADE_ALERT_SUPPRESSED]", signal["symbol"],
                      "reason=", reason, flush=True)
                continue

        previous_signal_state = signal_memory.get(signal["symbol"])
        if previous_signal_state is not None:
            previous_signal_state = dict(previous_signal_state)

       
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

        if os.getenv("PUMP_FLOW_MODE", "OFF").upper() == "ENFORCE":
            # Reserve duplicate suppression only after successful Telegram delivery.
            if previous_signal_state is None:
                signal_memory.pop(signal["symbol"], None)
            else:
                signal_memory[signal["symbol"]] = previous_signal_state

            def record_confirmed_flow(confirmed, tracked_dir=tracker_direction,
                                      tracked_pattern=pattern):
                should_send_signal(confirmed)
                if tracked_dir:
                    created = create_entry_tracker(
                        confirmed["symbol"], tracked_pattern, tracked_dir,
                        confirmed["price"],
                        entry_time=confirmed["entry_observed_at"],
                        context=confirmed,
                    )
                    if not created:
                        print("[ENTRY_ALREADY_TRACKED]", confirmed["symbol"], flush=True)
                register_signal(confirmed)
                print("[PUMP_FLOW_CONFIRMED_RECORDED]", confirmed["symbol"], flush=True)

            queued = dispatch_confirmed_flow(
                signal, send_telegram, build_short_message,
                on_sent=record_confirmed_flow,
            )
            if queued:
                print("[PUMP_FLOW] QUEUED_FOR_CONFIRMATION",
                      signal["symbol"], flush=True)
            else:
                print("[PUMP_FLOW] NOT_QUEUED", signal["symbol"], flush=True)
            continue

        sent = send_telegram(
            build_short_message(signal),
            symbol=signal["symbol"],
        )

        if sent is not True:
            if sent is False:
                if previous_signal_state is None:
                    signal_memory.pop(signal["symbol"], None)
                else:
                    signal_memory[signal["symbol"]] = previous_signal_state

            print(
                "[SIGNAL_SEND_UNCONFIRMED]",
                signal["symbol"],
                "status=", "REJECTED" if sent is False else "UNKNOWN",
                flush=True,
            )
            continue

        quote_to_confirmation = (
            datetime.now(UTC) - signal["entry_observed_at"]
        ).total_seconds()

        print(
            "[SIGNAL_TIMING]",
            signal["symbol"],
            "quote_to_confirmation_sec=",
            round(quote_to_confirmation, 3),
            flush=True,
        )

        if tracker_direction:
            created = create_entry_tracker(
                signal["symbol"],
                pattern,
                tracker_direction,
                signal["price"],
                entry_time=signal["entry_observed_at"],
                context=signal,
            )

            if not created:
                print(
                    "[ENTRY_ALREADY_TRACKED]",
                    signal["symbol"],
                    flush=True,
                )

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
