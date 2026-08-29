# v3_multi_test.py
# PumpDump Radar V3
#
# Многомонетный тест Smart Money Engine.
# Никаких сделок и Telegram.
# Только живые данные + поиск паттернов.

import time
import threading

from okx_trade_stream_v3 import run_stream_forever
from market_data_v3 import build_market_snapshot
from smart_money_engine_v3 import analyze_smart_money


# ============================================================
# TEST SYMBOLS
# ============================================================

SYMBOLS = [
    "NAVXUSDT",
    "MASKUSDT",
    "TURBOUSDT",
    "VINEUSDT",
    "CELRUSDT",
]


# ============================================================
# SETTINGS
# ============================================================

CHECK_EVERY_SEC = 30
TEST_DURATION_SEC = 600


# ============================================================
# START STREAM FOR EVERY SYMBOL
# ============================================================

def start_stream(symbol):
    print(
        f"[MULTI_STREAM_START] {symbol}",
        flush=True,
    )

    run_stream_forever(symbol)


def start_all_streams():
    for symbol in SYMBOLS:

        thread = threading.Thread(
            target=start_stream,
            args=(symbol,),
            daemon=True,
        )

        thread.start()

        # Небольшая пауза, чтобы не открывать
        # все соединения в одну миллисекунду.
        time.sleep(1)


# ============================================================
# PRINT RESULT
# ============================================================

def print_result(result):

    symbol = result.get(
        "symbol",
        "UNKNOWN",
    )

    ready = result.get(
        "ready",
        False,
    )

    pattern = result.get(
        "pattern",
        "WAIT",
    )

    raw = result.get("raw") or {}

    price = raw.get(
        "price_5m_change"
    )

    spot = raw.get(
        "spot_5m_imbalance"
    )

    futures = raw.get(
        "futures_5m_imbalance"
    )

    oi = raw.get(
        "oi_5m_change"
    )

    # Пока окно не готово
    if not ready:

        print(
            f"{symbol:<12} "
            f"WARMING",
            flush=True,
        )

        return


    # Обычный WAIT показываем коротко
    if pattern == "WAIT":

        print(
            f"{symbol:<12} "
            f"WAIT "
            f"| P={price} "
            f"| S={spot} "
            f"| F={futures} "
            f"| OI={oi}",
            flush=True,
        )

        return


    # ========================================================
    # НАЙДЕН ИНТЕРЕСНЫЙ ПАТТЕРН
    # ========================================================

    print()
    print(
        "=" * 80,
        flush=True,
    )

    print(
        f"🔥 PATTERN FOUND: "
        f"{symbol} → {pattern}",
        flush=True,
    )

    print(
        "=" * 80,
        flush=True,
    )

    print(
        f"PRICE 5m   : "
        f"{result.get('price_5m')} "
        f"| {price} %",
        flush=True,
    )

    print(
        f"SPOT 5m    : "
        f"{result.get('spot_5m')} "
        f"| {spot} %",
        flush=True,
    )

    print(
        f"FUTURES 5m : "
        f"{result.get('futures_5m')} "
        f"| {futures} %",
        flush=True,
    )

    print(
        f"OI 5m      : "
        f"{result.get('oi_5m')} "
        f"| {oi} %",
        flush=True,
    )

    print(
        f"SPOT 1m    : "
        f"{result.get('spot_1m')} "
        f"| {raw.get('spot_1m_imbalance')} %",
        flush=True,
    )

    print(
        f"FUTURES 1m : "
        f"{result.get('futures_1m')} "
        f"| {raw.get('futures_1m_imbalance')} %",
        flush=True,
    )

    print(
        f"LONG BLOCK : "
        f"{result.get('long_forbidden')}",
        flush=True,
    )

    print(
        f"SHORT BLOCK: "
        f"{result.get('short_forbidden')}",
        flush=True,
    )

    print(
        "WHY:",
        flush=True,
    )

    for reason in result.get(
        "reasons",
        [],
    ):

        print(
            f"  • {reason}",
            flush=True,
        )

    print(
        "=" * 80,
        flush=True,
    )

    print()


# ============================================================
# MAIN TEST
# ============================================================

def main():

    print(
        "=" * 80,
        flush=True,
    )

    print(
        "PUMPDUMP RADAR V3 — MULTI COIN TEST",
        flush=True,
    )

    print(
        "SYMBOLS:",
        ", ".join(SYMBOLS),
        flush=True,
    )

    print(
        "=" * 80,
        flush=True,
    )

    start_all_streams()

    started_at = time.time()

    while True:

        elapsed = int(
            time.time() - started_at
        )

        if elapsed >= TEST_DURATION_SEC:
            break

        print()
        print(
            "-" * 80,
            flush=True,
        )

        print(
            f"ELAPSED: {elapsed} sec",
            flush=True,
        )

        print(
            "-" * 80,
            flush=True,
        )

        for symbol in SYMBOLS:

            try:

                snapshot = (
                    build_market_snapshot(
                        symbol
                    )
                )

                result = (
                    analyze_smart_money(
                        snapshot
                    )
                )

                print_result(
                    result
                )

            except Exception as exc:

                print(
                    f"{symbol:<12} "
                    f"ERROR: {exc}",
                    flush=True,
                )

        time.sleep(
            CHECK_EVERY_SEC
        )

    print()
    print(
        "MULTI TEST FINISHED",
        flush=True,
    )


if __name__ == "__main__":
    main()
