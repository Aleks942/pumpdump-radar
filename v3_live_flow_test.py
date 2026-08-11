# v3_live_flow_test.py

import time
import threading

from okx_trade_stream_v3 import run_stream_forever
from trade_flow_collector_v3 import (
    get_spot_windows,
    get_futures_windows,
    get_history_size,
)


SYMBOL = "BTCUSDT"

PRINT_INTERVAL = 30
TEST_MINUTES = 6


def money(value):
    try:
        value = float(value or 0)

        if abs(value) >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}B"

        if abs(value) >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"

        if abs(value) >= 1_000:
            return f"${value / 1_000:.2f}K"

        return f"${value:.2f}"

    except Exception:
        return "N/A"


def print_window(name, data):

    if not data:
        print(f"{name}: NO DATA")
        return

    status = (
        "READY"
        if data.get("window_ready")
        else "WARMING"
    )

    print(
        f"{name:<4} | "
        f"{status:<7} | "
        f"coverage "
        f"{data.get('coverage_pct', 0):6.1f}% | "
        f"trades "
        f"{data.get('trade_count', 0):6d} | "
        f"BUY {money(data.get('buy_quote')):<10} | "
        f"SELL {money(data.get('sell_quote')):<10} | "
        f"DELTA {money(data.get('delta_quote')):<10} | "
        f"IMB {data.get('imbalance_pct', 0):+6.2f}%"
    )


def print_market(title, windows):

    print()
    print(title)
    print("-" * 110)

    for name in ("1m", "5m", "15m"):
        print_window(
            name,
            windows.get(name),
        )


def main():

    print("=" * 110)
    print("PUMPDUMP RADAR V3 — LIVE FLOW TEST")
    print("=" * 110)

    print("Symbol:", SYMBOL)
    print("Test:", TEST_MINUTES, "minutes")
    print()

    print(
        "Starting OKX live trade stream..."
    )

    stream_thread = threading.Thread(
        target=run_stream_forever,
        daemon=True,
    )

    stream_thread.start()

    # Даём WebSocket время подключиться.
    time.sleep(5)

    started = time.time()

    while True:

        elapsed = time.time() - started

        print()
        print("=" * 110)

        print(
            f"ELAPSED: {elapsed:.0f} sec "
            f"({elapsed / 60:.1f} min)"
        )

        print("=" * 110)

        spot = get_spot_windows(
            SYMBOL
        )

        futures = get_futures_windows(
            SYMBOL
        )

        print_market(
            "SPOT FLOW",
            spot,
        )

        print_market(
            "FUTURES FLOW",
            futures,
        )

        print()
        print(
            "MEMORY:",
            "SPOT =",
            get_history_size(
                SYMBOL,
                "spot",
            ),
            "| SWAP =",
            get_history_size(
                SYMBOL,
                "swap",
            ),
        )

        if elapsed >= TEST_MINUTES * 60:
            break

        time.sleep(
            PRINT_INTERVAL
        )

    print()
    print("=" * 110)
    print("TEST FINISHED")
    print("=" * 110)


if __name__ == "__main__":
    main()
