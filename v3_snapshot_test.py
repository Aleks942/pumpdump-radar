# v3_snapshot_test.py
# PumpDump Radar V3
#
# Проверка полного MarketSnapshot:
# PRICE + PRICE CHANGE + FUNDING + OI +
# LIVE SPOT FLOW + LIVE FUTURES FLOW + VOLUME

import time
import threading

from okx_trade_stream_v3 import run_stream_forever
from market_data_v3 import build_market_snapshot


SYMBOL = "BTCUSDT"
PRINT_INTERVAL = 30
TEST_MINUTES = 6


def fmt(value, digits=2):
    if value is None:
        return "N/A"

    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "N/A"


def money(value):
    if value is None:
        return "N/A"

    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"

    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"

    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"

    if abs(value) >= 1_000:
        return f"${value / 1_000:.2f}K"

    return f"${value:.2f}"


def print_flow(title, flow):
    print()
    print(title)
    print("-" * 90)

    if not flow:
        print("NO DATA")
        return

    for window in ("1m", "5m", "15m"):
        row = flow.get(window)

        if not row:
            print(f"{window}: NO DATA")
            continue

        print(
            f"{window:<4} | "
            f"{row.get('quality', 'N/A'):<7} | "
            f"coverage {row.get('coverage_pct', 0):6.1f}% | "
            f"trades {row.get('trade_count', 0):6d} | "
            f"delta {money(row.get('delta_quote')):<10} | "
            f"imb {row.get('imbalance_pct', 0):+7.2f}%"
        )


def print_snapshot(snapshot):
    print()
    print("=" * 100)
    print("V3 MARKET SNAPSHOT")
    print("=" * 100)

    if not snapshot:
        print("SNAPSHOT = NONE")
        return

    print("SYMBOL:", snapshot.get("symbol"))
    print("PRICE :", snapshot.get("price"))

    price_change = snapshot.get(
        "price_change"
    ) or {}

    print()
    print("PRICE CHANGE")
    print(
        "1m:",
        fmt(price_change.get("1m")),
        "%",
    )
    print(
        "5m:",
        fmt(price_change.get("5m")),
        "%",
    )
    print(
        "15m:",
        fmt(price_change.get("15m")),
        "%",
    )

    funding = snapshot.get(
        "funding"
    ) or {}

    print()
    print(
        "FUNDING:",
        fmt(
            funding.get("rate_pct"),
            4,
        ),
        "%",
    )

    oi = snapshot.get(
        "open_interest"
    ) or {}

    print()
    print(
        "OI:",
        fmt(
            oi.get("oi"),
            2,
        ),
    )

    oi_change = snapshot.get(
        "oi_change"
    ) or {}

    print("OI CHANGE")

    for window in ("1m", "5m", "15m"):
        row = oi_change.get(window)

        if row:
            print(
                f"{window}: "
                f"{fmt(row.get('change_pct'), 4)}% "
                f"| actual "
                f"{fmt(row.get('actual_seconds'), 1)} sec"
            )
        else:
            print(
                f"{window}: WARMING / N/A"
            )

    print_flow(
        "SPOT LIVE FLOW",
        snapshot.get("spot_flow"),
    )

    print_flow(
        "FUTURES LIVE FLOW",
        snapshot.get("futures_flow"),
    )

    volume = snapshot.get(
        "volume"
    ) or {}

    print()
    print("VOLUME")
    print(
        "latest:",
        money(volume.get("latest")),
    )
    print(
        "average:",
        money(volume.get("average")),
    )
    print(
        "ratio:",
        fmt(volume.get("ratio"), 2),
    )

    print("=" * 100)


def main():
    print(
        "PUMPDUMP RADAR V3 — "
        "FULL MARKET SNAPSHOT TEST"
    )

    stream_thread = threading.Thread(
        target=run_stream_forever,
        daemon=True,
    )

    stream_thread.start()

    # Даём WebSocket подключиться.
    time.sleep(5)

    started = time.time()

    while True:
        elapsed = time.time() - started

        print()
        print(
            f"ELAPSED: {elapsed:.0f} sec "
            f"({elapsed / 60:.1f} min)"
        )

        snapshot = build_market_snapshot(
            SYMBOL
        )

        print_snapshot(snapshot)

        if elapsed >= TEST_MINUTES * 60:
            break

        time.sleep(
            PRINT_INTERVAL
        )

    print()
    print("TEST FINISHED")


if __name__ == "__main__":
    main()
