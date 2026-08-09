# v3_test.py
# PumpDump Radar V3
#
# Первый диагностический тест нового Market Data Layer.
# НЕ запускает старый scanner.
# НЕ отправляет Telegram.
# НЕ принимает LONG / SHORT решения.

import time
import json

from market_data_v3 import (
    build_market_snapshot,
    normalize_swap_symbol,
)


TEST_SYMBOL = "BTCUSDT"

# Чтобы увидеть реальные OI changes,
# процесс должен некоторое время собирать snapshots.
TEST_INTERVAL_SECONDS = 30
TEST_CYCLES = 12


def fmt(value, digits=4):
    if value is None:
        return "N/A"

    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


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
    print(f"\n{title}")

    if not flow:
        print("  NO DATA")
        return

    print(
        "  trades:",
        flow.get("trade_count"),
    )

    print(
        "  buy:",
        money(flow.get("buy_quote")),
    )

    print(
        "  sell:",
        money(flow.get("sell_quote")),
    )

    print(
        "  delta:",
        money(flow.get("delta_quote")),
    )

    print(
        "  imbalance:",
        fmt(flow.get("imbalance_pct"), 2),
        "%",
    )

    print(
        "  coverage:",
        fmt(flow.get("coverage_seconds"), 1),
        "sec",
    )

    print(
        "  truncated:",
        flow.get("possibly_truncated"),
    )


def print_oi_change(label, data):
    if not data:
        print(
            f"  {label}: N/A "
            "(ещё нет достаточной OI history)"
        )
        return

    print(
        f"  {label}: "
        f"{fmt(data.get('change_pct'), 4)}% "
        f"[actual {fmt(data.get('actual_seconds'), 1)} sec]"
    )


def print_snapshot(snapshot, cycle):
    print("\n")
    print("=" * 70)
    print(
        f"PUMPDUMP RADAR V3 TEST | "
        f"CYCLE {cycle}/{TEST_CYCLES}"
    )
    print("=" * 70)

    if not snapshot:
        print("SNAPSHOT FAILED")
        return

    print("\nSYMBOL:", snapshot.get("symbol"))
    print("INST ID:", snapshot.get("inst_id"))
    print("PRICE:", snapshot.get("price"))

    price_change = snapshot.get(
        "price_change",
        {},
    )

    print("\nPRICE CHANGE")

    print(
        "  1m :",
        fmt(price_change.get("1m"), 4),
        "%",
    )

    print(
        "  5m :",
        fmt(price_change.get("5m"), 4),
        "%",
    )

    print(
        "  15m:",
        fmt(price_change.get("15m"), 4),
        "%",
    )

    funding = snapshot.get("funding")

    print("\nFUNDING")

    if funding:
        print(
            "  rate:",
            fmt(funding.get("rate_pct"), 6),
            "%",
        )
    else:
        print("  NO DATA")

    oi = snapshot.get("open_interest")

    print("\nOPEN INTEREST")

    if oi:
        print(
            "  OI:",
            fmt(oi.get("oi"), 2),
        )

        print(
            "  OI CCY:",
            fmt(oi.get("oi_ccy"), 2),
        )

        print(
            "  OI USD:",
            money(oi.get("oi_usd")),
        )
    else:
        print("  NO DATA")

    oi_change = snapshot.get(
        "oi_change",
        {},
    )

    print("\nOI CHANGE")

    print_oi_change(
        "1m ",
        oi_change.get("1m"),
    )

    print_oi_change(
        "5m ",
        oi_change.get("5m"),
    )

    print_oi_change(
        "15m",
        oi_change.get("15m"),
    )

    spot = snapshot.get(
        "spot_flow",
        {},
    )

    print_flow(
        "SPOT FLOW 1m",
        spot.get("1m"),
    )

    print_flow(
        "SPOT FLOW 5m",
        spot.get("5m"),
    )

    futures = snapshot.get(
        "futures_flow",
        {},
    )

    print_flow(
        "FUTURES FLOW 1m",
        futures.get("1m"),
    )

    print_flow(
        "FUTURES FLOW 5m",
        futures.get("5m"),
    )

    volume = snapshot.get("volume")

    print("\nVOLUME")

    if volume:
        print(
            "  latest:",
            money(volume.get("latest")),
        )

        print(
            "  average:",
            money(volume.get("average")),
        )

        print(
            "  ratio:",
            fmt(volume.get("ratio"), 2),
            "x",
        )
    else:
        print("  NO DATA")

    print("\nDATA QUALITY")

    spot_5m = spot.get("5m")
    futures_5m = futures.get("5m")

    if spot_5m:
        print(
            "  Spot 5m:",
            "TRUNCATED"
            if spot_5m.get("possibly_truncated")
            else "OK",
        )

    if futures_5m:
        print(
            "  Futures 5m:",
            "TRUNCATED"
            if futures_5m.get("possibly_truncated")
            else "OK",
        )

    print("=" * 70)


def main():
    symbol = normalize_swap_symbol(
        TEST_SYMBOL
    )

    print("=" * 70)
    print("PUMPDUMP RADAR V3")
    print("MARKET DATA DIAGNOSTIC")
    print("=" * 70)

    print("Symbol:", symbol)
    print(
        "Interval:",
        TEST_INTERVAL_SECONDS,
        "seconds",
    )
    print(
        "Cycles:",
        TEST_CYCLES,
    )

    print(
        "\nВАЖНО: первые OI windows будут N/A."
    )

    print(
        "Это нормально — V3 сначала должен "
        "накопить timestamped OI history."
    )

    for cycle in range(
        1,
        TEST_CYCLES + 1,
    ):
        try:
            snapshot = build_market_snapshot(
                TEST_SYMBOL
            )

            print_snapshot(
                snapshot,
                cycle,
            )

        except KeyboardInterrupt:
            print("\nTEST STOPPED")
            break

        except Exception as exc:
            print(
                "\n[V3_TEST_ERROR]",
                repr(exc),
            )

        if cycle < TEST_CYCLES:
            time.sleep(
                TEST_INTERVAL_SECONDS
            )

    print("\nV3 TEST FINISHED")


if __name__ == "__main__":
    main()
