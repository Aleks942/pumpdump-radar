import time

from market_data_v3 import build_market_snapshot
from smart_money_engine_v3 import analyze_smart_money
import threading

from okx_trade_stream_v3 import run_stream_forever


SYMBOL = "BTCUSDT"
TEST_MINUTES = 7
INTERVAL = 30


def show(result):
    print("\n" + "=" * 65)

    print("SMART MONEY V3")
    print("=" * 65)

    print("SYMBOL :", result.get("symbol"))
    print("READY  :", result.get("ready"))
    print()

    print("PATTERN :", result.get("pattern"))
    print(
        "DOMINANT:",
        result.get("dominant_side"),
    )

    

    print("\n--- 1 MINUTE ---")

    print(
        "PRICE   :",
        result.get("price_1m"),
        "|",
        raw.get("price_1m_change"),
        "%",
    )
    
    print(
        "SPOT    :",
        result.get("spot_1m"),
        "| IMB",
        raw.get("spot_1m_imbalance"),
        "%",
    )
    
    print(
        "FUTURES :",
        result.get("futures_1m"),
        "| IMB",
        raw.get("futures_1m_imbalance"),
        "%",
    )

    print("\n--- PROTECTION ---")

    print(
        "LONG FORBIDDEN :",
        result.get("long_forbidden"),
    )

    print(
        "SHORT FORBIDDEN:",
        result.get("short_forbidden"),
    )

    print("\n--- WHY ---")

    for reason in result.get(
        "reasons",
        []
    ):
        print("•", reason)

    print("=" * 65)


def main():

    print(
        "PUMPDUMP RADAR V3 — SMART MONEY LIVE TEST",
        flush=True,
    )

    print(
        "Symbol:",
        SYMBOL,
        flush=True,
    )

    print(
        "Starting OKX trade stream...",
        flush=True,
    )

    stream_thread = threading.Thread(
         target=run_stream_forever,
        daemon=True,
    )
    
    stream_thread.start()  

    started = time.time()

    while True:

        elapsed = (
            time.time() - started
        )

        if elapsed >= (
            TEST_MINUTES * 60
        ):
            break

        print(
            f"\nELAPSED: {elapsed:.0f} sec",
            flush=True,
        )

        snapshot = (
            build_market_snapshot(
                SYMBOL
            )
        )

        if snapshot is None:
            print(
                "SNAPSHOT ERROR",
                flush=True,
            )

        else:
            result = (
                analyze_smart_money(
                    snapshot
                )
            )

            show(result)

        time.sleep(
            INTERVAL
        )

    print(
        "\nTEST FINISHED",
        flush=True,
    )


if __name__ == "__main__":
    main()
