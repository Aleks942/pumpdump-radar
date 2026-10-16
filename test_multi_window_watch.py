"""No-network regression tests for real 10m/20m/30m pump/dump alerts."""
import ast
import math
from html import escape
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import patch
import multi_window_watch as watch

THRESHOLDS = {
    "5m": {"pump": 5, "dump": -5},
    "10m": {"pump": 6, "dump": -6},
    "20m": {"pump": 7, "dump": -7},
    "30m": {"pump": 8, "dump": -8},
}


def okx(price, symbol="PUFFER"):
    return [{"instId": f"{symbol}-USDT-SWAP", "last": str(price)}]


def bybit(price, symbol="PUFFER"):
    return [{"symbol": f"{symbol}USDT", "lastPrice": str(price)}]


class MultiWindowWatchTests(TestCase):
    def setUp(self):
        watch._history.clear()
        watch._notified.clear()

    def test_chronological_all_three_windows_and_exact_thresholds(self):
        rows = watch.sample_prices(okx(1.00), "OKX", now=1000)
        self.assertEqual(rows[0]["windows"], {})
        self.assertEqual(watch.select_events(rows, THRESHOLDS), [])
        rows = watch.sample_prices(okx(1.06), "OKX", now=1600)
        events = watch.select_events(rows, THRESHOLDS)
        self.assertEqual([(r["window"], r["type"]) for r in events],
                         [("10m", "PUMP")])
        watch.mark_sent(events[0])
        self.assertEqual(watch.select_events(rows, THRESHOLDS), [])

        rows = watch.sample_prices(okx(1.07), "OKX", now=2200)
        events = watch.select_events(rows, THRESHOLDS)
        self.assertEqual([(r["window"], r["type"]) for r in events],
                         [("20m", "PUMP")])
        watch.mark_sent(events[0])

        rows = watch.sample_prices(okx(1.08), "OKX", now=2800)
        events = watch.select_events(rows, THRESHOLDS)
        self.assertEqual([(r["window"], r["type"]) for r in events],
                         [("30m", "PUMP")])
        self.assertEqual(events[0]["sample_age_sec"], 1800)
        self.assertEqual(events[0]["exchange"], "OKX")

    def test_dump_is_negative_and_no_fabricated_history(self):
        self.assertEqual(watch.select_events(
            watch.sample_prices(bybit(1), "Bybit", now=1000), THRESHOLDS), [])
        events = watch.select_events(
            watch.sample_prices(bybit(.94), "Bybit", now=1600), THRESHOLDS)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["window"], "10m")
        self.assertEqual(events[0]["type"], "DUMP")
        self.assertEqual(events[0]["decision"]["direction"], "DOWN")

    def test_no_inherited_5m_shortcut_or_24h_change(self):
        tickers = okx(2)
        tickers[0]["open24h"] = ".01"
        result = watch.sample_prices(tickers, "OKX", now=2000)
        self.assertEqual(result[0]["windows"], {})
        self.assertEqual(watch.select_events(result, THRESHOLDS), [])

    def test_weakening_rearms_and_failed_send_can_retry(self):
        watch.sample_prices(okx(1), "OKX", now=1000)
        rows = watch.sample_prices(okx(1.07), "OKX", now=1600)
        first = watch.select_events(rows, THRESHOLDS)
        self.assertEqual(len(first), 1)
        self.assertEqual(len(watch.select_events(rows, THRESHOLDS)), 1)
        watch.mark_sent(first[0])
        self.assertEqual(watch.select_events(rows, THRESHOLDS), [])
        rows = watch.sample_prices(okx(1.001), "OKX", now=1900)
        self.assertEqual(watch.select_events(rows, THRESHOLDS), [])
        rows = watch.sample_prices(okx(1.08), "OKX", now=1930)
        self.assertEqual(len(watch.select_events(rows, THRESHOLDS)), 1)

    def test_cross_exchange_same_symbol_one_event(self):
        watch.sample_prices(okx(1), "OKX", now=1000)
        watch.sample_prices(bybit(1), "Bybit", now=1000)
        rows = (watch.sample_prices(okx(1.07), "OKX", now=1600)
                + watch.sample_prices(bybit(1.08), "Bybit", now=1600))
        events = watch.select_events(rows, THRESHOLDS)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["exchange"], "Bybit")
        watch.mark_sent(events[0])
        self.assertEqual(watch.select_events(rows, THRESHOLDS), [])

    def test_no_cross_exchange_price_history(self):
        watch.sample_prices(okx(1), "OKX", now=1000)
        bybit_rows = watch.sample_prices(bybit(2), "Bybit", now=1600)
        self.assertEqual(bybit_rows[0]["windows"], {})
        self.assertEqual(watch.select_events(bybit_rows, THRESHOLDS), [])

    def test_invalid_inputs_and_out_of_order_time(self):
        rows = watch.sample_prices([{"symbol":"PUFFERUSDT",
                                   "lastPrice":"NaN"}], "Bybit", now=1000)
        self.assertEqual(rows, [])
        watch.sample_prices(okx(1), "OKX", now=1600)
        older = watch.sample_prices(okx(5), "OKX", now=1500)
        self.assertEqual(older, [])
        self.assertEqual(watch.select_events([], THRESHOLDS), [])

    def test_message_and_main_wiring(self):
        source = Path(__file__).with_name("telegram_builder.py").read_text()
        tree = ast.parse(source)
        pure = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                and n.name in ("number", "build_market_watch_message")]
        ns = {"math": math, "escape": escape, "PATTERN_NAMES": {}}
        exec(compile(ast.fix_missing_locations(
            ast.Module(body=pure, type_ignores=[])), "formatter", "exec"), ns)
        msg = ns["build_market_watch_message"]({
            "symbol":"PUFFERUSDT", "price":.05, "change":8.4,
            "type":"PUMP", "decision":{"direction":"UP"},
            "window":"30m", "sample_age_sec":1800, "exchange":"Bybit",
        }, "PRICE_THRESHOLD")
        self.assertIn("около 30 минут", msg)
        self.assertIn("Bybit SWAP", msg)
        self.assertIn("НЕ сигнал для входа", msg)
        main_code = Path(__file__).with_name("main.py").read_text()
        self.assertIn("select_multi_window_events(", main_code)
        self.assertIn("mark_multi_window_sent(event)", main_code)
        self.assertIn('sample_multi_window_prices(tickers, "OKX")', main_code)
        self.assertIn('bybit_tickers, "Bybit"', main_code)


if __name__ == "__main__":
    main()
