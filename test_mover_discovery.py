"""Network-free regression tests for early mover research and volume units."""
import io
import math
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import mover_discovery as mover
from ticker_metrics import approx_quote_volume_usdt, spread_bps


def ticker(symbol="BTC", price=50_000, volume_base=300,
           opening=48_000, high=53_000, low=47_000, bid=None, ask=None):
    bid = float(price) * 0.9999 if bid is None else bid
    ask = float(price) * 1.0001 if ask is None else ask
    return {
        "instId": f"{symbol}-USDT-SWAP",
        "last": str(price),
        "volCcy24h": str(volume_base),
        "open24h": str(opening),
        "high24h": str(high),
        "low24h": str(low),
        "bidPx": str(bid),
        "askPx": str(ask),
    }


class SwapVolumeTests(unittest.TestCase):
    def test_base_units_are_not_usdt_volume(self):
        # 300 BTC at $50k = approximately 15m USDT.
        row = ticker()
        self.assertEqual(approx_quote_volume_usdt(row), 15_000_000)
        self.assertLess(float(row["volCcy24h"]), 10_000_000)
        self.assertAlmostEqual(spread_bps(row), 2.0, places=5)

    def test_missing_nan_invalid_or_non_usdt_quotes_fail_closed(self):
        for row in (
            {}, ticker(volume_base=0), ticker(price=0),
            ticker(price="nan"), ticker(volume_base="inf"),
            {**ticker(), "instId": "BTC-USD-SWAP"},
        ):
            self.assertIsNone(approx_quote_volume_usdt(row))
        self.assertIsNone(spread_bps(ticker(bid=51_000, ask=50_000)))


class MoverResearchTests(unittest.TestCase):
    def setUp(self):
        with patch.object(mover, "_history", {}), patch.object(mover, "_last_report", None):
            # Context managers stay active within each test via addCleanup.
            pass
        hist = patch.object(mover, "_history", {})
        last = patch.object(mover, "_last_report", None)
        hist.start()
        last.start()
        self.addCleanup(last.stop)
        self.addCleanup(hist.stop)

    def test_24h_range_is_not_falsely_labeled_5m(self):
        first = mover.inspect([ticker()], now=1000)[0]
        self.assertGreater(first["range24"], 7)
        self.assertIsNone(first["change5"])
        self.assertTrue(first["old_volume_filter_missed"])
        self.assertTrue(first["eligible_research"])

    def test_five_min_samples_both_up_and_down(self):
        mover.inspect([ticker(symbol="UP", price=100, volume_base=200_000,
                             opening=100, high=110, low=90)], now=1000)
        mover.inspect([ticker(symbol="DOWN", price=100, volume_base=200_000,
                             opening=100, high=110, low=90)], now=1000)
        r = mover.inspect([
            ticker(symbol="UP", price=103, volume_base=200_000,
                   opening=100, high=110, low=90),
            ticker(symbol="DOWN", price=97, volume_base=200_000,
                   opening=100, high=110, low=90),
        ], now=1300)
        a = {v["symbol"]: v for v in r}
        self.assertAlmostEqual(a["UPUSDT"]["change5"], 3)
        self.assertAlmostEqual(a["DOWNUSDT"]["change5"], -3)

    def test_research_logs_and_never_sends_alerts(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            mover.observe([ticker()], now=1000)
            mover.observe([ticker(price=50_500)], now=1300)
        output = buffer.getvalue()
        self.assertIn("[MOVER_RESEARCH]", output)
        self.assertIn("no_trade_alerts=true", output)
        self.assertIn("[MOVER_24H]", output)
        self.assertIn("[MOVER_EARLY]", output)
        self.assertIn("research_only=true", output)

    def test_bad_spread_and_low_quote_volume_rejected(self):
        bad = mover.inspect([
            ticker(symbol="BID", price=100, volume_base=200_000,
                   bid=90, ask=110, opening=100, high=112, low=90),
            ticker(symbol="LOW", price=100, volume_base=50,
                   opening=100, high=112, low=90),
        ], now=1000)
        self.assertFalse(bad[0]["eligible_research"])
        self.assertFalse(bad[1]["eligible_research"])

    def test_bad_ticks_are_ignored(self):
        rows = mover.inspect([ticker(price="nan"), ticker(volume_base="inf")], now=1000)
        self.assertEqual(rows, [])


class SingleInstrumentPriceRefreshTests(unittest.TestCase):
    """Run only this function from main.py: importing main starts bot workers."""

    def setUp(self):
        import ast
        import datetime
        from pathlib import Path
        from unittest.mock import Mock
        from types import SimpleNamespace

        source = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        functions = [n for n in tree.body
                     if isinstance(n, ast.FunctionDef)
                     and n.name == "refresh_signal_price"]
        self.assertEqual(len(functions), 1)
        isolated = ast.fix_missing_locations(
            ast.Module(body=functions, type_ignores=[]))
        self.get = Mock()
        namespace = {
            "requests": SimpleNamespace(get=self.get),
            "datetime": datetime.datetime,
            "UTC": datetime.timezone.utc,
            "print": lambda *args, **kwargs: None,
        }
        exec(compile(isolated, "isolated_refresh", "exec"), namespace)
        self.refresh = namespace["refresh_signal_price"]

    def test_refreshes_only_the_selected_contract(self):
        from unittest.mock import Mock
        response = Mock()
        response.json.return_value = {"code": "0", "data": [
            {"instId": "BTC-USDT-SWAP", "last": "83456.7"},
        ]}
        self.get.return_value = response
        signal = {"symbol": "BTCUSDT", "price": 83300}
        self.assertTrue(self.refresh(signal))
        self.assertEqual(signal["price"], 83456.7)
        self.assertIsNotNone(signal["entry_observed_at"])
        self.get.assert_called_once_with(
            "https://www.okx.com/api/v5/market/ticker",
            params={"instId": "BTC-USDT-SWAP"},
            timeout=10,
        )

    def test_invalid_or_mismatched_ticker_cannot_refresh_signal(self):
        from unittest.mock import Mock
        samples = [
            {"code": "500", "data": []},
            {"code": "0", "data": []},
            {"code": "0", "data": [
                {"instId": "ETH-USDT-SWAP", "last": "83456.7"}]},
            {"code": "0", "data": [
                {"instId": "BTC-USDT-SWAP", "last": "nan"}]},
            {"code": "0", "data": [
                {"instId": "BTC-USDT-SWAP", "last": "0"}]},
            {"code": "0", "data": [
                {"instId": "BTC-USDT-SWAP", "last": "83456"},
                {"instId": "BTC-USDT-SWAP", "last": "83457"}]},
        ]
        for payload in samples:
            with self.subTest(payload=payload):
                self.get.reset_mock()
                response = Mock()
                response.json.return_value = payload
                self.get.return_value = response
                original = {"symbol": "BTCUSDT", "price": 83300}
                self.assertFalse(self.refresh(original))
                self.assertEqual(original, {"symbol": "BTCUSDT", "price": 83300})
                self.get.assert_called_once()

    def test_invalid_symbol_does_not_call_exchange(self):
        signal = {"symbol": "BTC-EUR", "price": 83300}
        self.assertFalse(self.refresh(signal))
        self.get.assert_not_called()


class MarketWatchNotificationTests(unittest.TestCase):
    """Isolated market-watch tests; never import main.py with live workers."""

    def setUp(self):
        import ast
        import math
        from html import escape
        from pathlib import Path
        from unittest.mock import Mock

        source = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        parser = ast.parse(source)
        fn = [n for n in parser.body if isinstance(n, ast.FunctionDef)
              and n.name == "maybe_send_market_watch"]
        self.assertEqual(len(fn), 1)
        calls = []
        self.calls = calls
        self.sender_result = True

        def send(text, symbol="SYSTEM"):
            calls.append((symbol, text))
            return self.sender_result

        builder_source = Path(__file__).with_name("telegram_builder.py").read_text(
            encoding="utf-8")
        builder_tree = ast.parse(builder_source)
        builder_functions = [
            n for n in builder_tree.body if isinstance(n, ast.FunctionDef)
            and n.name in ("number", "build_market_watch_message")
        ]
        builder_ns = {
            "math": math, "escape": escape,
            "PATTERN_NAMES": {
                "NEW_LONG_BUILDUP": "Набор лонгов",
                "NEW_SHORT_BUILDUP": "Набор шортов",
            },
        }
        exec(compile(ast.fix_missing_locations(ast.Module(
            body=builder_functions, type_ignores=[])), "watch_builder", "exec"),
            builder_ns)
        self.builder = builder_ns["build_market_watch_message"]
        self.ns = {
            "math": math, "time": __import__("time"),
            "send_telegram": send,
            "build_market_watch_message": self.builder,
            "print": lambda *args, **kwargs: None,
            "_MARKET_WATCH_SPACING_SEC": 1200,
            "_MARKET_WATCH_SYMBOL_SEC": 7200,
            "_MARKET_WATCH_DAILY_MAX": 12,
            "_market_watch_delivery": {
                "day": None, "count": 0, "last_sent": None,
                "symbols": {}, "retry_after": 0.0,
            },
        }
        exec(compile(ast.fix_missing_locations(ast.Module(
            body=fn, type_ignores=[])), "watch_sender", "exec"), self.ns)
        self.notify = self.ns["maybe_send_market_watch"]
        self.t0 = 1791892800.0  # UTC noon; 12 spaced alerts remain same day

    def sample(self, symbol="ETHUSDT", direction="UP"):
        kind, pattern, pct = (
            ("PUMP", "NEW_LONG_BUILDUP", 2.3) if direction == "UP"
            else ("DUMP", "NEW_SHORT_BUILDUP", -2.3))
        return {
            "symbol": symbol, "type": kind, "price": 0.0012534,
            "window": "5m", "change": pct, "oi_change": 0.8,
            "futures_flow": {"imbalance_pct": 30},
            "spot_cvd": {"cvd_percent": 12},
            "decision": {"direction": direction, "pattern": pattern},
        }

    def test_existing_pattern_sends_plain_observation(self):
        self.assertTrue(self.notify(
            self.sample(), "EDGE_UNPROVEN", now=self.t0))
        symbol, text = self.calls[0]
        self.assertEqual(symbol, "ETHUSDT")
        self.assertIn("🚀 ПАМП — НАБЛЮДЕНИЕ", text)
        self.assertIn("Цена: 0.0012534 USDT", text)
        self.assertIn("НЕ сигнал для входа", text)
        self.assertNotIn("ПОДТВЕРЖДЁННЫЙ СИГНАЛ", text)
        self.assertNotIn("вход подтверждён", text)
        self.assertEqual(self.ns["_market_watch_delivery"]["count"], 1)

    def test_downtrend_watch_mentions_unsupported_trade(self):
        text = self.builder(self.sample("BTCUSDT", "DOWN"), "TREND_NOT_ALIGNED")
        self.assertIn("🔻 ДАМП — НАБЛЮДЕНИЕ", text)
        self.assertIn("Старший тренд не подтверждает направление", text)

    def test_invalid_signal_and_html_escape(self):
        self.assertFalse(self.notify(
            {"symbol": "BTCUSDT", "decision": None}, "EDGE_UNPROVEN",
            now=self.t0))
        self.assertFalse(self.notify(
            {"symbol": "BTCUSDT", "decision": {"direction": "DOWN"},
             "type": "PUMP"}, "EDGE_UNPROVEN", now=self.t0))
        self.assertEqual(self.calls, [])
        message = self.builder(self.sample("X<b>USDT"), "EDGE_UNPROVEN")
        self.assertIn("X&lt;b&gt;USDT", message)
        self.assertNotIn("X<b>USDT", message)

    def test_global_spacing_and_symbol_dedup(self):
        self.assertTrue(self.notify(self.sample(), "EDGE_UNPROVEN", now=self.t0))
        self.assertFalse(self.notify(
            self.sample("BTCUSDT"), "EDGE_UNPROVEN", now=self.t0+1199))
        self.assertFalse(self.notify(
            self.sample(), "EDGE_UNPROVEN", now=self.t0+1201))
        self.assertTrue(self.notify(
            self.sample("BTCUSDT"), "EDGE_UNPROVEN", now=self.t0+1201))
        self.assertEqual(len(self.calls), 2)

    def test_daily_limit(self):
        for i in range(12):
            self.assertTrue(self.notify(
                self.sample(f"T{i}USDT"), "EDGE_UNPROVEN",
                now=self.t0+i*1201))
        self.assertFalse(self.notify(
            self.sample("OTHERUSDT"), "EDGE_UNPROVEN",
            now=self.t0+12*1201))
        self.assertEqual(len(self.calls), 12)

    def test_failed_delivery_does_not_consume_budget(self):
        self.sender_result = False
        self.assertFalse(self.notify(
            self.sample(), "EDGE_UNPROVEN", now=self.t0))
        self.assertEqual(self.ns["_market_watch_delivery"]["count"], 0)
        self.assertFalse(self.notify(
            self.sample(), "EDGE_UNPROVEN", now=self.t0+250))
        self.sender_result = True
        self.assertTrue(self.notify(
            self.sample(), "EDGE_UNPROVEN", now=self.t0+301))
        self.assertEqual(self.ns["_market_watch_delivery"]["count"], 1)
        self.assertEqual(len(self.calls), 2)


class PriceOnlyMarketWatchTests(unittest.TestCase):
    def row(self, symbol, change, *, age=300, price=100, liquid=False):
        return {
            "symbol": symbol, "change5": change,
            "sample_age_sec": age, "price": price,
            "eligible_research": liquid, "spread_bps": 999,
            "quote_usdt_approx": 1,
        }

    def test_raw_ticker_price_move_needs_no_volume_or_oi_metadata(self):
        with patch.object(mover, "_history", {}):
            sample = {"instId": "ABC-USDT-SWAP", "last": "100"}
            self.assertEqual(
                mover.inspect_price_quotes([sample], now=1000)[0]["change5"],
                None,
            )
            sample["last"] = "101.8"
            rows = mover.inspect_price_quotes([sample], now=1300)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "ABCUSDT")
            self.assertAlmostEqual(rows[0]["change5"], 1.8)
            self.assertEqual(rows[0]["sample_age_sec"], 300)
            self.assertEqual(len(mover.select_threshold_moves(rows, 1.5, 1.5)), 1)

    def test_invalid_raw_quotes_never_create_signal(self):
        with patch.object(mover, "_history", {}):
            rows = mover.inspect_price_quotes([
                {"instId": "BTC-USDT-SWAP", "last": "nan"},
                {"instId": "ETH-USDT-SWAP", "last": "0"},
                {"instId": "X-USD-SWAP", "last": "100"},
                {"instId": "LTC-USDT-SWAP", "last": "bad"},
            ], now=1000)
            self.assertEqual(rows, [])
            self.assertEqual(mover.select_threshold_moves(rows, 1.5, 1.5), [])

    def test_thresholds_are_inclusive_for_both_directions(self):
        rows = [
            self.row("UPUSDT", 1.5),
            self.row("DOWNUSDT", -1.5),
            self.row("QUIETUSDT", 1.49),
            self.row("PUSHUSDT", 7.0),
        ]
        out = mover.select_threshold_moves(rows, 1.5, 1.5)
        self.assertEqual([r["symbol"] for r in out],
                         ["PUSHUSDT", "UPUSDT", "DOWNUSDT"])
        self.assertEqual([r["type"] for r in out],
                         ["PUMP", "PUMP", "DUMP"])
        self.assertEqual(out[1]["decision"]["direction"], "UP")
        self.assertEqual(out[2]["decision"]["direction"], "DOWN")
        self.assertEqual(out[2]["change"], -1.5)

    def test_user_can_change_existing_threshold_variables(self):
        rows = [
            self.row("UPUSDT", 1.8), self.row("DOWNUSDT", -1.8),
        ]
        self.assertEqual(len(mover.select_threshold_moves(rows, 1.5, 1.5)), 2)
        self.assertEqual(mover.select_threshold_moves(rows, 2.0, 2.0), [])

    def test_no_extra_liquidity_oi_cvd_or_pattern_gate(self):
        row = self.row("ANYUSDT", -2.2, price=0.00000012)
        self.assertEqual(row["eligible_research"], False)
        selected = mover.select_threshold_moves([row], 1.5, 1.5)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["type"], "DUMP")
        self.assertEqual(selected[0]["price"], 0.00000012)
        self.assertNotIn("spot_cvd", selected[0])
        self.assertNotIn("aggregated_oi", selected[0])

    def test_invalid_data_and_missing_history_fail_closed(self):
        rows = [
            self.row("EARLYUSDT", 5, age=120),
            self.row("LATEUSDT", -4, age=450),
            self.row("NANUSDT", math.nan),
            self.row("INFUSDT", math.inf),
            self.row("ZEROPRICEUSDT", 3, price=0),
            self.row("EURUSD", 4),
            self.row("BADUSDT", 4, age=None),
            self.row("GOODUSDT", 2.0, age=420),
        ]
        got = mover.select_threshold_moves(rows, 1.5, 1.5)
        self.assertEqual([r["symbol"] for r in got], ["GOODUSDT"])
        for bad in (0, -1, float("nan"), float("inf"), None):
            self.assertEqual(mover.select_threshold_moves(rows, bad, 1.5), [])
            self.assertEqual(mover.select_threshold_moves(rows, 1.5, bad), [])

    def test_existing_notification_format_and_delivery_are_separate_from_trading(self):
        import ast
        from html import escape
        from pathlib import Path

        # Extract pure message formatter to avoid importing live bot modules.
        source = Path(__file__).with_name("telegram_builder.py").read_text(
            encoding="utf-8")
        tree = ast.parse(source)
        pure = [node for node in tree.body
                if isinstance(node, ast.FunctionDef)
                and node.name in ("number", "build_market_watch_message")]
        ns = {"math": math, "escape": escape, "PATTERN_NAMES": {}}
        exec(compile(ast.fix_missing_locations(
            ast.Module(body=pure, type_ignores=[])), "test_formatter", "exec"), ns)

        rows = [self.row("BTCUSDT", 2.3, age=300)]
        selected = mover.select_threshold_moves(rows, 1.5, 1.5)
        self.assertEqual(len(selected), 1)
        msg = ns["build_market_watch_message"](
            selected[0], "PRICE_THRESHOLD")
        self.assertIn("ПАМП — НАБЛЮДЕНИЕ", msg)
        self.assertIn("Движение: +2.30%", msg)
        self.assertIn("Период наблюдения: около 5 минут", msg)
        self.assertIn("НЕ сигнал для входа", msg)
        self.assertNotIn("ПОДТВЕРЖДЁННЫЙ СИГНАЛ", msg)
        self.assertNotIn("Полный паттерн", msg)
        self.assertNotIn("OI 5м:", msg)

    def test_main_wires_price_candidates_once_before_trading_gate(self):
        from pathlib import Path
        source = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        self.assertEqual(source.count('maybe_send_market_watch(market_move, "PRICE_THRESHOLD")'), 1)
        self.assertNotIn("maybe_send_market_watch(signal, reason)", source)
        self.assertIn('inspect_price_quotes(tickers)', source)
        self.assertIn('TIME_WINDOWS["5m"]["pump"]', source)
        self.assertIn('-TIME_WINDOWS["5m"]["dump"]', source)


if __name__ == "__main__":
    unittest.main()
