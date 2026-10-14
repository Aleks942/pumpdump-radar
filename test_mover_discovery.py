"""Network-free regression tests for early mover research and volume units."""
import io
import math
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import mover_discovery as mover
import coinglass_context as cg
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
        self.assertEqual(source.count('maybe_send_market_watch(market_move, "PRICE_CONTINUATION")'), 1)
        self.assertNotIn("maybe_send_market_watch(signal, reason)", source)
        self.assertIn('inspect_price_quotes(tickers)', source)
        self.assertIn('TIME_WINDOWS["5m"]["pump"]', source)
        self.assertIn('-TIME_WINDOWS["5m"]["dump"]', source)


class FollowThroughWatchTests(unittest.TestCase):
    """A one-bar burst is NOT enough for a continuation notification."""

    def setUp(self):
        pending = patch.object(mover, "_continuation_pending", {})
        pending.start()
        self.addCleanup(pending.stop)
        self.t0 = 1000.0

    def row(self, price, move, symbol="ABCUSDT", age=300):
        return {
            "symbol": symbol, "price": price,
            "change5": move, "sample_age_sec": age,
        }

    def check(self, price, move, dt, symbol="ABCUSDT",
              pump=1.5, dump=1.5):
        rows = [self.row(price, move, symbol=symbol)]
        return mover.select_continuing_moves(
            rows, pump, dump, now=self.t0+dt)

    def test_pump_needs_two_separate_advances(self):
        self.assertEqual(self.check(101.5, 1.5, 0), [])
        self.assertEqual(self.check(101.62, 1.6, 60), [])
        results = self.check(101.80, 1.8, 120)
        self.assertEqual(len(results), 1)
        s = results[0]
        self.assertEqual(s["type"], "PUMP")
        self.assertEqual(s["decision"]["direction"], "UP")
        self.assertAlmostEqual(s["trigger_change"], 1.5)
        self.assertGreater(s["followthrough_pct"], 0.2)
        self.assertEqual(s["confirmation_seconds"], 120)
        self.assertEqual(self.check(101.95, 1.95, 180), [])

    def test_short_requires_continued_fall(self):
        self.assertEqual(self.check(100, -1.5, 0, "DNUSDT"), [])
        self.assertEqual(self.check(99.88, -1.6, 60, "DNUSDT"), [])
        s = self.check(99.70, -1.8, 120, "DNUSDT")
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0]["type"], "DUMP")
        self.assertEqual(s[0]["decision"]["direction"], "DOWN")
        self.assertGreater(s[0]["followthrough_pct"], 0.2)

    def test_price_spike_then_retrace_is_rejected_even_if_it_recovers(self):
        self.assertEqual(self.check(101.5, 1.5, 0), [])
        self.assertEqual(self.check(100.5, 0.5, 60), [])
        self.assertEqual(self.check(101.8, 1.8, 120), [])
        self.assertEqual(self.check(102.0, 2.0, 180), [])

    def test_flat_after_initial_spike_never_alerts(self):
        self.assertEqual(self.check(101.5, 1.5, 0), [])
        self.assertEqual(self.check(101.5, 1.5, 60), [])
        self.assertEqual(self.check(101.5, 1.5, 120), [])
        self.assertEqual(self.check(101.5, 1.5, 180), [])

    def test_one_followup_step_is_not_enough(self):
        self.check(101.5, 1.5, 0)
        self.assertEqual(self.check(102.0, 2, 120), [])

    def test_quick_late_chase_is_rejected(self):
        self.check(101.5, 1.5, 0)
        self.assertEqual(self.check(104, 4.0, 60), [])
        self.assertEqual(self.check(104.2, 4.2, 120), [])

    def test_threshold_change_remains_controlled_by_existing_variables(self):
        self.assertEqual(self.check(101.5, 1.5, 0, pump=2), [])
        self.assertEqual(self.check(101.62, 1.6, 60, pump=2), [])
        self.assertEqual(self.check(101.8, 1.8, 120, pump=2), [])
        self.assertEqual(self.check(101.8, 1.8, 180, pump=1.5), [])
        self.assertEqual(self.check(101.94, 1.9, 240, pump=1.5), [])
        self.assertEqual(len(self.check(102.08, 2.0, 300, pump=1.5)), 1)

    def test_aged_or_invalid_quotes_do_not_confirm(self):
        self.check(101.5, 1.5, 0)
        self.assertEqual(self.check(101.65, 1.65, 60), [])
        self.assertEqual(mover.select_continuing_moves(
            [self.row(float("nan"), 2)], 1.5, 1.5, now=self.t0+120), [])
        self.assertEqual(self.check(101.9, 2.0, 120, pump=-1), [])

    def test_telegram_message_says_followthrough_not_guaranteed_profits(self):
        import ast
        from html import escape
        from pathlib import Path
        source = Path(__file__).with_name("telegram_builder.py").read_text(
            encoding="utf-8")
        tree = ast.parse(source)
        pure = [node for node in tree.body
                if isinstance(node, ast.FunctionDef)
                and node.name in ("number", "build_market_watch_message")]
        ns = {"math": math, "escape": escape, "PATTERN_NAMES": {}}
        exec(compile(ast.fix_missing_locations(
            ast.Module(body=pure, type_ignores=[])), "test_formatter", "exec"), ns)
        self.check(101.5, 1.5, 0)
        self.check(101.62, 1.6, 60)
        [market_move] = self.check(101.8, 1.8, 120)
        msg = ns["build_market_watch_message"](market_move, "PRICE_CONTINUATION")
        self.assertIn("ПРОДОЛЖЕНИЕ", msg)
        self.assertIn("Первый импульс", msg)
        self.assertIn("следующие 2.0 мин", msg)
        self.assertIn("НЕ сигнал для входа", msg)
        self.assertIn("не гарантирует", msg)
        self.assertNotIn("OI 5м:", msg)


class CoinGlassResearchTests(unittest.TestCase):
    def fake(self, rows, status_code=200):
        class Response:
            def __init__(self, value, code):
                self.value = value
                self.status_code = code
            def json(self):
                return self.value
        return Response({"code": "0", "data": rows}, status_code)

    def test_no_key_means_zero_api_calls(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(cg.collect("GRASSUSDT")["status"], "NOT_CONFIGURED")
            self.assertFalse(cg.schedule("GRASSUSDT"))
        self.assertEqual(cg.collect("GRASS/USDT", api_key="key")["status"],
                         "INVALID_SYMBOL")

    def test_closed_candles_and_timestamp_units_only(self):
        now = 1800000000.0
        t1 = now-7200
        t2 = now-5400
        t3 = now-600  # still open, must be ignored
        rows = [{"time": int(t1*1000), "close": "100"},
                {"time": int(t2*1000), "close": "90"},
                {"time": int(t3*1000), "close": "500"}]
        bars = cg._closed_rows(self.fake(rows).json(), now)
        self.assertEqual(len(bars), 2)
        metric = cg._metric("aggregated_oi", bars)
        self.assertEqual(metric["oi_close_source_units"], 90)
        self.assertAlmostEqual(metric["oi_change_pct_30m"], -10)

    def test_cvd_and_net_position_are_distinct_provenance(self):
        now = 1800000000.0
        rows = [
            {"time": int((now-3600)*1000), "cum_vol_delta": "-300"},
            {"time": int((now-1800)*1000), "cum_vol_delta": "-375"},
        ]
        bars = cg._closed_rows(self.fake(rows).json(), now)
        metric = cg._metric("futures_cvd_binance", bars)
        self.assertEqual(metric["cvd_cumulative_source_units"], -375)
        self.assertEqual(metric["cvd_change_30m_source_units"], -75)
        pos = cg._metric("net_position_v2_binance", [(
            now-1800, {"net_long_change": -70, "net_short_change": 90})])
        self.assertEqual(pos["net_long_change_source_units"], -70)
        self.assertNotIn("net_longs_new", pos)

    def test_partial_data_never_fill_missing_values(self):
        now = 1800000000.0
        t = int((now-1800)*1000)
        calls = []
        def request(url, headers, params, timeout):
            calls.append((url, params, timeout))
            self.assertEqual(headers["CG-API-KEY"], "secret")
            if "open-interest" in url:
                return self.fake([{"time": t, "close": "23000000"}])
            if "liquidation" in url:
                return self.fake([{"time": t, "aggregated_long_liquidation_usd": 50,
                                   "aggregated_short_liquidation_usd": 40}])
            return self.fake([])
        result = cg.collect("GRASSUSDT", observed_at=now,
                            request_get=request, api_key="secret")
        self.assertEqual(result["status"], "PARTIAL")
        self.assertEqual(len(calls), len(cg.ENDPOINTS))
        self.assertEqual(calls[0][1]["symbol"], "GRASS")
        self.assertEqual(calls[-1][1]["symbol"], "GRASSUSDT")
        self.assertEqual(result["metrics"]["aggregated_oi"]["oi_close_source_units"],
                         23000000)
        self.assertEqual(result["metrics"]["liquidations_binance"][
                         "long_liquidation_usd"], 50)
        self.assertIn("futures_cvd_binance", result["unavailable"])
        self.assertTrue(result["not_trading_confirmation"])

    def test_record_in_existing_memory_database_no_trade_change(self):
        import tempfile
        import sqlite3
        from pathlib import Path
        with tempfile.TemporaryDirectory() as temp:
            with patch.dict("os.environ",
                            {"MARKET_MEMORY_DB": str(Path(temp) / "memory.db")}):
                data = {"symbol": "GRASSUSDT", "observed_at": 1791980000,
                        "retrieved_at": 1791980030, "status": "PARTIAL",
                        "metrics": {}, "unavailable": {"spot_cvd_binance":"HTTP_403"}}
                cg._save(data)
                with sqlite3.connect(str(Path(temp) / "memory.db")) as db:
                    saved = db.execute("SELECT symbol,payload_json FROM coinglass_context").fetchone()
                self.assertEqual(saved[0], "GRASSUSDT")
                self.assertIn("HTTP_403", saved[1])

    def test_bad_http_or_wrong_api_schema_do_not_manufacture_signals(self):
        now = 1800000000
        t = int((now-1800)*1000)
        bad = [
            ({"code": "400", "data":[{"time":t,"close":1}]}, []),
            ({"code":"0", "data":{"time":t,"close":1}}, []),
            ({"code":"0", "data":[{"time":t,"close":"NaN"}]}, [(now-1800, {"time":t,"close":"NaN"})]),
        ]
        for data, expected in bad:
            # Wrong shapes and non-success codes must never masquerade as quotes.
            self.assertEqual(cg._closed_rows(data, now), expected)
        self.assertIsNone(cg._metric(
            "aggregated_oi", [(now-1800, {"close":"nan"})]))

if __name__ == "__main__":
    unittest.main()
