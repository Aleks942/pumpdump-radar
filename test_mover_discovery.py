"""Network-free regression tests for early mover research and volume units."""
import io
import math
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import mover_discovery as mover
import coinglass_context as cg
import public_exchange_context as public_data
import pump_cycle_research as cycle
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

class PublicExchangeContextTests(unittest.TestCase):
    NOW = 1800000000.0
    TS = int((NOW - 300) * 1000)

    def response(self, body, code=200):
        class Result:
            def __init__(self, body, code):
                self.status_code = code
                self.body = body
            def json(self):
                return self.body
        return Result(body, code)

    def record(self, api_key=""):
        now = self.NOW
        last = self.TS
        earlier = last-300000
        requests_seen = []
        def request(url, params, headers, timeout):
            requests_seen.append((url, params, headers, timeout))
            self.assertEqual(params["symbol"], "GRASSUSDT")
            self.assertEqual(timeout, 3)
            if url.endswith("/globalLongShortAccountRatio"):
                return self.response([{"symbol": "GRASSUSDT", "timestamp": last,
                    "longAccount":"0.61", "shortAccount":"0.39"}])
            if url.endswith("/takerlongshortRatio"):
                return self.response([{"timestamp": earlier,
                    "buyVol":"120", "sellVol":"80"}])
            if url.endswith("/openInterestHist"):
                return self.response([
                    {"symbol":"GRASSUSDT","timestamp":earlier,
                     "sumOpenInterest":"2000","sumOpenInterestValue":"2500000"},
                    {"symbol":"GRASSUSDT","timestamp":last,
                     "sumOpenInterest":"1800","sumOpenInterestValue":"2400000"}])
            if url.endswith("/premiumIndex"):
                return self.response({"symbol":"GRASSUSDT","time":int(now*1000),
                    "lastFundingRate":"0.0001","nextFundingTime":int((now+1000)*1000)})
            if url.endswith("/account-ratio"):
                return self.response({"retCode":0,"result":{"list":[{
                    "symbol":"GRASSUSDT","buyRatio":"0.58",
                    "sellRatio":"0.42","timestamp":str(last)}]}})
            if url.endswith("/v5/market/open-interest"):
                return self.response({"retCode":0,"result":{"list":[
                    {"symbol":"GRASSUSDT","singleOpenInterest":"900",
                     "timestamp":str(last)},
                    {"symbol":"GRASSUSDT","singleOpenInterest":"1000",
                     "timestamp":str(earlier)}]}})
            if "topLongShort" in url:
                self.assertEqual(headers.get("X-MBX-APIKEY"), "abc123")
                return self.response([{"symbol":"GRASSUSDT",
                    "longAccount":"0.65","shortAccount":"0.35",
                    "timestamp":last}])
            self.fail("Unexpected public endpoint: "+url)
        with patch.dict("os.environ", {"BINANCE_MARKET_API_KEY":api_key}):
            result = public_data.collect("GRASSUSDT", observed_at=now,
                                         request_get=request)
        return result, requests_seen

    def test_all_six_open_endpoints_plus_optional_two(self):
        result, sent = self.record()
        self.assertEqual(len(sent), 6)
        self.assertEqual(len(result["metrics"]), 6)
        self.assertEqual(result["status"], "PARTIAL")
        self.assertEqual(result["unavailable"]["binance_top_accounts"],
                         "BINANCE_MARKET_API_KEY_NOT_CONFIGURED")
        self.assertEqual(result["metrics"]["binance_global_account_ratio"][
                         "values"]["long_share"], 0.61)
        self.assertEqual(result["metrics"]["binance_taker_buy_sell"][
                         "values"]["delta_ratio"], 0.2)
        self.assertEqual(result["metrics"]["binance_oi_history"][
                         "values"]["oi_change_pct_5m"], -10)
        self.assertEqual(result["metrics"]["bybit_oi_history"][
                         "values"]["oi_change_pct_5m"], -10)
        self.assertAlmostEqual(result["metrics"]["binance_funding"][
                               "values"]["last_funding_rate_fraction"], 0.0001)
        self.assertTrue(result["not_coinglass_net_new_positions"])
        self.assertFalse(result["trade_approved"])
        result2, sent2 = self.record(api_key="abc123")
        self.assertEqual(result2["status"], "OK")
        self.assertEqual(len(sent2), 8)
        self.assertEqual(result2["metrics"]["binance_top_positions"][
                         "values"]["metric_kind"], "top_trader_position_share")

    def test_missing_and_future_bars_cannot_invent_data(self):
        now = self.NOW
        bad = [
            {"timestamp":int((now-3500)*1000),"symbol":"GRASSUSDT",
             "longAccount":"0.8","shortAccount":"0.2"},
            {"timestamp":int((now+800)*1000),"symbol":"GRASSUSDT",
             "longAccount":"0.8","shortAccount":"0.2"},
            {"timestamp":self.TS,"symbol":"BTCUSDT",
             "longAccount":"0.8","shortAccount":"0.2"},
            {"timestamp":self.TS,"symbol":"GRASSUSDT",
             "longAccount":"nan","shortAccount":"0.2"},
            {"timestamp":self.TS,"symbol":"GRASSUSDT",
             "longAccount":"0.7","shortAccount":"0.1"},
        ]
        for row in bad:
            self.assertIsNone(public_data._select_metric(
                "binance_global_account_ratio","binance_series",
                [row],"GRASSUSDT",now))
        # Binance taker timestamps are bar start; still-forming 5m bar ignored.
        current = {"timestamp":int((now-100)*1000),"buyVol":"900","sellVol":"1"}
        self.assertIsNone(public_data._select_metric(
            "binance_taker_buy_sell","binance_series",
            [current],"GRASSUSDT",now))
        self.assertIsNone(public_data._select_metric(
            "bybit_account_ratio","bybit_series",
            {"retCode":10001,"result":{"list":[{"timestamp":self.TS}]}},
            "GRASSUSDT", now))
        self.assertEqual(public_data.collect("GRASS/USDT")["status"],
                         "INVALID_SYMBOL")

    def test_rate_limit_does_not_trigger_more_queries_on_same_exchange(self):
        calls=[]
        def request(url, params, headers, timeout):
            calls.append(url)
            return self.response({}, 429) if "fapi.binance.com" in url else self.response(
                {"retCode":0,"result":{"list":[]}})
        result=public_data.collect("GRASSUSDT", observed_at=self.NOW,
                                   request_get=request,binance_key="")
        self.assertEqual(len(calls), 3)  # 1 Binance + 2 Bybit
        self.assertEqual(result["status"], "UNAVAILABLE")
        self.assertEqual(result["unavailable"]["binance_taker_buy_sell"],
                         "EXCHANGE_RATE_LIMITED")

    def test_store_isolated_research_rows_only(self):
        import tempfile
        import sqlite3
        from pathlib import Path
        result,_=self.record()
        with tempfile.TemporaryDirectory() as d:
            with patch.dict("os.environ",{
                    "MARKET_MEMORY_DB":str(Path(d)/"memory.sqlite3")}):
                public_data._save(result)
                with sqlite3.connect(Path(d)/"memory.sqlite3") as db:
                    row=db.execute("""SELECT symbol,triggered_at,
                        payload_json FROM public_derivatives_context""").fetchone()
                    self.assertEqual(db.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE name='market_signals'").fetchone(),None)
            self.assertEqual(row[0], "GRASSUSDT")
            self.assertEqual(row[1], self.NOW)
            self.assertIn("bybit_account_ratio",row[2])
            self.assertIn("research_only",row[2])

    def test_original_alert_path_kept_and_exchange_context_only_after_ack(self):
        from pathlib import Path
        code=Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        self.assertEqual(code.count("schedule_public_exchange_context(symbol, observed_at=now)"),1)
        self.assertIn("if accepted is not True:",code)
        self.assertIn("maybe_send_market_watch(market_move, \"PRICE_CONTINUATION\")",code)
        self.assertIn("schedule_coinglass_context(symbol, observed_at=now)",code)


class PumpCycleResearchTests(unittest.TestCase):
    """Actual chronological sample chain, no exchange/API connections."""

    def setUp(self):
        import sqlite3
        import tempfile
        from pathlib import Path
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = str(Path(self.temp.name)/"watch.db")
        self.t0 = 1800000000.0
        self.symbol = "GRASSUSDT"
        self.sequence = []
        self.sent = []
        cycle._prices.clear()
        cycle._flow_cooldown.clear()
        with sqlite3.connect(self.db_path) as db:
            db.execute("""CREATE TABLE pump_watch_state (
                symbol TEXT PRIMARY KEY, anchor REAL NOT NULL, peak REAL NOT NULL,
                rise_pct REAL NOT NULL, falling INTEGER NOT NULL,
                updated_at REAL NOT NULL)""")

    def point(self, price, index, falling=0, side="GRASS"):
        import sqlite3
        at = self.t0+index*60
        self.sequence.append((at, price))
        peak = max(p for _,p in self.sequence)
        with sqlite3.connect(self.db_path) as db:
            db.execute("""INSERT OR REPLACE INTO pump_watch_state
                (symbol,anchor,peak,rise_pct,falling,updated_at)
                VALUES (?,?,?,?,?,?)""",
                (self.symbol,1,peak,(peak-1)*100,falling,at))
        return cycle.observe(
            [{"instId":side+"-USDT-SWAP", "last":str(price)}],
            now=at, db_path=self.db_path,
            flow_sender=lambda sym, px, ts, path: (
                self.sent.append((sym,px,ts,path)) or True))

    def state(self):
        import sqlite3
        with sqlite3.connect(self.db_path) as db:
            db.row_factory=sqlite3.Row
            return dict(db.execute("SELECT * FROM pump_cycle_state").fetchone())

    def prepared(self):
        seq=[1.17,1.21,1.19,1.16,1.18,1.20,1.23,1.26,1.24,1.19]
        for i,price in enumerate(seq):
            self.point(price,i,falling=2 if i==9 else 0)
        x=self.state()
        self.assertEqual(x["stage"],"PUMP_WEAKENING")
        self.assertAlmostEqual(x["support"],1.16)
        # A failed early pump can weaken, recover, then weaken again.
        # Both causal episodes may ask Flow; neither is a trade approval.
        self.assertGreaterEqual(len(self.sent),1)
        return x

    def test_full_cycle_requires_valid_pivot_and_fresh_sell_flow(self):
        x=self.prepared()
        at=self.t0+9*60+10
        self.assertTrue(cycle._record_flow(
            self.symbol,at,
            {"status":"CONFIRMED","delta_ratio":-.42,
             "finished_at":at+21,"evidence_event":"NONE"},
            now=at+23,db_path=self.db_path))
        self.point(1.155,10,falling=3)
        e=self.point(1.15,11,falling=4)
        self.assertEqual(self.state()["stage"],"REVERSAL_CONFIRMED")
        self.assertTrue(any(stage=="TREND_BREAK_CANDIDATE" for _,stage,_ in e))
        self.assertTrue(any(stage=="REVERSAL_CONFIRMED" for _,stage,_ in e))
        self.point(1.128,12,falling=5)
        self.assertEqual(self.state()["stage"],"DUMP_CONTINUATION")
        self.point(1.10,13,falling=6)
        self.point(1.122,14)
        self.assertEqual(self.state()["stage"],"DUMP_EXHAUSTION")
        import sqlite3
        with sqlite3.connect(self.db_path) as db:
            rows=db.execute("SELECT stage FROM pump_cycle_events ORDER BY id").fetchall()
        sequence=[r[0] for r in rows]
        # The impulse can briefly weaken, recover to a new high and
        # legitimately weaken again before the first confirmed breakdown.
        self.assertEqual(sequence[0],"PUMP_START")
        self.assertEqual(sequence[-5:],
                         ["PUMP_CONTINUATION","PUMP_WEAKENING",
                          "REVERSAL_CONFIRMED","DUMP_CONTINUATION",
                          "DUMP_EXHAUSTION"])
        self.assertTrue(all(s in cycle.STAGES for (s,) in rows))

    def test_absorption_and_positive_delta_do_not_approve_short(self):
        self.prepared()
        at=self.t0+9*60+10
        self.assertTrue(cycle._record_flow(
            self.symbol,at,
            {"status":"CONTRADICTION","delta_ratio":.6,
             "finished_at":at+21,
             "evidence_event":"BUY_ABSORPTION_CANDIDATE"},
            now=at+22,db_path=self.db_path))
        self.point(1.155,10,falling=3)
        self.point(1.15,11,falling=4)
        x=self.state()
        self.assertEqual(x["stage"],"PUMP_WEAKENING")
        self.assertIsNotNone(x["break_price"])
        self.assertEqual(x["flow_evidence"],"BUY_ABSORPTION_CANDIDATE")
        self.assertGreater(x["flow_delta"],0)

    def test_break_without_sell_flow_or_confirmed_pivot_stays_observation(self):
        self.prepared()
        self.point(1.15,10,falling=3)
        self.point(1.13,11,falling=4)
        self.assertEqual(self.state()["stage"],"PUMP_WEAKENING")
        self.assertIsNotNone(self.state()["break_at"])

    def test_reclaimed_high_invalidates_weakening_and_old_flow(self):
        self.prepared()
        at=self.t0+9*60+10
        self.assertTrue(cycle._record_flow(
            self.symbol,at,
            {"status":"CONFIRMED","delta_ratio":-.4,"finished_at":at+20},
            now=at+22,db_path=self.db_path))
        e=self.point(1.28,10,falling=0)
        self.assertEqual(self.state()["stage"],"PUMP_CONTINUATION")
        self.assertIsNone(self.state()["flow_delta"])
        self.assertIsNone(self.state()["support"])
        self.assertTrue(any(s=="PUMP_CONTINUATION" for _,s,_ in e))

    def test_no_invented_pivot_on_monotonic_dump(self):
        for i,px in enumerate([1.25,1.24,1.23,1.20,1.17,1.12]):
            self.point(px,i,falling=i)
        self.assertEqual(self.state()["stage"],"PUMP_WEAKENING")
        self.assertIsNone(self.state()["support"])
        self.point(1.07,6,falling=6)
        self.assertEqual(self.state()["stage"],"PUMP_WEAKENING")

    def test_early_continuation_starts_cycle_below_ten_percent(self):
        # No entry in the existing >10% Pump Watch table.
        first = cycle.observe(
            [{"instId":"GRASS-USDT-SWAP","last":"1.02"}],
            now=self.t0,db_path=self.db_path,
            seeded_pumps=[{"symbol":"GRASSUSDT","type":"PUMP",
                           "price":1.02,"change":2.0}],
            flow_sender=lambda *args: False)
        self.assertEqual(first[0][1],"PUMP_START")
        self.assertEqual(first[0][2],"EARLY_CONTINUATION_OBSERVED")
        self.assertEqual(self.state()["seed_source"],"EARLY")
        prices=[1.05,1.04,1.02,1.03,1.04,1.06,1.07,1.06,1.05]
        for i,price in enumerate(prices,1):
            cycle.observe(
                [{"instId":"GRASS-USDT-SWAP","last":str(price)}],
                now=self.t0+i*60,db_path=self.db_path,
                flow_sender=lambda *args: False)
        state=self.state()
        self.assertEqual(state["stage"],"PUMP_WEAKENING")
        self.assertAlmostEqual(state["support"],1.02)
        self.assertEqual(state["initial_move_pct"],2.0)
        self.assertLess(state["peak"],1.10)

    def test_early_dump_cannot_seed_bullish_lifecycle(self):
        events = cycle.observe(
            [{"instId":"GRASS-USDT-SWAP","last":"1.02"}],
            now=self.t0,db_path=self.db_path,
            seeded_pumps=[{"symbol":"GRASSUSDT","type":"DUMP",
                           "price":1.02,"change":-2.0}],
            flow_sender=lambda *args:False)
        self.assertEqual(events,[])
        import sqlite3
        with sqlite3.connect(self.db_path) as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM pump_cycle_state").fetchone()[0],0)

    def test_missing_and_invalid_samples_do_not_change_state(self):
        self.point(1.20,0)
        self.point(float("nan"),1)
        self.assertEqual(self.state()["last_at"],self.t0)
        self.assertEqual(cycle.observe(
            [{"instId":"GRASS-USDT-SWAP","last":"1.25"}],
            now=self.t0,db_path=self.db_path),[])
        self.assertEqual(self.state()["stage"],"PUMP_START")

    def test_malformed_or_stale_flow_result_rejected(self):
        self.prepared()
        at=self.t0+9*60+10
        invalid=[
            {"status":"PENDING","finished_at":at+22},
            {"status":"CONFIRMED","delta_ratio":-.6,"finished_at":at+100},
            {"status":"CONFIRMED","delta_ratio":-99,"finished_at":at+20},
            {"status":"CONFIRMED","delta_ratio":-.5,"finished_at":at-1},
            {"status":"CONFIRMED","delta_ratio":-.5,"finished_at":float("nan")},
        ]
        for res in invalid:
            self.assertFalse(cycle._record_flow(
                self.symbol,at,res,now=at+25,db_path=self.db_path))
        self.assertIsNone(self.state()["flow_status"])

    def test_trade_gate_and_telegram_not_touched(self):
        from pathlib import Path
        code=Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        self.assertEqual(code.count("observe_pump_cycle(tickers, seeded_pumps=early_continuations)"),1)
        self.assertIn("maybe_send_market_watch(market_move, \"PRICE_CONTINUATION\")",code)
        self.assertIn('if TELEGRAM_POLICY == "CONFIRMED_ONLY":',code)
        self.assertIn('confirm_trend_edge(signal)',code)


class PumpCycleOutcomeAuditTests(unittest.TestCase):
    """Research markouts use only actual observed future quotes."""

    def setUp(self):
        import tempfile
        from pathlib import Path
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path=str(Path(self.temp.name)/"pump_cycle.db")
        self.t0=1800000000.0
        cycle._last_cycle_quality_report=None
        self.create_case()

    def create_case(self,price=1.20):
        import sqlite3
        with sqlite3.connect(self.db_path) as db:
            cycle._init(db)
            db.execute("""INSERT INTO pump_cycle_events
              (symbol,at,stage,price,peak,reason)
              VALUES (?,?,?,?,?,?)""",
              ("GRASSUSDT",self.t0,"REVERSAL_CONFIRMED",
               price,1.40,"PRICE_STRUCTURE_AND_FRESH_SELL_FLOW"))

    def quote(self,price):
        return [{"instId":"GRASS-USDT-SWAP","last":str(price)}]

    def test_exact_horizon_markout_and_no_relabel_on_new_price(self):
        import sqlite3
        self.assertEqual(cycle.observe_outcomes(
            self.quote(1.10),now=self.t0+299,db_path=self.db_path),[])
        rows=cycle.observe_outcomes(
            self.quote(1.18),now=self.t0+305,db_path=self.db_path)
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0][3],"DONE")
        self.assertAlmostEqual(rows[0][2],(1.20-1.18)/1.20*100-0.22,
                               places=4)
        # Later better price cannot be substituted for the horizon snapshot.
        self.assertEqual(cycle.observe_outcomes(
            self.quote(.90),now=self.t0+330,db_path=self.db_path),[])
        with sqlite3.connect(self.db_path) as db:
            stored=db.execute("""SELECT sampled_at,quote_price,
                net_short_pct FROM pump_cycle_outcomes WHERE horizon_sec=300"""
                ).fetchone()
        self.assertEqual(stored[0],self.t0+305)
        self.assertEqual(stored[1],1.18)
        self.assertAlmostEqual(stored[2],(1.20-1.18)/1.20*100-0.22,
                               places=5)

    def test_missing_sample_is_not_synthetic_winner(self):
        import sqlite3
        marks=cycle.observe_outcomes([],now=self.t0+450,db_path=self.db_path)
        self.assertEqual(marks,[("GRASSUSDT",5,None,"MISSING")])
        cycle.observe_outcomes(self.quote(.50),now=self.t0+480,
                               db_path=self.db_path)
        with sqlite3.connect(self.db_path) as db:
            row=db.execute("""SELECT status,quote_price,net_short_pct
                FROM pump_cycle_outcomes WHERE horizon_sec=300""").fetchone()
        self.assertEqual(row,("MISSING",None,None))

    def test_adverse_short_is_negative_after_costs(self):
        import sqlite3
        rows=cycle.observe_outcomes(self.quote(1.22),
                                   now=self.t0+300,db_path=self.db_path)
        self.assertLess(rows[0][2],-0.22)
        report=cycle.report_outcomes(now=self.t0+301,db_path=self.db_path)
        self.assertEqual(len(report),1)
        self.assertEqual(report[0]["done"],1)
        self.assertEqual(report[0]["symbols"],1)
        self.assertEqual(report[0]["days"],1)
        self.assertEqual(report[0]["positive"],0)

    def test_at_30min_all_three_horizons_are_isolated(self):
        results=cycle.observe_outcomes(self.quote(1.15),
            now=self.t0+1800,db_path=self.db_path)
        self.assertEqual([(h,s) for _,h,_,s in results],
                         [(5,"MISSING"),(15,"MISSING"),(30,"DONE")])
        self.assertGreater(results[-1][2],0)


if __name__ == "__main__":
    unittest.main()
