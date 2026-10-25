from pathlib import Path
import ast
import math
import unittest


class BybitMoveVerifierTests(unittest.TestCase):
    def source(self):
        return Path(__file__).with_name("bybit_move_verifier.py").read_text(
            encoding="utf-8")

    def test_main_wires_bybit_watch_into_verifier(self):
        code = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        self.assertIn("schedule_bybit_move_verification", code)
        self.assertIn('str(signal.get("exchange") or "").upper() == "BYBIT"', code)

    def test_verifier_checks_required_context_and_fails_closed(self):
        code = self.source()
        for required in (
            "/v5/market/open-interest",
            "/v5/market/recent-trade",
            "/v5/market/tickers",
            "/v5/market/kline",
            "watch_bybit_symbol",
            "get_liquidation_summary",
            "get_aggregated_oi",
            "detect_pattern",
            '"PARTIAL"',
            '"quality": "UNAVAILABLE"',
            '"trade_approved": False',
            '"research_only": True',
            "[BYBIT_VERIFY_SENT]",
        ):
            self.assertIn(required, code)

    def test_confirmed_only_telegram_suppresses_watch_noise(self):
        main = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        strict = main.index('globals().get("TELEGRAM_POLICY", "LEGACY") == "CONFIRMED_ONLY"')
        legacy_send = main.index("message = build_market_watch_message")
        self.assertLess(strict, legacy_send)
        self.assertIn("[PUMP_INFO_SILENT]", main)

        code = self.source()
        self.assertIn('checkpoint == "5m"', code)
        self.assertIn('verdict_code == "CONFIRMED"', code)
        self.assertIn("[BYBIT_VERIFY_INTERNAL]", code)
        self.assertIn("telegram=confirmed_5m_only", code)

    def test_sequential_checkpoints_and_live_stream_are_wired(self):
        code = self.source()
        self.assertIn(
            'CHECKPOINTS = ((2.0, "NOW"), (60.0, "1m"), '
            '(180.0, "3m"), (300.0, "5m"))',
            code,
        )
        self.assertIn("watch_flow_symbol(symbol, started_at=at)", code)
        self.assertIn("get_live_flow", code)
        self.assertIn("get_fallback_spot_flow", code)
        self.assertIn("watch_fallback_spot(symbol, started_at=at)", code)
        self.assertIn("unwatch_fallback_spot(symbol)", code)
        self.assertIn("unwatch_flow_symbol(symbol)", code)
        self.assertIn("futures_coverage_sec=", code)
        stream = Path(__file__).with_name("bybit_taker_stream.py").read_text(
            encoding="utf-8")
        self.assertIn("publicTrade.", stream)
        self.assertIn("BYBIT_PUBLIC_TRADE_WS", stream)
        self.assertIn("coverage >= 240.0", stream)

    def test_followup_message_reports_all_layers(self):
        code = self.source()
        for label in (
            "OI Bybit",
            "Aggregated OI",
            "Futures CVD",
            "Spot CVD",
            "Funding Bybit",
            "Тренд закрытых 5м",
            "Тренд закрытых H1",
            "Ликвидации 5м",
            "Research-only: это не команда на сделку",
            "ФИНАЛ 5М",
        ):
            self.assertIn(label, code)

    def classification_namespace(self):
        tree = ast.parse(self.source())
        nodes = [
            node for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name in ("_finite", "classify")
        ]
        ns = {
            "math": math,
            "REVERSAL_BREAK_PCT": 2.0,
            "FLOW_CONFIRM_PCT": 10.0,
            "SPOT_CONFIRM_PCT": 5.0,
        }
        exec(compile(ast.fix_missing_locations(
            ast.Module(body=nodes, type_ignores=[])),
            "classification", "exec"), ns)
        return ns["classify"]

    def test_classification_confirms_only_with_ready_aligned_evidence(self):
        classify = self.classification_namespace()
        base = {
            "type": "DUMP",
            "post_trigger_pct": -1.0,
            "futures": {"ready": True, "imbalance_pct": -20},
            "spot": {"ready": True, "imbalance_pct": -8},
            "trend_5m": {"trend": "DOWN"},
            "trend_60m": {"trend": "DOWN"},
            "pattern": {"direction": "DOWN"},
        }
        self.assertEqual(classify(base)["code"], "CONFIRMED")
        partial = dict(base)
        partial["futures"] = {"ready": False, "imbalance_pct": -40}
        self.assertEqual(classify(partial)["code"], "WATCH")

    def test_classification_breaks_on_real_reversal(self):
        classify = self.classification_namespace()
        result = {
            "type": "DUMP",
            "post_trigger_pct": 2.5,
            "futures": {"ready": False, "imbalance_pct": None},
            "spot": {"ready": False, "imbalance_pct": None},
            "trend_5m": {"trend": "UP"},
            "trend_60m": {"trend": "UP"},
            "pattern": {"direction": "NONE"},
        }
        self.assertEqual(classify(result)["code"], "BROKEN")


if __name__ == "__main__":
    unittest.main()
