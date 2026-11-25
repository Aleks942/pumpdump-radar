import ast
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest


class SwingResearchGateTests(unittest.TestCase):
    def setUp(self):
        self.code = Path(__file__).with_name("main.py").read_text(encoding="utf-8")

    def test_swing_is_primary_and_post_pump_has_its_own_switch(self):
        self.assertIn('PRIMARY_SCAN_MODE = "SWING"', self.code)
        self.assertIn('SWING_LEGACY_TELEGRAM = False', self.code)
        self.assertIn('SWING_RESEARCH_ENABLED = False', self.code)
        self.assertIn('RESEARCH_PIPELINE_ENABLED = False', self.code)
        self.assertIn('POST_PUMP_SHORT_ENABLED = os.getenv("POST_PUMP_SHORT_ENABLED", "1") == "1"', self.code)

    def test_market_research_is_gated_before_swing_core(self):
        research_gate = self.code.index("    if RESEARCH_PIPELINE_ENABLED:\n        try:\n            observe_retrace_tickers")
        swing_core = self.code.index("    current_chunk = get_rotation_chunk(tickers)")
        self.assertLess(research_gate, swing_core)
        block = self.code[research_gate:swing_core]
        self.assertIn("observe_movers(tickers)", block)
        self.assertIn("observe_pump_cycle(tickers", block)
        self.assertIn("observe_post_pump_short(tickers)", block)
        self.assertIn("observe_post_dump_long(tickers)", block)
        self.assertIn("observe_post_dump_accumulation(tickers)", block)

    def test_post_pump_runs_and_delivers_with_legacy_pipeline_disabled(self):
        tree = ast.parse(self.code)
        runner = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                      and node.name == "run_post_pump_short_scan")
        scan_loop = next(node for node in tree.body if isinstance(node, ast.While))
        activation = next(node for node in scan_loop.body if isinstance(node, ast.If)
                          and "POST_PUMP_SHORT_ENABLED or RESEARCH_PIPELINE_ENABLED" in ast.unparse(node.test))
        tasks = []

        def start(target, args, **options):
            tasks.append(options["name"])
            if options["name"] == "post-pump-short-scan":
                target(*args)

        def thread(**options):
            return SimpleNamespace(start=lambda: start(**options), is_alive=lambda: True)

        tickers = [{"instId": "AAA-USDT-SWAP"}]
        namespace = {
            "POST_PUMP_SHORT_ENABLED": True, "RESEARCH_PIPELINE_ENABLED": False,
            "_post_pump_flow_thread": None, "_post_pump_scan_thread": None,
            "threading": SimpleNamespace(Thread=thread), "tickers": tickers,
            "bybit_rows": [{"symbol": "AAAUSDT", "lastPrice": "101", "bid1Price": "100"}],
            "bybit_quotes_at": 1000,
            "run_stream_forever": Mock(), "observe_complete_post_pump_short": Mock(),
            "prepare_post_pump_notifications": Mock(), "deliver_post_pump_notifications": Mock(), "send_telegram": Mock(), "os": os,
        }
        exec(compile(ast.Module(body=[runner], type_ignores=[]), "main.py", "exec"), namespace)
        code = compile(ast.Module(body=[activation], type_ignores=[]), "main.py", "exec")
        with patch.dict(os.environ, {"POST_PUMP_TELEGRAM": "1"}):
            exec(code, namespace)
            exec(code, namespace)
        self.assertEqual(tasks, ["okx-swap-flow", "post-pump-short-scan"])
        namespace["observe_complete_post_pump_short"].assert_called_once_with(tickers)
        namespace["deliver_post_pump_notifications"].assert_called_once_with(
            namespace["send_telegram"], tickers, bybit_prices={"AAAUSDT": "100"}, bybit_observed_at=1000,
        )

    def test_hourly_research_reports_are_gated(self):
        self.assertIn(
            "if RESEARCH_PIPELINE_ENABLED and time.monotonic() - _last_edge_report_mono >= 3600:",
            self.code,
        )


    def test_legacy_startup_workers_are_not_default_swing_core(self):
        gate = self.code.index(
            "if RESEARCH_PIPELINE_ENABLED:\n    # Legacy/research infrastructure only."
        )
        startup = self.code[gate:self.code.index('print("🚀 PumpDump Radar V2 ONLINE"', gate)]
        self.assertIn("probe_bridge", startup)
        self.assertIn("initialize_market_memory()", startup)
        self.assertIn("test_binance()", startup)
        self.assertIn("test_bybit()", startup)

        workers = self.code.index(
            "if RESEARCH_PIPELINE_ENABLED:\n    threading.Thread("
        )
        worker_block = self.code[workers:self.code.index(
            'print(\n    "[RESEARCH_PIPELINE]"', workers
        )]
        self.assertNotIn("run_stream_forever", worker_block)
        self.assertIn("run_entry_tracker", worker_block)


if __name__ == "__main__":
    unittest.main()
