from pathlib import Path
import unittest


class SwingResearchGateTests(unittest.TestCase):
    def setUp(self):
        self.code = Path(__file__).with_name("main.py").read_text(encoding="utf-8")

    def test_swing_research_is_off_by_default(self):
        self.assertIn(
            'SWING_RESEARCH_ENABLED = os.getenv("SWING_RESEARCH_ENABLED", "0") == "1"',
            self.code,
        )
        self.assertIn(
            'RESEARCH_PIPELINE_ENABLED = (\n    PRIMARY_SCAN_MODE == "LEGACY" or SWING_RESEARCH_ENABLED\n)',
            self.code,
        )

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
            "if RESEARCH_PIPELINE_ENABLED:\n    initial_tickers = get_market_tickers()"
        )
        worker_block = self.code[workers:self.code.index(
            'print(\n    "[RESEARCH_PIPELINE]"', workers
        )]
        self.assertIn("run_stream_forever", worker_block)
        self.assertIn("run_entry_tracker", worker_block)


if __name__ == "__main__":
    unittest.main()
