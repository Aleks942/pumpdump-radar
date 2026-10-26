from pathlib import Path
import unittest

import okx_move_verifier as verifier


class OkxMoveVerifierTests(unittest.TestCase):
    def source(self):
        return Path(__file__).with_name("okx_move_verifier.py").read_text(
            encoding="utf-8"
        )

    def test_okx_verifier_uses_existing_strict_layers(self):
        code = self.source()
        for required in (
            "get_futures_windows",
            "get_aggregated_oi",
            "get_fallback_spot_flow",
            "fetch_okx_liquidations",
            "detect_pattern",
            'checkpoint == "5m"',
            'verdict_code == "CONFIRMED"',
            "[OKX_VERIFY_SENT]",
            "telegram=confirmed_5m_only",
        ):
            self.assertIn(required, code)

    def test_okx_futures_flow_fails_closed_without_real_trade_quality(self):
        original = verifier.get_futures_windows
        try:
            verifier.get_futures_windows = lambda _symbol: {
                "5m": {
                    "window_ready": True,
                    "quality": "READY",
                    "imbalance_pct": -35.0,
                    "trade_count": 2,
                    "total_quote": 100000,
                    "last_trade_age_sec": 2,
                    "coverage_seconds": 300,
                    "delta_quote": -5000,
                }
            }
            row = verifier._okx_futures_flow("TESTUSDT")
            self.assertFalse(row["ready"])

            verifier.get_futures_windows = lambda _symbol: {
                "5m": {
                    "window_ready": True,
                    "quality": "READY",
                    "imbalance_pct": -35.0,
                    "trade_count": 20,
                    "total_quote": 100000,
                    "last_trade_age_sec": 2,
                    "coverage_seconds": 300,
                    "delta_quote": -5000,
                }
            }
            row = verifier._okx_futures_flow("TESTUSDT")
            self.assertTrue(row["ready"])
            self.assertEqual(row["source"], "OKX_SWAP_WS")
        finally:
            verifier.get_futures_windows = original

    def test_okx_oi_comes_from_ready_okx_aggregate_source(self):
        agg = {
            "sources": {
                "OKX": {
                    "quality": "READY",
                    "change_pct": 2.5,
                    "oi_coin": 1000,
                }
            }
        }
        row = verifier._okx_oi_from_aggregate(agg)
        self.assertTrue(row["ready"])
        self.assertEqual(row["change_pct"], 2.5)

        bad = verifier._okx_oi_from_aggregate({
            "sources": {"OKX": {"quality": "STALE", "change_pct": 2.5}}
        })
        self.assertFalse(bad["ready"])


if __name__ == "__main__":
    unittest.main()
