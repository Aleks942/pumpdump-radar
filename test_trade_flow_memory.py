"""Regression tests for PumpDump's compact per-trade storage."""
import time
import unittest

import trade_flow_collector_v3 as flow


class TradeFlowMemoryTests(unittest.TestCase):
    def setUp(self):
        flow.mark_stream_connected("spot")

    def tearDown(self):
        flow.mark_stream_disconnected("spot")

    def test_signed_quote_flow_preserves_trade_counts(self):
        now = time.time()
        self.assertTrue(flow.save_trade(
            "BTCUSDT", "spot", "BUY", price=100.0, size=2.0,
            event_ts=now - 30,
        ))
        self.assertTrue(flow.save_trade(
            "BTCUSDT", "spot", "SELL", price=50.0, size=3.0,
            event_ts=now - 15,
        ))
        observed = flow.get_flow("BTCUSDT", "spot", 60)
        self.assertEqual(observed["trade_count"], 2)
        self.assertEqual(observed["buy_count"], 1)
        self.assertEqual(observed["sell_count"], 1)
        self.assertAlmostEqual(observed["buy_quote"], 200.0)
        self.assertAlmostEqual(observed["sell_quote"], 150.0)
        self.assertAlmostEqual(observed["delta_quote"], 50.0)
        with flow._LOCK:
            row = flow.TRADE_HISTORY["spot"]["BTCUSDT"][0]
        self.assertIsInstance(row, tuple)
        self.assertEqual(len(row), 3)

    def test_old_trades_pruned_and_market_isolated(self):
        now = time.time()
        flow.save_trade("ETHUSDT", "spot", "BUY", 100, 1, event_ts=now - 901)
        # Stale rows must not affect new flow.
        self.assertEqual(
            flow.get_flow("ETHUSDT", "spot", 60)["trade_count"], 0
        )
        self.assertEqual(flow.get_history_size("ETHUSDT", "spot"), 0)
        self.assertEqual(flow.get_history_size("ETHUSDT", "swap"), 0)


if __name__ == "__main__":
    unittest.main()
