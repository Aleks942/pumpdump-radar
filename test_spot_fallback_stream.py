from collections import deque
from pathlib import Path
import unittest

import spot_fallback_stream as stream


class SpotFallbackStreamTests(unittest.TestCase):
    def setUp(self):
        stream._watch_ref.clear()
        stream._watch_at.clear()
        stream._source.clear()
        stream._events.clear()
        stream._subscribed["OKX"].clear()
        stream._subscribed["Binance"].clear()
        stream._connected["OKX"] = False
        stream._connected["Binance"] = False

    def test_ready_window_uses_real_accumulated_trades(self):
        symbol = "TESTUSDT"
        now = 10_000.0
        stream._watch_ref[symbol] = 1
        stream._watch_at[symbol] = now - 300
        stream._source[symbol] = "OKX"
        stream._connected["OKX"] = True
        rows = deque()
        for i in range(30):
            ts = now - 250 + i * (249 / 29)
            signed_quote = 100.0 if i < 20 else -100.0
            rows.append((ts, signed_quote))
        stream._events[symbol] = rows

        result = stream.get_flow(symbol, now=now)
        self.assertTrue(result["ready"])
        self.assertEqual(result["quality"], "READY")
        self.assertEqual(result["source"], "OKX_SPOT_WS")
        self.assertGreaterEqual(result["coverage_sec"], 240)
        self.assertGreater(result["imbalance_pct"], 0)

    def test_unlisted_market_stays_unsupported(self):
        symbol = "NOPEUSDT"
        stream._watch_ref[symbol] = 1
        stream._watch_at[symbol] = 1000
        stream._source[symbol] = "UNSUPPORTED"
        result = stream.get_flow(symbol, now=1100)
        self.assertFalse(result["ready"])
        self.assertEqual(result["quality"], "UNSUPPORTED")

    def test_source_contains_okx_and_binance_fallbacks(self):
        code = Path(__file__).with_name("spot_fallback_stream.py").read_text(
            encoding="utf-8")
        self.assertIn("wss://ws.okx.com:8443/ws/v5/public", code)
        self.assertIn("wss://stream.binance.com:9443/ws", code)
        self.assertIn('source = "OKX"', code)
        self.assertIn('source = "Binance"', code)
        self.assertIn("coverage >= 240.0", code)


if __name__ == "__main__":
    unittest.main()
