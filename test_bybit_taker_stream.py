from collections import deque
import unittest

import bybit_taker_stream as stream


class BybitTakerStreamTests(unittest.TestCase):
    def setUp(self):
        stream._watch_ref.clear()
        stream._watch_at.clear()
        stream._events.clear()
        stream._spot_support.clear()
        stream._connected["linear"] = True
        stream._connected["spot"] = True

    def test_since_excludes_pre_trigger_trades(self):
        symbol = "TESTUSDT"
        now = 10_000.0
        stream._watch_ref[symbol] = 2
        stream._watch_at[symbol] = now - 300
        rows = deque()
        # Old sequence is strongly BUY before the second trigger.
        for i in range(20):
            rows.append((now - 250 + i * 5, 100.0))
        # New sequence is SELL after the second trigger.
        for i in range(20):
            rows.append((now - 100 + i * 5, -100.0))
        stream._events[("linear", symbol)] = rows

        result = stream.get_flow(
            symbol,
            category="linear",
            now=now,
            since=now - 105,
        )
        self.assertLess(result["imbalance_pct"], 0)
        self.assertLessEqual(result["coverage_sec"], 105)

    def test_no_since_keeps_normal_rolling_window(self):
        symbol = "TESTUSDT"
        now = 10_000.0
        stream._watch_ref[symbol] = 1
        stream._watch_at[symbol] = now - 300
        rows = deque(
            (now - 250 + i * (249 / 29), 100.0 if i < 20 else -100.0)
            for i in range(30)
        )
        stream._events[("linear", symbol)] = rows
        result = stream.get_flow(symbol, category="linear", now=now)
        self.assertTrue(result["ready"])
        self.assertGreaterEqual(result["coverage_sec"], 240)


if __name__ == "__main__":
    unittest.main()
