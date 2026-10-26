"""Deterministic tests for strict aggregated OI consensus."""
import copy
import time
import unittest
from collections import deque

import aggregated_oi as agg


class AggregatedOiConsensusTests(unittest.TestCase):
    def setUp(self):
        with agg._lock:
            self.saved = {
                "maps": copy.deepcopy(agg._maps),
                "meta_at": dict(agg._meta_at),
                "history": copy.deepcopy(agg._history),
                "errors": dict(agg._errors),
            }
            agg._maps = {}
            agg._meta_at = {}
            agg._history = {}
            agg._errors = {}

    def tearDown(self):
        with agg._lock:
            agg._maps = self.saved["maps"]
            agg._meta_at = self.saved["meta_at"]
            agg._history = self.saved["history"]
            agg._errors = self.saved["errors"]

    def seed(self, symbol, supported, changes):
        now = time.time()
        with agg._lock:
            for exchange in agg.EXCHANGES:
                agg._meta_at[exchange] = now
                agg._maps[exchange] = (
                    {symbol: symbol}
                    if exchange in supported
                    else {}
                )

            for exchange, change_pct in changes.items():
                start = 1000.0
                end = start * (1.0 + change_pct / 100.0)
                agg._history[(exchange, symbol)] = deque([
                    (now - 300.0, start),
                    (now, end),
                ])

        return now

    def test_two_of_two_supported_is_medium_ready(self):
        symbol = "TESTUSDT"
        self.seed(
            symbol,
            supported={"OKX", "Binance"},
            changes={"OKX": 2.0, "Binance": 3.0},
        )

        window = agg.get_aggregated_oi(symbol)["windows"]["5m"]

        self.assertTrue(window["ready"])
        self.assertEqual(window["confidence"], "MEDIUM")
        self.assertEqual(window["consensus_mode"], "2/2_SUPPORTED")
        self.assertEqual(window["direction_consensus"], "EXPANSION")
        self.assertEqual(window["supported_sources"], 2)
        self.assertEqual(window["unsupported_exchanges"], ["Bybit"])

    def test_three_supported_with_direction_disagreement_is_blocked(self):
        symbol = "MIXUSDT"
        self.seed(
            symbol,
            supported=set(agg.EXCHANGES),
            changes={"OKX": 2.0, "Binance": 1.0, "Bybit": -1.0},
        )

        window = agg.get_aggregated_oi(symbol)["windows"]["5m"]

        self.assertFalse(window["ready"])
        self.assertEqual(window["quality"], "DIRECTION_MISMATCH")
        self.assertEqual(window["confidence"], "BLOCK")

    def test_supported_exchange_data_failure_does_not_become_two_of_two(self):
        symbol = "FAILUSDT"
        self.seed(
            symbol,
            supported=set(agg.EXCHANGES),
            changes={"OKX": 2.0, "Binance": 2.0},
        )

        window = agg.get_aggregated_oi(symbol)["windows"]["5m"]

        self.assertFalse(window["ready"])
        self.assertEqual(window["quality"], "INCOMPLETE")
        self.assertEqual(window["supported_sources"], 3)

    def test_one_supported_exchange_is_never_enough(self):
        symbol = "ONEUSDT"
        self.seed(
            symbol,
            supported={"Bybit"},
            changes={"Bybit": -2.0},
        )

        window = agg.get_aggregated_oi(symbol)["windows"]["5m"]

        self.assertFalse(window["ready"])
        self.assertEqual(window["quality"], "INSUFFICIENT_SOURCES")
        self.assertEqual(window["confidence"], "BLOCK")


if __name__ == "__main__":
    unittest.main()
