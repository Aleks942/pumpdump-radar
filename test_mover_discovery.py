"""Network-free regression tests for early mover research and volume units."""
import io
import math
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import mover_discovery as mover
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


if __name__ == "__main__":
    unittest.main()
