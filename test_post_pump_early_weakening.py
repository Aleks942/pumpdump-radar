"""Offline, no-network tests for early post-pump research observations."""
import unittest
from post_pump_early_weakening import is_early_weakening


def bar(ts, close):
    return {"ready": True, "bearish5": True,
            "last5": {"ts": ts, "c": close}}


class EarlyWeakeningTests(unittest.TestCase):
    def test_fresh_closed_bar_with_lower_price(self):
        self.assertTrue(is_early_weakening(
            149.2, 147.8, 150.0, bar(1000, 148.5), bar(1300, 147.0)))

    def test_stale_or_same_candle_cannot_confirm(self):
        for ts in (999, 1000, 2000):
            with self.subTest(ts=ts):
                self.assertFalse(is_early_weakening(
                    149.2, 147.8, 150, bar(1000, 148.5), bar(ts, 147.0)))

    def test_rolling_high_alone_not_evidence(self):
        self.assertFalse(is_early_weakening(
            147.8, 147.8, 150, bar(1000, 148.5), bar(1300, 147.0)))

    def test_unchanged_five_minute_close_blocks(self):
        self.assertFalse(is_early_weakening(
            149.2, 147.8, 150, bar(1000, 147.0), bar(1300, 147.0)))

    def test_no_bearish_or_unready_structure_blocks(self):
        a=bar(1300,147.0)
        for field in ("ready","bearish5"):
            bad={**a,field:False}
            with self.subTest(field=field):
                self.assertFalse(is_early_weakening(
                    149.2,147.8,150,bar(1000,148.5),bad))

    def test_early_band_only_and_bad_inputs(self):
        self.assertFalse(is_early_weakening(
            150,146,150,bar(1000,148.5),bar(1300,147.0)))
        self.assertFalse(is_early_weakening(
            150,149,150,bar(1000,148.5),bar(1300,147.0)))
        self.assertFalse(is_early_weakening(
            float("nan"),147.8,150,bar(1000,148.5),bar(1300,147.0)))
        self.assertFalse(is_early_weakening(
            149.2,147.8,150,None,bar(1300,147.0)))


if __name__ == "__main__":
    unittest.main()
