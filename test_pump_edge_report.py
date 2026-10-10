"""Deterministic PumpDump 1-minute first-touch audit tests."""
import unittest

from pump_edge_report import first_touch, _full_history


def candle(i, hi, lo, close):
    return (i * 60000, hi, lo, close)


class FirstTouchTests(unittest.TestCase):
    def test_long_first_tp(self):
        bars = [candle(0, 100.5, 99.8, 100.1),
                candle(1, 101.2, 100.0, 101.0)]
        self.assertEqual(
            first_touch(100, "LONG", bars, 2),
            ("TP_FIRST", 0.78),
        )

    def test_short_first_stop(self):
        bars = [candle(0, 101.2, 99.9, 100.8)]
        self.assertEqual(
            first_touch(100, "SHORT", bars, 1),
            ("SL_FIRST", -1.22),
        )

    def test_both_touch_same_minute_excluded(self):
        bars = [candle(0, 101.5, 98.5, 100.0)]
        self.assertEqual(
            first_touch(100, "LONG", bars, 1),
            ("SAME_MINUTE", None),
        )

    def test_time_exit_marked_to_close(self):
        bars = [candle(0, 100.4, 99.8, 100.2),
                candle(1, 100.4, 99.9, 100.3)]
        self.assertEqual(
            first_touch(100, "LONG", bars, 2),
            ("TIME_EXIT", 0.08),
        )

    def test_incomplete_or_gapped_data_not_accepted(self):
        self.assertEqual(
            first_touch(100, "LONG", [], 5),
            ("INCOMPLETE", None),
        )
        candles = [candle(i, 100.5, 99.5, 100.0) for i in range(30)]
        self.assertTrue(_full_history(candles, 0))
        candles[2] = candle(4, 100.5, 99.5, 100)
        self.assertFalse(_full_history(candles, 0))


if __name__ == "__main__":
    unittest.main()
