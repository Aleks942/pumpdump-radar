"""Deterministic regression tests for exploratory exit candidates."""
import unittest

from pump_exit_research import evaluate_exit, _cohort_split, _stats


def bar(minute, high, low, close):
    return (minute * 60000, high, low, close)


class ExitVariantTests(unittest.TestCase):
    def test_long_target_first(self):
        candles = [
            bar(0, 100.4, 99.7, 100.1),
            bar(1, 101.6, 100.0, 101.3),
        ]
        self.assertEqual(
            evaluate_exit(100, "LONG", candles, 1.5, 1.0, 2, 0.22),
            ("TP_FIRST", 1.28),
        )

    def test_short_target_first(self):
        candles = [
            bar(0, 100.2, 99.8, 100.0),
            bar(1, 99.9, 97.9, 98.5),
        ]
        self.assertEqual(
            evaluate_exit(100, "SHORT", candles, 2.0, 1.0, 2, 0.22),
            ("TP_FIRST", 1.78),
        )

    def test_stop_first(self):
        self.assertEqual(
            evaluate_exit(
                100, "SHORT", [bar(0, 101.7, 99.5, 101.0)],
                1.5, 1.5, 1, 0.22,
            ),
            ("SL_FIRST", -1.72),
        )

    def test_same_minute_assumes_stop(self):
        self.assertEqual(
            evaluate_exit(
                100, "LONG", [bar(0, 102.0, 98.0, 100.0)],
                1.5, 1.0, 1, 0.22,
            ),
            ("BOTH_ASSUME_STOP", -1.22),
        )

    def test_no_touch_exits_on_last_close(self):
        self.assertEqual(
            evaluate_exit(
                100, "LONG", [bar(0, 100.6, 99.6, 100.3)],
                1.5, 1.0, 1, 0.22,
            ),
            ("TIME_EXIT", 0.08),
        )

    def test_invalid_and_incomplete(self):
        self.assertEqual(
            evaluate_exit(100, "LONG", [], 1.5, 1.0, 5, 0.22),
            ("INCOMPLETE", None),
        )
        self.assertEqual(
            evaluate_exit(0, "LONG", [], 1.5, 1.0, 5, 0.22),
            ("INVALID_ENTRY", None),
        )

    def test_first_full_minute_open_changes_execution_result(self):
        candles = [
            bar(i, 102.5, 99.5, 100.4)
            for i in range(30)
        ]
        cases = [(3600.0, "BTCUSDT", "NEW_LONG_BUILDUP", "LONG",
                  100.0, 0, 102.0, candles)]
        alert = _stats(cases, 1.0, 1.0, 10, 0.22)
        delayed = _stats(
            cases, 1.0, 1.0, 10, 0.22,
            entry_mode="NEXT_FULL_1M_OPEN",
        )
        self.assertEqual(alert["n"], 1)
        self.assertEqual(delayed["n"], 1)
        self.assertEqual(alert["mean"], 0.78)
        self.assertEqual(delayed["mean"], -1.22)

    def test_time_split_keeps_boundary_out(self):
        entries = [(i * 3600.0, "BTCUSDT", "NEW_LONG_BUILDUP",
                    "LONG", 100.0, 0, [])
                   for i in range(20)]
        older, later = _cohort_split(entries)
        self.assertLess(older[-1][0], later[0][0])
        self.assertNotIn(entries[14], older)
        self.assertNotIn(entries[14], later)


if __name__ == "__main__":
    unittest.main()
