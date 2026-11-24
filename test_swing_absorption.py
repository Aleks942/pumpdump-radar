from contextlib import closing
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import swing_absorption as absorption
from swing_result_tracker import _conn, register_entry, update_results


def candles():
    return [
        {"ts": i * 3600, "o": 100 if i < 18 else 106,
         "c": 106 if i >= 17 else 100,
         "h": 106.5 if i >= 17 else 101,
         "l": 105.5 if i >= 18 else 99, "v": 200 if i >= 18 else 100}
        for i in range(21)
    ]


class AbsorptionTests(unittest.TestCase):
    def test_watch_requires_oi_aligned_to_range(self):
        self.assertIsNone(absorption.detect_watch(candles(), [(64801, 100), (75600, 102)]))
        self.assertEqual(absorption.detect_watch(candles(), [(64800, 100), (75600, 102)])["range_low"], 105.5)

    def test_watch_to_retest_links_only_existing_short_and_keeps_delta(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"SWING_RESULT_DB": str(Path(tmp) / "results.db")}
        ):
            hourly = candles()
            result = {"_research_h1": hourly, "oi4h": {"samples": [(64800, 100), (75600, 102)]}, "price": 106}
            absorption.record_observation("TESTUSDT", result, now=75600)
            absorption.record_observation(
                "TESTUSDT", result, now=75900,
                flow_get=lambda *args, **kwargs: {"ready": True, "imbalance_pct": 25},
            )
            hourly.append({"ts": 75600, "o": 106, "h": 106.3, "l": 104.5, "c": 105, "v": 200})
            # Actual closed 15m candles touch the broken range and then close lower.
            minute = [{"ts": 79200 - (23-i)*900, "o": 105, "h": 105.6, "l": 104.8, "c": 105}
                      for i in range(25)]
            minute[-1].update(h=105.2, l=104.2, c=104.4)
            result["_research_m15"] = minute
            absorption.record_observation("TESTUSDT", result, now=81000)
            with closing(_conn()):
                pass
            absorption.link_short_entry("TESTUSDT", 79200)
            with closing(absorption._connect()) as db:
                row = db.execute("SELECT phase, positive_delta_at, entry_id FROM swing_absorption").fetchone()
                self.assertEqual(tuple(row), ("RETEST", 75900, None))
            plan = {"ready": True, "stop": 106, "tp1": 100, "tp2": 98}
            self.assertTrue(register_entry(
                "TESTUSDT", "SHORT", 104, 12, now=81000, setup_at=79200, trade_plan=plan,
            ))
            absorption.link_short_entry("TESTUSDT", 79200)
            with closing(absorption._connect()) as db:
                self.assertEqual(db.execute("SELECT entry_id FROM swing_absorption").fetchone()[0], 1)
            update_results({"TESTUSDT": 106}, now=81100)
            with closing(absorption._connect()) as db:
                terminal = db.execute(
                    "SELECT event_type, r_multiple FROM swing_absorption "
                    "JOIN swing_events USING (entry_id)"
                ).fetchone()
                self.assertEqual(tuple(terminal), ("STOP", -1.0))

    def test_breakout_up_invalidates_watch_without_inventing_delta(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"SWING_RESULT_DB": str(Path(tmp) / "results.db")}
        ):
            hourly = candles()
            result = {"_research_h1": hourly, "oi4h": {"samples": [(64800, 100), (75600, 102)]}, "price": 106}
            absorption.record_observation("TESTUSDT", result, now=75600)
            hourly.append({"ts": 75600, "c": 107})
            absorption.record_observation("TESTUSDT", result, now=79200)
            with closing(absorption._connect()) as db:
                row = db.execute("SELECT phase, delta_at FROM swing_absorption").fetchone()
                self.assertEqual(tuple(row), ("INVALIDATED", None))
