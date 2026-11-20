import os
import tempfile
import unittest

import swing_result_tracker as s


class SwingResultTrackerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        os.environ["SWING_RESULT_DB"] = self.tmp.name

    def tearDown(self):
        os.environ.pop("SWING_RESULT_DB", None)
        try:
            os.remove(self.tmp.name)
        except FileNotFoundError:
            pass

    def test_long_markout_checkpoints(self):
        now = 1_700_000_000
        self.assertTrue(s.register_entry("AAAUSDT", "LONG", 100, 82, now))
        out = s.update_results({"AAAUSDT": 105}, now + 4 * 3600 + 1)
        hours = [x["checkpoint_h"] for x in out]
        self.assertEqual(hours, [1, 4])
        self.assertAlmostEqual(out[-1]["return_pct"], 5.0, places=6)

    def test_short_markout_is_direction_adjusted(self):
        now = 1_700_000_000
        self.assertTrue(s.register_entry("BBBUSDT", "SHORT", 100, 20, now))
        out = s.update_results({"BBBUSDT": 95}, now + 3601)
        self.assertEqual(len(out), 1)
        self.assertAlmostEqual(out[0]["return_pct"], 5.0, places=6)

    def test_duplicate_entry_same_hour_is_ignored(self):
        now = 1_700_000_000
        self.assertTrue(s.register_entry("AAAUSDT", "LONG", 100, 82, now))
        self.assertFalse(s.register_entry("AAAUSDT", "LONG", 101, 84, now + 60))


if __name__ == "__main__":
    unittest.main()
