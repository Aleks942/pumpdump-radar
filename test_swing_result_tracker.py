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
        markouts=[x for x in out if x.get("kind")=="MARKOUT"]
        hours = [x["checkpoint_h"] for x in markouts]
        self.assertEqual(hours, [1, 4])
        self.assertAlmostEqual(markouts[-1]["return_pct"], 5.0, places=6)

    def test_short_markout_is_direction_adjusted(self):
        now = 1_700_000_000
        self.assertTrue(s.register_entry("BBBUSDT", "SHORT", 100, 20, now))
        out = s.update_results({"BBBUSDT": 95}, now + 3601)
        markouts=[x for x in out if x.get("kind")=="MARKOUT"]
        self.assertEqual(len(markouts), 1)
        self.assertAlmostEqual(markouts[0]["return_pct"], 5.0, places=6)

    def test_duplicate_active_entry_is_ignored_beyond_one_hour(self):
        now = 1_700_000_000
        plan={"ready":True,"stop":98,"tp1":104,"tp2":106}
        self.assertTrue(
            s.register_entry("AAAUSDT","LONG",100,82,now,trade_plan=plan)
        )
        self.assertFalse(
            s.register_entry("AAAUSDT","LONG",101,84,now+2*3600,trade_plan={
                "ready":True,"stop":99,"tp1":105,"tp2":107
            })
        )

    def test_long_plan_records_tp1_and_tp2_observed_events(self):
        now=1_700_000_000
        plan={"ready":True,"stop":98,"tp1":104,"tp2":106}
        self.assertTrue(
            s.register_entry("AAAUSDT","LONG",100,82,now,trade_plan=plan)
        )
        first=s.update_results({"AAAUSDT":104.2},now+600)
        events=[x for x in first if x.get("kind")=="EVENT"]
        self.assertEqual([x["event_type"] for x in events],["TP1"])
        self.assertEqual(events[0]["r_multiple"],2.0)

        second=s.update_results({"AAAUSDT":106.2},now+1200)
        events=[x for x in second if x.get("kind")=="EVENT"]
        self.assertEqual([x["event_type"] for x in events],["TP2"])
        self.assertEqual(events[0]["r_multiple"],3.0)

    def test_short_plan_records_stop(self):
        now=1_700_000_000
        plan={"ready":True,"stop":102,"tp1":96,"tp2":94}
        self.assertTrue(
            s.register_entry("BBBUSDT","SHORT",100,18,now,trade_plan=plan)
        )
        out=s.update_results({"BBBUSDT":102.5},now+300)
        events=[x for x in out if x.get("kind")=="EVENT"]
        self.assertEqual(len(events),1)
        self.assertEqual(events[0]["event_type"],"STOP")
        self.assertEqual(events[0]["r_multiple"],-1.0)

    def test_old_strategy_version_does_not_block_final_cohort(self):
        now=1_700_000_000
        plan={"ready":True,"stop":98,"tp1":104,"tp2":106}
        self.assertTrue(
            s.register_entry(
                "AAAUSDT","LONG",100,82,now,trade_plan=plan,
                strategy_version="legacy"
            )
        )
        self.assertTrue(
            s.register_entry(
                "AAAUSDT","LONG",100.2,83,now+60,trade_plan={
                    "ready":True,"stop":98.2,"tp1":104.2,"tp2":106.2
                }
            )
        )

    def test_invalid_trade_plan_is_rejected(self):
        now=1_700_000_000
        bad={"ready":True,"stop":101,"tp1":104,"tp2":106}
        self.assertFalse(
            s.register_entry("AAAUSDT","LONG",100,82,now,trade_plan=bad)
        )


if __name__ == "__main__":
    unittest.main()
