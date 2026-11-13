import sqlite3
import unittest
import long_forward_stats as s

class ForwardStatsTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(":memory:")
        self.db.row_factory=sqlite3.Row
        s.setup(self.db)
    def tearDown(self):
        self.db.close()
    def test_register_and_resolve_all_horizons(self):
        s.register(self.db,"ACCUM",1,"TESTUSDT","LONG_TRIGGER",1000,100)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM long_forward_outcomes").fetchone()[0],6)
        s.advance(self.db,"ACCUM",{"TESTUSDT":102},1300)
        r=self.db.execute("""SELECT status,markout_pct FROM long_forward_outcomes
            WHERE horizon_min=5""").fetchone()
        self.assertEqual(r["status"],"DONE")
        self.assertAlmostEqual(r["markout_pct"],2.0)
    def test_report_groups_phase_and_horizon(self):
        s.register(self.db,"LONG",2,"AUSDT","LONG_CONFIRMED",1000,100)
        s.register(self.db,"LONG",3,"BUSDT","LONG_CONFIRMED",1000,100)
        s.advance(self.db,"LONG",{"AUSDT":104,"BUSDT":98},1300)
        rows=s.report_rows(self.db,"LONG",days=30,now=1400)
        five=[x for x in rows if x["horizon_min"]==5][0]
        self.assertEqual(five["n"],2)
        self.assertEqual(five["positive_pct"],50.0)
        self.assertEqual(five["gain_ge_2_pct"],50.0)
    def test_late_quote_marked_missing(self):
        s.register(self.db,"LONG",4,"CUSDT","ENTRY_ZONE",1000,100)
        s.advance(self.db,"LONG",{"CUSDT":105},1700)
        r=self.db.execute("""SELECT status FROM long_forward_outcomes
            WHERE audit_id=4 AND horizon_min=5""").fetchone()
        self.assertEqual(r["status"],"MISSING")

if __name__=="__main__":
    unittest.main()
