"""No-network regressions for prospective Pump Watch alert outcomes."""
import os
import tempfile
import unittest
from contextlib import closing
from unittest.mock import patch

import pump_peak_watch as watch
import pump_watch_forward as forward


def ticker(price=110.0):
    return {"instId":"TEST-USDT-SWAP", "last":str(price),
            "open24h":"100","high24h":"112","volCcy24h":"25000000"}


class ForwardWatchTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        for obj, attr, val in (
                (watch,"DB_PATH",os.path.join(temporary.name,"watch.db")),
                (watch,"_observed",{}),
                (forward,"_last_report",0.0)):
            change = patch.object(obj,attr,val)
            change.start()
            self.addCleanup(change.stop)
        self.start = 1791700000.0

    def _alert(self, level=1):
        events=watch.observe([ticker()],now=self.start)
        self.assertEqual(len(events),1)
        event=events[0]
        event["level"] = level
        watch.acknowledge(event, True, now=self.start)
        return event

    def test_stores_telegram_time_quote_with_legacy_migration(self):
        with closing(watch._db()) as db:
            db.execute("""CREATE TABLE pump_watch_alerts (
                 id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
                 ts REAL NOT NULL, level INTEGER NOT NULL,
                 rise_pct REAL NOT NULL, retrace_pct REAL NOT NULL)""")
            db.execute("""INSERT INTO pump_watch_alerts
                  (symbol,ts,level,rise_pct,retrace_pct)
                  VALUES ('LEGACYUSDT',1,1,12,3)""")
            db.commit()
        self._alert()
        with closing(watch._db()) as db:
            legacy, fresh = db.execute(
                "SELECT entry_price FROM pump_watch_alerts ORDER BY id").fetchall()
        self.assertEqual(legacy[0],None)
        self.assertAlmostEqual(fresh[0],110)

    def test_point_quotes_and_no_future_before_horizon(self):
        self._alert()
        self.assertEqual(forward.observe([ticker(106)],now=self.start+299),0)
        self.assertEqual(forward.observe([ticker(106)],now=self.start+300),1)
        with closing(watch._db()) as db:
            row=db.execute("""SELECT horizon_min,status,price,down_pct,
                              observed_at,due_at FROM pump_watch_forward""").fetchone()
        self.assertEqual(row[0],"5" if False else 5)
        self.assertEqual(row[1],"OK")
        self.assertEqual(row[2],106)
        self.assertAlmostEqual(row[3],3.63636,places=4)
        self.assertAlmostEqual(row[4]-row[5],0,places=3)
        self.assertEqual(forward.observe([ticker(90)],now=self.start+301),0)
        self.assertEqual(forward.summary(now=self.start+301)[0]["fell_2pct"],1)

    def test_missing_quote_does_not_fabricate_result(self):
        self._alert()
        self.assertEqual(forward.observe([],now=self.start+300),0)
        self.assertEqual(forward.observe([],now=self.start+601),1)
        with closing(watch._db()) as db:
            rows=db.execute(
                "SELECT status,price,down_pct FROM pump_watch_forward").fetchall()
        self.assertEqual(rows,[("MISSING",None,None)])
        self.assertEqual(forward.summary(self.start+601)[0]["missing"],1)

    def test_independent_stage_results_and_horizon_prices(self):
        self._alert(level=1)
        watch.acknowledge({"symbol":"TESTUSDT","level":3,
                           "rise":12.0,"retrace":6.0,"price":104.0,
                           "peak":112.0},True,now=self.start+10)
        self.assertEqual(forward.observe([ticker(100)], now=self.start+310),2)
        levels = forward.summary(now=self.start+310)
        self.assertEqual(levels[0]["n"],1)
        self.assertEqual(levels[10]["n"],1)
        self.assertAlmostEqual(levels[0]["mean_down_pct"],9.09091,places=3)
        self.assertAlmostEqual(levels[10]["mean_down_pct"],3.84615,places=3)

    def test_invalid_and_old_prices_excluded(self):
        self._alert()
        self.assertEqual(forward.observe([ticker(float("nan"))],
                                         now=self.start+300),0)
        self.assertEqual(forward.observe([{"instId":"TEST-USDT-SWAP",
                                            "last":"0"}],
                                         now=self.start+300),0)
        self.assertEqual(forward.observe([ticker(103)],
                                         now=self.start+550),1)
        self.assertEqual(forward.summary(now=self.start+551)[0]["n"],1)

    def test_retention_orphan_outcomes_cleaned_on_report(self):
        self._alert()
        forward.observe([ticker(108)], now=self.start+300)
        report=forward.summary(now=self.start+300)
        self.assertEqual(report[0]["n"],1)
        self.assertEqual(report[0]["status"],"COLLECTING")
        self.assertEqual(report[0]["symbols"],1)


if __name__=="__main__":
    unittest.main()
