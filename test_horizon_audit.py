"""No-network regression tests for 48-hour PumpDump audit."""
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import horizon_audit as study
import trend_probability_gate as gate

NOW = 1800000000.0


def candidate(ts=NOW, price=100):
    return {"symbol":"TESTUSDT", "price":price,
            "entry_observed_at":datetime.fromtimestamp(ts,timezone.utc),
            "decision":{"pattern":"NEW_LONG_BUILDUP","direction":"UP"}}


def history(ts, due, high=100.2, low=99.8, close=100, both=False):
    first=(int(ts//900)+1)*900
    last=int(due//900)*900-900
    data=[]
    for i,at in enumerate(range(first,last+1,900)):
        hi = 102 if both and i == 1 else high
        lo = 98 if both and i == 1 else low
        data.append([str(at*1000),str(close),str(hi),str(lo),
                     str(close),"1","1","1","1"])
    return list(reversed(data))


class HorizonAuditTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        for obj,name,value in (
            (gate,"DB_PATH",os.path.join(temp.name,"audit.sqlite3")),
            (study,"_last_request",0.0),
            (study,"_last_cleanup",0.0),
            (study,"_last_report",0.0)):
            p=patch.object(obj,name,value);p.start();self.addCleanup(p.stop)

    def test_records_quote_no_candles_and_deduplicates(self):
        self.assertTrue(study.record(candidate(), NOW))
        self.assertFalse(study.record(candidate(NOW+60), NOW+60))
        with study._conn() as db:
            result=db.execute("SELECT ts,entry_price FROM horizon_cases").fetchall()
            self.assertEqual(result,[(NOW,100.0)])
        self.assertFalse(study.record(candidate(price=0),NOW+600))

    def test_incomplete_future_or_gapped_bars_rejected(self):
        ts=NOW+17
        due=ts+3600
        rows=history(ts,due)
        self.assertIsNotNone(study._analyze(rows,ts,due,"LONG",100))
        self.assertIsNone(study._analyze(rows[1:],ts,due,"LONG",100))
        forming=[list(r) for r in rows]
        forming[0][-1]="0"
        self.assertIsNone(study._analyze(forming,ts,due,"LONG",100))
        # Rows outside the window (including future price spikes) do not count.
        future=["9999999999999","100","500","1","100","1","1","1","1"]
        self.assertEqual(study._analyze(rows+[future],ts,due,"LONG",100),
                         study._analyze(rows,ts,due,"LONG",100))

    def test_first_touch_ambiguous_is_not_a_winner(self):
        ts=NOW+17
        due=ts+3600
        ambiguous=study._analyze(history(ts,due,both=True),
                                  ts,due,"LONG",100)
        self.assertEqual(ambiguous[0],"AMBIGUOUS_SAME_BAR")
        self.assertEqual(study._analyze(history(ts,due,high=102),
                                         ts,due,"LONG",100)[0],
                         "TP_OBSERVED_AFTER_GAP")
        self.assertEqual(study._analyze(history(ts,due,low=98),
                                         ts,due,"SHORT",100)[0],
                         "TP_OBSERVED_AFTER_GAP")

    def test_due_checks_end_at_48_hours_and_are_bounded(self):
        ts=NOW+17
        self.assertTrue(study.record(candidate(ts),ts))
        self.assertEqual(study.advance(ts+3500),0)
        queries=[]
        def fake(symbol,bar,limit):
            queries.append((symbol,bar,limit))
            return history(ts,ts+3600)
        with patch.object(gate,"_okx",side_effect=fake):
            self.assertEqual(study.advance(ts+3600+120),1)
            self.assertEqual(study.advance(ts+3600+140),0)
        self.assertEqual(queries,[("TESTUSDT","15m",200)])
        with study._conn() as db:
            r=db.execute("SELECT horizon_h,status FROM horizon_outcomes").fetchall()
            self.assertEqual(r,[(1,"OK")])
        # All other horizons expire cleanly when their exchange data is unavailable.
        for hours in (4,24,48):
            now=ts+hours*3600+3601
            self.assertEqual(study.advance(now),1)
        with study._conn() as db:
            rows=db.execute("""SELECT horizon_h,status FROM horizon_outcomes
                ORDER BY horizon_h""").fetchall()
            self.assertEqual(rows,[(1,"OK"),(4,"MISSING_LATE"),
                                   (24,"MISSING_LATE"),(48,"MISSING_LATE")])
            self.assertNotIn(168, [h for h,stat in rows])

    def test_stores_48h_result_without_retaining_bars(self):
        ts=NOW+17
        self.assertTrue(study.record(candidate(ts),ts))
        due=ts+48*3600
        # First make 1h/4h/24h checkpoints missing without exchange calls.
        for hour in (1,4,24):
            self.assertEqual(study.advance(ts+hour*3600+3700),1)
        with patch.object(gate,"_okx",return_value=history(ts,due)):
            self.assertEqual(study.advance(due+120),1)
        with study._conn() as db:
            c=db.execute("SELECT COUNT(*) FROM horizon_cases").fetchone()[0]
            r=db.execute("""SELECT horizon_h,status,verdict,net_close_pct
                FROM horizon_outcomes WHERE horizon_h=48""").fetchone()
            self.assertEqual(c,1)
            self.assertEqual(r[:3],(48,"OK","NO_OBSERVED_TOUCH"))
            self.assertAlmostEqual(r[3],-0.22)
            cols=[x[1] for x in db.execute("PRAGMA table_info(horizon_cases)")]
            self.assertNotIn("candles",cols)


if __name__=="__main__":
    unittest.main()
