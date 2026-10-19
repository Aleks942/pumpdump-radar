"""Offline 60-minute first-touch execution-proxy safety regression tests."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

import post_pump_short_paper as paper

START=1800000000.0


def bars(start=START,opens=None,highs=None,lows=None,missing=None):
    first=(int(start)//60+1)*60
    out=[]
    for i in range(60):
        if i==missing:
            continue
        o=(opens or {}).get(i,100.0)
        h=(highs or {}).get(i,max(o,100.2))
        l=(lows or {}).get(i,min(o,99.8))
        out.append(dict(ts=int((first+i*60)*1000),open=o,high=h,
                        low=l,close=100.0,confirmed=True))
    return out


class FirstTouchTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db=sqlite3.connect(str(Path(tmp.name)/"test.sqlite"))
        self.db.row_factory=sqlite3.Row
        self.addCleanup(self.db.close)
        paper.setup(self.db)
        self.at=START+4000

    def test_stop_first_over_later_take_profit(self):
        candles=bars(highs={6:102.5},lows={10:97.5})
        r=paper.first_touch(START,100,102,98,candles,self.at)
        self.assertEqual(r["status"],"SL")
        self.assertEqual(r["exit_bar_ts"],START+7*60)
        self.assertAlmostEqual(r["modeled_net_pct"],-2.32)

    def test_tp_first_and_modeled_transaction_costs(self):
        candles=bars(lows={4:97.8},highs={9:103})
        r=paper.first_touch(START,100,102,98,candles,self.at)
        self.assertEqual(r["status"],"TP1")
        self.assertAlmostEqual(r["modeled_net_pct"],1.68)

    def test_both_targets_same_candle_ambiguous_never_counts_win(self):
        r=paper.first_touch(START,100,102,98,
             bars(highs={8:104},lows={8:97}),self.at)
        self.assertEqual(r["status"],"AMBIGUOUS")
        self.assertNotIn("modeled_net_pct",r)

    def test_time_expiry_complete_sixty_minute_path(self):
        r=paper.first_touch(START,100,102,98,bars(),self.at)
        self.assertEqual(r["status"],"TIME")
        self.assertAlmostEqual(r["modeled_net_pct"],-0.32)

    def test_missing_unconfirmed_or_not_due_never_assumed(self):
        self.assertEqual(paper.first_touch(
            START,100,102,98,bars(missing=17),self.at)["status"],"MISSING")
        all_bars=bars()
        all_bars[13]["confirmed"]=False
        self.assertEqual(paper.first_touch(
            START,100,102,98,all_bars,self.at)["status"],"MISSING")
        self.assertEqual(paper.first_touch(
            START,100,102,98,bars(),START+120)["status"],"PENDING")

    def test_bad_entry_gap_is_not_a_filled_short(self):
        result=paper.first_touch(START,100,102,98,bars(opens={0:98.0},
                            lows={0:97.0}),self.at)
        self.assertEqual(result["status"],"ENTRY_GAP")
        self.assertNotIn("modeled_net_pct",result)

    def test_delayed_stop_fill_more_conservative_than_exact_stop(self):
        r=paper.first_touch(START,100,102,98,bars(opens={7:104},
                         highs={7:104.1}),self.at)
        self.assertEqual(r["status"],"SL")
        self.assertAlmostEqual(r["modeled_net_pct"],-4.32)

    def test_reregister_idempotent_and_settlement_budgeted(self):
        self.assertTrue(paper.register(
            self.db,1,"TESTUSDT",START,100,102,98))
        self.assertFalse(paper.register(
            self.db,1,"TESTUSDT",START,100,102,98))
        self.assertTrue(paper.register(
            self.db,2,"TEST2USDT",START,100,102,98))
        seen=[]
        def reader(sym):
            seen.append(sym)
            return bars(lows={6:97.5})
        self.assertEqual(paper.advance(
            self.db,self.at,reader,max_calls=1),1)
        self.assertEqual(len(seen),1)
        rows=self.db.execute("""SELECT status,modeled_net_pct
            FROM post_pump_paper_trades ORDER BY audit_id""").fetchall()
        self.assertEqual([x[0] for x in rows],["TP1","PENDING"])
        self.assertAlmostEqual(rows[0][1],1.68)
        self.assertEqual(paper.advance(
            self.db,self.at,reader,max_calls=2),1)
        self.assertEqual(len(seen),2)

    def test_partial_candle_retry_then_missing_without_fabrication(self):
        self.assertTrue(paper.register(
            self.db,1,"TESTUSDT",START,100,102,98))
        t=START+4000
        self.assertEqual(paper.advance(self.db,t,
                         lambda sym:bars(missing=4)),1)
        row=self.db.execute("SELECT status,next_try_at FROM post_pump_paper_trades").fetchone()
        self.assertEqual(row[0],"PENDING")
        self.assertGreater(row[1],t)
        self.assertEqual(paper.advance(
            self.db,t+1800,lambda sym:bars(missing=4)),1)
        self.assertEqual(self.db.execute(
            "SELECT status FROM post_pump_paper_trades").fetchone()[0],
            "MISSING")

    def test_invalid_inputs_are_not_trades(self):
        self.assertFalse(paper.register(
            self.db,1,"TESTUSDT",START,100,99,98))
        self.assertEqual(paper.first_touch(
            START,100,99,98,bars(),self.at)["status"],"INVALID_PLAN")


if __name__=="__main__":
    unittest.main()
