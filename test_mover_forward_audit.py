"""Network-free tests: sampled quote markouts must not imply executable profit."""
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import mover_forward_audit as audit


def mover(symbol, pct, price=100, eligible=True, spread=2):
    return {"symbol":symbol,"change5":pct,"price":price,
            "spread_bps":spread,"eligible_research":eligible}


class MoverForwardTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        for key,value in (
            ("DB_PATH",os.path.join(temp.name,"movers.sqlite")),
            ("_last_report",None),("_last_cleanup",None),
        ):
            p=patch.object(audit,key,value)
            p.start()
            self.addCleanup(p.stop)

    def test_forward_5_15_30_quotes_cost_and_dedup(self):
        first=[mover("UPUSDT",2,100),mover("DNUSDT",-1.5,100)]
        audit.observe(first,1000)
        audit.observe([mover("UPUSDT",2,101),
                       mover("DNUSDT",-1.5,99)],1300)
        with audit._connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0],2)
            r=db.execute("""SELECT c.side,o.horizon_min,o.status,o.net_pct
                 FROM mover_cases c JOIN mover_outcomes o ON o.case_id=c.id
                 ORDER BY c.side,o.horizon_min""").fetchall()
            self.assertEqual([x[1] for x in r if x[2]=="DONE"],[5,5])
            for side,h,status,net in r:
                if h==5:
                    self.assertEqual(status,"DONE")
                    self.assertAlmostEqual(net,0.78,places=5)
                else:
                    self.assertEqual(status,"PENDING")
        audit.observe([mover("UPUSDT",2,98),mover("DNUSDT",-1.5,102)],1900)
        audit.observe([mover("UPUSDT",2,99),mover("DNUSDT",-1.5,101)],2800)
        with audit._connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0],2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM mover_outcomes WHERE status='DONE'").fetchone()[0],6)
            rows=audit.quality_snapshot(db)
            self.assertEqual(len(rows),6)
            self.assertTrue(all(r["done"]==1 and r["missing"]==0 for r in rows))
            self.assertTrue(any(r["mean_net"]>0 for r in rows))
            self.assertTrue(any(r["mean_net"]<0 for r in rows))

    def test_missing_quote_does_not_invent_good_entry_or_exit(self):
        audit.observe([mover("UPUSDT",1.5,100)],1000)
        audit.observe([],1300)
        with audit._connect() as db:
            status=db.execute("""SELECT status FROM mover_outcomes
                 WHERE horizon_min=5""").fetchone()[0]
            self.assertEqual(status,"PENDING")
        audit.observe([],1481)
        with audit._connect() as db:
            row=db.execute("""SELECT status,net_pct FROM mover_outcomes
                 WHERE horizon_min=5""").fetchone()
            self.assertEqual(row,("MISSING",None))
        audit.observe([mover("UPUSDT",1.5,140)],1500)
        with audit._connect() as db:
            self.assertEqual(db.execute("""SELECT status FROM mover_outcomes
                WHERE horizon_min=5""").fetchone()[0],"MISSING")

    def test_opposing_direction_is_separate_and_invalid_inputs_do_not_journal(self):
        audit.observe([mover("ETHUSDT",1.5),mover("INVALIDUSDT",1.0,100,False),
                       mover("NANUSDT",2.5,float("nan")),
                       mover("ILLUSDT",2,100,True,55)],1000)
        audit.observe([mover("ETHUSDT",-1.5)],1200)
        audit.observe([mover("ETHUSDT",1.6)],1300)
        with audit._connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0],2)
            self.assertEqual(set(db.execute("SELECT side FROM mover_cases").fetchall()),
                             {("LONG",),("SHORT",)})

    def test_bounded_candidate_selection_and_research_only_output(self):
        candidates=[mover(f"TEST{k}USDT",1+k/100) for k in range(50)]
        output=io.StringIO()
        with redirect_stdout(output):
            audit.observe(candidates,1000)
            audit.report(1000)
        with audit._connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM mover_cases").fetchone()[0],audit.MAX_NEW_PER_SCAN)
        raw=output.getvalue()
        self.assertIn("[MOVER_FWD_CASE]",raw)
        self.assertIn("[MOVER_FWD_SUMMARY]",raw)
        self.assertIn("execution_not_proven=true",raw)
        self.assertIn("no_trade_approval=true",raw)

    def test_no_current_or_future_observation_labels(self):
        audit.observe([mover("TESTUSDT",1.5)],1000)
        audit.observe([mover("TESTUSDT",1.5,105)],1299)
        with audit._connect() as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM mover_outcomes WHERE status='DONE'").fetchone()[0],0)
        audit.observe([mover("TESTUSDT",1.5,105)],1300)
        with audit._connect() as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM mover_outcomes WHERE status='DONE'").fetchone()[0],1)


    def test_sampled_time_to_peak_and_adverse_move_by_direction(self):
        audit.observe([mover("UPUSDT",1.5,100),
                       mover("DOWNUSDT",-1.5,100)],1000)
        for minute in range(1,31):
            # Up peaks at minute 3, then gives some gain back.
            up={1:101,2:103,3:104,4:102.5}.get(minute,102.5)
            # Down bottoms at minute 3, then bounces.
            down={1:99.5,2:98,3:97,4:98.5}.get(minute,98.5)
            audit.observe([mover("UPUSDT",1.5,up),
                           mover("DOWNUSDT",-1.5,down)],1000+60*minute)
        with audit._connect() as db:
            cases=db.execute("""SELECT side,path_max_favor,path_best_sec,
                    path_max_adverse,path_samples,path_max_gap
                    FROM mover_cases ORDER BY side""").fetchall()
            self.assertEqual(len(cases),2)
            by_side={r[0]:r[1:] for r in cases}
            self.assertAlmostEqual(by_side["LONG"][0],4)
            self.assertAlmostEqual(by_side["LONG"][1],180)
            self.assertAlmostEqual(by_side["SHORT"][0],3)
            self.assertAlmostEqual(by_side["SHORT"][1],180)
            self.assertEqual(by_side["SHORT"][3],31)
            self.assertEqual(by_side["LONG"][4],60)
            summary=audit.path_quality_snapshot(db,2800)
            self.assertEqual(len(summary),2)
            self.assertEqual({r["side"] for r in summary},{"LONG","SHORT"})
            self.assertTrue(all(r["n"]==1 and r["median_peak_min"]==3
                                for r in summary))
            self.assertAlmostEqual(next(r["mean_max_favor"] for r in summary
                                if r["side"]=="LONG"),4)

    def test_gappy_history_excluded_from_peak_statistics(self):
        audit.observe([mover("GAPUSDT",1.5,100)],1000)
        audit.observe([mover("GAPUSDT",1.5,110)],1300)
        audit.observe([mover("GAPUSDT",1.5,120)],2800)
        with audit._connect() as db:
            self.assertEqual(audit.path_quality_snapshot(db,2800),[])
            gap=db.execute("SELECT path_max_gap FROM mover_cases").fetchone()[0]
            self.assertGreater(gap,180)

    def test_legacy_rows_not_retroactively_labeled_with_future_quotes(self):
        import sqlite3
        with sqlite3.connect(audit.DB_PATH) as db:
            db.execute("""CREATE TABLE mover_cases (
                id INTEGER PRIMARY KEY,ts REAL NOT NULL,symbol TEXT NOT NULL,
                side TEXT NOT NULL,band TEXT NOT NULL,change5 REAL NOT NULL,
                entry REAL NOT NULL,spread_bps REAL NOT NULL)""")
            db.execute("""INSERT INTO mover_cases
                (ts,symbol,side,band,change5,entry,spread_bps)
                VALUES (1000,'OLDUSDT','LONG','1_TO_2P5',1.5,100,2)""")
        with audit._connect() as db:
            names={r[1] for r in db.execute("PRAGMA table_info(mover_cases)")}
            self.assertIn("path_samples",names)
            self.assertIsNone(db.execute(
                "SELECT path_samples FROM mover_cases").fetchone()[0])
        audit.observe([mover("OLDUSDT",1.6,120)],1600)
        with audit._connect() as db:
            self.assertIsNone(db.execute(
                "SELECT path_samples FROM mover_cases WHERE id=1").fetchone()[0])
            self.assertEqual(audit.path_quality_snapshot(db,2800),[])

    def test_old_forward_quality_metrics_remain_unchanged(self):
        audit.observe([mover("STABLEUSDT",1.5,100)],1000)
        for minute in range(1,31):
            audit.observe([mover("STABLEUSDT",1.5,101)],1000+60*minute)
        with audit._connect() as db:
            markouts=audit.quality_snapshot(db)
            self.assertEqual({r["horizon"] for r in markouts},{5,15,30})
            self.assertTrue(all(r["done"]==1 for r in markouts))
            self.assertTrue(all(abs(r["mean_net"]-0.78)<1e-8
                                for r in markouts))
        buffer=io.StringIO()
        with redirect_stdout(buffer):
            audit.report(2800)
        logs=buffer.getvalue()
        self.assertIn("[MOVER_PATH_QUALITY]",logs)
        self.assertIn("median_time_to_peak_min=",logs)
        self.assertIn("no_trade_approval=true",logs)


if __name__ == "__main__":
    unittest.main()
