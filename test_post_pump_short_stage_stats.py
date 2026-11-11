"""Regression tests for stage-specific post-pump short statistics."""
import sqlite3
import unittest

import post_pump_short_strategy as strategy

NOW=1800000000.0


class StageStatsTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(":memory:")
        self.db.row_factory=sqlite3.Row
        self.addCleanup(self.db.close)
        strategy.setup(self.db)

    def add_audit(self,audit_id,symbol,phase,at=NOW-600):
        self.db.execute("""INSERT INTO post_pump_short_strategy_audit(
            id,symbol,episode_at,at,phase,reason,rise_pct,retrace_pct,
            reference_price,paper_only)
            VALUES (?,?,?,?,?,?,?,?,?,1)""",
            (audit_id,symbol,NOW-3600,at,phase,"TEST",50.0,20.0,100.0))

    def test_setup_backfills_all_horizons_for_real_audits_only(self):
        self.add_audit(1,"REALUSDT","SHORT_CONFIRMED")
        self.add_audit(2,"TESTUSDT","SHORT_CONFIRMED")
        strategy.setup(self.db)
        real=[r[0] for r in self.db.execute(
            "SELECT horizon_min FROM post_pump_short_strategy_outcomes "
            "WHERE audit_id=1 ORDER BY horizon_min")]
        test=[r[0] for r in self.db.execute(
            "SELECT horizon_min FROM post_pump_short_strategy_outcomes "
            "WHERE audit_id=2 ORDER BY horizon_min")]
        self.assertEqual(real,[5,10,15,30,60,120])
        self.assertEqual(test,[])

    def test_stage_report_excludes_test_symbols_and_reports_move_thresholds(self):
        self.add_audit(1,"REALUSDT","SHORT_CONFIRMED")
        self.add_audit(2,"TESTUSDT","SHORT_CONFIRMED")
        strategy.setup(self.db)
        self.db.execute("""UPDATE post_pump_short_strategy_outcomes
            SET status='DONE',model_net_short_pct=2.78
            WHERE audit_id=1 AND horizon_min=30""")
        self.db.execute("""INSERT OR REPLACE INTO post_pump_short_strategy_outcomes(
            audit_id,horizon_min,status,model_net_short_pct)
            VALUES (2,30,'DONE',20.0)""")
        rows=strategy.stage_markout_report(self.db,NOW,days=30)
        row=next(x for x in rows
                 if x["phase"]=="SHORT_CONFIRMED" and x["horizon_min"]==30)
        self.assertEqual(row["n"],1)
        self.assertEqual(row["avg_net_pct"],2.78)
        self.assertEqual(row["profitable_pct"],100.0)
        self.assertEqual(row["fall_ge_2_pct"],100.0)
        self.assertEqual(row["fall_ge_4_pct"],0.0)


if __name__=="__main__":
    unittest.main()
