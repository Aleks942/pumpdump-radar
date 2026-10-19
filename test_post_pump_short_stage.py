"""Deterministic stage-to-stage buyer/seller pressure analysis tests.

All observations causal, no REST, Telegram, scoring or executed trade claims.
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path

import post_pump_short_research as research
import post_pump_short_stage as stage


class ControlShiftTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.db=sqlite3.connect(str(Path(t.name)/"test.sqlite"))
        self.db.row_factory=sqlite3.Row
        self.addCleanup(self.db.close)
        research._setup(self.db)
        self.start=1800000000.0
        self.snapshot_id=0

    def add(self,phase,when,futures,spot,local_oi,agg_oi,
            r5=-1.0,band="50_TO_99",symbol="TESTUSDT",stage_time=None):
        self.snapshot_id+=1
        current={
            "futures_taker_5m_imbalance_pct":futures,
            "spot_taker_5m_imbalance_pct":spot,
            "local_oi_5m_pct":local_oi,"agg_oi_5m_pct":agg_oi,
            "sampled_return_5m_pct":r5,
        }
        self.db.execute("""INSERT INTO post_pump_short_events(
            symbol,episode_at,observed_at,phase,rise_band,reference_price,
            rise_24h_pct,retrace_from_24h_high_pct,cycle_stage,
            evidence_captured_at,local_oi_5m_pct,agg_oi_5m_pct,
            futures_taker_5m_imbalance_pct,spot_taker_5m_imbalance_pct)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol,self.start,when,phase,band,150.0,70,8,
             "PUMP_WEAKENING",when,local_oi,agg_oi,futures,spot))
        event_id=self.db.execute("SELECT last_insert_rowid()").fetchone()[0]
        result=stage.record(
            self.db,event_id=event_id,symbol=symbol,episode_at=self.start,
            band=band,phase=phase,now=when,current=current)
        self.db.execute("""INSERT INTO post_pump_short_markouts(
            event_id,horizon_min,status,model_net_short_pct)
            VALUES (?,30,'DONE',?)""",(event_id,0.75))
        return event_id,result

    def test_pump_buy_to_weakening_sell_then_structure_break(self):
        watch,w=self.add("WATCH",self.start,65,35,1.2,0.9,r5=6)
        self.assertEqual(w["control"],"NO_PRIOR_STAGE")
        weaken,w=self.add("WEAKENING",self.start+300,-25,-20,-0.6,-1.1)
        self.assertEqual(w["previous_phase"],"WATCH")
        self.assertEqual(w["control"],"BOTH_SELL_PRESSURE_STRENGTHENING")
        self.assertEqual(w["price_response"],"SELL_PRESSURE_AND_PRICE_DOWN")
        self.assertEqual(w["oi"],"OI_BOTH_CONTRACTING")
        self.assertAlmostEqual(w["changes"]["futures_imbalance_delta_pp"],-90)
        self.assertAlmostEqual(w["changes"]["spot_imbalance_delta_pp"],-55)
        broken,b=self.add("STRUCTURE_BREAK",self.start+420,
                          -40,-35,-1.5,-1.8,r5=-3.4)
        self.assertEqual(b["previous_phase"],"WEAKENING")
        self.assertEqual(b["control"],"BOTH_SELL_PRESSURE_STRENGTHENING")
        with self.db:
            group=stage.quality(self.db)
            self.assertEqual(len(group),3)
            self.assertTrue(all(g["mean_model_short_pct"]==0.75 for g in group))
            self.assertEqual(next(g["priced"] for g in group
                             if g["stage"]=="STRUCTURE_BREAK"),1)
            stored=self.db.execute("""SELECT previous_event_id,
                spot_imbalance_delta_pp,oi_context
                FROM post_pump_stage_comparisons WHERE event_id=?""",
                (broken,)).fetchone()
            self.assertEqual(stored[0],weaken)
            self.assertEqual(stored[1],-15)
            self.assertEqual(stored[2],"OI_BOTH_CONTRACTING")

    def test_missing_spot_does_not_turn_into_sell_confirmation(self):
        self.add("WATCH",self.start,70,40,1,2)
        _,w=self.add("WEAKENING",self.start+300,-35,None,-2,-2)
        self.assertEqual(w["control"],"INSUFFICIENT_DATA")
        self.assertEqual(w["price_response"],"INSUFFICIENT_DATA")
        self.assertIsNone(w["changes"]["spot_imbalance_delta_pp"])

    def test_negative_flow_without_price_down_is_not_validated(self):
        self.add("WATCH",self.start,70,40,1,2)
        _,w=self.add("WEAKENING",self.start+300,-35,-28,-2,-2,r5=+1.3)
        self.assertEqual(w["control"],"BOTH_SELL_PRESSURE_STRENGTHENING")
        self.assertEqual(w["price_response"],"SELL_PRESSURE_WITHOUT_PRICE_DROP")

    def test_simultaneous_stage_cannot_use_future_or_same_timestamp(self):
        self.add("WATCH",self.start,70,40,1,2)
        _,w=self.add("WEAKENING",self.start,-30,-30,-2,-2)
        self.assertIsNone(w["previous_phase"])
        self.assertEqual(w["control"],"NO_PRIOR_STAGE")

    def test_prior_across_30_50_100_bands_is_a_baseline(self):
        watch,_=self.add("WATCH",self.start,70,40,1,2,band="30_TO_49")
        _,w=self.add("WEAKENING",self.start+300,-30,-30,-2,-2,
                     band="50_TO_99")
        self.assertEqual(w["previous_phase"],"WATCH")
        self.assertEqual(w["control"],"BOTH_SELL_PRESSURE_STRENGTHENING")
        _,w2=self.add("STRUCTURE_BREAK",self.start+420,-40,-40,-3,-4,
                      band="100_PLUS")
        self.assertEqual(w2["previous_phase"],"WEAKENING")
        self.assertEqual(w2["control"],"BOTH_SELL_PRESSURE_STRENGTHENING")

    def test_baseline_older_than_three_hours_not_used(self):
        self.add("WATCH",self.start,70,40,1,2)
        _,w=self.add("WEAKENING",self.start+4*3600,-30,-30,-2,-2)
        self.assertEqual(w["control"],"NO_PRIOR_STAGE")

    def test_non_ready_future_stages_cannot_override_prior(self):
        self.add("WATCH",self.start,50,50,1,2)
        event_id,w=self.add("WEAKENING",self.start+300,-22,-12,-1,-2)
        second=stage.record(self.db,event_id=event_id,
                symbol="TESTUSDT",episode_at=self.start,band="50_TO_99",
                phase="WEAKENING",now=self.start+500,
                current={"futures_taker_5m_imbalance_pct":90})
        self.assertIsNone(second)
        self.assertEqual(self.db.execute("""SELECT flow_control
            FROM post_pump_stage_comparisons WHERE event_id=?""",
            (event_id,)).fetchone()[0],"BOTH_SELL_PRESSURE_STRENGTHENING")

    def test_pruning_follows_retained_event_rows(self):
        e,_=self.add("WATCH",self.start,50,50,1,2)
        self.db.execute("DELETE FROM post_pump_short_events WHERE id=?",(e,))
        stage.prune(self.db)
        self.assertIsNone(self.db.execute("""SELECT event_id
           FROM post_pump_stage_comparisons WHERE event_id=?""",
           (e,)).fetchone())


if __name__=="__main__":
    unittest.main()
