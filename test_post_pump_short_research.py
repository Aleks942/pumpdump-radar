"""Network-free tests for strong-pump research, chronology and trade isolation."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

import post_pump_short_research as post
import pump_cycle_research as cycle


START=1800000000.0


def ticker(price=160,high=170,opening=100,symbol="TESTUSDT",volume=200000):
    return {"instId":symbol[:-4]+"-USDT-SWAP",
            "last":str(price),"high24h":str(high),"open24h":str(opening),
            "volCcy24h":str(volume),"bidPx":str(price*.9995),
            "askPx":str(price*1.0005)}


class PostPumpResearchTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.path=str(Path(t.name)/"watch.sqlite3")
        self.at=START
        with sqlite3.connect(self.path) as db:
            cycle._init(db)
            db.execute("""INSERT INTO pump_cycle_state
                (symbol,start_at,last_at,stage,peak,peak_at,
                 observed_peak,observed_peak_at,last_price,
                 trough,stage_changed_at,anchor_estimate,seed_source,
                 initial_move_pct,down_count)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                ("TESTUSDT",self.at,self.at,"PUMP_START",160,self.at,
                 160,self.at,160,160,self.at,100,"WATCH",60,0))

    def update_cycle(self,at,stage,price,support=None,weak=None,break_at=None,
                     flow_status=None,delta=None,flow_at=None):
        with sqlite3.connect(self.path) as db:
            db.execute("""UPDATE pump_cycle_state SET last_at=?,stage=?,
                last_price=?,support=?,weakening_at=?,break_at=?,
                flow_status=?,flow_delta=?,flow_verified_at=?,
                flow_observed_at=? WHERE symbol='TESTUSDT'""",
                (at,stage,price,support,weak,break_at,flow_status,
                 delta,flow_at,flow_at))

    def events(self):
        with sqlite3.connect(self.path) as db:
            return db.execute("""SELECT phase,rise_band,reference_price,
                                 mtf_confirmed
               FROM post_pump_short_events ORDER BY id""").fetchall()

    def test_50_percent_watch_and_forward_same_exchange(self):
        self.assertEqual(post.observe([ticker()],self.at,self.path),(1,0))
        self.assertEqual(self.events(),[("WATCH","50_TO_99",160,0)])
        self.assertEqual(post.observe([ticker()],self.at,self.path),(0,0))
        self.update_cycle(self.at+300,"PUMP_START",155)
        self.assertEqual(post.observe(
            [ticker(price=155)],self.at+300,self.path),(0,1))
        with sqlite3.connect(self.path) as db:
            result=db.execute("""SELECT status,model_net_short_pct
                FROM post_pump_short_markouts
                WHERE horizon_min=5""").fetchone()
        self.assertEqual(result[0],"DONE")
        self.assertAlmostEqual(result[1],2.905,places=4)

    def test_30_percent_minimum_and_three_nonoverlapping_rise_bands(self):
        # 24h high/open is the observable rolling-day rise, not last/open.
        self.assertEqual(post.MIN_RISE_PCT,30.0)
        cases=[
            (129.99,None),
            (130.0,"30_TO_49"),
            (149.99,"30_TO_49"),
            (150.0,"50_TO_99"),
            (199.99,"50_TO_99"),
            (200.0,"100_PLUS"),
        ]
        for high,band in cases:
            with self.subTest(high=high):
                # At the current peak, volume/spread checks are otherwise valid.
                item=post._ticker(ticker(price=high,high=high))
                self.assertEqual(item[-1] if item else None,band)
        self.assertEqual(post.observe(
            [ticker(price=134,high=134)],self.at,self.path),(1,0))
        self.assertEqual(self.events(),[("WATCH","30_TO_49",134,0)])

    def test_30_to_50_to_100_is_forward_only_not_retroactive(self):
        self.assertEqual(post.observe(
            [ticker(price=135,high=135)],self.at,self.path),(1,0))
        self.update_cycle(self.at+60,"PUMP_CONTINUATION",152)
        self.assertEqual(post.observe(
            [ticker(price=152,high=152)],self.at+60,self.path),(1,0))
        self.update_cycle(self.at+120,"PUMP_CONTINUATION",205)
        self.assertEqual(post.observe(
            [ticker(price=205,high=205)],self.at+120,self.path),(1,0))
        self.assertEqual([x[:2] for x in self.events()],[
            ("WATCH","30_TO_49"),("WATCH","50_TO_99"),
            ("WATCH","100_PLUS")])
        # No overwriting a prior band to make history look more predictive.
        self.assertEqual([x[2] for x in self.events()],[135,152,205])

    def test_100_plus_is_separate_real_time_band_not_fake_backfill(self):
        self.assertEqual(post.observe([ticker()],self.at,self.path),(1,0))
        self.update_cycle(self.at+60,"PUMP_CONTINUATION",198)
        # 100% tag appears ONLY when the later ticker actually crosses +100%.
        self.assertEqual(post.observe([ticker(price=198,high=215)],
                                      self.at+60,self.path),(1,0))
        self.assertEqual([x[:2] for x in self.events()],
                         [("WATCH","50_TO_99"),("WATCH","100_PLUS")])
        self.assertEqual(post.observe([ticker(price=198,high=215)],
                                      self.at+60,self.path),(0,0))

    def test_stage_sequence_and_missing_flow_fails_closed(self):
        post.observe([ticker()],self.at,self.path)
        at=self.at+30
        self.update_cycle(at,"PUMP_WEAKENING",145,support=150,weak=at)
        self.assertEqual(post.observe(
            [ticker(price=145)],at,self.path),(1,0))
        self.assertEqual(self.events()[-1][0],"WEAKENING")
        # An actual support break alone is NOT Order Flow confirmation.
        at=self.at+60
        self.update_cycle(at,"PUMP_WEAKENING",140,support=150,
                          weak=self.at+30,break_at=at,
                          flow_status="INSUFFICIENT_DATA")
        self.assertEqual(post.observe(
            [ticker(price=140)],at,self.path),(1,0))
        self.assertEqual(self.events()[-1][0],"STRUCTURE_BREAK")
        at=self.at+90
        self.update_cycle(at,"REVERSAL_CONFIRMED",137,support=150,
                          weak=self.at+30,break_at=self.at+60,
                          flow_status="INSUFFICIENT_DATA",delta=None,
                          flow_at=at)
        self.assertEqual(post.observe(
            [ticker(price=137)],at,self.path),(0,0))
        self.assertNotIn("FLOW_ALIGNED_RESEARCH",[x[0] for x in self.events()])
        # Fresh confirmed sell taker flow creates RESEARCH ONLY, not trade ready.
        at=self.at+120
        self.update_cycle(at,"REVERSAL_CONFIRMED",136,support=150,
                          weak=self.at+30,break_at=self.at+60,
                          flow_status="CONFIRMED",delta=-0.35,
                          flow_at=at)
        self.assertEqual(post.observe(
            [ticker(price=136)],at,self.path),(1,0))
        self.assertEqual(self.events()[-1],(
            "FLOW_ALIGNED_RESEARCH","50_TO_99",136,0))
        self.assertEqual(len(self.events()),4)

    def test_no_state_or_no_valid_24h_quote_is_not_a_signal(self):
        self.assertEqual(post.observe(
            [ticker(high=129,price=120)],self.at,self.path),(0,0))
        self.assertEqual(post.observe(
            [ticker(volume=1000)],self.at,self.path),(0,0))
        self.assertEqual(post.observe(
            [ticker(opening=0)],self.at,self.path),(0,0))
        self.assertEqual(post.observe(
            [{**ticker(),"bidPx":None}],self.at,self.path),(0,0))
        self.update_cycle(self.at-1000,"PUMP_START",160)
        self.assertEqual(post.observe([ticker()],self.at,self.path),(0,0))
        self.assertEqual(self.events(),[])

    def test_missing_quote_is_not_fabricated_using_later_price(self):
        post.observe([ticker()],self.at,self.path)
        self.update_cycle(self.at+541,"PUMP_START",150)
        self.assertEqual(post.observe(
            [ticker(price=150)],self.at+541,self.path),(0,0))
        with sqlite3.connect(self.path) as db:
            outcome=db.execute("""SELECT status,model_net_short_pct
                FROM post_pump_short_markouts WHERE horizon_min=5""").fetchone()
        self.assertEqual(outcome,("MISSING",None))
        self.update_cycle(self.at+600,"PUMP_START",145)
        post.observe([ticker(price=145)],self.at+600,self.path)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("""SELECT status
                FROM post_pump_short_markouts WHERE horizon_min=5"""
                         ).fetchone()[0],"MISSING")

    def test_reporting_descriptive_and_no_trading_permissions(self):
        post.observe([ticker()],self.at,self.path)
        self.update_cycle(self.at+300,"PUMP_START",155)
        post.observe([ticker(price=155)],self.at+300,self.path)
        result=post.report(self.at+310,self.path)
        group=[x for x in result if x["horizon_min"]==5 and x["phase"]=="WATCH"]
        self.assertEqual(group[0]["done"],1)
        self.assertEqual(group[0]["symbols"],1)
        self.assertEqual(group[0]["positive"],1)
        self.assertEqual(group[0]["mean_model_short_pct"],2.905)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute(
                "SELECT mtf_confirmed FROM post_pump_short_events"
            ).fetchone()[0],0)


if __name__=="__main__":
    unittest.main()
