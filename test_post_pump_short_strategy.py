"""Offline tests for complete existing PumpDump post-pump SHORT pipeline."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import post_pump_short_strategy as strat

NOW=1800000000.0


def ticker(last=110,high=150,opening=100,volume=200000):
    return {"instId":"TEST-USDT-SWAP",
            "last":str(last),"high24h":str(high),"open24h":str(opening),
            "volCcy24h":str(volume),
            "bidPx":str(last*.9997),"askPx":str(last*1.0003)}


def candles(now,seconds,base=100):
    start=(int(now//seconds)-70)*seconds
    rows=[]
    for i in range(70):
        ts=(start+i*seconds)*1000
        close=base-0.10*i
        rows.append(dict(ts=ts,open=close+0.04,high=close+0.2,
                         low=close-0.2,close=close,confirmed=True))
    return rows


def structure(now,broken=False,retest=False):
    return dict(ready=True,reason="OK",support=112.0,
                broken=broken,bearish5=True,bearish15=True,h1="DOWN",
                atr=3,recent_hi=116.0,
                last5={"ts":now-300,"h":112.5,"l":109.5,"c":110.4},
                last15={"ts":now-900,"c":112},
                last1h={"ts":now-3600,"c":116},
                last5_close=110.4,trend_shift=True)


def evidence(symbol,at):
    return {"futures_taker_5m_imbalance_pct":-30.0,
            "local_oi_5m_pct":-0.70,"agg_oi_5m_pct":None}


class PostPumpFullStrategyTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path=str(Path(tmp.name)/"watch.sqlite")
        self.calls=[]

    def data(self):
        with sqlite3.connect(self.path) as db:
            db.row_factory=sqlite3.Row
            states=[dict(row) for row in db.execute(
                "SELECT * FROM post_pump_short_strategy")]
            events=[dict(row) for row in db.execute(
                "SELECT * FROM post_pump_short_strategy_audit ORDER BY id")]
        return states,events

    def drive(self,at,last=110,high=150,**kwargs):
        return strat.observe([ticker(last=last,high=high)],now=at,
            db_path=self.path,fetch_context=lambda sym:{
                "5m":[],"15m":[],"1H":[]},
            fetch_spot=lambda sym:[],
            evidence_reader=evidence,**kwargs)

    def test_direct_30pct_discovery_despite_26pct_pullback(self):
        self.assertIsNone(strat.safe_profile(ticker(last=110,high=129)))
        self.assertEqual(strat.safe_profile(ticker(last=110,high=130))["band"],
                         "30_TO_49")
        self.assertEqual(strat.safe_profile(ticker(last=110,high=150))["band"],
                         "50_TO_99")
        self.assertEqual(strat.safe_profile(ticker(last=110,high=205))["band"],
                         "100_PLUS")
        self.drive(NOW)
        states,events=self.data()
        self.assertEqual(len(states),1)
        self.assertEqual(states[0]["phase"],"WATCH")
        self.assertAlmostEqual(states[0]["last_retrace_pct"],26.6666666667)
        self.assertEqual(events[0]["phase"],"DISCOVERED")

    def test_incorrect_spread_or_illiquid_quote_not_discovered(self):
        bad={**ticker(),"bidPx":None}
        x=strat.observe([bad],NOW,self.path,fetch_context=lambda s:{},
                         fetch_spot=lambda s:[],evidence_reader=evidence)
        self.assertEqual(x["over30"],0)
        self.assertEqual(self.data()[0],[])
        x=strat.observe([ticker(volume=12)],NOW+10,self.path,
                        fetch_context=lambda s:{},fetch_spot=lambda s:[],
                        evidence_reader=evidence)
        self.assertEqual(x["over30"],1)
        self.assertEqual(x["liquid"],0)
        self.assertEqual(self.data()[0],[])

    def test_candle_validation_excludes_forming_and_stale(self):
        rows=candles(NOW,300)
        self.assertEqual(len(strat.parse_closed_candles(rows,300,NOW)),70)
        self.assertFalse(strat.parse_closed_candles(
            [{**r,"confirmed":False} for r in rows],300,NOW))
        self.assertFalse(strat.parse_closed_candles(rows[:-3],300,NOW))
        # A 15-minute gap means no valid multi-timeframe read.
        self.assertFalse(strat.parse_closed_candles(rows[:35]+rows[36:],
                                                      300,NOW))
        self.assertFalse(strat.analyze_structure(rows,[],[],NOW)["ready"])

    def test_spot_samples_require_pagination_overlap_and_full_5m(self):
        with sqlite3.connect(self.path) as db:
            db.row_factory=sqlite3.Row
            strat.setup(db)
            def batch(start,end):
                return [dict(tradeId="T"+str(i),side="sell",
                             px="1",sz="25",ts=str((NOW+i)*1000))
                        for i in range(start,end)]
            self.assertEqual(strat.store_spot_poll(
                db,"TESTUSDT",NOW+99,batch(0,100)),"WARMING")
            self.assertEqual(strat.store_spot_poll(
                db,"TESTUSDT",NOW+169,batch(99,169)),"OVERLAP")
            self.assertFalse(strat.spot_status(db,"TESTUSDT",NOW+169)["ready"])
            self.assertEqual(strat.store_spot_poll(
                db,"TESTUSDT",NOW+239,batch(168,239)),"OVERLAP")
            self.assertEqual(strat.store_spot_poll(
                db,"TESTUSDT",NOW+309,batch(238,309)),"OVERLAP")
            self.assertEqual(strat.store_spot_poll(
                db,"TESTUSDT",NOW+379,batch(308,379)),"OVERLAP")
            self.assertEqual(strat.store_spot_poll(
                db,"TESTUSDT",NOW+449,batch(378,449)),"OVERLAP")
            result=strat.spot_status(db,"TESTUSDT",NOW+449)
            self.assertTrue(result["ready"])
            self.assertLess(result["imbalance_pct"],-90)
            # A missing overlap forces a cold restart, not a fabricated 5m CVD.
            self.assertEqual(strat.store_spot_poll(
                db,"TESTUSDT",NOW+519,batch(500,560)),"WARMING")
            self.assertFalse(strat.spot_status(db,"TESTUSDT",NOW+519)["ready"])

    def test_no_signal_without_verified_spot_oi_or_price_trend(self):
        self.assertFalse(strat._evidence_ok(evidence("TESTUSDT",NOW),
                          {"ready":False,"imbalance_pct":-90}))
        self.assertFalse(strat._evidence_ok(
            {"futures_taker_5m_imbalance_pct":-60},
            {"ready":True,"imbalance_pct":-60}))
        self.assertFalse(strat._evidence_ok(
            {"futures_taker_5m_imbalance_pct":60,"local_oi_5m_pct":2},
            {"ready":True,"imbalance_pct":-60}))
        self.assertTrue(strat._evidence_ok(
            evidence("TESTUSDT",NOW),
            {"ready":True,"imbalance_pct":-40}))

    def test_stop_tp_and_anti_late(self):
        st=structure(NOW)
        plan=strat._plan(110,st,112,20,6,50)
        self.assertIsNotNone(plan)
        self.assertEqual(plan["entry"],110)
        self.assertGreater(plan["stop"],110)
        self.assertLess(plan["tp1"],110)
        self.assertLess(plan["tp2"],plan["tp1"])
        self.assertAlmostEqual(plan["quantity"]*
            (plan["stop"]-110+110*(strat.ROUND_TRIP_COST_PCT+0.10)/100),0.25)
        self.assertIsNone(strat._plan(102,st,112,32,6,50))
        self.assertIsNone(strat._plan(110,st,112,20,60,50))
        self.assertIsNone(strat._plan(110,st,112,20,6,None)["quantity"])

    def test_end_to_end_progression_has_real_later_retest(self):
        # Test at three strictly increasing 5m closes, NOT an instant signal
        # triggered by reading a candle that was already a retest.
        def mk(at):
            out=structure(at,broken=at>=NOW+130)
            return out
        with patch.object(strat,"analyze_structure",
                          side_effect=lambda a,b,c,now:mk(now)):
            with patch.object(strat,"spot_status",
                side_effect=lambda db,symbol,now:
                   {"ready":now>=NOW+390,"imbalance_pct":-40,
                    "reason":"OVERLAP_VERIFIED_5M"}):
                self.drive(NOW,last=115,equity_usdt=50)
                self.assertEqual(self.data()[0][0]["phase"],"WEAKENING")
                self.drive(NOW+130,last=111,equity_usdt=50)
                self.assertEqual(self.data()[0][0]["phase"],"TREND_SHIFT")
                self.drive(NOW+260,last=110,equity_usdt=50)
                self.assertEqual(self.data()[0][0]["phase"],"RETEST")
                self.drive(NOW+390,last=110,equity_usdt=50)
        state,events=self.data()
        self.assertEqual(state[0]["phase"],"READY_RESEARCH")
        self.assertEqual([x["phase"] for x in events],
            ["DISCOVERED","WEAKENING","TREND_SHIFT","RETEST","READY_RESEARCH"])
        plan=events[-1]
        self.assertEqual(plan["paper_only"],1)
        self.assertLess(plan["tp1"],plan["entry"])
        self.assertGreater(plan["stop"],plan["entry"])
        self.assertIsNotNone(plan["quantity"])
        self.assertEqual(state[0]["watch_reason"],"STRUCTURE_RETEST_SPOT_FUTURES_OI")

    def test_no_real_decision_when_spot_missing_despite_break_and_retest(self):
        with patch.object(strat,"analyze_structure",
            side_effect=lambda a,b,c,now:structure(now,broken=now>NOW)):
            self.drive(NOW,last=115)
            self.drive(NOW+130,last=111)
            self.drive(NOW+260,last=110)
        state,events=self.data()
        self.assertEqual(state[0]["phase"],"RETEST")
        self.assertNotIn("READY_RESEARCH",[e["phase"] for e in events])


if __name__=="__main__":
    unittest.main()
