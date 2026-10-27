"""Offline tests for complete existing PumpDump post-pump SHORT pipeline."""
import json
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
        # Strategy progression tests stay deterministic and network-free;
        # feature enrichment has its own dedicated test module.
        patcher=patch.object(
            strat,"enrich_research_evidence",
            side_effect=lambda symbol,now,evidence,spot,structure,
                               liquidations=None,db_path=None,episode_at=None:
                dict(evidence,order_flow={"ready":True,"status":"CONFIRMED",
                     "price_change_pct":-0.09,
                     "source":"PUMPDUMP_LOCAL_TAKER_FLOW"}))
        patcher.start()
        self.addCleanup(patcher.stop)

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

    def test_discovery_persists_ready_cached_oi_immediately(self):
        self.drive(NOW)
        _,events=self.data()
        snapshot=json.loads(events[0]["evidence_json"])
        self.assertEqual(snapshot["local_oi_5m_pct"],-0.70)

    def test_same_phase_candidate_with_large_retrace_gets_priority(self):
        cold={"symbol":"COLDUSDT","phase":"WATCH","peak":150,
              "rise_max":50,"last_price":148,"last_context_at":1}
        hot={"symbol":"HOTUSDT","phase":"WATCH","peak":150,
             "rise_max":50,"last_price":134,"last_context_at":999}
        profiles={
            "COLDUSDT":{"last":148},
            "HOTUSDT":{"last":134},
        }
        ranked=sorted(
            [cold,hot],
            key=lambda z:strat._candidate_sort_key(z,{},profiles)
        )
        self.assertEqual(ranked[0]["symbol"],"HOTUSDT")
        self.assertTrue(strat._candidate_priority_snapshot(
            hot,{},profiles)["urgent"])

    def test_entry_zone_rejects_price_that_already_chased_too_far(self):
        st=structure(NOW)
        self.assertIsNone(strat._plan(109,st,112,20,6,50))
        plan=strat._plan(110,st,112,20,6,50)
        self.assertIsNotNone(plan)
        self.assertLessEqual(plan["zone_low"],110)
        self.assertGreaterEqual(plan["zone_high"],110)

    def test_passive_candidate_keeps_peak_retrace_memory(self):
        with sqlite3.connect(self.path) as db:
            db.row_factory=sqlite3.Row
            strat.setup(db)
            db.execute("""INSERT INTO post_pump_short_strategy
                (symbol,episode_at,last_seen_at,phase,phase_at,peak,rise_max,
                 band,reference_open,last_price,last_retrace_pct,peak_at)
                VALUES ('PASSIVEUSDT',?,?,?,?,150,50,'50_TO_99',100,145,3.33,?)""",
                (NOW,NOW,'WATCH',NOW,NOW))
            row=db.execute("SELECT * FROM post_pump_short_strategy WHERE symbol='PASSIVEUSDT'").fetchone()
            strat._refresh_passive_state(
                db,row,{"last":132,"high":150,"opening":100},NOW+600)
            saved=db.execute("""SELECT last_seen_at,last_price,last_retrace_pct,peak
                FROM post_pump_short_strategy WHERE symbol='PASSIVEUSDT'""").fetchone()
        self.assertEqual(saved[0],NOW+600)
        self.assertEqual(saved[1],132)
        self.assertAlmostEqual(saved[2],12.0)
        self.assertEqual(saved[3],150)

    def test_discovered_pump_subscribes_bybit_liquidations(self):
        with patch.object(strat,"_watch_bybit_liquidations",return_value=True) as watch:
            self.drive(NOW)
        watch.assert_called_once_with("TESTUSDT")

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
                self.assertEqual(self.data()[0][0]["phase"],"SHORT_CONFIRMED")
                self.drive(NOW+390,last=110,equity_usdt=50)
                self.assertEqual(self.data()[0][0]["phase"],"RETEST")
                self.drive(NOW+520,last=110,equity_usdt=50)
        state,events=self.data()
        self.assertEqual(state[0]["phase"],"ENTRY_ZONE")
        self.assertEqual([x["phase"] for x in events],
            ["DISCOVERED","WEAKENING","TREND_SHIFT","SHORT_CONFIRMED",
             "RETEST","ENTRY_ZONE"])
        plan=events[-1]
        self.assertEqual(plan["paper_only"],1)
        self.assertLess(plan["tp1"],plan["entry"])
        self.assertGreater(plan["stop"],plan["entry"])
        self.assertIsNotNone(plan["quantity"])
        self.assertEqual(state[0]["watch_reason"],
                         "RETEST_REJECTION_SPOT_FUTURES_OI_SELLERS_CONFIRMED")

    def test_early_weakening_is_research_only_before_full_retrace(self):
        def mk(at):
            result=structure(at)
            result["last5"]={"ts":at-300, "h":150, "l":147,
                             "c":148-(at-NOW)*0.003}
            return result

        with patch.object(strat,"analyze_structure",side_effect=lambda a,b,c,at:mk(at)):
            self.drive(NOW,last=149.2,high=150)
            self.assertEqual(self.data()[0][0]["phase"],"WATCH")
            self.drive(NOW+130,last=147.8,high=150)
            self.assertEqual(self.data()[0][0]["phase"],"EARLY_WEAKENING")
            self.drive(NOW+260,last=145.5,high=150)
        states,events=self.data()
        self.assertEqual(states[0]["phase"],"WEAKENING")
        self.assertEqual([row["phase"] for row in events],
                         ["DISCOVERED","EARLY_WEAKENING","WEAKENING"])
        self.assertTrue(all(row["paper_only"]==1 for row in events))
        self.assertFalse(any(row["entry"] is not None for row in events))

    def test_early_weakening_rejects_reused_closed_candle(self):
        def mk(at):
            result=structure(at)
            result["last5"]={"ts":NOW-300,"h":150,"l":147,"c":148}
            return result
        with patch.object(strat,"analyze_structure",side_effect=lambda a,b,c,at:mk(at)):
            self.drive(NOW,last=149.2,high=150)
            self.drive(NOW+130,last=147.8,high=150)
        self.assertEqual(self.data()[0][0]["phase"],"WATCH")

    def test_early_weakening_recovers_without_short_setup(self):
        def mk(at):
            result=structure(at)
            result["last5"]={"ts":at-300,"h":150,"l":147,
                             "c":148-(at-NOW)*0.003}
            if at==NOW+260:
                result["bearish5"]=False
            return result
        with patch.object(strat,"analyze_structure",side_effect=lambda a,b,c,at:mk(at)):
            self.drive(NOW,last=149.2,high=150)
            self.drive(NOW+130,last=147.8,high=150)
            self.assertEqual(self.data()[0][0]["phase"],"EARLY_WEAKENING")
            self.drive(NOW+260,last=149.0,high=150)
        states,events=self.data()
        self.assertEqual(states[0]["phase"],"WATCH")
        self.assertEqual(events[-1]["reason"],"BUYERS_RECOVERED_EARLY_WEAKENING")
        self.assertNotIn("ENTRY_ZONE",[x["phase"] for x in events])

    def test_missing_local_aggression_confirmation_blocks_research_ready(self):
        with patch.object(strat,"analyze_structure",
                          side_effect=lambda a,b,c,now:
                            structure(now,broken=now>=NOW+130)):
          with patch.object(strat,"spot_status",return_value={
                  "ready":True,"imbalance_pct":-50,
                  "reason":"OVERLAP_VERIFIED_5M"}):
            with patch.object(strat,"enrich_research_evidence",
                              side_effect=lambda *args,**kwargs:
                                  {"futures_taker_5m_imbalance_pct":-30,
                                   "local_oi_5m_pct":-0.7,
                                   "order_flow":{"status":"INSUFFICIENT_DATA",
                                                 "ready":False}}):
                self.drive(NOW,last=115)
                self.drive(NOW+130,last=111)
                self.drive(NOW+260,last=110)
                self.drive(NOW+390,last=110)
        state,events=self.data()
        self.assertEqual(state[0]["phase"],"RETEST")
        self.assertEqual(state[0]["watch_reason"],
                         "WAITING_FOR_LOCAL_AGGRESSION_RESPONSE")
        self.assertNotIn("ENTRY_ZONE",[e["phase"] for e in events])

    def test_paper_plan_registered_only_after_final_research_confirmation(self):
        with patch.object(strat,"analyze_structure",
                          side_effect=lambda a,b,c,now:
                              structure(now,broken=now>=NOW+130)):
          with patch.object(strat,"spot_status",return_value={
                  "ready":True,"imbalance_pct":-40,
                  "reason":"OVERLAP_VERIFIED_5M"}):
            self.drive(NOW,last=115,equity_usdt=50)
            self.drive(NOW+130,last=111,equity_usdt=50)
            self.drive(NOW+260,last=110,equity_usdt=50)
            self.drive(NOW+390,last=110,equity_usdt=50)
            self.drive(NOW+520,last=110,equity_usdt=50)
        with sqlite3.connect(self.path) as db:
            rows=db.execute("""SELECT status,signal_price,stop,tp1,model_only
                FROM post_pump_paper_trades""").fetchall()
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0][0],"PENDING")
        self.assertLess(rows[0][1],rows[0][2])
        self.assertGreater(rows[0][1],rows[0][3])
        self.assertEqual(rows[0][4],1)

    def test_new_price_high_invalidates_prior_short_thesis(self):
        with patch.object(strat,"analyze_structure",
            side_effect=lambda a,b,c,now:structure(
                now,broken=now>NOW)):
            self.drive(NOW,last=115)
            self.drive(NOW+130,last=111)
            self.assertEqual(self.data()[0][0]["phase"],"TREND_SHIFT")
            # Old peak was 150. Ticker prints a NEW 151 high.
            self.drive(NOW+260,last=151,high=151)
        st,events=self.data()
        self.assertEqual(st[0]["phase"],"WATCH")
        self.assertIsNone(st[0]["break_level"])
        self.assertIn("BUYERS_RECLAIMED_NEW_HIGH",[x["reason"] for x in events])

    def test_active_episode_keeps_real_liquidity_after_30pct_window_rolls_off(self):
        self.assertIsNone(strat.safe_profile(ticker(last=110,high=125,opening=100)))
        live=strat.live_market_profile(ticker(last=110,high=125,opening=100))
        self.assertTrue(live["liquid"])
        with patch.object(strat,"analyze_structure",
            side_effect=lambda a,b,c,now:structure(now,broken=now>NOW)):
            with patch.object(strat,"spot_status",return_value={
                "ready":True,"imbalance_pct":-60,"count":30,
                "reason":"OVERLAP_VERIFIED_5M"}):
                self.drive(NOW,last=115,high=150,equity_usdt=50)
                self.drive(NOW+130,last=111,high=150,equity_usdt=50)
                self.drive(NOW+260,last=110,high=150,equity_usdt=50)
                # The old +30% 24h high has rolled away, but the CURRENT
                # ticker is still liquid. This must not fabricate illiquidity.
                rolled=ticker(last=110,high=125,opening=100,volume=200000)
                strat.observe([rolled],NOW+390,self.path,
                    fetch_context=lambda symbol:{"5m":[],"15m":[],"1H":[]},
                    fetch_spot=lambda symbol:[],
                    evidence_reader=evidence,equity_usdt=50)
                strat.observe([rolled],NOW+520,self.path,
                    fetch_context=lambda symbol:{"5m":[],"15m":[],"1H":[]},
                    fetch_spot=lambda symbol:[],
                    evidence_reader=evidence,equity_usdt=50)
        st,events=self.data()
        self.assertEqual(st[0]["phase"],"ENTRY_ZONE")
        self.assertNotEqual(st[0]["watch_reason"],"WAITING_FOR_LIQUID_MARKET")
        self.assertIn("ENTRY_ZONE",[x["phase"] for x in events])

    def test_no_approval_after_market_liquidity_disappears(self):
        with patch.object(strat,"analyze_structure",
            side_effect=lambda a,b,c,now:structure(now,broken=now>NOW)):
            with patch.object(strat,"spot_status",return_value={
                "ready":True,"imbalance_pct":-60,
                "reason":"OVERLAP_VERIFIED_5M"}):
                self.drive(NOW,last=115)
                self.drive(NOW+130,last=111)
                self.drive(NOW+260,last=110)
                # Hourly OKX liquidity is now unavailable while the old
                # stage still exists. No imaginary zero spread is allowed.
                bad={**ticker(last=109),"volCcy24h":"1"}
                strat.observe([bad],NOW+390,self.path,
                    fetch_context=lambda symbol:{
                        "5m":[],"15m":[],"1H":[]},
                    fetch_spot=lambda symbol:[],
                    evidence_reader=evidence,equity_usdt=50)
                strat.observe([bad],NOW+520,self.path,
                    fetch_context=lambda symbol:{
                        "5m":[],"15m":[],"1H":[]},
                    fetch_spot=lambda symbol:[],
                    evidence_reader=evidence,equity_usdt=50)
        st,events=self.data()
        self.assertEqual(st[0]["phase"],"RETEST")
        self.assertEqual(st[0]["watch_reason"],"WAITING_FOR_LIQUID_MARKET")
        self.assertNotIn("ENTRY_ZONE",[x["phase"] for x in events])

    def test_no_real_decision_when_spot_missing_despite_break_and_retest(self):
        with patch.object(strat,"analyze_structure",
            side_effect=lambda a,b,c,now:structure(now,broken=now>NOW)):
            self.drive(NOW,last=115)
            self.drive(NOW+130,last=111)
            self.drive(NOW+260,last=110)
            self.drive(NOW+390,last=110)
        state,events=self.data()
        self.assertEqual(state[0]["phase"],"RETEST")
        self.assertNotIn("ENTRY_ZONE",[e["phase"] for e in events])


if __name__=="__main__":
    unittest.main()
