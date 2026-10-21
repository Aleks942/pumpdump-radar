"""Deterministic checks: cached evidence never invents CVD or trade approval."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import post_pump_short_evidence as evidence
import post_pump_short_research as post
import pump_cycle_research as cycle

START=1800000000.0


def quotes(price=160,high=170,symbol="TESTUSDT"):
    return {"instId":symbol[:-4]+"-USDT-SWAP",
            "last":str(price),"high24h":str(high),"open24h":"100",
            "volCcy24h":"200000",
            "bidPx":str(price*.9995),"askPx":str(price*1.0005)}


def ready_sources(now):
    def oi(symbol,when):
        return {"ready":True,"quality":"READY",
                "age_sec":20,"change_pct":-0.65}
    def agg(symbol):
        return {"observed_ts":now,
                "windows":{"5m":{"ready":True,"quality":"READY",
                                 "change_pct":-1.28}}}
    def futures(symbol):
        return {"5m":{"window_ready":True,"stream_healthy":True,
                "quality":"READY","last_trade_age_sec":2,"stream_stale_seconds":3,
                "imbalance_pct":-35,"delta_quote":-18500}}
    def spot(symbol):
        return {"5m":{"window_ready":True,"stream_healthy":True,
                "quality":"READY","last_trade_age_sec":2,"stream_stale_seconds":4,
                "imbalance_pct":-22,"delta_quote":-5000}}
    return (oi,agg,futures,spot)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path=str(Path(temp.name)/"watch.sqlite")
        with sqlite3.connect(self.path) as db:
            cycle._init(db)
            post._setup(db)
            db.execute("""INSERT INTO pump_cycle_state
                (symbol,start_at,last_at,stage,peak,peak_at,
                 observed_peak,observed_peak_at,last_price,
                 trough,stage_changed_at,anchor_estimate,seed_source,
                 initial_move_pct,down_count)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                ("TESTUSDT",START,START,"PUMP_START",160,START,
                 160,START,160,160,START,100,"WATCH",60,0))

    def test_ready_cache_values_all_present_with_provenance(self):
        captured=evidence.take_cached_evidence(
            "TESTUSDT",START,sources=ready_sources(START))
        self.assertEqual(captured["local_oi_5m_pct"],-0.65)
        self.assertEqual(captured["agg_oi_5m_pct"],-1.28)
        self.assertEqual(captured["futures_taker_5m_imbalance_pct"],-35)
        self.assertEqual(captured["spot_taker_5m_delta_quote"],-5000)
        self.assertEqual(evidence.missing_counter(captured),0)
        self.assertEqual(json.loads(captured["evidence_quality_json"])["spot"],
                         "READY")

    def test_unready_stale_and_broken_cache_are_missing_not_zero(self):
        def stale(symbol,now):
            return {"ready":True,"quality":"READY","age_sec":180,
                    "change_pct":-1}
        def old(symbol):
            return {"observed_ts":START-600,"windows":{
                "5m":{"ready":True,"quality":"READY","change_pct":-3}}}
        def damaged(symbol):
            return {"5m":{"window_ready":False,"stream_healthy":False,
                          "quality":"WARMING","imbalance_pct":-99,
                          "delta_quote":-1000}}
        def crashed(symbol):
            raise ConnectionError("unready")
        x=evidence.take_cached_evidence(
            "TESTUSDT",START,sources=(stale,old,damaged,crashed))
        self.assertEqual(evidence.missing_counter(x),6)
        self.assertIsNone(x["local_oi_5m_pct"])
        self.assertIsNone(x["agg_oi_5m_pct"])
        self.assertIsNone(x["futures_taker_5m_imbalance_pct"])
        self.assertEqual(json.loads(x["evidence_quality_json"])["spot"],
                         "ConnectionError")

    def test_realistic_5_15_60_minute_past_only_returns(self):
        with sqlite3.connect(self.path) as db:
            for n in range(0,4501,120):
                evidence.record_quote(db,"TESTUSDT",START+n,START/1e7+n/100)
            now=START+4500
            x=evidence.sampled_returns(db,"TESTUSDT",now,170)
            self.assertIsNotNone(x["sampled_return_5m_pct"])
            self.assertIsNotNone(x["sampled_return_15m_pct"])
            self.assertIsNotNone(x["sampled_return_60m_pct"])
            # Never read same-time or future quotes as history.
            evidence.record_quote(db,"TESTUSDT",now,1)
            self.assertEqual(x,evidence.sampled_returns(
                db,"TESTUSDT",now,170))
            evidence.prune_quote_samples(db,START+4*3600)
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM post_pump_short_quote_samples"
                ).fetchone()[0],0)

    def test_disconnected_quote_history_does_not_imply_trend(self):
        with sqlite3.connect(self.path) as db:
            evidence.record_quote(db,"TESTUSDT",START,160)
            evidence.record_quote(db,"TESTUSDT",START+500,145)
            self.assertIsNone(evidence.sampled_returns(
                db,"TESTUSDT",START+600,140)["sampled_return_5m_pct"])

    def test_event_captures_immutable_snapshot_only_on_insert(self):
        with patch.object(post,"take_cached_evidence",
                side_effect=lambda symbol,now:
                    evidence.take_cached_evidence(
                        symbol,now,sources=ready_sources(now))):
            self.assertEqual(post.observe(
                [quotes()],START,self.path),(1,0))
        with sqlite3.connect(self.path) as db:
            row=db.execute("""SELECT local_oi_5m_pct,
                spot_taker_5m_imbalance_pct,evidence_captured_at,
                sampled_return_60m_pct,mtf_confirmed
                FROM post_pump_short_events""").fetchone()
        self.assertEqual(row,(-0.65,-22,START,None,0))
        # Rerun at same time with contradictory pressure: do not update prior.
        def changed(symbol,when):
            return {"local_oi_5m_pct":+5,"spot_taker_5m_imbalance_pct":+90}
        with patch.object(post,"take_cached_evidence",side_effect=changed):
            self.assertEqual(post.observe([quotes()],START,self.path),(0,0))
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute(
                "SELECT local_oi_5m_pct FROM post_pump_short_events"
                ).fetchone()[0],-0.65)

    def test_quote_collection_without_stage_events(self):
        # Pump cycle stage is stale: no new events but quote is recorded.
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE pump_cycle_state SET last_at=?",(START-1000,))
        self.assertEqual(post.observe([quotes()],START,self.path),(0,0))
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM post_pump_short_quote_samples"
                ).fetchone()[0],1)
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM post_pump_short_events"
                ).fetchone()[0],0)

    def test_legacy_table_migrates_without_backfilling_evidence(self):
        with sqlite3.connect(self.path) as db:
            db.execute("""INSERT INTO post_pump_short_events
                (symbol,episode_at,observed_at,phase,rise_band,
                reference_price,rise_24h_pct,retrace_from_24h_high_pct,
                cycle_stage)
                VALUES ('LEGACYUSDT',?,?,?,?,?,?,?,?)""",
                (START,START,"WATCH","50_TO_99",160,60,1.5,"PUMP_START"))
            post._setup(db)
            row=db.execute("""SELECT local_oi_5m_pct,
                futures_taker_5m_delta_quote,evidence_captured_at
                FROM post_pump_short_events WHERE symbol='LEGACYUSDT'""").fetchone()
            self.assertEqual(row,(None,None,None))


if __name__=="__main__":
    unittest.main()
