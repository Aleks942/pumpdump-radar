"""Autonomous local POST-PUMP aggression/absorption verifier tests."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

import post_pump_short_flow as flow
import post_pump_context_v2 as context

AT=1800000000.0


def window(imbalance,trades=50,total=10000):
    return {"window_ready":True,"stream_healthy":True,"quality":"READY",
            "last_trade_age_sec":2,"stream_stale_seconds":1,"trade_count":trades,
            "total_quote":total,"imbalance_pct":imbalance}


class LocalFlowTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path=str(Path(tmp.name)/"research.sqlite")
        self.db=sqlite3.connect(self.path)
        self.db.row_factory=sqlite3.Row
        self.addCleanup(self.db.close)
        flow.setup(self.db)
        flow._recent.clear()
        self.db.execute("""CREATE TABLE post_pump_short_strategy(
            symbol TEXT PRIMARY KEY,episode_at REAL NOT NULL,
            last_structure_json TEXT)""")
        self.db.execute("""INSERT INTO post_pump_short_strategy VALUES(
            'TESTUSDT',?,?)""",(AT-1800,
            '{"trend_shift":true,"bearish5":true,'
            '"last5":{"o":101,"c":99}}'))
        self.db.commit()

    def test_sell_aggression_confirms_only_with_spot_and_bearish_response(self):
        result=flow.verify_local(98,
            {"1m":window(-35),"5m":window(-25)},
            {"5m":window(-20)},
            {"trend_shift":True,"bearish5":True,
             "last5":{"o":101,"c":99}},AT)
        self.assertEqual(result["status"],"CONFIRMED")
        self.assertEqual(result["evidence_event"],"SELL_AGGRESSION_CONFIRMED")
        self.assertEqual(result["source"],"PUMPDUMP_LOCAL_TAKER_FLOW")
        self.assertFalse(result["orderbook_verified"])

    def test_buy_aggression_against_bearish_price_is_absorption_candidate(self):
        result=flow.verify_local(99,
            {"1m":window(38),"5m":window(24)},
            {"5m":window(18)},
            {"trend_shift":True,"bearish5":True,
             "last5":{"o":101,"c":99}},AT)
        self.assertEqual(result["status"],"CONTRADICTION")
        self.assertEqual(result["reason"],"LOCAL_BUY_AGGRESSION_ABSORBED")
        self.assertEqual(result["evidence_event"],"BUY_ABSORPTION_CANDIDATE")

    def test_stale_or_small_sample_fails_closed(self):
        bad=window(-50,trades=2)
        result=flow.verify_local(99,
            {"1m":bad,"5m":window(-30)},
            {"5m":window(-25)},
            {"trend_shift":True,"bearish5":True,
             "last5":{"o":101,"c":99}},AT)
        self.assertEqual(result["status"],"INSUFFICIENT_DATA")
        self.assertEqual(result["reason"],"LOCAL_FLOW_NOT_READY")
        self.assertEqual(result["missing_windows"],["FUTURES_1M"])

    def test_stale_symbol_trades_are_not_approved_by_global_health(self):
        fw={"1m":window(-30),"5m":window(-30)}
        sw={"5m":dict(window(-30),last_trade_age_sec=100)}
        result=flow.verify_local(100,fw,sw,{"trend_shift":True,"bearish5":True},now=AT)
        self.assertEqual(result["status"],"INSUFFICIENT_DATA")
        self.assertIn("SPOT_5M",result["missing_windows"])

    def test_missing_spot_window_is_identified_without_approval(self):
        result=flow.verify_local(99,
            {"1m":window(-40),"5m":window(-30)}, {},
            {"trend_shift":True,"bearish5":True,
             "last5":{"o":101,"c":99}},AT)
        self.assertEqual(result["status"],"INSUFFICIENT_DATA")
        self.assertEqual(result["missing_windows"],["SPOT_5M"])

    def test_dispatch_uses_local_readers_and_episode_scoped_storage(self):
        readers=(lambda symbol:{"1m":window(-40),"5m":window(-30)},
                 lambda symbol:{"5m":window(-25)})
        self.assertTrue(flow.dispatch("TESTUSDT",98,AT-1800,
                                     self.path,AT,readers=readers))
        record=flow.latest(self.db,"TESTUSDT",AT,AT-1800)
        self.assertEqual(record["status"],"CONFIRMED")
        self.assertIsNone(flow.latest(self.db,"TESTUSDT",AT,AT-900))
        # Bounded local refresh: repeated scan inside cooldown does no work.
        self.assertFalse(flow.dispatch("TESTUSDT",98,AT-1800,
                                      self.path,AT+30,readers=readers))

    def test_context_reads_local_source(self):
        readers=(lambda symbol:{"1m":window(40),"5m":window(25)},
                 lambda symbol:{"5m":window(15)})
        self.assertTrue(flow.dispatch("TESTUSDT",99,AT-1800,
                                     self.path,AT,readers=readers))
        e=context.read_order_flow("TESTUSDT",AT,self.path,
                                  episode_at=AT-1800)
        self.assertEqual(e["status"],"CONTRADICTION")
        self.assertTrue(e["buy_absorption_candidate"])
        self.assertEqual(e["source"],"PUMPDUMP_LOCAL_TAKER_FLOW")
        self.assertFalse(e["orderbook_verified"])


if __name__=="__main__":
    unittest.main()
