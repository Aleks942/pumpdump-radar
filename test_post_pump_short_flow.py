"""Authenticated, episode-scoped external Order Flow evidence tests."""
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import post_pump_short_flow as flow
import post_pump_context_v2 as context

AT=1800000000.0


class DirectFlowTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path=str(Path(tmp.name)/"research.sqlite")
        self.db=sqlite3.connect(self.path)
        self.db.row_factory=sqlite3.Row
        self.addCleanup(self.db.close)
        flow.setup(self.db)
        flow._recent.clear()

    def store(self,status="CONFIRMED",reason="TAKER_PRESSURE_PRICE_ALIGNED"):
        return flow.store_result(self.db,"TESTUSDT",AT-1800,
              AT,{"status":status,"finished_at":AT+20,"delta_ratio":-0.5,
                  "price_change_pct":-0.1,"trade_span_sec":20,
                  "reason":reason,"evidence_event":"NONE"},AT+22)

    def test_confirmed_result_fresh_and_matched_to_episode(self):
        self.assertTrue(self.store())
        record=flow.latest(self.db,"TESTUSDT",AT+22,AT-1800)
        self.assertEqual(record["status"],"CONFIRMED")
        self.assertEqual(record["price_change_pct"],-0.1)
        self.assertIsNone(flow.latest(self.db,"TESTUSDT",AT+22,AT-900))
        self.assertIsNone(flow.latest(self.db,"TESTUSDT",AT+300,AT-1800))

    def test_invalid_stale_flow_never_authenticated(self):
        self.assertFalse(flow.store_result(self.db,"TESTUSDT",AT-1800,AT,
                   {"status":"CONFIRMED","finished_at":AT-2},AT+22))
        self.assertFalse(flow.store_result(self.db,"TESTUSDT",AT-1800,AT,
                   {"status":"CONFIRMED","finished_at":AT+21,
                    "delta_ratio":-2},AT+22))
        self.assertFalse(flow.store_result(self.db,"TESTUSDT",AT-1800,AT,
                   {"status":"READY","finished_at":AT+21},AT+22))

    def test_context_reads_direct_source_and_preserves_contradiction(self):
        self.assertTrue(self.store(status="CONTRADICTION",
                 reason="TAKER_PRESSURE_ABSORBED"))
        e=context.read_order_flow("TESTUSDT",AT+22,self.path,
                                  episode_at=AT-1800)
        self.assertEqual(e["status"],"CONTRADICTION")
        self.assertTrue(e["ready"])
        self.assertEqual(e["source"],"POST_PUMP_DIRECT_AUTHENTICATED_FLOW")
        self.assertFalse(context.read_order_flow(
            "TESTUSDT",AT+22,self.path,episode_at=AT-800)["ready"])

    def test_missing_configuration_cannot_submit_candidate(self):
        with patch.dict(os.environ,{"PUMP_FLOW_MODE":"OFF",
                                   "PUMP_FLOW_URL":"","PUMP_FLOW_SHARED_TOKEN":""}):
            self.assertFalse(flow.dispatch("TESTUSDT",100,AT-10,
                                           db_path=self.path,now=AT))

if __name__=="__main__":
    unittest.main()
