"""Network-free tests for POST-PUMP research evidence v2."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

import post_pump_context_v2 as ctx

NOW=1800000000.0


class ContextV2Tests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path=str(Path(tmp.name)/"watch.sqlite")
        with sqlite3.connect(self.path) as db:
            db.execute("""CREATE TABLE pump_cycle_state(
                symbol TEXT PRIMARY KEY,flow_status TEXT,flow_delta REAL,
                flow_verified_at REAL,flow_observed_at REAL,
                flow_evidence TEXT,weakening_at REAL)""")
            db.execute("""INSERT INTO pump_cycle_state VALUES(
                'TESTUSDT','CONTRADICTION',0.65,?,?,?,?)""",
                (NOW-5,NOW-25,"BUY_ABSORPTION_CANDIDATE",NOW-60))

    def test_authenticated_absorption_evidence_is_research_only(self):
        x=ctx.read_order_flow("TESTUSDT",NOW,self.path)
        self.assertTrue(x["ready"])
        self.assertEqual(x["status"],"CONTRADICTION")
        self.assertTrue(x["buy_absorption_candidate"])
        self.assertEqual(x["delta_ratio"],0.65)
        stale=ctx.read_order_flow("TESTUSDT",NOW+500,self.path)
        self.assertFalse(stale["ready"])
        self.assertEqual(stale["reason"],"ORDER_FLOW_STALE")

    def test_bounded_cvd_distinguishes_divergence(self):
        base={"futures_taker_5m_imbalance_pct":-35,
              "futures_taker_5m_delta_quote":-12000}
        spot={"ready":True,"reason":"OKX_SPOT_WS_5M",
              "imbalance_pct":25,"delta_quote":7000}
        x=ctx.classify_bounded_cvd(base,spot)
        self.assertEqual(x["state"],"FUTURES_SELL_SPOT_BUY_DIVERGENCE")
        self.assertTrue(x["not_full_history_cvd"])
        y=ctx.classify_bounded_cvd(base,{**spot,"imbalance_pct":-22})
        self.assertEqual(y["state"],"SPOT_AND_FUTURES_SELL")

    def test_funding_rejects_stale_missing_and_wrong_instrument(self):
        row={"instId":"TEST-USDT-SWAP","fundingRate":"0.0002",
             "ts":str(int(NOW*1000)),"fundingTime":str(int((NOW+3600)*1000))}
        response=Mock()
        get=Mock(return_value=response)
        with patch.object(ctx.time,"time",return_value=NOW):
            response.json.return_value={"code":"0","data":[row]}
            rate,meta=ctx._fetch_funding("TEST-USDT-SWAP",get)
            self.assertAlmostEqual(rate,0.02)
            self.assertEqual(meta["exchange_ts"],NOW)
            for changes in ({"ts":str(int((NOW-301)*1000))},
                            {"fundingRate":None},{"fundingRate":"NaN"},
                            {"instId":"OTHER-USDT-SWAP"}):
                response.json.return_value={"code":"0","data":[dict(row,**changes)]}
                with self.assertRaises(ValueError):
                    ctx._fetch_funding("TEST-USDT-SWAP",get)

    def test_unready_spot_cannot_be_replaced_by_cached_bearish_value(self):
        ev={"futures_taker_5m_imbalance_pct":-30,"futures_taker_5m_delta_quote":-1000,
            "spot_taker_5m_imbalance_pct":-80,"spot_taker_5m_delta_quote":-5000}
        result=ctx.classify_bounded_cvd(ev,{"ready":False,"reason":"STALE"})
        self.assertEqual(result["state"],"INSUFFICIENT_DATA")
        self.assertIsNone(result["spot_delta_quote"])

    def test_candidate_refresh_is_bounded_and_isolates_failures(self):
        import post_pump_market_context as market
        import sys
        liq=Mock(side_effect=[Exception(),True,True])
        module=Mock(fetch_okx_liquidations=liq)
        with patch.dict(sys.modules,{'liquidation_engine':module}), \
             patch.object(ctx,'funding_context') as funding:
            market.collect(['AAAUSDT','BBBUSDT','CCCUSDT','DDDUSDT'])
        self.assertEqual(liq.call_count,3)
        self.assertEqual(funding.call_count,2)

    def test_capital_state_needs_bearish_structure(self):
        evidence={"agg_oi_5m_pct":0.42}
        self.assertEqual(ctx.classify_capital(
            evidence,{"trend_shift":True})["state"],
            "NEW_SHORTS_WITH_BEARISH_STRUCTURE")
        self.assertEqual(ctx.classify_capital(
            evidence,{"trend_shift":False})["state"],
            "NO_BEARISH_STRUCTURE")
        self.assertEqual(ctx.classify_capital(
            {"agg_oi_5m_pct":-0.7},{"trend_shift":True})["state"],
            "LONG_UNWIND_WITH_BEARISH_STRUCTURE")

    def test_liquidations_are_split_and_context_only(self):
        x=ctx.classify_liquidations({
            "available":True,"complete":True,"quality":"READY",
            "long_usdt":9000,"short_usdt":1000})
        self.assertEqual(x["dominance"],"LONG_LIQUIDATIONS_DOMINANT")
        self.assertTrue(x["context_only"])
        y=ctx.classify_liquidations({"available":False})
        self.assertEqual(y["dominance"],"UNAVAILABLE")

    def test_funding_is_bounded_cached_context(self):
        ctx._funding_cache.clear()
        calls=[]
        def getter(inst):
            calls.append(inst)
            return 0.12
        x=ctx.funding_context("TESTUSDT",NOW,getter)
        self.assertEqual(x["state"],"EXTREME_POSITIVE")
        self.assertEqual(calls,["TEST-USDT-SWAP"])
        y=ctx.funding_context("TESTUSDT",NOW+60,
                              lambda inst: (_ for _ in ()).throw(Exception()))
        self.assertEqual(y["funding_pct"],0.12)
        self.assertEqual(len(calls),1)

    def test_enrichment_preserves_non_gating_feature_tags(self):
        ctx._funding_cache.clear()
        evidence={"futures_taker_5m_imbalance_pct":-30,
                  "futures_taker_5m_delta_quote":-5000,
                  "agg_oi_5m_pct":0.4}
        spot={"ready":True,"reason":"OKX_SPOT_WS_5M",
              "imbalance_pct":-20,"delta_quote":-3000}
        with patch.object(ctx,"funding_context",return_value={
                "available":True,"state":"ELEVATED_POSITIVE",
                "funding_pct":0.05,"context_only":True}):
            x=ctx.enrich("TESTUSDT",NOW,evidence,spot,
                         {"trend_shift":True},
                         {"available":True,"complete":True,
                          "quality":"READY","long_usdt":10000,
                          "short_usdt":1000},
                         db_path=self.path)
        self.assertFalse(x["trade_approved"])
        self.assertIn("CVD_SPOT_AND_FUTURES_SELL",
                      x["research_feature_tags"])
        self.assertIn("CAPITAL_NEW_SHORTS_WITH_BEARISH_STRUCTURE",
                      x["research_feature_tags"])
        self.assertIn("ABSORPTION_YES",x["research_feature_tags"])

    def test_cohort_report_uses_immutable_30m_markouts(self):
        with sqlite3.connect(self.path) as db:
            db.execute("""CREATE TABLE post_pump_short_strategy_audit(
                id INTEGER PRIMARY KEY,at REAL,evidence_json TEXT)""")
            db.execute("""CREATE TABLE post_pump_short_strategy_outcomes(
                audit_id INTEGER,horizon_min INTEGER,status TEXT,
                model_net_short_pct REAL)""")
            ev=json.dumps({"research_feature_tags":[
                "ABSORPTION_YES","CVD_SPOT_AND_FUTURES_SELL"],
                "feature_combo":"ABSORPTION_YES|CVD_SPOT_AND_FUTURES_SELL"})
            db.execute("INSERT INTO post_pump_short_strategy_audit VALUES(1,?,?)",
                       (NOW-100,ev))
            db.execute("""INSERT INTO post_pump_short_strategy_outcomes
                VALUES(1,30,'DONE',3.25)""")
            ctx._last_report_at=0
            report=ctx.maybe_report_cohorts(db,NOW)
        all_row=next(x for x in report if x["cohort"]=="ALL")
        self.assertEqual(all_row["n"],1)
        self.assertEqual(all_row["positive"],1)
        self.assertAlmostEqual(all_row["mean_net_short_pct"],3.25)


if __name__=="__main__":
    unittest.main()
