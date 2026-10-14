"""Deterministic proof that a sampled price-only lifecycle cannot approve trades."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pump_lifecycle as life


def quote(price, move=0.0, symbol="GRASSUSDT"):
    return {"symbol":symbol,"price":price,"change5":move,
            "sample_age_sec":300}


class PumpLifecycleTests(unittest.TestCase):
    def test_pump_to_weakening_break_and_reclaim(self):
        with tempfile.TemporaryDirectory() as temp:
            path=str(Path(temp)/"market.db")
            sequence=[
                (0,100.0,2.0,"PUMP_START"),
                (30,101.0,1.0,None),
                (60,102.0,1.0,None),
                (90,101.6,0.0,None),
                (120,101.0,0.0,"PUMP_WEAKENING"),
                (150,100.4,0.0,"PRICE_BREAK_CANDIDATE"),
                (180,101.2,0.0,"PRICE_BREAK_INVALIDATED"),
            ]
            for delta,price,move,expected in sequence:
                result=life.observe([quote(price,move)],1.5,
                                    now=100000+delta,db_path=path)
                self.assertEqual(result[0][1] if result else None,expected)
            with sqlite3.connect(path) as db:
                events=[row[0] for row in db.execute(
                    "SELECT state FROM pump_lifecycle_events ORDER BY id")]
                state=db.execute("SELECT state FROM pump_lifecycle").fetchone()[0]
            self.assertEqual(events,["PUMP_START","PUMP_WEAKENING",
                                     "PRICE_BREAK_CANDIDATE",
                                     "PRICE_BREAK_INVALIDATED"])
            self.assertEqual(state,"PUMP_WEAKENING")

    def test_stale_samples_not_traded_and_no_fake_history(self):
        with tempfile.TemporaryDirectory() as temp:
            path=str(Path(temp)/"market.db")
            self.assertEqual(life.observe([quote(100,2)],1.5,
                                          now=200000,db_path=path),
                             [("GRASSUSDT","PUMP_START")])
            self.assertEqual(life.observe([quote(92,0)],1.5,
                                          now=200001,db_path=path),[])
            self.assertEqual(life.observe([quote(95,0)],1.5,
                                          now=200500,db_path=path),[])
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute(
                    "SELECT COUNT(*) FROM pump_lifecycle").fetchone()[0],0)
                self.assertEqual(db.execute(
                    "SELECT COUNT(*) FROM pump_lifecycle_events").fetchone()[0],1)

    def test_recovery_to_new_high_clears_exhaustion(self):
        with tempfile.TemporaryDirectory() as temp:
            path=str(Path(temp)/"market.db")
            prices=[(100,2),(101,0),(102,0),(101.6,0),(101,0),(103,0)]
            for i,(price,move) in enumerate(prices):
                life.observe([quote(price,move)],1.5,
                             now=300000+i*30,db_path=path)
            with sqlite3.connect(path) as db:
                state,peak,weak=db.execute(
                    "SELECT state,peak_price,weak_ts FROM pump_lifecycle"
                ).fetchone()
            self.assertEqual(state,"PUMP_CONTINUATION")
            self.assertEqual(peak,103)
            self.assertIsNone(weak)

    def test_missing_or_unordered_prices_do_not_trigger(self):
        with tempfile.TemporaryDirectory() as temp:
            path=str(Path(temp)/"market.db")
            rows=[quote("nan",12),quote(100,float("nan")),
                  {**quote(100,2),"sample_age_sec":15},
                  quote(100,2,symbol="DOGEUSDT")]
            self.assertEqual(life.observe(rows,1.5,now=400000,
                                          db_path=path),
                             [("DOGEUSDT","PUMP_START")])
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute(
                    "SELECT COUNT(*) FROM pump_lifecycle").fetchone()[0],1)


if __name__=="__main__":
    unittest.main()
