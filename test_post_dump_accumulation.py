import os
import sqlite3
import tempfile
import unittest
import post_dump_accumulation as s

NOW=1800000000.0

def ticker(last=70,high=100):
    return {"instId":"TEST-USDT-SWAP","last":str(last),"high24h":str(high),
            "low24h":"68","open24h":"95","volCcy24h":"200000000",
            "askPx":str(last*1.0005),"bidPx":str(last*0.9995)}

def ev(fut=12,spot=2,oi=0.5):
    return {"futures_taker_5m_imbalance_pct":fut,
            "spot_taker_5m_imbalance_pct":spot,
            "agg_oi_5m_pct":oi}

class AccumTests(unittest.TestCase):
    def setUp(self):
        f=tempfile.NamedTemporaryFile(delete=False); f.close(); self.path=f.name
    def tearDown(self):
        try: os.unlink(self.path)
        except FileNotFoundError: pass
    def drive(self,at,last,evidence=None):
        s.observe([ticker(last=last)],at,self.path,
                  evidence_reader=lambda symbol,now:evidence or ev())
    def state(self):
        with sqlite3.connect(self.path) as db:
            db.row_factory=sqlite3.Row
            r=db.execute("SELECT * FROM post_dump_accumulation_state").fetchone()
            return dict(r) if r else None
    def test_requires_strong_drop(self):
        self.assertLess(s._profile(ticker(last=90))["drop_from_high_pct"],
                        s.MIN_DROP_FROM_HIGH_PCT)
    def test_full_progression(self):
        self.drive(NOW,70)
        for i in range(1,11):
            self.drive(NOW+i*60,70.2 if i%2 else 70.0)
        self.assertEqual(self.state()["phase"],"BASE_FORMING")
        self.drive(NOW+660,70.2)
        self.assertEqual(self.state()["phase"],"BUYER_ABSORPTION")
        self.drive(NOW+720,70.2)
        self.assertEqual(self.state()["phase"],"OI_BUILDUP")
        self.drive(NOW+780,70.2)
        self.assertEqual(self.state()["phase"],"BULLS_ACCUMULATING")
        ceiling=self.state()["base_ceiling"]
        self.drive(NOW+840,ceiling*1.004)
        self.assertEqual(self.state()["phase"],"BREAKOUT_READY")
        self.drive(NOW+900,ceiling*1.006)
        self.assertEqual(self.state()["phase"],"LONG_TRIGGER")
    def test_spot_selling_blocks_accumulation(self):
        bad=ev(fut=20,spot=-50,oi=1)
        self.drive(NOW,70,bad)
        for i in range(1,12):
            self.drive(NOW+i*60,70.1,bad)
        self.assertEqual(self.state()["phase"],"BASE_FORMING")

if __name__=="__main__":
    unittest.main()
