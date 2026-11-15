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
        for i in range(11,31):
            self.drive(NOW+i*60,70.2 if i%2 else 70.0)
        self.drive(NOW+1860,70.2)
        self.assertEqual(self.state()["phase"],"BUYER_ABSORPTION")
        self.drive(NOW+1920,70.2)
        self.assertEqual(self.state()["phase"],"OI_BUILDUP")
        self.drive(NOW+1980,70.2)
        self.assertEqual(self.state()["phase"],"ENERGY_BUILDUP")
        ceiling=self.state()["base_ceiling"]
        self.drive(NOW+2040,ceiling*1.004)
        self.assertEqual(self.state()["phase"],"BULLS_ACCUMULATING")
        self.drive(NOW+2100,ceiling*1.004)
        self.assertEqual(self.state()["phase"],"BULLS_ACCUMULATING")
        self.drive(NOW+2160,ceiling*1.004)
        self.assertEqual(self.state()["phase"],"BREAKOUT_READY")
        self.drive(NOW+2220,ceiling*1.004)
        self.assertEqual(self.state()["phase"],"LONG_TRIGGER")
    def test_spot_selling_blocks_accumulation(self):
        bad=ev(fut=20,spot=-50,oi=1)
        self.drive(NOW,70,bad)
        for i in range(1,32):
            self.drive(NOW+i*60,70.1,bad)
        self.assertEqual(self.state()["phase"],"BASE_FORMING")

    def test_energy_building_does_not_claim_bullish_direction(self):
        self.drive(NOW,70)
        for i in range(1,31):
            self.drive(NOW+i*60,70.1)
        self.drive(NOW+1860,70.1)
        self.drive(NOW+1920,70.1)
        self.drive(NOW+1980,70.1)
        self.assertEqual(self.state()["phase"],"ENERGY_BUILDUP")
        with sqlite3.connect(self.path) as db:
            db.row_factory=sqlite3.Row
            row=db.execute("""SELECT payload FROM post_dump_accumulation_notify
                WHERE phase='ENERGY_BUILDUP' ORDER BY audit_id DESC LIMIT 1""").fetchone()
            self.assertIsNotNone(row)
            msg=s._message(__import__("json").loads(row["payload"]))
            self.assertIn("НАПРАВЛЕНИЕ НЕ ПОДТВЕРЖДЕНО",msg)
            self.assertNotIn("РАБОЧИЙ СИГНАЛ",msg)

    def test_failed_break_returns_to_direction_unconfirmed(self):
        self.drive(NOW,70)
        for i in range(1,31):
            self.drive(NOW+i*60,70.1)
        self.drive(NOW+1860,70.1)
        self.drive(NOW+1920,70.1)
        self.drive(NOW+1980,70.1)
        ceiling=self.state()["base_ceiling"]
        self.drive(NOW+2040,ceiling*1.004)
        self.assertEqual(self.state()["phase"],"BULLS_ACCUMULATING")
        self.drive(NOW+2100,ceiling*0.999)
        self.assertEqual(self.state()["phase"],"ENERGY_BUILDUP")

    def test_ten_minutes_is_early_base_not_mature_signal(self):
        self.drive(NOW,70)
        for i in range(1,11):
            self.drive(NOW+i*60,70.1)
        self.assertEqual(self.state()["phase"],"BASE_FORMING")
        self.drive(NOW+660,70.1)
        self.assertEqual(self.state()["phase"],"BASE_FORMING")

    def test_long_trigger_is_not_green_actionable_label(self):
        event={"phase":"LONG_TRIGGER","symbol":"TESTUSDT","price":70.5,
               "drop_from_high_pct":29.5,
               "evidence":{"conditions":{"oi_5m_pct":0.5,
               "futures_imbalance_pct":15},"base":{"range_pct":1.2}}}
        msg=s._message(event)
        self.assertIn("ТЕСТОВЫЙ LONG",msg)
        self.assertNotIn("🟢 РАБОЧИЙ СИГНАЛ",msg)

if __name__=="__main__":
    unittest.main()
