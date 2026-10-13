import os
import tempfile
import unittest
from unittest.mock import patch
import pump_peak_watch as watch


def ticker(price=110, high=112, opening=100, vol=25_000_000):
    return {"instId":"TEST-USDT-SWAP","last":str(price),
            "high24h":str(high),"open24h":str(opening),
            "volCcy24h":str(vol)}


class PumpWatchTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        for prop,val in (("DB_PATH",os.path.join(tmp.name,"watch.db")),
                         ("_observed",{})):
            mock=patch.object(watch,prop,val)
            mock.start()
            self.addCleanup(mock.stop)
        self.now=1791712800.0

    def observe(self,p,now=0,high=112,opening=100,vol=25_000_000):
        return watch.observe([ticker(price=p,high=high,opening=opening,vol=vol)],
                             now=self.now+now)

    def test_rise_then_weakening_then_risk(self):
        a=self.observe(110)
        self.assertEqual(len(a),1)
        self.assertEqual(a[0]["level"],1)
        self.assertEqual(a[0]["rise"],12)
        watch.acknowledge(a[0],True,now=self.now)
        self.assertEqual(self.observe(109,now=60),[])
        b=self.observe(107,now=120)
        self.assertEqual([x["level"] for x in b],[2])
        watch.acknowledge(b[0],True,now=self.now+120)
        c=self.observe(104,now=180)
        self.assertEqual([x["level"] for x in c],[3])
        watch.acknowledge(c[0],True,now=self.now+180)
        self.assertEqual(watch.status(),(1,1,1,1))
        self.assertEqual(self.observe(104,now=240),[])

    def test_failed_telegram_keeps_watch_pending_with_backoff(self):
        a=self.observe(111)
        watch.acknowledge(a[0],False,now=self.now)
        self.assertFalse(self.observe(111,now=60))
        self.assertEqual(self.observe(111,now=360)[0]["level"],1)

    def test_no_watch_for_small_pump_and_unliquid_ticker(self):
        self.assertEqual(self.observe(104,high=105),[])
        self.assertEqual(self.observe(110,high=112,now=60,vol=1000),[])
        self.assertEqual(watch.status()[0],0)

    def test_missing_open_or_risk_already_realized_no_watch(self):
        self.assertEqual(watch.observe([ticker(opening=0)],now=self.now),[])
        self.assertEqual(self.observe(77,high=115),[])

    def test_daily_throttle_and_message_is_not_trading_alert(self):
        e=self.observe(110)[0]
        msg=watch.message(e)
        self.assertIn("НЕ подтверждённый сигнал SHORT",msg)
        self.assertIn("\nРост до максимума",msg)
        self.assertNotIn("\\n",msg)
        watch.acknowledge(e,True,now=self.now)
        with watch._db() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM pump_watch_alerts").fetchone()[0],1)


    def test_liquid_btc_base_volume_below_old_raw_threshold(self):
        # The old code rejected 100k base units as <10m;
        # at a $110 quote that is about 11m USDT notional.
        rows = self.observe(110, vol=100_000)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["level"], 1)

    def test_microprice_requires_healthy_spread(self):
        low_price = ticker(price=0.005, opening=0.004,
                           high=0.006, vol=3_000_000_000)
        self.assertEqual(watch.observe([low_price], now=self.now), [])
        low_price["bidPx"] = "0.004999"
        low_price["askPx"] = "0.005001"
        self.assertEqual(len(watch.observe([low_price], now=self.now)), 1)


if __name__=="__main__":
    unittest.main()
