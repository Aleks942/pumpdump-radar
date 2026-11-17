import unittest
from swing_trade_backtest import simulate, report

class TradeTests(unittest.TestCase):
    def setUp(self):
        self.event={"symbol":"TESTUSDT","ts":0.0,"direction":"LONG","score":80,"entry":100}
    def test_stop_before_target_same_candle(self):
        x=simulate(self.event,[{"ts":0.0,"h":105,"l":97,"c":104}])
        self.assertEqual(x["reason"],"STOP")
        self.assertAlmostEqual(x["net_pct"],-2.15)
    def test_target(self):
        x=simulate(self.event,[{"ts":0.0,"h":105,"l":99,"c":103}])
        self.assertEqual(x["reason"],"TARGET")
        self.assertAlmostEqual(x["net_pct"],3.85)
    def test_short_stop(self):
        e=dict(self.event,direction="SHORT")
        x=simulate(e,[{"ts":0.0,"h":103,"l":95,"c":96}])
        self.assertEqual(x["reason"],"STOP")
    def test_no_candles(self):
        self.assertIsNone(simulate(self.event,[]))
    def test_empty_report(self):
        self.assertEqual(report([])["n"],0)

if __name__=="__main__":unittest.main()
