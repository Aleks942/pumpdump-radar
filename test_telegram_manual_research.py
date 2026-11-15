import unittest
from telegram_manual_research import normalize_symbol,parse_command,_verdict

class ManualResearchTests(unittest.TestCase):
    def test_symbol_normalization(self):
        self.assertEqual(normalize_symbol("jct"),"JCTUSDT")
        self.assertEqual(normalize_symbol("JCT-USDT-SWAP"),"JCTUSDT")
        self.assertEqual(normalize_symbol("$JCT"),"JCTUSDT")
    def test_commands(self):
        self.assertEqual(parse_command("/check jct"),("CHECK","JCTUSDT"))
        self.assertEqual(parse_command("JCTUSDT"),("CHECK","JCTUSDT"))
        self.assertEqual(parse_command("/stop JCT"),("STOP","JCTUSDT"))
        self.assertEqual(parse_command("/watchlist"),("LIST",None))
    def test_bullish_pressure_requires_multiple_factors(self):
        v,_=_verdict({},{"ready":True,"change_pct":0.3},{},
                     {"quality":"READY","imbalance_pct":15},
                     {"available":True,"cvd_percent":20},-0.05)
        self.assertIn("БЫЧЬЕ ДАВЛЕНИЕ",v)
    def test_funding_alone_cannot_create_direction(self):
        v,_=_verdict({},{"ready":False},{},{"quality":"WARMING"},
                     {"available":False},-0.20)
        self.assertIn("НЕ ПОДТВЕРЖДЕНО",v)

if __name__=="__main__":
    unittest.main()
