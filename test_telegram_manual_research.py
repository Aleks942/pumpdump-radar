import unittest
from unittest.mock import patch
import telegram_manual_research as manual
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
        v,_=_verdict({},{"ready":True,"change_pct":0.2},
                     {"ready":True,"change_pct":0.3},{},
                     {"quality":"READY","imbalance_pct":15},
                     {"available":True,"cvd_percent":20},-0.05,{})
        self.assertIn("БЫЧЬЕ ДАВЛЕНИЕ",v)
    def test_funding_alone_cannot_create_direction(self):
        v,_=_verdict({},{"ready":False},{"ready":False},{},
                     {"quality":"WARMING"},{"available":False},-0.20,{})
        self.assertIn("НЕ ПОДТВЕРЖДЕНО",v)

    def test_spot_absorption_label(self):
        v,_=_verdict({},{"ready":True,"change_pct":-0.1},
                     {"ready":True,"change_pct":0.0},{},
                     {"quality":"READY","imbalance_pct":-25},
                     {"available":True,"cvd_percent":18},0.01,{})
        self.assertIn("ПОГЛОЩЕНИЕ ПРОДАЖ",v)

    def test_short_squeeze_label(self):
        v,_=_verdict({},{"ready":True,"change_pct":1.2},
                     {"ready":True,"change_pct":-0.8},{},
                     {"quality":"READY","imbalance_pct":12},
                     {"available":False},-0.05,
                     {"short_liq":5000,"long_liq":100})
        self.assertIn("ШОРТ-СКВИЗ",v)

    def test_new_short_buildup_label(self):
        v,_=_verdict({},{"ready":True,"change_pct":-1.1},
                     {"ready":True,"change_pct":0.6},{},
                     {"quality":"READY","imbalance_pct":-20},
                     {"available":True,"cvd_percent":-15},0.02,{})
        self.assertIn("НОВЫХ ШОРТОВ",v)

    def test_new_long_buildup_label(self):
        v,_=_verdict({},{"ready":True,"change_pct":1.1},
                     {"ready":True,"change_pct":0.6},{},
                     {"quality":"READY","imbalance_pct":20},
                     {"available":True,"cvd_percent":15},0.01,{})
        self.assertIn("НОВЫХ ЛОНГОВ",v)

    def test_long_liquidation_label(self):
        v,_=_verdict({},{"ready":True,"change_pct":-1.1},
                     {"ready":True,"change_pct":-0.6},{},
                     {"quality":"READY","imbalance_pct":-5},
                     {"available":False},0.01,{})
        self.assertIn("ЛИКВИДАЦИЯ",v)

    @patch.object(manual,"fetch_ticker")
    @patch.object(manual,"_price5")
    @patch.object(manual,"_bybit_oi5")
    @patch.object(manual,"_agg_oi5")
    @patch.object(manual,"_flow5")
    @patch.object(manual,"get_spot_cvd")
    @patch.object(manual,"_liqs")
    def test_report_uses_real_liquidation_keys(self,m_liqs,m_spot,m_flow,m_agg,m_local,m_price,m_ticker):
        m_ticker.return_value={"last":1.0,"change24h_pct":0.0,
                               "drop_from_high_pct":0.0,
                               "open_interest_value":1000.0,
                               "funding_pct":0.005}
        m_price.return_value={"ready":False}
        m_local.return_value={"ready":False}
        m_agg.return_value={}
        m_flow.return_value={}
        m_spot.return_value={"available":False}
        m_liqs.return_value={"available":True,"quality":"OBSERVED_PARTIAL",
                             "exchanges":"Bybit","long_liq":1234.0,
                             "short_liq":5678.0}
        report,_=manual.build_snapshot("TESTUSDT")
        self.assertIn("1,234 USD / 5,678 USD · Bybit",report)

    def test_direction_change_requires_two_refreshes(self):
        sent=[]
        bot=manual.ManualResearchBot("token","1",lambda text,**kw: sent.append(text))
        bot.watch["TESTUSDT"]={"expires":99999999999.0,
                               "last_verdict":"NEUTRAL","last_sent":0.0,
                               "candidate_verdict":None,"candidate_count":0}
        with patch.object(manual,"build_snapshot",return_value=("R","BEAR")):
            bot.refresh(now=1000.0)
            self.assertEqual(sent,[])
            bot.refresh(now=1060.0)
            self.assertEqual(sent,["R"])

if __name__=="__main__":
    unittest.main()
