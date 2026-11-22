import unittest
from unittest.mock import patch

import swing_replay_backtest as r


def c15_rows(n=100,start=0,price=100.0):
    return [
        {"ts":float(start+i*900),"o":price+i*0.01,"h":price+i*0.01+0.3,
         "l":price+i*0.01-0.3,"c":price+i*0.01+0.05,"v":1000}
        for i in range(n)
    ]


class ReplayTests(unittest.TestCase):
    @patch.object(r,"_trade_plan")
    @patch.object(r,"_entry_setup")
    @patch.object(r,"_breakout_state")
    @patch.object(r,"_trend")
    @patch.object(r,"_score_at")
    def test_ready_entry_uses_next_15m_open(
        self,m_score,m_trend,m_break,m_setup,m_plan
    ):
        rows=c15_rows(90,start=0)
        # Only one decision bar qualifies.
        def score_at(c1,c4,cd,oi,ts):
            return {"bias":"BULLISH","score":82,"h1":[{"ts":0,"o":1,"h":2,"l":.5,"c":1.5,"v":1}]*30} if ts==81*900 else None
        m_score.side_effect=score_at
        m_trend.return_value={"ready":True,"regime":"UP"}
        m_break.return_value={"phase":"BREAKOUT_CONFIRMED","level":99}
        m_setup.return_value={"phase":"READY_ENTRY"}
        m_plan.side_effect=lambda c,b,p: {
            "ready":True,"stop":p-1,"tp1":p+2,"tp2":p+3,"stop_pct":1.0
        }
        out=r.build_ready_entries("TESTUSDT",rows,[{"ts":0}],[(0,1)])
        self.assertEqual(len(out),1)
        event=out[0]
        self.assertEqual(event["decision_ts"],81*900)
        self.assertEqual(event["entry_ts"],81*900)
        self.assertEqual(event["entry"],rows[81]["o"])

    def test_same_candle_stop_wins_over_targets(self):
        event={"symbol":"TESTUSDT","entry_ts":0.0,"direction":"LONG",
               "score":80,"entry":100.0,"stop":98.0,"tp1":104.0,"tp2":106.0,
               "stop_pct":2.0}
        rows=[{"ts":0.0,"o":100,"h":107,"l":97,"c":105,"v":1}]
        out=r.simulate_trade(event,rows,max_hours=1)
        self.assertEqual(out["reason"],"STOP")
        self.assertEqual(out["r"],-1.0)

    def test_tp2_records_three_r(self):
        event={"symbol":"TESTUSDT","entry_ts":0.0,"direction":"SHORT",
               "score":20,"entry":100.0,"stop":102.0,"tp1":96.0,"tp2":94.0,
               "stop_pct":2.0}
        rows=[
            {"ts":0.0,"o":100,"h":101,"l":95.5,"c":96,"v":1},
            {"ts":900.0,"o":96,"h":97,"l":93.5,"c":94,"v":1},
        ]
        out=r.simulate_trade(event,rows,max_hours=1)
        self.assertEqual(out["reason"],"TP2")
        self.assertEqual(out["r"],3.0)
        self.assertTrue(out["tp1_hit"])
        self.assertTrue(out["tp2_hit"])

    def test_summary_expectancy_in_r(self):
        trades=[{"r":3,"tp1_hit":True,"tp2_hit":True,"stop_hit":False},
                {"r":-1,"tp1_hit":False,"tp2_hit":False,"stop_hit":True}]
        s=r.summarize(trades)
        self.assertEqual(s["n"],2)
        self.assertEqual(s["expectancy_r"],1.0)


if __name__=="__main__":
    unittest.main()

# workflow trigger: replay validation
