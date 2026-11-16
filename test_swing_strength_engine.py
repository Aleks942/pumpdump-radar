import unittest

import swing_strength_engine as s


class SwingStrengthTests(unittest.TestCase):
    def test_bullish_medium_term_score(self):
        up={"ready":True,"regime":"UP","hh_hl":True,"lh_ll":False,
            "change_3bars_pct":2.0}
        oi1={"ready":True,"change_pct":1.0}
        oi4={"ready":True,"change_pct":2.0}
        vol={"ready":True,"ratio":1.6}
        r=s._score(up,up,up,oi1,oi4,85,vol,
                   {"price5_pct":0.1,"futures_imbalance_pct":-15})
        self.assertEqual(r["bias"],"BULLISH")
        self.assertGreaterEqual(r["score"],70)
        self.assertTrue(any("поглощение" in x.lower() for x in r["reasons"]))

    def test_bearish_medium_term_score(self):
        down={"ready":True,"regime":"DOWN","hh_hl":False,"lh_ll":True,
              "change_3bars_pct":-2.0}
        oi1={"ready":True,"change_pct":1.0}
        oi4={"ready":True,"change_pct":2.0}
        vol={"ready":True,"ratio":1.6}
        r=s._score(down,down,down,oi1,oi4,15,vol,
                   {"price5_pct":-0.5,"futures_imbalance_pct":-20})
        self.assertEqual(r["bias"],"BEARISH")
        self.assertLessEqual(r["score"],30)

    def test_five_minute_flow_does_not_define_direction_by_itself(self):
        mixed={"ready":True,"regime":"MIXED","hh_hl":False,"lh_ll":False,
               "change_3bars_pct":0.0}
        nooi={"ready":False}
        vol={"ready":True,"ratio":1.0}
        r=s._score(mixed,mixed,mixed,nooi,nooi,50,vol,
                   {"price5_pct":2.0,"futures_imbalance_pct":50})
        self.assertEqual(r["bias"],"NEUTRAL")
        self.assertLess(r["score"],70)

    def test_weekly_position_is_clamped_to_0_100(self):
        daily=[
            {"l":1.0,"h":2.0},{"l":1.1,"h":2.1},{"l":1.2,"h":2.2},
            {"l":1.3,"h":2.3},{"l":1.4,"h":2.4},{"l":1.5,"h":2.5},
            {"l":1.6,"h":2.6},
        ]
        self.assertEqual(s._weekly_position(daily,3.0),100.0)
        self.assertEqual(s._weekly_position(daily,0.5),0.0)

    def test_compact_report_is_short_and_actionable(self):
        r={"score":82,"bias":"BULLISH",
           "label":"🟢 СИЛЬНАЯ МОНЕТА / ПОКУПАТЕЛЬ КОНТРОЛИРУЕТ",
           "trend1h":{"regime":"UP"},"trend4h":{"regime":"UP"},
           "trend1d":{"regime":"MIXED"},
           "oi1h":{"change_pct":1.2},"oi4h":{"change_pct":2.3},
           "reasons":["1H: структура вверх","4H: структура вверх"],
           "risks":["Фандинг перегрет"]}
        lines=s.format_compact_report(r,"🟡 БЫЧЬЕ ДАВЛЕНИЕ")
        text="\n".join(lines)
        self.assertLessEqual(len(lines),10)
        self.assertIn("82/100",text)
        self.assertIn("План: искать ЛОНГ",text)
        self.assertIn("Отмена:",text)
        self.assertIn("/detail",text)

    def test_report_states_medium_term_horizon(self):
        r={"score":80,"bias":"BULLISH",
           "label":"🟢 СИЛЬНАЯ МОНЕТА / ПОКУПАТЕЛЬ КОНТРОЛИРУЕТ",
           "trend15":{"regime":"UP"},"trend1h":{"regime":"UP"},
           "trend4h":{"regime":"UP"},"trend1d":{"regime":"UP"},
           "oi1h":{"change_pct":1.2},"oi4h":{"change_pct":2.3},
           "weekly_position_pct":80,
           "reasons":["1H: структура вверх"],"risks":[]}
        text="\n".join(s.format_report(r))
        self.assertIn("СРЕДНЕСРОК: 1 ЧАС — 3 ДНЯ",text)
        self.assertIn("СИЛЬНАЯ МОНЕТА",text)
        self.assertIn("5м используется только для точного входа",text)


if __name__=="__main__":
    unittest.main()
