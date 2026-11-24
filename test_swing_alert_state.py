import os
import tempfile
import unittest

import swing_alert_state as s


class SwingAlertStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        os.environ["SWING_ALERT_DB"]=self.tmp.name
        s.clear_state()

    def tearDown(self):
        os.environ.pop("SWING_ALERT_DB",None)
        try:
            os.remove(self.tmp.name)
        except FileNotFoundError:
            pass

    def test_persists_and_blocks_duplicate(self):
        now=1_700_000_000
        self.assertTrue(s.alert_allowed("AAAUSDT","BULLISH",80,7200,now))
        s.mark_alert("AAAUSDT","BULLISH",80,now)
        self.assertFalse(s.alert_allowed("AAAUSDT","BULLISH",80,7200,now+60))

    def test_bias_flip_or_score_change_can_alert(self):
        now=1_700_000_000
        s.mark_alert("AAAUSDT","BULLISH",80,now)
        self.assertTrue(s.alert_allowed("AAAUSDT","BEARISH",20,7200,now+60))
        self.assertTrue(s.alert_allowed("AAAUSDT","BULLISH",89,7200,now+60))


    def test_compression_watch_dedup_is_separate_from_trade_alert(self):
        now=1_700_000_000
        self.assertTrue(
            s.compression_alert_allowed(
                "EVAAUSDT", 10.0, 10.5, 60, 21600, now
            )
        )
        s.mark_compression_alert("EVAAUSDT", 10.0, 10.5, 60, now)
        self.assertFalse(
            s.compression_alert_allowed(
                "EVAAUSDT", 10.0, 10.5, 60, 21600, now+60
            )
        )

        # A WATCH must not consume or overwrite the real SWING alert state.
        self.assertTrue(
            s.alert_allowed("EVAAUSDT", "BULLISH", 80, 7200, now+60)
        )
        s.mark_alert("EVAAUSDT", "BULLISH", 80, now+60)
        self.assertFalse(
            s.alert_allowed("EVAAUSDT", "BULLISH", 80, 7200, now+120)
        )

    def test_delivered_entry_survives_intervening_phase_change(self):
        now = 1_700_000_000
        symbol, bias, setup = "AAAUSDT", "BULLISH", "READY_ENTRY:1699999200"
        s.mark_alert(symbol, bias, 80, now, entry_state=setup)
        s.mark_alert(symbol, bias, 80, now + 60, entry_state="RETEST")
        self.assertFalse(s.alert_allowed(symbol, bias, 90, 7200, now + 120, entry_state=setup))
        self.assertTrue(s.alert_allowed(
            symbol, bias, 90, 7200, now + 120, entry_state="READY_ENTRY:1700002800"
        ))

    def test_entry_state_change_can_alert_even_when_score_is_same(self):
        now=1_700_000_000
        s.mark_alert(
            "AAAUSDT","BULLISH",80,now,entry_state="WAIT_RETEST"
        )
        self.assertTrue(
            s.alert_allowed(
                "AAAUSDT","BULLISH",80,7200,now+60,
                entry_state="BLOCKED",
            )
        )


if __name__=="__main__":
    unittest.main()
