import unittest

from post_pump_context_v2 import final_short_confirmation


class FinalShortConfirmationTests(unittest.TestCase):
    def test_requires_sequence_takeover_cvd_and_directional_oi(self):
        flow={"status":"CONFIRMED","evidence_event":"SELLER_TAKEOVER_CONFIRMED"}
        cvd={"state":"SPOT_AND_FUTURES_SELL"}
        self.assertTrue(final_short_confirmation(
            flow,cvd,{"state":"NEW_SHORTS_WITH_BEARISH_STRUCTURE"})["ready"])
        self.assertTrue(final_short_confirmation(
            flow,cvd,{"state":"LONG_UNWIND_WITH_BEARISH_STRUCTURE"})["ready"])

    def test_plain_sell_aggression_is_not_enough(self):
        result=final_short_confirmation(
            {"status":"CONFIRMED","evidence_event":"SELL_AGGRESSION_CONFIRMED"},
            {"state":"SPOT_AND_FUTURES_SELL"},
            {"state":"NEW_SHORTS_WITH_BEARISH_STRUCTURE"})
        self.assertFalse(result["ready"])
        self.assertEqual(result["reason"],"SELLER_TAKEOVER_NOT_CONFIRMED")

    def test_neutral_oi_is_not_confirmation(self):
        result=final_short_confirmation(
            {"status":"CONFIRMED","evidence_event":"SELLER_TAKEOVER_CONFIRMED"},
            {"state":"SPOT_AND_FUTURES_SELL"},
            {"state":"OI_NEUTRAL_WITH_BEARISH_STRUCTURE"})
        self.assertFalse(result["ready"])
        self.assertEqual(result["reason"],"OI_DIRECTION_NOT_CONFIRMED")


if __name__=="__main__":
    unittest.main()
