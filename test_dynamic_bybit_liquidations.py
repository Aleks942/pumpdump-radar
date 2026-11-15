import json
import unittest

import liquidation_engine as liq


class FakeWS:
    def __init__(self):
        self.sent=[]
    def send(self,message):
        self.sent.append(json.loads(message))


class DynamicBybitLiquidationTests(unittest.TestCase):
    def setUp(self):
        with liq._LOCK:
            liq._BYBIT_WS=None
            liq._BYBIT_WATCH=set(liq.BYBIT_SYMBOLS)
            liq._BYBIT_SUBSCRIBED=set()

    def test_active_candidate_is_added_and_subscribed(self):
        ws=FakeWS()
        liq.bybit_on_open(ws)
        with liq._LOCK:
            liq._BYBIT_WS=ws
        self.assertTrue(liq.watch_bybit_symbol("IRYSUSDT"))
        self.assertIn("IRYSUSDT",liq._BYBIT_WATCH)
        topics=[topic for message in ws.sent for topic in message.get("args",[])]
        self.assertIn("allLiquidation.IRYSUSDT",topics)

    def test_rejected_subscription_does_not_poison_global_source(self):
        ws=FakeWS()
        liq._status("Bybit","*",True)
        liq.bybit_on_message(ws,json.dumps({
            "success":False,
            "ret_msg":"symbol unsupported"
        }))
        summary=liq.get_liquidation_summary("BTCUSDT")
        self.assertNotEqual(summary["sources"]["Bybit"],"ERROR")

    def test_malformed_row_is_skipped_not_global_error(self):
        ws=FakeWS()
        with liq._LOCK:
            liq._BYBIT_WATCH.add("BTCUSDT")
        liq.bybit_on_message(ws,json.dumps({
            "topic":"allLiquidation.BTCUSDT",
            "data":[{"s":"WRONGUSDT","S":"Buy","p":"1","v":"1","T":"1"}]
        }))
        summary=liq.get_liquidation_summary("BTCUSDT")
        self.assertNotEqual(summary["sources"]["Bybit"],"ERROR")

    def test_duplicate_watch_does_not_duplicate_subscription(self):
        ws=FakeWS()
        with liq._LOCK:
            liq._BYBIT_WS=ws
        liq.watch_bybit_symbol("IRYSUSDT")
        first=len(ws.sent)
        liq.watch_bybit_symbol("IRYSUSDT")
        self.assertEqual(len(ws.sent),first)


if __name__=="__main__":
    unittest.main()
