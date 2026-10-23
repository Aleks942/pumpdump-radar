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
