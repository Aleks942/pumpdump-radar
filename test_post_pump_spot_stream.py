"""Network-free tests for dedicated bounded POST-PUMP spot websocket."""
import json
import unittest
from unittest.mock import patch

import post_pump_spot_stream as stream


class ThreadAlive:
    def is_alive(self):
        return True


class SpotStreamTests(unittest.TestCase):
    def test_only_valid_spot_trade_messages_enter_cache(self):
        msg={"arg":{"channel":"trades","instId":"TEST-USDT"},"data":[
            {"ts":"1800000000000","px":"1.25","sz":"12",
             "side":"sell"},
            {"ts":"1800000000000","px":"0","sz":"12",
             "side":"buy"}]}
        with patch.object(stream,"_watched",("TEST-USDT",)):
            with patch.object(stream,"save_trade") as save:
                with patch.object(stream,"mark_stream_activity") as activity:
                    stream._message(json.dumps(msg))
                    self.assertEqual(save.call_count,1)
                    self.assertEqual(save.call_args.kwargs["market"],"spot")
                    self.assertEqual(save.call_args.kwargs["side"],"SELL")
                    self.assertEqual(save.call_args.kwargs["quote_value"],15.0)
                    activity.assert_called_once_with("spot")
        with patch.object(stream,"_watched",("ELSE-USDT",)):
            with patch.object(stream,"save_trade") as save:
                stream._message(json.dumps(msg))
                save.assert_not_called()

    def test_5m_must_be_ready_with_notional_and_trade_count(self):
        def reader(count=30,ready=True,total=2500):
            return {"5m":{"window_ready":ready,"stream_healthy":True,
                           "quality":"READY" if ready else "WARMING",
                           "stream_stale_seconds":4,
                           "trade_count":count,"total_quote":total,
                           "imbalance_pct":-44.0,
                           "delta_quote":-1110}}
        with patch.object(stream,"get_spot_windows",return_value=reader()):
            r=stream.verified("TESTUSDT")
            self.assertTrue(r["ready"])
            self.assertEqual(r["imbalance_pct"],-44)
        for item in (reader(count=3),reader(ready=False),reader(total=20)):
            with patch.object(stream,"get_spot_windows",return_value=item):
                self.assertFalse(stream.verified("TESTUSDT")["ready"])

    def test_subscription_is_stable_for_warmup(self):
        with patch.object(stream,"_thread",ThreadAlive()):
            with patch.object(stream,"_socket",None):
                with patch.object(stream,"_watched",()):
                    with patch.object(stream,"_lease_at",0.0):
                        with patch.object(stream.time,"time",return_value=10000.0):
                            self.assertEqual(stream.watch(
                                ["AAAUSDT","BBBUSDT","CCCUSDT"]),(
                                    "AAA-USDT","BBB-USDT","CCC-USDT"))
                        # Different ranking a minute later must NOT reset
                        # the previously selected 5-minute observations.
                        with patch.object(stream.time,"time",return_value=10060.0):
                            self.assertEqual(stream.watch(
                                ["DDDUSDT","AAAUSDT","BBBUSDT","CCCUSDT"]),(
                                    "AAA-USDT","BBB-USDT","CCC-USDT"))
                        with patch.object(stream.time,"time",return_value=11000.0):
                            self.assertEqual(stream.watch(
                                ["DDDUSDT","AAAUSDT","BBBUSDT","CCCUSDT"]),(
                                    "DDD-USDT","AAA-USDT","BBB-USDT"))


if __name__=="__main__":
    unittest.main()
