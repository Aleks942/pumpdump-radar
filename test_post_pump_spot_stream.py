"""Network-free tests for dedicated bounded POST-PUMP spot websocket."""
import json
import unittest
from unittest.mock import patch, Mock

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

    def test_rotation_keeps_retained_window_and_warms_new_symbol(self):
        import trade_flow_collector_v3 as flow
        ws=Mock()
        with patch.dict(flow.SYMBOL_STARTED, {"spot":{}}, clear=False), \
             patch.dict(flow.TRADE_HISTORY, {"spot":flow.defaultdict(flow.deque)}, clear=False), \
             patch.dict(flow.STREAM_STATE, {"spot":dict(flow.STREAM_STATE["spot"])}, clear=False), \
             patch.object(stream,"_subscribed",set()), \
             patch.object(stream,"_watched",("AAA-USDT","BBB-USDT")):
            with patch.object(flow.time,"time",return_value=1000):
                flow.mark_stream_connected("spot")
                stream._sync_subscriptions(ws)
                flow.save_trade("AAAUSDT","spot","SELL",10,100,event_ts=1000)
                flow.save_trade("BBBUSDT","spot","SELL",10,100,event_ts=1000)
            for at in range(1020,1321,20):
                with patch.object(flow.time,"time",return_value=at):
                    flow.save_trade("AAAUSDT","spot","SELL",10,100,event_ts=at)
            with patch.object(flow.time,"time",return_value=1320):
                before=flow.get_flow("AAAUSDT","spot",300)
                self.assertTrue(before["window_ready"])
                stream._watched=("AAA-USDT","CCC-USDT")
                ws.reset_mock()
                stream._sync_subscriptions(ws)
                ws.close.assert_not_called()
                self.assertEqual([json.loads(c.args[0])["op"] for c in ws.send.call_args_list],
                                 ["unsubscribe","subscribe"])
                after=flow.get_flow("AAAUSDT","spot",300)
                self.assertEqual(before,after)
                self.assertEqual(flow.get_history_size("BBBUSDT","spot"),0)
                flow.save_trade("CCCUSDT","spot","SELL",10,100,event_ts=1320)
                self.assertFalse(flow.get_flow("CCCUSDT","spot",300)["window_ready"])
                stream._watched=("CCC-USDT","AAA-USDT")
                ws.reset_mock()
                stream._sync_subscriptions(ws)
                ws.send.assert_not_called()
                flow.mark_stream_disconnected("spot")
                self.assertFalse(flow.get_flow("AAAUSDT","spot",300)["window_ready"])
                self.assertEqual(flow.get_history_size("AAAUSDT","spot"),0)
            with patch.object(flow.time,"time",return_value=1321):
                flow.mark_stream_connected("spot")
                flow.save_trade("AAAUSDT","spot","SELL",10,100,event_ts=1321)
                self.assertFalse(flow.get_flow("AAAUSDT","spot",300)["window_ready"])

    def test_5m_must_be_ready_with_notional_and_trade_count(self):
        def reader(count=30,ready=True,total=2500):
            return {"5m":{"window_ready":ready,"stream_healthy":True,
                           "quality":"READY" if ready else "WARMING",
                           "stream_stale_seconds":4,
                           "last_trade_age_sec":3,
                           "trade_count":count,"total_quote":total,
                           "imbalance_pct":-44.0,
                           "delta_quote":-1110}}
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=({"TEST-USDT"},"READY")):
            with patch.object(stream,"get_spot_windows",return_value=reader()):
                r=stream.verified("TESTUSDT")
                self.assertTrue(r["ready"])
                self.assertEqual(r["imbalance_pct"],-44)
            for item in (reader(count=3),reader(ready=False),reader(total=20),
                         {"5m":{**reader()["5m"],
                                "last_trade_age_sec":200}},
                         {"5m":{**reader()["5m"],
                                "last_trade_age_sec":None}}):
                with patch.object(stream,"get_spot_windows",return_value=item):
                    self.assertFalse(stream.verified("TESTUSDT")["ready"])

    def test_subscription_is_stable_for_warmup(self):
        universe={"AAA-USDT","BBB-USDT","CCC-USDT","DDD-USDT"}
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=(universe,"READY")):
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

    def test_ranked_out_pair_keeps_subscription_during_lease(self):
        universe={"AAA-USDT","BBB-USDT","CCC-USDT","DDD-USDT"}
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=(universe,"READY")):
          with patch.object(stream,"_thread",ThreadAlive()):
            with patch.object(stream,"_socket",None):
              with patch.object(stream,"_watched",("AAA-USDT","BBB-USDT","CCC-USDT")):
                with patch.object(stream,"_lease_at",10000.0):
                  with patch.object(stream.time,"time",return_value=10100.0):
                    self.assertEqual(stream.watch(["DDDUSDT"]),(
                        "AAA-USDT","BBB-USDT","CCC-USDT"))
                  with patch.object(stream.time,"time",return_value=11000.0):
                    self.assertEqual(stream.watch(["DDDUSDT"]),("DDD-USDT",))

    def test_instrument_outage_retains_verified_subscriptions(self):
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=(set(),"SPOT_DATA_ERROR")):
          with patch.object(stream,"_thread",ThreadAlive()):
            with patch.object(stream,"_socket",None):
              with patch.object(stream,"_watched",("AAA-USDT",)):
                with patch.object(stream,"_lease_at",10000.0):
                  with patch.object(stream.time,"time",return_value=10050.0):
                    self.assertEqual(stream.watch(["BBBUSDT"]),("AAA-USDT",))

    def test_missing_spot_contract_fails_closed_before_subscription(self):
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=({"AAA-USDT"},"READY")):
            with patch.object(stream,"_thread",ThreadAlive()):
                with patch.object(stream,"_socket",None):
                    with patch.object(stream,"_watched",()):
                        with patch.object(stream,"_lease_at",0.0):
                            selected=stream.watch(["AAAUSDT","APRUSDT"])
                            self.assertEqual(selected,("AAA-USDT",))
            x=stream.spot_availability("APR-USDT")
            self.assertEqual(x["reason"],"SPOT_UNAVAILABLE")

    def test_universe_transport_failure_is_not_spot_unavailable(self):
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=(set(),"SPOT_DATA_ERROR")):
            self.assertEqual(stream.spot_availability("AAA-USDT")["reason"],
                             "SPOT_DATA_ERROR")


if __name__=="__main__":
    unittest.main()
