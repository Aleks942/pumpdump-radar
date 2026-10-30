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

    def test_websocket_heartbeat_keeps_transport_continuity_without_fake_trade(self):
        ws=object()
        with patch.object(stream,"_socket",ws), \
             patch.object(stream,"_socket_ready",True), \
             patch.object(stream,"mark_stream_activity") as activity:
            stream._heartbeat(ws,b"pong")
            activity.assert_called_once_with("spot")
        other=object()
        with patch.object(stream,"_socket",ws), \
             patch.object(stream,"_socket_ready",True), \
             patch.object(stream,"mark_stream_activity") as activity:
            stream._heartbeat(other,b"pong")
            activity.assert_not_called()

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

    def test_snapshot_fallback_accepts_quiet_complete_5m_window(self):
        item={"5m":{"window_ready":True,"stream_healthy":True,
                     "quality":"READY","stream_stale_seconds":2,
                     "last_trade_age_sec":180,"trade_count":22,
                     "total_quote":4200,"imbalance_pct":-31.5,
                     "delta_quote":-1323}}
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=({"TEST-USDT"},"READY")):
            with patch.object(stream,"get_spot_windows",return_value=item):
                self.assertFalse(stream.verified("TESTUSDT")["ready"])
                fallback=stream.snapshot_fallback("TESTUSDT")
        self.assertTrue(fallback["ready"])
        self.assertEqual(fallback["reason"],"SNAPSHOT_FALLBACK")
        self.assertEqual(fallback["last_trade_age_sec"],180)

    def test_snapshot_fallback_rejects_data_older_than_5m(self):
        item={"5m":{"window_ready":True,"stream_healthy":True,
                     "quality":"READY","stream_stale_seconds":2,
                     "last_trade_age_sec":301,"trade_count":22,
                     "total_quote":4200,"imbalance_pct":-31.5,
                     "delta_quote":-1323}}
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=({"TEST-USDT"},"READY")):
            with patch.object(stream,"get_spot_windows",return_value=item):
                fallback=stream.snapshot_fallback("TESTUSDT")
        self.assertFalse(fallback["ready"])

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
                                    "DDD-USDT","AAA-USDT","BBB-USDT","CCC-USDT"))

    def test_urgent_candidate_is_added_without_dropping_warm_slots_when_capacity_exists(self):
        universe={"AAA-USDT","BBB-USDT","CCC-USDT","SOON-USDT"}
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=(universe,"READY")):
          with patch.object(stream,"_thread",ThreadAlive()):
            with patch.object(stream,"_socket",None):
              with patch.object(stream,"_watched",(
                      "AAA-USDT","BBB-USDT","CCC-USDT")):
                with patch.object(stream,"_lease_at",10000.0):
                  with patch.object(stream.time,"time",return_value=10100.0):
                    selected=stream.watch(
                        ["SOONUSDT","AAAUSDT","BBBUSDT","CCCUSDT"],
                        urgent_symbols=["SOONUSDT"],
                    )
        self.assertEqual(selected,("SOON-USDT","AAA-USDT","BBB-USDT","CCC-USDT"))

    def test_spot_watch_is_bounded_to_ten_symbols(self):
        pairs=[f"S{i:02d}USDT" for i in range(12)]
        universe={p[:-4]+"-USDT" for p in pairs}
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=(universe,"READY")):
          with patch.object(stream,"_thread",ThreadAlive()):
            with patch.object(stream,"_socket",None):
              with patch.object(stream,"_watched",()):
                with patch.object(stream,"_lease_at",0.0):
                  selected=stream.watch(pairs)
        self.assertEqual(len(selected),10)
        self.assertEqual(selected[0],"S00-USDT")
        self.assertEqual(selected[-1],"S09-USDT")

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


    def test_reconnect_delay_backs_off_and_is_bounded(self):
        self.assertEqual([stream._reconnect_delay(n) for n in range(1,7)],
                         [5,10,20,40,60,60])
        self.assertEqual(stream._reconnect_delay(0),5)
        self.assertEqual(stream._reconnect_delay(100),60)

    def test_spot_gap_reason_separates_data_gap_from_low_liquidity(self):
        base={"window_ready":True,"stream_healthy":True,
              "quality":"READY","stream_stale_seconds":1,
              "last_trade_age_sec":1,"trade_count":30,
              "total_quote":2500,"imbalance_pct":-20}
        self.assertEqual(stream._spot_gap_reason({**base,
                         "stream_healthy":False}),
                         "STREAM_DISCONNECTED_OR_STALE")
        self.assertEqual(stream._spot_gap_reason({**base,
                         "window_ready":False,"quality":"WARMING"}),
                         "WARMING_AFTER_CONNECTION_OR_SUBSCRIPTION")
        self.assertEqual(stream._spot_gap_reason({**base,
                         "last_trade_age_sec":80}),
                         "SYMBOL_TRADES_MISSING_OR_STALE")
        self.assertEqual(stream._spot_gap_reason({**base,
                         "trade_count":2}),"TOO_FEW_REAL_TRADES")
        self.assertEqual(stream._spot_gap_reason({**base,
                         "total_quote":200}),"TOO_LITTLE_TRADED_NOTIONAL")
        self.assertEqual(stream._spot_gap_reason({**base,
                         "imbalance_pct":None}),"BAD_IMBALANCE_DATA")

    def test_unready_spot_diagnostic_does_not_approve_short(self):
        item={"5m":{"window_ready":False,"stream_healthy":True,
                     "quality":"WARMING","stream_stale_seconds":1,
                     "last_trade_age_sec":1,"trade_count":30,
                     "total_quote":2500,"imbalance_pct":-50}}
        with patch.object(stream,"_refresh_spot_universe",
                          return_value=({"TEST-USDT"},"READY")):
            with patch.object(stream,"get_spot_windows",return_value=item):
                with patch.object(stream,"_log_spot_gap") as reporter:
                    result=stream.verified("TESTUSDT")
        self.assertFalse(result["ready"])
        self.assertEqual(result["reason"],"SPOT_WS_NOT_READY")
        self.assertEqual(result["diagnostic"],
                         "WARMING_AFTER_CONNECTION_OR_SUBSCRIPTION")
        reporter.assert_called_once_with("TEST-USDT",result["diagnostic"])


if __name__=="__main__":
    unittest.main()
