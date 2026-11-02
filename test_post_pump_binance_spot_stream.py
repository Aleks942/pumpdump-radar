"""Tests for independent Binance post-pump Spot fallback."""
import json
import unittest
from collections import deque
from unittest.mock import patch, Mock

import post_pump_binance_spot_stream as stream


class ThreadAlive:
    def is_alive(self):
        return True


class BinancePostPumpSpotTests(unittest.TestCase):
    def test_ready_requires_warm_fresh_real_window(self):
        now=10000.0
        rows=deque(
            (now-250+i*(249/29), -100.0 if i<20 else 50.0)
            for i in range(30)
        )
        with patch.object(stream.time,"time",return_value=now),              patch.object(stream,"_watched",("TESTUSDT",)),              patch.object(stream,"_socket_ready",True),              patch.dict(stream._symbol_started,{"TESTUSDT":now-300},clear=True),              patch.dict(stream._events,{"TESTUSDT":rows},clear=True):
            result=stream.verified("TESTUSDT")
        self.assertTrue(result["ready"])
        self.assertEqual(result["source"],"BINANCE_SPOT_WS")
        self.assertLess(result["imbalance_pct"],-10)

    def test_stale_or_thin_window_fails_closed(self):
        now=10000.0
        stale=deque((now-250+i*5,-100.0) for i in range(20))
        with patch.object(stream.time,"time",return_value=now),              patch.object(stream,"_watched",("TESTUSDT",)),              patch.object(stream,"_socket_ready",True),              patch.dict(stream._symbol_started,{"TESTUSDT":now-300},clear=True),              patch.dict(stream._events,{"TESTUSDT":stale},clear=True):
            self.assertFalse(stream.verified("TESTUSDT")["ready"])

    def test_urgent_symbol_preempts_when_pool_full(self):
        pairs=tuple(f"S{i:02d}USDT" for i in range(10))
        universe=set(pairs)|{"HOTUSDT"}
        with patch.object(stream,"_refresh_universe",
                          return_value=(universe,"READY")),              patch.object(stream,"_thread",ThreadAlive()),              patch.object(stream,"_socket",None),              patch.object(stream,"_watched",pairs),              patch.object(stream,"_lease_at",10000.0),              patch.object(stream.time,"time",return_value=10100.0):
            selected=stream.watch(
                ("HOTUSDT",)+pairs,
                urgent_symbols=("HOTUSDT",),
            )
        self.assertEqual(len(selected),10)
        self.assertEqual(selected[0],"HOTUSDT")
        self.assertNotIn("S09USDT",selected)

    def test_binance_trade_message_side_and_quote(self):
        now=1800000000.0
        with patch.object(stream.time,"time",return_value=now),              patch.object(stream,"_watched",("TESTUSDT",)),              patch.dict(stream._events,{"TESTUSDT":deque()},clear=True):
            stream._message(json.dumps({
                "e":"trade","s":"TESTUSDT","T":int(now*1000),
                "p":"2.0","q":"10","m":True,
            }))
            self.assertEqual(len(stream._events["TESTUSDT"]),1)
            self.assertEqual(stream._events["TESTUSDT"][0][1],-20.0)


if __name__=="__main__":
    unittest.main()
