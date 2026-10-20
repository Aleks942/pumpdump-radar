"""Regression checks for independent OKX spot/swap stream health."""
import json
import unittest
from unittest.mock import Mock, patch
import okx_trade_stream_v3 as stream


class StreamHealthTests(unittest.TestCase):
    def test_swap_close_does_not_mark_spot_disconnected(self):
        with patch.object(stream, "mark_stream_disconnected") as mark:
            stream.on_close(Mock(), 1006, "lost connection")
            mark.assert_called_once_with("swap")

    def test_control_message_is_not_trade_activity(self):
        with patch.object(stream, "mark_stream_activity") as active:
            stream.on_message(Mock(), json.dumps({"event": "subscribe", "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}}))
            stream.on_message(Mock(), json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": []}))
            active.assert_not_called()

    def test_actual_swap_trade_counts_as_activity(self):
        payload = {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [{"side": "buy", "px": "1", "sz": "1", "ts": "1800000000000"}]}
        with patch.object(stream, "mark_stream_activity") as active:
            with patch.object(stream, "handle_trade") as trade:
                stream.on_message(Mock(), json.dumps(payload))
                active.assert_called_once_with("swap")
                trade.assert_called_once()


if __name__ == "__main__":
    unittest.main()
