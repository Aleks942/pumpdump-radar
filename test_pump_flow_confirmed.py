"""Regression checks: never notify for missing, rejected or stale Flow results."""
import time
import unittest
from unittest.mock import patch
from pump_flow_confirmed import send_confirmed, dispatch, _slots, _inflight, _inflight_lock


class FakeResponse:
    def __init__(self, code=202, body=None):
        self.status_code = code
        self.body = body or {}
    def json(self):
        return self.body


class ConfirmedDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.signal = {"symbol": "XRPUSDT", "price": 1.0,
                       "decision": {"direction": "UP"}}
        self.sent = []
        self.send = lambda text, symbol: self.sent.append((text, symbol))

    def test_missing_config_never_sends(self):
        with patch.dict("os.environ", {}, clear=True):
            _slots.acquire()
            send_confirmed(self.signal, self.send, str)
        self.assertEqual(self.sent, [])

    def test_rejected_never_sends(self):
        env = {"PUMP_FLOW_URL": "https://example.org/api/pump-candidate",
               "PUMP_FLOW_SHARED_TOKEN": "test"}
        with patch.dict("os.environ", env), patch("requests.post", return_value=FakeResponse(401)):
            _slots.acquire()
            send_confirmed(self.signal, self.send, str)
        self.assertEqual(self.sent, [])

    def test_confirmed_without_timestamp_never_sends(self):
        env = {"PUMP_FLOW_URL": "https://example.org/api/pump-candidate",
               "PUMP_FLOW_SHARED_TOKEN": "test"}
        with patch.dict("os.environ", env), patch("requests.post", return_value=FakeResponse()), \
             patch("requests.get", return_value=FakeResponse(200, {"status": "CONFIRMED"})), \
             patch("time.sleep", return_value=None):
            _slots.acquire()
            send_confirmed(self.signal, self.send, str)
        self.assertEqual(self.sent, [])


    def test_confirmed_registers_only_after_successful_telegram(self):
        env = {"PUMP_FLOW_URL": "https://example.org/api/pump-candidate",
               "PUMP_FLOW_SHARED_TOKEN": "test"}
        recorded = []
        def send_ok(text, symbol):
            self.sent.append((text, symbol))
            return True
        with patch.dict("os.environ", env), \
             patch("requests.post", return_value=FakeResponse()), \
             patch("requests.get", return_value=FakeResponse(
                 200, {"status": "CONFIRMED", "finished_at": 1000.0})), \
             patch("time.time", return_value=1000.0), \
             patch("time.sleep", return_value=None):
            _slots.acquire()
            send_confirmed(self.signal, send_ok, str,
                           on_sent=lambda signal: recorded.append(signal["symbol"]))
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(recorded, ["XRPUSDT"])

    def test_failed_telegram_does_not_register(self):
        env = {"PUMP_FLOW_URL": "https://example.org/api/pump-candidate",
               "PUMP_FLOW_SHARED_TOKEN": "test"}
        recorded = []
        with patch.dict("os.environ", env), \
             patch("requests.post", return_value=FakeResponse()), \
             patch("requests.get", return_value=FakeResponse(
                 200, {"status": "CONFIRMED", "finished_at": 1000.0})), \
             patch("time.time", return_value=1000.0), \
             patch("time.sleep", return_value=None):
            _slots.acquire()
            send_confirmed(self.signal, lambda text, symbol: False, str,
                           on_sent=lambda signal: recorded.append(signal["symbol"]))
        self.assertEqual(recorded, [])
        self.assertEqual(self.sent, [])


    def test_same_symbol_is_not_dispatched_twice(self):
        with patch("pump_flow_confirmed.threading.Thread") as mock_thread:
            try:
                self.assertTrue(dispatch(self.signal, lambda *a, **k: True, str))
                self.assertFalse(dispatch(self.signal, lambda *a, **k: True, str))
                mock_thread.assert_called_once()
            finally:
                with _inflight_lock:
                    _inflight.discard("XRPUSDT")
                _slots.release()


if __name__ == "__main__":
    unittest.main()
