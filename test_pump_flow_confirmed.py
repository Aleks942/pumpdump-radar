"""Regression checks: never notify for missing, rejected or stale Flow results."""
import time
import unittest
from unittest.mock import patch
from pump_flow_confirmed import send_confirmed, _slots


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


if __name__ == "__main__":
    unittest.main()
