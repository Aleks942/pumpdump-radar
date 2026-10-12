import tempfile
import unittest
from unittest.mock import patch, Mock
from pathlib import Path
from flow_bridge import enqueue_candidate, pending_candidates, probe_bridge


class BridgeTests(unittest.TestCase):
    def test_candidate_expiry_and_direction(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "bridge.db")
            signal = {"symbol": "XRPUSDT", "price": 1.5,
                      "decision": {"direction": "UP", "pattern": "NEW_LONG_BUILDUP"}}
            self.assertTrue(enqueue_candidate(signal, now=1000, db_path=db))
            rows = pending_candidates(now=1001, db_path=db)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["side"], "LONG")
            self.assertEqual(pending_candidates(now=1121, db_path=db), [])

    def test_reject_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "bridge.db")
            self.assertFalse(enqueue_candidate(
                {"symbol": "BTCUSDT", "price": 1, "decision": {"direction": "NONE"}},
                db_path=db))


    def test_probe_is_read_only_and_authenticated(self):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"status": "NOT_FOUND"}
        env = {
            "PUMP_FLOW_MODE": "RESEARCH",
            "PUMP_FLOW_URL": "https://example.org/api/pump-candidate",
            "PUMP_FLOW_SHARED_TOKEN": "test-token",
        }
        with patch.dict("os.environ", env, clear=True), patch(
            "requests.get", return_value=response
        ) as get:
            self.assertTrue(probe_bridge())
        get.assert_called_once()
        args, kwargs = get.call_args
        self.assertEqual(args[0], "https://example.org/api/pump-result")
        self.assertEqual(kwargs["params"]["observed_at"], "0")
        self.assertEqual(kwargs["headers"]["X-Pump-Flow-Token"], "test-token")

    def test_probe_never_sends_without_research_mode(self):
        with patch.dict("os.environ", {"PUMP_FLOW_MODE": "OFF"}, clear=True), patch(
            "requests.get"
        ) as get:
            self.assertFalse(probe_bridge())
            get.assert_not_called()

    def test_probe_rejects_unauthorized_response(self):
        response = Mock()
        response.status_code = 401
        env = {
            "PUMP_FLOW_MODE": "RESEARCH",
            "PUMP_FLOW_URL": "https://example.org/api/pump-candidate",
            "PUMP_FLOW_SHARED_TOKEN": "test-token",
        }
        with patch.dict("os.environ", env, clear=True), patch(
            "requests.get", return_value=response
        ):
            self.assertFalse(probe_bridge())


if __name__ == "__main__":
    unittest.main()
