import tempfile
import unittest
from pathlib import Path
from flow_bridge import enqueue_candidate, pending_candidates


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


if __name__ == "__main__":
    unittest.main()
