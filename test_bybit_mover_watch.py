"""Bybit movement watch: chrono quotes, provenance and no fabricated alerts."""
import unittest
from unittest.mock import patch
import bybit_mover_watch as w
from mover_discovery import select_threshold_moves


class BybitMoverTests(unittest.TestCase):
    def setUp(self):
        w._HISTORY.clear()

    def test_real_five_minute_move_and_isolated_history(self):
        self.assertIsNone(w.sample([{"symbol": "PUFFERUSDT", "lastPrice": "1"}], 1000)[0]["change5"])
        rows = w.sample([{"symbol": "PUFFERUSDT", "lastPrice": "1.8"}], 1300)
        self.assertAlmostEqual(rows[0]["change5"], 80)
        self.assertEqual(rows[0]["exchange"], "Bybit")
        self.assertEqual(select_threshold_moves(rows, 1.5, 1.5)[0]["symbol"], "PUFFERUSDT")

    def test_invalid_and_missing_history_never_alert(self):
        rows = w.sample([{"symbol": "PUFFERUSDT", "lastPrice": "NaN"},
                         {"symbol": "BTCUSDT", "lastPrice": "0"},
                         {"symbol": "ETHUSDT", "lastPrice": "100"}], 1000)
        self.assertEqual([r["symbol"] for r in rows], ["ETHUSDT"])
        self.assertEqual(select_threshold_moves(rows, 1.5, 1.5), [])

    def test_fetch_validation(self):
        class Response:
            status_code = 200
            def json(self):
                return {"retCode": 0, "result": {"list": [
                    {"symbol": "PUFFERUSDT", "lastPrice": "0.05"}]}}
        assert w.fetch(lambda *args, **kwargs: Response())[0]["symbol"] == "PUFFERUSDT"


if __name__ == "__main__":
    unittest.main()
