"""Offline regressions for cross-exchange Spot CVD fallback."""
import unittest
from unittest.mock import Mock, patch

import spot_cvd_engine as spot


def response(status, payload):
    item = Mock(status_code=status)
    item.json.return_value = payload
    return item


class SpotFallbackTests(unittest.TestCase):
    def setUp(self):
        with spot._INVALID_LOCK:
            spot._INVALID_SPOT_UNTIL.clear()

    def test_all_unsupported_are_cached_and_never_vote(self):
        replies = [
            response(400, {"code": -1121, "msg": "Invalid symbol."}),
            response(200, {"retCode": 10029, "retMsg": "symbol invalid"}),
            response(200, {"code": "51001", "data": [],
                           "msg": "Instrument ID doesn't exist."}),
        ]
        with patch.object(spot.requests, "get", side_effect=replies) as getter:
            first = spot.get_spot_cvd("INVALID-USDT-SWAP")
            second = spot.get_spot_cvd("INVALID-USDT-SWAP")

        self.assertEqual(getter.call_count, 3)
        self.assertFalse(first["available"])
        self.assertFalse(second["available"])
        self.assertEqual(second["state"], "SPOT_UNAVAILABLE")
        self.assertEqual(second["vote"], "UNKNOWN")
        self.assertIsNone(second["source"])

    def test_bybit_is_used_when_binance_pair_is_missing(self):
        replies = [
            response(400, {"code": -1121, "msg": "Invalid symbol."}),
            response(200, {
                "retCode": 0,
                "result": {
                    "list": [
                        {"side": "Buy", "price": "2", "size": "10"},
                        {"side": "Sell", "price": "2", "size": "5"},
                    ]
                },
            }),
        ]
        with patch.object(spot.requests, "get", side_effect=replies) as getter:
            result = spot.get_spot_cvd("TEST-USDT-SWAP")

        self.assertEqual(getter.call_count, 2)
        self.assertTrue(result["available"])
        self.assertEqual(result["source"], "Bybit")
        self.assertAlmostEqual(result["cvd_percent"], 100.0 / 3.0, places=6)

    def test_binance_is_preferred_when_available(self):
        replies = [
            response(200, [
                {"price": "1", "qty": "20", "isBuyerMaker": False},
                {"price": "1", "qty": "10", "isBuyerMaker": True},
            ])
        ]
        with patch.object(spot.requests, "get", side_effect=replies) as getter:
            result = spot.get_spot_cvd("TEST-USDT-SWAP")

        self.assertEqual(getter.call_count, 1)
        self.assertTrue(result["available"])
        self.assertEqual(result["source"], "Binance")
        self.assertAlmostEqual(result["cvd_percent"], 100.0 / 3.0, places=6)


if __name__ == "__main__":
    unittest.main()
