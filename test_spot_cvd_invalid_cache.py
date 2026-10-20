"""Offline regression for unavailable OKX spot instruments."""
import unittest
from unittest.mock import Mock, patch
import spot_cvd_engine as spot


class InvalidSpotCacheTests(unittest.TestCase):
    def test_invalid_symbol_is_cached_and_never_votes_sell(self):
        with spot._INVALID_LOCK:
            spot._INVALID_SPOT_UNTIL.clear()
        response = Mock(status_code=200)
        response.json.return_value = {"code": "51001", "data": [],
                                      "msg": "Instrument ID doesn't exist."}
        with patch.object(spot.requests, "get", return_value=response) as getter:
            first = spot.get_spot_cvd("INVALID-USDT-SWAP")
            second = spot.get_spot_cvd("INVALID-USDT-SWAP")
        self.assertEqual(getter.call_count, 1)
        self.assertFalse(first["available"])
        self.assertFalse(second["available"])
        self.assertEqual(second["state"], "SPOT_UNAVAILABLE")
        self.assertEqual(second["vote"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
