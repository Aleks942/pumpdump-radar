"""Regression checks for preloading contract multipliers before trade stream."""
import unittest
from unittest.mock import patch

import okx_trade_stream_v3 as stream


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class ContractPrefetchTests(unittest.TestCase):
    def setUp(self):
        with stream.CACHE_LOCK:
            self.old_cache = dict(stream.CONTRACT_CACHE)
            stream.CONTRACT_CACHE.clear()

    def tearDown(self):
        with stream.CACHE_LOCK:
            stream.CONTRACT_CACHE.clear()
            stream.CONTRACT_CACHE.update(self.old_cache)

    @patch.object(stream.requests, "get")
    def test_one_request_populates_contracts_and_avoids_per_trade_rest(self, get):
        get.return_value = FakeResponse({
            "code": "0",
            "data": [
                {"instId": "BTC-USDT-SWAP", "ctVal": "0.01",
                 "ctValCcy": "BTC", "settleCcy": "USDT", "ctType": "linear"},
                {"instId": "ETH-USDT-SWAP", "ctVal": "0.1",
                 "ctValCcy": "ETH", "settleCcy": "USDT", "ctType": "linear"},
                {"instId": "BTC-USD-SWAP", "ctVal": "100"},
                {"instId": "BAD-USDT-SWAP", "ctVal": "0"},
            ],
        })
        self.assertEqual(stream.prefetch_swap_contracts(), 2)
        self.assertEqual(
            stream.calc_swap_quote_value("BTC-USDT-SWAP", 100000, 2),
            2000.0,
        )
        self.assertEqual(
            stream.calc_swap_quote_value("ETH-USDT-SWAP", 2500, 3),
            750.0,
        )
        self.assertEqual(get.call_count, 1)

    @patch.object(stream.requests, "get")
    def test_failed_refresh_preserves_cached_valid_contracts(self, get):
        with stream.CACHE_LOCK:
            stream.CONTRACT_CACHE["BTC-USDT-SWAP"] = {
                "ct_val": 0.01, "ct_val_ccy": "BTC",
                "settle_ccy": "USDT", "ct_type": "linear",
            }
        get.side_effect = OSError("network")
        self.assertEqual(stream.prefetch_swap_contracts(), 0)
        self.assertEqual(
            stream.calc_swap_quote_value("BTC-USDT-SWAP", 100000, 2),
            2000.0,
        )
        self.assertEqual(get.call_count, 1)


if __name__ == "__main__":
    unittest.main()
