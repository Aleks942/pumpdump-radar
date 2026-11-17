import unittest

import swing_market_scanner as s


class _Response:
    def __init__(self, payload):
        self._payload = payload
    def raise_for_status(self):
        return None
    def json(self):
        return self._payload


def _rows(up=True):
    rows = []
    base = 100.0
    for i in range(36):
        step = i * 0.6
        close = base + step if up else base - step
        open_ = close - 0.2 if up else close + 0.2
        high = max(open_, close) + 0.4
        low = min(open_, close) - 0.4
        volume = 100 + i * 3
        # newest-first, as OKX returns
        rows.append([str(1_700_000_000_000 + i * 3_600_000),
                     str(open_), str(high), str(low), str(close),
                     str(volume), "0", "0", "1"])
    return list(reversed(rows))


class SwingMarketScannerTests(unittest.TestCase):
    def setUp(self):
        s.clear_cache()

    def test_prefilter_detects_hourly_long_without_five_minute_input(self):
        def fake_get(url, params=None, timeout=None):
            return _Response({"code": "0", "data": _rows(up=True)})
        ticker = {"instId": "AAA-USDT-SWAP", "last": "121"}
        out = s.scan([ticker], max_refresh=1, top_n=3,
                     now=1_800_000_000, request_get=fake_get)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["direction"], "LONG")
        self.assertGreater(out[0]["signed_score"], 0)

    def test_prefilter_detects_hourly_short(self):
        def fake_get(url, params=None, timeout=None):
            return _Response({"code": "0", "data": _rows(up=False)})
        ticker = {"instId": "BBB-USDT-SWAP", "last": "79"}
        out = s.scan([ticker], max_refresh=1, top_n=3,
                     now=1_800_000_000, request_get=fake_get)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["direction"], "SHORT")
        self.assertLess(out[0]["signed_score"], 0)

    def test_unclosed_hour_is_ignored(self):
        rows = _rows(up=True)
        rows[0][8] = "0"
        def fake_get(url, params=None, timeout=None):
            return _Response({"code": "0", "data": rows})
        ticker = {"instId": "AAA-USDT-SWAP", "last": "121"}
        out = s.scan([ticker], max_refresh=1, top_n=3,
                     now=1_800_000_000, request_get=fake_get)
        self.assertEqual(len(out), 1)


if __name__ == "__main__":
    unittest.main()
