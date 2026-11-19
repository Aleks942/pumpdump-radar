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


def _compression_rows(post_drop=True):
    rows = []
    ts0 = 1_700_000_000_000
    price = 120.0

    # 18 older hours: mostly stable context.
    for i in range(18):
        close = price - i * 0.05
        rows.append([
            str(ts0 + i * 3_600_000),
            str(close + 0.15), str(close + 0.5),
            str(close - 0.5), str(close), "150", "0", "0", "1",
        ])

    # 12-hour impulse before compression: clear drop/rise of >3%.
    start = rows[-1][4]
    start = float(start)
    for j in range(12):
        step = (j + 1) * 0.65
        close = start - step if post_drop else start + step
        rows.append([
            str(ts0 + (18 + j) * 3_600_000),
            str(close + (0.18 if post_drop else -0.18)),
            str(close + 0.75), str(close - 0.75),
            str(close), "180", "0", "0", "1",
        ])

    # Last 6 closed hours: tight range, no directional claim.
    anchor = float(rows[-1][4])
    offsets = [0.05, -0.08, 0.03, -0.04, 0.02, 0.00]
    for k, off in enumerate(offsets):
        close = anchor + off
        rows.append([
            str(ts0 + (30 + k) * 3_600_000),
            str(close - 0.03), str(close + 0.20),
            str(close - 0.20), str(close), "120", "0", "0", "1",
        ])
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

    def test_hourly_compression_watch_is_direction_neutral(self):
        def fake_get(url, params=None, timeout=None):
            return _Response({"code": "0", "data": _compression_rows(post_drop=True)})

        ticker = {"instId": "EVAA-USDT-SWAP", "last": "110"}
        s.scan([ticker], max_refresh=1, top_n=3,
               now=1_800_000_000, request_get=fake_get)
        watches = s.compression_watches(
            [ticker], top_n=3, now=1_800_000_000
        )

        self.assertEqual(len(watches), 1)
        watch = watches[0]
        self.assertEqual(watch["symbol"], "EVAAUSDT")
        self.assertEqual(watch["context"], "POST_DROP")
        self.assertIsNone(watch["direction_hint"])
        self.assertLess(watch["prior_move_pct"], -3.0)
        self.assertLess(watch["width_ratio"], 0.68)
        self.assertLess(watch["candle_ratio"], 0.72)
        self.assertGreater(watch["range_high"], watch["range_low"])

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
