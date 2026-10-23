import unittest

from post_pump_volume import analyze_volume_windows


def w(total, imbalance, trades=20):
    return {
        "window_ready": True,
        "quality": "READY",
        "total_quote": total,
        "imbalance_pct": imbalance,
        "trade_count": trades,
    }


class VolumeAnalysisTests(unittest.TestCase):
    def test_sell_expansion_with_spot_confirmation(self):
        result = analyze_volume_windows(
            {"1m": w(4000, -35), "5m": w(8000, -20)},
            {"5m": w(5000, -18)},
        )
        self.assertTrue(result["ready"])
        self.assertEqual(result["state"], "SELL_VOLUME_EXPANSION")
        self.assertTrue(result["volume_expansion"])
        self.assertTrue(result["seller_shift"])

    def test_volume_fade(self):
        result = analyze_volume_windows(
            {"1m": w(1000, -2), "5m": w(9000, -1)},
            {"5m": w(3000, 0)},
        )
        self.assertEqual(result["state"], "VOLUME_FADE")
        self.assertTrue(result["volume_fade"])

    def test_fails_closed_when_windows_not_ready(self):
        result = analyze_volume_windows(
            {"1m": {"window_ready": False}, "5m": w(5000, -10)},
            {},
        )
        self.assertFalse(result["ready"])
        self.assertEqual(result["state"], "INSUFFICIENT_VOLUME_DATA")


if __name__ == "__main__":
    unittest.main()
