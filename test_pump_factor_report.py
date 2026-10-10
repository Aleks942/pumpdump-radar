"""Regression tests for read-only PumpDump pre-signal selection factors."""
import unittest

from pump_factor_report import extract_features, _metric


def snapshot(spot=30, futures=35, oi=0.6, agg=0.7, move=2.4):
    return {
        "change": move,
        "oi_change": oi,
        "futures_flow": {"window_ready": True, "imbalance_pct": futures},
        "spot_cvd": {"available": True, "cvd_percent": spot},
        "aggregated_oi": {"windows": {"5m": {"ready": True, "change_pct": agg}}},
        "liquidations": {
            "available": True, "long_liq": 100, "short_liq": 800,
        },
        "anti_late": {"allow": True, "retrace_share": 5},
    }


class EntryFactorTests(unittest.TestCase):
    def test_directional_factors_and_oi(self):
        s = snapshot()
        f = extract_features(s, "NEW_LONG_BUILDUP", "LONG")
        for name in (
            "SPOT_CVD_GE20", "FUTURES_IMBALANCE_GE20",
            "BOTH_CVD_GE20", "LOCAL_OI_ABS_GE05",
            "AGGREGATED_OI_ABS_GE05", "MOVE_15_TO_30",
            "LOW_RETRACE_10PCT",
        ):
            self.assertIs(f[name], True, name)
        self.assertIsNone(f["LIQUIDATION_DOMINANCE_3X"])

    def test_direction_changes_flow_alignment(self):
        f = extract_features(snapshot(), "NEW_SHORT_BUILDUP", "SHORT")
        self.assertFalse(f["SPOT_CVD_GE20"])
        self.assertFalse(f["FUTURES_IMBALANCE_GE20"])

    def test_squeeze_liquidation_dominance(self):
        f = extract_features(
            snapshot(), "SHORT_SQUEEZE", "LONG"
        )
        self.assertTrue(f["LIQUIDATION_DOMINANCE_3X"])

    def test_unknown_data_is_not_a_zero_signal(self):
        s = snapshot()
        s["futures_flow"]["window_ready"] = False
        s["spot_cvd"]["available"] = False
        s.pop("anti_late")
        f = extract_features(s, "NEW_LONG_BUILDUP", "LONG")
        self.assertIsNone(f["SPOT_CVD_GE20"])
        self.assertIsNone(f["FUTURES_IMBALANCE_GE20"])
        self.assertIsNone(f["BOTH_CVD_GE20"])
        self.assertIsNone(f["LOW_RETRACE_10PCT"])

    def test_candle_audit_uses_first_bar_and_fixed_cost(self):
        bars = [
            (i * 60000, 101.5, 99.8, 101.0)
            for i in range(30)
        ]
        case = (
            86400.0, "BTCUSDT", "NEW_LONG_BUILDUP", "LONG",
            100.0, 0, snapshot(), 100.0, bars,
        )
        report = _metric([case], "BOTH_CVD_GE20",
                         1.0, 1.0, 10, 0.22, "ALERT_PRICE")
        self.assertEqual(report["n"], 1)
        self.assertAlmostEqual(report["mean"], 0.78, places=6)
        self.assertEqual(report["days"], 1)


if __name__ == "__main__":
    unittest.main()
