import unittest

from move_reversal_maturity import (
    evaluate,
    reversal_from_extreme_pct,
    update_extreme,
)


def flow(value, ready=True, quality="READY", coverage=300, trades=40, stale=2):
    return {
        "ready": ready,
        "quality": quality,
        "imbalance_pct": value,
        "coverage_sec": coverage,
        "trade_count": trades,
        "last_trade_age_sec": stale,
    }


class ReversalMaturityTests(unittest.TestCase):
    def test_two_of_five_becomes_early_not_pre(self):
        result = {
            "type": "PUMP",
            "reversal_from_extreme_pct": -0.8,
            "futures": flow(-15, ready=False, quality="PARTIAL", coverage=120),
            "spot": flow(3),
            "agg_5m": {"ready": False, "change_pct": None},
            "trend_5m": {"trend": "MIXED"},
        }
        out = evaluate(result)
        self.assertEqual(out["code"], "EARLY")
        self.assertEqual(out["side"], "SHORT")
        self.assertEqual(out["early_score"], 2)
        self.assertTrue(out["continue_watch"])

    def test_pump_can_become_pre_short_from_early_evidence(self):
        result = {
            "type": "PUMP",
            "reversal_from_extreme_pct": -0.9,
            "futures": flow(-18, ready=False, quality="PARTIAL", coverage=120),
            "spot": flow(-8, ready=False, quality="PARTIAL", coverage=120),
            "agg_5m": {"ready": False, "change_pct": None},
            "trend_5m": {"trend": "MIXED"},
        }
        out = evaluate(result)
        self.assertEqual(out["code"], "PRE")
        self.assertEqual(out["side"], "SHORT")
        self.assertGreaterEqual(out["early_score"], 3)
        self.assertTrue(out["continue_watch"])

    def test_pump_short_requires_strict_futures_and_price_break(self):
        result = {
            "type": "PUMP",
            "reversal_from_extreme_pct": -2.2,
            "futures": flow(-22),
            "spot": flow(-9),
            "agg_5m": {"ready": True, "change_pct": 0.7},
            "trend_5m": {"trend": "DOWN"},
        }
        out = evaluate(result)
        self.assertEqual(out["code"], "CONFIRMED")
        self.assertEqual(out["side"], "SHORT")
        self.assertGreaterEqual(out["strong_score"], 4)

    def test_dump_is_mirrored_into_long_reversal(self):
        result = {
            "type": "DUMP",
            "reversal_from_extreme_pct": 2.0,
            "futures": flow(19),
            "spot": flow(7),
            "agg_5m": {"ready": True, "change_pct": -0.4},
            "trend_5m": {"trend": "UP"},
        }
        out = evaluate(result)
        self.assertEqual(out["code"], "CONFIRMED")
        self.assertEqual(out["side"], "LONG")

    def test_no_maturity_does_not_force_15_minute_watch(self):
        result = {
            "type": "PUMP",
            "reversal_from_extreme_pct": -0.1,
            "futures": flow(20),
            "spot": flow(10),
            "agg_5m": {"ready": True, "change_pct": 0.5},
            "trend_5m": {"trend": "UP"},
        }
        out = evaluate(result)
        self.assertEqual(out["code"], "BROKEN")
        self.assertFalse(out["continue_watch"])

    def test_missing_data_fails_closed(self):
        out = evaluate({
            "type": "PUMP",
            "reversal_from_extreme_pct": -3.0,
            "futures": {"ready": False, "quality": "UNAVAILABLE"},
            "spot": {"ready": False, "quality": "UNAVAILABLE"},
            "agg_5m": {"ready": False},
            "trend_5m": {"trend": "DOWN"},
        })
        self.assertNotEqual(out["code"], "CONFIRMED")

    def test_extreme_tracking_cancels_only_after_material_reclaim(self):
        extreme, reclaimed = update_extreme("PUMP", 101.0, 100.0, pre_active=False)
        self.assertEqual(extreme, 101.0)
        self.assertFalse(reclaimed)

        extreme2, reclaimed2 = update_extreme("PUMP", 101.10, 101.0, pre_active=True)
        self.assertFalse(reclaimed2)
        self.assertEqual(extreme2, 101.0)

        extreme3, reclaimed3 = update_extreme("PUMP", 101.30, extreme2, pre_active=True)
        self.assertTrue(reclaimed3)
        self.assertEqual(extreme3, 101.30)

    def test_reversal_is_measured_from_observed_extreme(self):
        self.assertAlmostEqual(
            reversal_from_extreme_pct("PUMP", 95, 100),
            -5.0,
            places=6,
        )


if __name__ == "__main__":
    unittest.main()
