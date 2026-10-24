from pathlib import Path
import unittest

import bybit_move_verifier as verifier


class BybitMoveVerifierTests(unittest.TestCase):
    def test_main_wires_bybit_watch_into_verifier(self):
        code = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        self.assertIn("schedule_bybit_move_verification", code)
        self.assertIn('str(signal.get("exchange") or "").upper() == "BYBIT"', code)

    def test_message_marks_partial_data(self):
        result = {
            "symbol": "EPICUSDT",
            "type": "DUMP",
            "change": -5.05,
            "oi": {"quality": "UNAVAILABLE", "ready": False},
            "futures": {"quality": "PARTIAL", "ready": False,
                        "trade_count": 20, "coverage_sec": 30,
                        "imbalance_pct": -20},
            "spot": {"quality": "UNAVAILABLE", "ready": False},
            "funding": {"quality": "UNAVAILABLE", "ready": False},
            "trend_5m": {"trend": "MIXED"},
            "trend_60m": {"trend": "UNKNOWN"},
            "liquidations": {"long_liq": 0, "short_liq": 0,
                             "quality": "OBSERVED_PARTIAL"},
            "agg_5m": {"ready": False, "ready_sources": 1},
            "pattern": {"pattern": "NONE", "direction": "NONE"},
            "trend_aligned": False,
            "anti_late_ok": True,
        }
        message = verifier.build_message(result)
        self.assertIn("АВТОПРОВЕРКА ДАМП", message)
        self.assertIn("готово источников 1/3", message)
        self.assertIn("достаточного покрытия", message)


if __name__ == "__main__":
    unittest.main()
