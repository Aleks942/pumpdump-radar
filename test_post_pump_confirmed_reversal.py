from contextlib import closing
import sqlite3
import unittest

from post_pump_short_strategy import _observe_confirmed_reversal, setup


class ConfirmedReversalTests(unittest.TestCase):
    def test_requires_retest_two_down_hours_and_seller_metrics(self):
        now = 40 * 3600
        state = {
            "symbol": "TESTUSDT", "episode_at": now - 10 * 3600,
            "phase": "CANCELLED", "phase_at": now - 3600, "watch_reason": "PAPER_PLAN_EXPIRED",
            "break_level": 100, "break_candle_ts": now - 3 * 3600,
            "last_price": 91, "peak": 130, "last_retrace_pct": 30, "rise_max": 35,
        }
        hourly = [
            {"ts": i * 3600, "open": 130 - i, "high": 131 - i,
             "low": 128 - i, "close": 129 - i, "confirmed": True}
            for i in range(1, 40)
        ]
        structure = {"trend_shift": True}
        evidence = {"final_short_confirmation": {"ready": True}}
        with closing(sqlite3.connect(":memory:")) as db:
            db.row_factory = sqlite3.Row
            setup(db)
            self.assertFalse(_observe_confirmed_reversal(db, state, now, structure, hourly, evidence))
            db.execute(
                "INSERT INTO post_pump_short_strategy_audit "
                "(symbol,episode_at,at,phase,reason,rise_pct,retrace_pct,reference_price) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (state["symbol"], state["episode_at"], now - 3601, "RETEST", "TEST", 35, 30, 94),
            )
            self.assertFalse(_observe_confirmed_reversal(db, state, now, structure, hourly, {}))
            self.assertFalse(_observe_confirmed_reversal(db, state, now - 3600, structure, hourly, evidence))
            self.assertTrue(_observe_confirmed_reversal(db, state, now, structure, hourly, {}))
            self.assertEqual(state["phase"], "CANCELLED")
            self.assertFalse(_observe_confirmed_reversal(db, state, now + 60, structure, hourly, evidence))
