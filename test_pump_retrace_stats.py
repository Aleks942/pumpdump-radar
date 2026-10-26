import sqlite3
import tempfile
import unittest
from pathlib import Path

import pump_retrace_stats as stats


class PumpRetraceStatsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "stats.sqlite3")

    def tearDown(self):
        self.tmp.cleanup()

    def test_rolling_t0_peak_reset_and_final_15pct_retrace(self):
        event = {
            "symbol": "TESTUSDT",
            "type": "PUMP",
            "decision": {"direction": "UP"},
            "price": 116.0,
            "change": 16.0,
            "window": "5m",
            "sample_age_sec": 300,
            "exchange": "OKX",
        }
        self.assertTrue(stats.register_event(event, now=1000, db_path=self.path))

        # First peak then an 8% pullback.
        stats.observe_tickers(
            [{"instId": "TEST-USDT-SWAP", "last": "130", "sodUtc0": "200"}],
            "OKX", now=1060, db_path=self.path,
        )
        stats.observe_tickers(
            [{"instId": "TEST-USDT-SWAP", "last": "119.6", "sodUtc0": "200"}],
            "OKX", now=1120, db_path=self.path,
        )

        # A new high must reset old retrace clocks.
        stats.observe_tickers(
            [{"instId": "TEST-USDT-SWAP", "last": "140", "sodUtc0": "200"}],
            "OKX", now=1180, db_path=self.path,
        )
        stats.observe_tickers(
            [{"instId": "TEST-USDT-SWAP", "last": "119", "sodUtc0": "200"}],
            "OKX", now=1240, db_path=self.path,
        )

        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("""SELECT * FROM pump_retrace_milestone
                WHERE threshold_pct=15""").fetchone()
            self.assertAlmostEqual(row["max_price"], 140.0)
            self.assertIsNotNone(row["retrace15_at"])
            self.assertAlmostEqual(row["retrace_pct"], 15.0, places=5)

    def test_utc_day_uses_sodutc0_and_records_50_to_15(self):
        # +51% from true UTC-day open should create 15/20/30/50 milestones.
        ticker = {
            "instId": "DAY-USDT-SWAP",
            "last": "151",
            "sodUtc0": "100",
        }
        stats.observe_tickers([ticker], "OKX", now=200000, db_path=self.path)
        stats.observe_tickers(
            [{"instId": "DAY-USDT-SWAP", "last": "170", "sodUtc0": "100"}],
            "OKX", now=200060, db_path=self.path,
        )
        stats.observe_tickers(
            [{"instId": "DAY-USDT-SWAP", "last": "144.5", "sodUtc0": "100"}],
            "OKX", now=200120, db_path=self.path,
        )

        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            ep = db.execute("""SELECT * FROM pump_retrace_episode
                WHERE symbol='DAYUSDT' AND basis='UTC_DAY'""").fetchone()
            self.assertIsNotNone(ep)
            self.assertAlmostEqual(ep["reference_price"], 100.0)
            levels = {
                r["threshold_pct"]: r
                for r in db.execute("""SELECT * FROM pump_retrace_milestone
                    WHERE episode_id=?""", (ep["id"],)).fetchall()
            }
            self.assertEqual(set(levels), {15, 20, 30, 50})
            self.assertAlmostEqual(levels[50]["max_price"], 170.0)
            self.assertIsNotNone(levels[50]["retrace15_at"])

    def test_dump_is_not_registered_as_pump_stats(self):
        event = {
            "symbol": "DOWNUSDT",
            "type": "DUMP",
            "decision": {"direction": "DOWN"},
            "price": 80.0,
            "change": -20.0,
            "window": "10m",
            "sample_age_sec": 600,
            "exchange": "Bybit",
        }
        self.assertFalse(stats.register_event(event, now=1000, db_path=self.path))

    def test_summary_uses_only_closed_episodes(self):
        event = {
            "symbol": "TESTUSDT",
            "type": "PUMP",
            "decision": {"direction": "UP"},
            "price": 120.0,
            "change": 20.0,
            "window": "10m",
            "sample_age_sec": 600,
            "exchange": "Bybit",
        }
        stats.register_event(event, now=1000, db_path=self.path)
        self.assertEqual(stats.summary(now=1100, db_path=self.path), [])

        # Force the 6h close while keeping a valid quote.
        stats.observe_tickers(
            [{"symbol": "TESTUSDT", "lastPrice": "110"}],
            "Bybit", now=1000 + 6 * 3600 + 1, db_path=self.path,
        )
        rows = stats.summary(now=1000 + 6 * 3600 + 2, db_path=self.path)
        self.assertTrue(any(r["basis"] == "10m" and r["threshold_pct"] == 15
                            for r in rows))


if __name__ == "__main__":
    unittest.main()
