import os
import sqlite3
import tempfile
import unittest

import post_dump_long_strategy as s

NOW = 1800000000.0


def ticker(last=70, high=150, opening=100, low=65):
    return {
        "instId": "TEST-USDT-SWAP",
        "last": str(last),
        "high24h": str(high),
        "low24h": str(low),
        "open24h": str(opening),
        "volCcy24h": "200000000",
        "askPx": str(last * 1.0005),
        "bidPx": str(last * 0.9995),
    }


def evidence(fut=20, spot=5, oi=1.0):
    return {
        "futures_taker_5m_imbalance_pct": fut,
        "spot_taker_5m_imbalance_pct": spot,
        "agg_oi_5m_pct": oi,
    }


class PostDumpLongTests(unittest.TestCase):
    def setUp(self):
        f = tempfile.NamedTemporaryFile(delete=False)
        f.close()
        self.path = f.name

    def tearDown(self):
        try:
            os.unlink(self.path)
        except FileNotFoundError:
            pass

    def state(self):
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            return [dict(x) for x in db.execute(
                "SELECT * FROM post_dump_long_strategy")]

    def drive(self, at, last, ev=None, funding=-0.06):
        s.observe(
            [ticker(last=last)],
            now=at,
            db_path=self.path,
            evidence_reader=lambda symbol, now: ev or evidence(),
            funding_reader=lambda inst, now: {
                "available": True, "funding_pct": funding, "source": "TEST"
            },
        )

    def test_requires_prior_pump_and_return_to_base(self):
        p = s._profile(ticker(last=120, high=125, opening=100))
        self.assertFalse(s._candidate(p))
        p = s._profile(ticker(last=106, high=150, opening=100))
        self.assertFalse(s._candidate(p))
        p = s._profile(ticker(last=100, high=150, opening=100))
        self.assertTrue(s._candidate(p))

    def test_full_long_progression(self):
        self.drive(NOW, 70)
        self.assertEqual(self.state()[0]["phase"], "POST_DUMP_WATCH")
        self.drive(NOW+60, 71.5)
        self.assertEqual(self.state()[0]["phase"], "BUYERS_APPEAR")
        self.drive(NOW+120, 72.0)
        self.assertEqual(self.state()[0]["phase"], "SQUEEZE_SETUP")
        self.drive(NOW+180, 73.0)
        self.assertEqual(self.state()[0]["phase"], "LONG_CONFIRMED")
        self.drive(NOW+240, 72.5)
        self.assertEqual(self.state()[0]["phase"], "RETEST")
        self.drive(NOW+300, 73.0)
        self.assertEqual(self.state()[0]["phase"], "ENTRY_ZONE")

    def test_negative_funding_is_required_for_confirmation(self):
        self.drive(NOW, 70, funding=0.01)
        self.drive(NOW+60, 72, funding=0.01)
        self.drive(NOW+120, 73, funding=0.01)
        self.assertEqual(self.state()[0]["phase"], "BUYERS_APPEAR")

    def test_strong_spot_selling_blocks_squeeze(self):
        bad = evidence(fut=25, spot=-40, oi=2.0)
        self.drive(NOW, 70, ev=bad)
        self.drive(NOW+60, 72, ev=bad)
        self.drive(NOW+120, 73, ev=bad)
        self.assertEqual(self.state()[0]["phase"], "BUYERS_APPEAR")

    def test_deliver_sends_only_strategy_notifications(self):
        self.drive(NOW, 70)
        self.drive(NOW+60, 72)
        self.drive(NOW+120, 73)
        sent = []
        n = s.deliver(lambda msg, symbol=None: sent.append((symbol,msg)) or True,
                      now=NOW+121, db_path=self.path)
        self.assertEqual(n, 1)
        self.assertEqual(sent[0][0], "TESTUSDT")
        self.assertIn("POST-DUMP SQUEEZE", sent[0][1])


if __name__ == "__main__":
    unittest.main()
