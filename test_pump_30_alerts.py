"""Offline regression tests for opt-in +30% informational alerts."""
import os
import tempfile
import unittest
from unittest.mock import patch

import pump_30_alerts as alerts


def ticker(price=139, high=140, opening=100, vol=25_000_000):
    return {
        "instId": "TEST-USDT-SWAP",
        "last": str(price),
        "open24h": str(opening),
        "high24h": str(high),
        "volCcy24h": str(vol),
        "bidPx": str(price * 0.999),
        "askPx": str(price * 1.001),
    }


class Pump30AlertsTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        mock = patch.object(alerts, "DB_PATH", os.path.join(tmp.name, "watch.sqlite3"))
        mock.start()
        self.addCleanup(mock.stop)
        self.now = 1792500000.0

    def observe(self, price=139, *, offset=0, high=140, opening=100, vol=25_000_000):
        return alerts.observe(
            [ticker(price=price, high=high, opening=opening, vol=vol)],
            now=self.now + offset,
        )

    def test_distinct_30pct_pump_weakening_and_dump_risk_stages(self):
        first = self.observe()[0]
        self.assertEqual(first["stage"], 1)
        self.assertIn("НЕ подтверждённый сигнал SHORT", alerts.message(first))
        alerts.acknowledge(first, True, now=self.now)
        self.assertEqual(self.observe(139, offset=60), [])
        self.assertEqual(self.observe(136, offset=120), [])
        weak = self.observe(134, offset=180)[0]
        self.assertEqual(weak["stage"], 2)
        alerts.acknowledge(weak, True, now=self.now + 180)
        self.assertEqual(self.observe(130, offset=240), [])
        risk = self.observe(127, offset=300)[0]
        self.assertEqual(risk["stage"], 3)
        alerts.acknowledge(risk, True, now=self.now + 300)
        self.assertEqual(self.observe(127, offset=360), [])

    def test_failed_delivery_does_not_mark_alert_as_delivered(self):
        first = self.observe()[0]
        alerts.acknowledge(first, False, now=self.now)
        self.assertEqual(self.observe(offset=60), [])
        retry = self.observe(offset=360)
        self.assertEqual(len(retry), 1)
        self.assertEqual(retry[0]["stage"], 1)

    def test_excludes_small_move_low_volume_and_late_dump(self):
        self.assertEqual(self.observe(125, high=128), [])
        self.assertEqual(self.observe(139, vol=1000), [])
        self.assertEqual(self.observe(100, high=140), [])

    def test_info_alert_is_opt_in_and_independent_of_trade_confirmation(self):
        from pathlib import Path
        code = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        self.assertIn('os.getenv("PUMP_30_INFO_ALERTS", "0") == "1"', code)
        self.assertIn("for pump30_event in observe_30_pumps(tickers)", code)
        self.assertIn("ack_30_pump(pump30_event, delivered)", code)


if __name__ == "__main__":
    unittest.main()
