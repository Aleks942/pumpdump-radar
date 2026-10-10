"""Regression checks for immutable prospective PumpDump OI rules."""
import sqlite3
import unittest
from unittest.mock import patch

from pump_forward_oi import ID, RULE_HASH, _registered, _format


class ForwardRegistryTests(unittest.TestCase):
    def test_registration_remains_frozen_across_restarts(self):
        db = sqlite3.connect(":memory:")
        try:
            with patch("pump_forward_oi.time.time", return_value=100.0):
                first = _registered(db)
            with patch("pump_forward_oi.time.time", return_value=900.0):
                second = _registered(db)
            self.assertEqual(first, 101.0)
            self.assertEqual(second, first)
            saved = db.execute(
                "SELECT rule_hash FROM pump_forward_oi_registry "
                "WHERE experiment_id=?", (ID,),
            ).fetchone()
            self.assertEqual(saved[0], RULE_HASH)
        finally:
            db.close()

    def test_mutated_registry_is_rejected(self):
        db = sqlite3.connect(":memory:")
        try:
            _registered(db)
            db.execute(
                "UPDATE pump_forward_oi_registry SET rule_hash = 'mismatch' "
                "WHERE experiment_id=?", (ID,),
            )
            with self.assertRaisesRegex(ValueError, "RULE_HASH_MISMATCH"):
                _registered(db)
        finally:
            db.close()

    def test_missing_mean_printed_explicitly(self):
        self.assertEqual(_format(None), "NA")
        self.assertEqual(_format(0.12), "+0.1200%")


if __name__ == "__main__":
    unittest.main()
