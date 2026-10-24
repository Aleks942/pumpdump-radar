from pathlib import Path
import unittest


class StartupTelegramTests(unittest.TestCase):
    def test_startup_healthcheck_is_not_legacy_only(self):
        code = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        self.assertIn(
            'startup_sent = send_telegram("🚀 PumpDump Radar V2 ONLINE", symbol="SYSTEM")',
            code,
        )
        self.assertIn(
            'print("[STARTUP_TELEGRAM] sent=", startup_sent is True, flush=True)',
            code,
        )
        self.assertNotIn(
            'if TELEGRAM_POLICY == "LEGACY":\n    send_telegram("🚀 PumpDump Radar V2 ONLINE")',
            code,
        )


if __name__ == "__main__":
    unittest.main()
